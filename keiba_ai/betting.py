"""
ベッティング戦略モジュール

Kelly 基準に基づく期待値ポジティブなベットを選定する。

用語:
  - EV (Expected Value) = p * odds - 1  → 1.0 超えなら期待収益がプラス
  - Kelly fraction = (p * odds - 1) / (odds - 1)
  - Fractional Kelly = Kelly fraction * KELLY_FRACTION (過剰賭け防止)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from keiba_ai.config import (
    KELLY_FRACTION,
    MIN_EXPECTED_VALUE,
    MAX_BET_RATIO,
    MIN_BET_UNIT,
    WIN_ODDS_MIN,
    WIN_ODDS_MAX,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class BetDecision:
    """1頭分のベット判断。"""
    race_id: str
    horse_no: int
    horse_name: str
    bet_type: str           # "win" | "place"
    odds: float
    pred_prob: float        # モデルの予測確率
    implied_prob: float     # オッズ逆数 (市場確率)
    expected_value: float   # EV = pred_prob * odds
    kelly_fraction: float   # full Kelly
    bet_ratio: float        # 実際に賭ける資金比率
    bet_amount: int         # 賭け金 (円)
    is_bet: bool            # 実際に賭けるか
    reason: str             # 見送り理由など

    def to_dict(self) -> dict:
        return {
            "race_id": self.race_id,
            "horse_no": self.horse_no,
            "horse_name": self.horse_name,
            "bet_type": self.bet_type,
            "odds": self.odds,
            "pred_prob": f"{self.pred_prob:.3f}",
            "implied_prob": f"{self.implied_prob:.3f}",
            "ev": f"{self.expected_value:.3f}",
            "kelly": f"{self.kelly_fraction:.3f}",
            "bet_ratio": f"{self.bet_ratio:.3f}",
            "bet_amount": self.bet_amount,
            "is_bet": self.is_bet,
            "reason": self.reason,
        }


@dataclass
class RaceBetPlan:
    """1レース分のベット計画。"""
    race_id: str
    decisions: list[BetDecision] = field(default_factory=list)

    @property
    def total_bet(self) -> int:
        return sum(d.bet_amount for d in self.decisions if d.is_bet)

    @property
    def selected(self) -> list[BetDecision]:
        return [d for d in self.decisions if d.is_bet]

    def print_summary(self) -> None:
        print(f"\n=== Race {self.race_id} ===")
        for d in sorted(self.decisions, key=lambda x: x.pred_prob, reverse=True):
            mark = "★ BET" if d.is_bet else "  ---"
            print(
                f"  {mark} #{d.horse_no:>2} {d.horse_name:<12} "
                f"オッズ{d.odds:>6.1f}倍  予測確率{d.pred_prob:.3f}  "
                f"EV={d.expected_value:.3f}  賭け金={d.bet_amount:>6,}円"
                + (f"  [{d.reason}]" if d.reason else "")
            )
        if self.selected:
            print(f"  → 合計賭け金: {self.total_bet:,}円")
        else:
            print("  → このレースは見送り")


# ---------------------------------------------------------------------------
# メインベッティング計算
# ---------------------------------------------------------------------------

class KellyBettingStrategy:
    """
    Kelly 基準ベッティング戦略。

    フィルター:
      1. EV >= MIN_EXPECTED_VALUE
      2. オッズが WIN_ODDS_MIN ～ WIN_ODDS_MAX の範囲内
      3. 1レース内で最も EV が高い1頭のみ (分散防止)
      4. 賭け金は fractional Kelly だが MAX_BET_RATIO でキャップ
    """

    def __init__(
        self,
        kelly_fraction: float = KELLY_FRACTION,
        min_ev: float = MIN_EXPECTED_VALUE,
        max_bet_ratio: float = MAX_BET_RATIO,
        min_bet_unit: int = MIN_BET_UNIT,
        odds_min: float = WIN_ODDS_MIN,
        odds_max: float = WIN_ODDS_MAX,
        max_bets_per_race: int = 1,
    ):
        self.kelly_fraction = kelly_fraction
        self.min_ev = min_ev
        self.max_bet_ratio = max_bet_ratio
        self.min_bet_unit = min_bet_unit
        self.odds_min = odds_min
        self.odds_max = odds_max
        self.max_bets_per_race = max_bets_per_race

    def decide_race(
        self,
        race_id: str,
        pred_df: pd.DataFrame,
        bankroll: float,
        bet_type: str = "win",
    ) -> RaceBetPlan:
        """
        1レースのベット計画を立てる。

        Args:
            race_id: レースID
            pred_df: 列 horse_no, horse_name, odds, pred_prob が必要
            bankroll: 現在の資金 (円)
            bet_type: "win" or "place"

        Returns:
            RaceBetPlan
        """
        plan = RaceBetPlan(race_id=race_id)

        for _, row in pred_df.iterrows():
            horse_no = int(row.get("horse_no", 0))
            horse_name = str(row.get("horse_name", ""))
            odds = float(row.get("odds", 0.0) or 0.0)
            pred_prob = float(row.get("pred_prob", 0.0) or 0.0)

            if odds <= 0 or pred_prob <= 0:
                decision = BetDecision(
                    race_id=race_id, horse_no=horse_no, horse_name=horse_name,
                    bet_type=bet_type, odds=odds, pred_prob=pred_prob,
                    implied_prob=0, expected_value=0, kelly_fraction=0,
                    bet_ratio=0, bet_amount=0, is_bet=False, reason="データ不正",
                )
                plan.decisions.append(decision)
                continue

            implied_prob = 1.0 / odds
            ev = pred_prob * odds  # EV = 期待値 (1.0超え = プラス期待値)
            kelly = self._kelly(pred_prob, odds)

            decision = self._evaluate(
                race_id, horse_no, horse_name, bet_type,
                odds, pred_prob, implied_prob, ev, kelly, bankroll,
            )
            plan.decisions.append(decision)

        # レース内で最大EV の馬のみ選択 (複数賭けを制限)
        plan = self._select_top_bets(plan)
        return plan

    def _evaluate(
        self, race_id, horse_no, horse_name, bet_type,
        odds, pred_prob, implied_prob, ev, kelly, bankroll,
    ) -> BetDecision:
        """1頭の評価。フィルターを順番に適用する。"""

        # フィルター1: オッズ範囲外
        if odds < self.odds_min:
            return BetDecision(
                race_id=race_id, horse_no=horse_no, horse_name=horse_name,
                bet_type=bet_type, odds=odds, pred_prob=pred_prob,
                implied_prob=implied_prob, expected_value=ev, kelly_fraction=kelly,
                bet_ratio=0, bet_amount=0, is_bet=False,
                reason=f"オッズ低過ぎ({odds:.1f}<{self.odds_min})",
            )
        if odds > self.odds_max:
            return BetDecision(
                race_id=race_id, horse_no=horse_no, horse_name=horse_name,
                bet_type=bet_type, odds=odds, pred_prob=pred_prob,
                implied_prob=implied_prob, expected_value=ev, kelly_fraction=kelly,
                bet_ratio=0, bet_amount=0, is_bet=False,
                reason=f"オッズ高過ぎ({odds:.1f}>{self.odds_max})",
            )

        # フィルター2: EV 不足
        if ev < self.min_ev:
            return BetDecision(
                race_id=race_id, horse_no=horse_no, horse_name=horse_name,
                bet_type=bet_type, odds=odds, pred_prob=pred_prob,
                implied_prob=implied_prob, expected_value=ev, kelly_fraction=kelly,
                bet_ratio=0, bet_amount=0, is_bet=False,
                reason=f"EV不足({ev:.3f}<{self.min_ev})",
            )

        # フィルター3: Kelly がゼロ以下
        if kelly <= 0:
            return BetDecision(
                race_id=race_id, horse_no=horse_no, horse_name=horse_name,
                bet_type=bet_type, odds=odds, pred_prob=pred_prob,
                implied_prob=implied_prob, expected_value=ev, kelly_fraction=kelly,
                bet_ratio=0, bet_amount=0, is_bet=False, reason="Kelly≤0",
            )

        # 賭け金計算
        bet_ratio = min(kelly * self.kelly_fraction, self.max_bet_ratio)
        raw_amount = bankroll * bet_ratio
        # 100円単位に切り捨て
        bet_amount = max(
            self.min_bet_unit,
            int(raw_amount / 100) * 100,
        )

        return BetDecision(
            race_id=race_id, horse_no=horse_no, horse_name=horse_name,
            bet_type=bet_type, odds=odds, pred_prob=pred_prob,
            implied_prob=implied_prob, expected_value=ev, kelly_fraction=kelly,
            bet_ratio=bet_ratio, bet_amount=bet_amount, is_bet=True, reason="",
        )

    def _select_top_bets(self, plan: RaceBetPlan) -> RaceBetPlan:
        """レース内で EV 上位 max_bets_per_race 頭のみ is_bet=True にする。"""
        candidates = [d for d in plan.decisions if d.is_bet]
        # EVの高い順にソート
        candidates.sort(key=lambda d: d.expected_value, reverse=True)
        keep = set(id(d) for d in candidates[:self.max_bets_per_race])
        for d in plan.decisions:
            if d.is_bet and id(d) not in keep:
                d.is_bet = False
                d.reason = "同レース内でより高EV馬あり"
                d.bet_amount = 0
        return plan

    @staticmethod
    def _kelly(p: float, b: float) -> float:
        """
        Kelly 最適比率を計算。
        p: 勝率, b: オッズ (払い戻し倍率)
        f* = (b*p - (1-p)) / (b - 1) = (b*p - 1 + p) / (b - 1)
        """
        if b <= 1.0:
            return 0.0
        return max(0.0, (b * p - 1) / (b - 1))


# ---------------------------------------------------------------------------
# 回収率計算ユーティリティ
# ---------------------------------------------------------------------------

def calc_recovery_rate(bets: list[BetDecision], results: dict) -> dict:
    """
    ベット結果から回収率を計算する。

    Args:
        bets: BetDecision のリスト
        results: {race_id: {"winner_no": int, "win_odds": float}}

    Returns:
        {"total_bet": int, "total_return": float, "recovery_rate": float,
         "n_hits": int, "n_bets": int}
    """
    total_bet = 0
    total_return = 0.0
    n_bets = 0
    n_hits = 0

    for bet in bets:
        if not bet.is_bet:
            continue
        n_bets += 1
        total_bet += bet.bet_amount

        race_result = results.get(bet.race_id, {})
        winner_no = race_result.get("winner_no")
        win_odds = race_result.get("win_odds", 0)

        if bet.bet_type == "win":
            if winner_no == bet.horse_no and win_odds:
                payout = bet.bet_amount * win_odds
                total_return += payout
                n_hits += 1
        elif bet.bet_type == "place":
            top3 = race_result.get("top3_nos", [])
            place_odds = race_result.get("place_odds", {})
            if bet.horse_no in top3:
                p_odds = place_odds.get(bet.horse_no, 0)
                payout = bet.bet_amount * p_odds
                total_return += payout
                n_hits += 1

    recovery_rate = (total_return / total_bet * 100) if total_bet > 0 else 0.0
    return {
        "total_bet": total_bet,
        "total_return": total_return,
        "recovery_rate": recovery_rate,
        "n_hits": n_hits,
        "n_bets": n_bets,
        "hit_rate": n_hits / n_bets if n_bets else 0,
    }

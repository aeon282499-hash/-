"""
バックテストエンジン

過去のレース結果データ + 学習済みモデルを使って
実際の回収率をシミュレートする。

出力:
  - 月次回収率レポート (中央・地方別)
  - 資金推移グラフ (bankroll curve)
  - 詳細なベット履歴 CSV
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from keiba_ai.betting import KellyBettingStrategy, BetDecision, calc_recovery_rate
from keiba_ai.config import INITIAL_BANKROLL, BET_TYPE, JRA_VENUES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# バックテスト結果
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
    bets: list[BetDecision] = field(default_factory=list)
    bankroll_history: list[tuple[str, float]] = field(default_factory=list)  # (date, bankroll)
    monthly_stats: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def total_bets(self) -> int:
        return len([b for b in self.bets if b.is_bet])

    @property
    def total_bet_amount(self) -> int:
        return sum(b.bet_amount for b in self.bets if b.is_bet)

    @property
    def final_bankroll(self) -> float:
        if self.bankroll_history:
            return self.bankroll_history[-1][1]
        return INITIAL_BANKROLL

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("バックテスト結果サマリー")
        print("=" * 60)
        print(f"  総ベット数       : {self.total_bets:>8,} 件")
        print(f"  総投資額         : {self.total_bet_amount:>8,} 円")
        print(f"  最終資金         : {self.final_bankroll:>10,.0f} 円")
        if not self.monthly_stats.empty:
            print(f"\n  月次回収率 (中央):")
            if "jra_recovery" in self.monthly_stats.columns:
                for _, row in self.monthly_stats.iterrows():
                    rate = row.get("jra_recovery", 0)
                    bar = "█" * min(int(rate / 5), 40)
                    flag = " ★" if rate >= 100 else ""
                    print(f"    {row.get('month', '')}  {bar:<40} {rate:>6.1f}%{flag}")
        print("=" * 60)

    def save_csv(self, path: str | Path) -> None:
        path = Path(path)
        rows = [b.to_dict() for b in self.bets if b.is_bet]
        pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Bet history saved → {path}")

    def save_monthly_report(self, path: str | Path) -> None:
        path = Path(path)
        if not self.monthly_stats.empty:
            self.monthly_stats.to_csv(path, index=False, encoding="utf-8-sig")
            logger.info(f"Monthly report saved → {path}")


# ---------------------------------------------------------------------------
# バックテストエンジン
# ---------------------------------------------------------------------------

class Backtester:
    """
    バックテストエンジン。

    使い方:
        bt = Backtester(model, strategy)
        result = bt.run(race_df, history_dict)
        result.print_summary()
    """

    def __init__(
        self,
        model,
        strategy: KellyBettingStrategy | None = None,
        initial_bankroll: float = INITIAL_BANKROLL,
        bet_type: str = BET_TYPE,
    ):
        self.model = model
        self.strategy = strategy or KellyBettingStrategy()
        self.bankroll = initial_bankroll
        self.initial_bankroll = initial_bankroll
        self.bet_type = bet_type

    def run(
        self,
        race_df: pd.DataFrame,
        history_dict: dict | None = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BacktestResult:
        """
        バックテストを実行する。

        Args:
            race_df: scraper.collect_race_results() の出力 DataFrame
                     必須カラム: race_id, race_date, order, odds, horse_no,
                                 horse_name, venue_code
            history_dict: {horse_id: horse_history_df}
            start_date: "YYYY-MM-DD" 開始日 (None=全期間)
            end_date:   "YYYY-MM-DD" 終了日 (None=全期間)
        """
        from keiba_ai.features import build_features, _get_feature_columns

        df = race_df.copy()

        # 日付フィルター
        if "race_date" in df.columns:
            df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
            if start_date:
                df = df[df["race_date"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["race_date"] <= pd.to_datetime(end_date)]

        df = df.sort_values("race_date") if "race_date" in df.columns else df

        result = BacktestResult()
        monthly_rows = []

        # レースごとに処理
        for race_id, race_group in df.groupby("race_id"):
            race_date_str = str(race_group["race_date"].iloc[0].date()) \
                if "race_date" in race_group.columns else "unknown"

            try:
                bets, bankroll_after = self._simulate_race(
                    race_id, race_group, history_dict or {}, race_date_str,
                )
                result.bets.extend(bets)
                self.bankroll = bankroll_after
                result.bankroll_history.append((race_date_str, self.bankroll))
            except Exception as e:
                logger.warning(f"Race {race_id} failed: {e}")
                continue

        # 月次集計
        result.monthly_stats = self._calc_monthly_stats(result.bets, df)
        return result

    def _simulate_race(
        self,
        race_id: str,
        race_group: pd.DataFrame,
        history_dict: dict,
        race_date_str: str,
    ) -> tuple[list[BetDecision], float]:
        """1レースをシミュレート。"""
        from keiba_ai.features import build_features, _get_feature_columns

        feat_df = build_features(race_group, history_dict)
        feature_cols = _get_feature_columns(feat_df)
        X = feat_df[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

        if X.empty:
            return [], self.bankroll

        proba = self.model.predict_proba(X)

        pred_df = race_group[["horse_no", "horse_name", "odds", "venue_code"]].copy()
        pred_df = pred_df.reset_index(drop=True)
        pred_df["pred_prob"] = proba

        # ベット計画
        plan = self.strategy.decide_race(
            race_id=str(race_id),
            pred_df=pred_df,
            bankroll=self.bankroll,
            bet_type=self.bet_type,
        )

        # 結果を反映して資金更新
        winner_row = race_group[pd.to_numeric(race_group["order"], errors="coerce") == 1]
        winner_no = int(winner_row["horse_no"].iloc[0]) if not winner_row.empty else None
        win_odds = float(winner_row["odds"].iloc[0]) if not winner_row.empty else 0.0

        bankroll = self.bankroll
        for decision in plan.selected:
            bankroll -= decision.bet_amount
            if decision.bet_type == "win" and decision.horse_no == winner_no:
                payout = decision.bet_amount * win_odds
                bankroll += payout
                logger.debug(
                    f"HIT! Race {race_id} #{decision.horse_no} "
                    f"オッズ{win_odds:.1f} 払戻{payout:.0f}円"
                )

        return plan.decisions, bankroll

    def _calc_monthly_stats(
        self, bets: list[BetDecision], race_df: pd.DataFrame
    ) -> pd.DataFrame:
        """月次回収率を計算する。"""
        if not bets:
            return pd.DataFrame()

        # race_id → date マッピング
        if "race_date" in race_df.columns and "race_id" in race_df.columns:
            date_map = race_df.drop_duplicates("race_id").set_index("race_id")["race_date"]
        else:
            date_map = {}

        # venue_code → JRA/NAR マッピング
        venue_map = race_df.drop_duplicates("race_id").set_index("race_id")["venue_code"] \
            if "venue_code" in race_df.columns else {}

        rows = []
        for bet in bets:
            if not bet.is_bet:
                continue
            race_id = bet.race_id
            dt = date_map.get(race_id)
            if pd.isna(dt) if isinstance(dt, float) else dt is None:
                continue
            vc = str(venue_map.get(race_id, ""))
            is_jra = vc in JRA_VENUES.keys()
            rows.append({
                "month": pd.to_datetime(dt).strftime("%Y/%m") if dt else "unknown",
                "is_jra": is_jra,
                "bet_amount": bet.bet_amount,
                "race_id": race_id,
                "horse_no": bet.horse_no,
                "odds": bet.odds,
                "pred_prob": bet.pred_prob,
            })

        if not rows:
            return pd.DataFrame()

        bet_df = pd.DataFrame(rows)

        # 結果 (勝敗) を race_df から取得
        winners = race_df[pd.to_numeric(race_df.get("order", pd.Series()), errors="coerce") == 1][
            ["race_id", "horse_no", "odds"]
        ].rename(columns={"horse_no": "winner_no", "odds": "win_odds"})

        bet_df = bet_df.merge(
            winners, on="race_id", how="left"
        )
        bet_df["is_hit"] = (bet_df["horse_no"] == bet_df["winner_no"])
        bet_df["return"] = np.where(
            bet_df["is_hit"],
            bet_df["bet_amount"] * bet_df["win_odds"],
            0,
        )

        monthly = []
        for month, grp in bet_df.groupby("month"):
            jra = grp[grp["is_jra"]]
            nar = grp[~grp["is_jra"]]
            row = {
                "month": month,
                "jra_bets": len(jra),
                "jra_bet_amount": int(jra["bet_amount"].sum()),
                "jra_return": float(jra["return"].sum()),
                "jra_recovery": float(jra["return"].sum() / jra["bet_amount"].sum() * 100)
                               if jra["bet_amount"].sum() > 0 else 0.0,
                "jra_hits": int(jra["is_hit"].sum()),
                "nar_bets": len(nar),
                "nar_bet_amount": int(nar["bet_amount"].sum()),
                "nar_return": float(nar["return"].sum()),
                "nar_recovery": float(nar["return"].sum() / nar["bet_amount"].sum() * 100)
                               if nar["bet_amount"].sum() > 0 else 0.0,
                "nar_hits": int(nar["is_hit"].sum()),
            }
            monthly.append(row)

        return pd.DataFrame(monthly).sort_values("month").reset_index(drop=True)


# ---------------------------------------------------------------------------
# プロット (matplotlib が使える場合)
# ---------------------------------------------------------------------------

def plot_bankroll_curve(result: BacktestResult, save_path: str | Path | None = None) -> None:
    """資金推移をプロットする。"""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        dates = [pd.to_datetime(d) for d, _ in result.bankroll_history]
        bankrolls = [b for _, b in result.bankroll_history]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(dates, bankrolls, lw=1.5, color="royalblue")
        ax.axhline(result.initial_bankroll, color="gray", linestyle="--", lw=1, label="初期資金")
        ax.set_title("資金推移 (Bankroll Curve)", fontsize=14)
        ax.set_ylabel("資金 (円)")
        ax.set_xlabel("日付")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y/%m"))
        ax.legend()
        ax.grid(alpha=0.3)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150)
            logger.info(f"Plot saved → {save_path}")
        else:
            plt.show()
    except ImportError:
        logger.warning("matplotlib not installed. Skipping plot.")

"""
当日予測ランナー

毎朝実行して当日のレース予測とベット計画を出力する。
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from keiba_ai.config import DATA_DIR, MODEL_DIR, INITIAL_BANKROLL, BET_TYPE
from keiba_ai.scraper import get_race_list_from_calendar, scrape_race_result, scrape_horse_history, scrape_odds
from keiba_ai.features import build_features, _get_feature_columns
from keiba_ai.betting import KellyBettingStrategy
from keiba_ai.model import KeibaPredictor

logger = logging.getLogger(__name__)


def run_today(
    target_date: date | None = None,
    model_path: str | Path | None = None,
    bankroll: float = INITIAL_BANKROLL,
    bet_type: str = BET_TYPE,
    jra_only: bool = True,
    output_json: str | Path | None = None,
) -> list[dict]:
    """
    当日のレース予測を実行し、ベット計画を返す。

    Args:
        target_date: 予測対象日 (None=今日)
        model_path: 学習済みモデルのパス
        bankroll: 現在の資金
        bet_type: "win" or "place"
        jra_only: 中央競馬のみ (True) or 地方も含む (False)
        output_json: 結果をJSONに保存するパス

    Returns:
        ベット計画のリスト
    """
    from keiba_ai.config import JRA_VENUES

    target_date = target_date or date.today()
    logger.info(f"=== 競馬AI 予測開始 {target_date} ===")

    # モデルロード
    if model_path is None:
        model_path = MODEL_DIR / "model.pkl"
    model = KeibaPredictor.load(model_path)
    strategy = KellyBettingStrategy()

    # 当日レース一覧取得
    race_ids = get_race_list_from_calendar(target_date)
    if jra_only:
        race_ids = [r for r in race_ids if r[4:6] in JRA_VENUES.keys()]

    logger.info(f"対象レース数: {len(race_ids)}")

    all_bets = []

    for race_id in race_ids:
        logger.info(f"予測中: {race_id}")
        try:
            result = _predict_single_race(race_id, model, strategy, bankroll, bet_type)
            all_bets.extend(result)
        except Exception as e:
            logger.warning(f"Race {race_id} skip: {e}")
            continue

    # 出力
    _print_daily_summary(all_bets, target_date)

    if output_json:
        Path(output_json).write_text(
            json.dumps(all_bets, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"JSON saved → {output_json}")

    return all_bets


def _predict_single_race(
    race_id: str,
    model: KeibaPredictor,
    strategy: KellyBettingStrategy,
    bankroll: float,
    bet_type: str,
) -> list[dict]:
    """1レースを予測してベット計画を返す。"""
    # 出馬表取得 (レース結果ではなく出走情報として使う)
    race_df = scrape_race_result(race_id)
    if race_df is None or race_df.empty:
        return []

    # 各馬の過去成績取得
    history_dict = {}
    for horse_id in race_df["horse_id"].dropna().unique():
        if horse_id:
            hist = scrape_horse_history(horse_id, limit=10)
            if hist is not None:
                history_dict[horse_id] = hist

    # 特徴量生成
    feat_df = build_features(race_df, history_dict)
    feature_cols = _get_feature_columns(feat_df)
    X = feat_df[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    if X.empty:
        return []

    proba = model.predict_proba(X)

    pred_df = race_df[["horse_no", "horse_name", "odds"]].copy().reset_index(drop=True)
    pred_df["pred_prob"] = proba

    plan = strategy.decide_race(
        race_id=race_id,
        pred_df=pred_df,
        bankroll=bankroll,
        bet_type=bet_type,
    )

    plan.print_summary()
    return [d.to_dict() for d in plan.decisions]


def _print_daily_summary(bets: list[dict], target_date: date) -> None:
    selected = [b for b in bets if b.get("is_bet")]
    print(f"\n{'=' * 60}")
    print(f"  競馬AI ベット計画  {target_date}")
    print(f"{'=' * 60}")
    if not selected:
        print("  本日は賭ける馬がありません。")
        return
    total_bet = sum(int(b.get("bet_amount", 0)) for b in selected)
    print(f"  選択ベット数: {len(selected)}件  合計: {total_bet:,}円")
    print()
    for b in selected:
        print(
            f"  ★ Race {b['race_id'][-4:]} #{b['horse_no']:>2} "
            f"{b['horse_name']:<12} "
            f"オッズ{float(b['odds']):>6.1f}倍  "
            f"予測確率{float(b['pred_prob']):.3f}  "
            f"EV={float(b['ev']):.3f}  "
            f"賭け金={int(b['bet_amount']):>6,}円"
        )
    print(f"{'=' * 60}")

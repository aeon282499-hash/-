"""
特徴量エンジニアリング

競馬予測に有効な特徴量を生成する:
  - 馬の過去成績集計 (直近n走勝率・連対率・上がり3F平均など)
  - 騎手・調教師勝率
  - クラス差分 (昇級/降級)
  - 距離適性・コース適性
  - 人気オッズ特徴 (過大人気/過小人気の検出)
  - 体重変化
  - レース間隔
"""

import logging
import re
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# コース種別マッピング
COURSE_MAP = {"芝": 0, "ダート": 1, "障害": 2}

# 馬場状態マッピング  (良→速い, 不良→遅い)
COND_MAP = {"良": 0, "稍重": 1, "重": 2, "不良": 3}

# 性別マッピング
SEX_MAP = {"牡": 0, "牝": 1, "セ": 2}


# ---------------------------------------------------------------------------
# メイン特徴量生成
# ---------------------------------------------------------------------------

def build_features(race_df: pd.DataFrame, history_dict: dict | None = None) -> pd.DataFrame:
    """
    レース出馬表 DataFrame から特徴量 DataFrame を生成する。

    Args:
        race_df: scraper.scrape_race_result() の出力 or 出馬表 DataFrame
                 必須カラム: race_id, horse_id, horse_no, odds, weight_carried,
                             distance, course_type, track_cond, head_count,
                             horse_weight, horse_weight_diff, jockey_id, trainer_id
        history_dict: {horse_id: DataFrame(horse history)} キャッシュ

    Returns:
        特徴量 DataFrame (行 = 各出走馬)
    """
    df = race_df.copy()
    df = _encode_categoricals(df)
    df = _add_horse_features(df, history_dict or {})
    df = _add_race_features(df)
    df = _add_odds_features(df)
    df = _add_relative_features(df)  # レース内相対特徴
    return df


def _encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    if "course_type" in df.columns:
        df["course_type_enc"] = df["course_type"].map(COURSE_MAP).fillna(-1).astype(int)
    if "track_cond" in df.columns:
        df["track_cond_enc"] = df["track_cond"].map(COND_MAP).fillna(-1).astype(int)
    if "sex_age" in df.columns:
        df["sex_enc"] = df["sex_age"].str[0].map(SEX_MAP).fillna(-1).astype(int)
        df["age"] = df["sex_age"].str[1:].apply(_safe_int).fillna(4)
    if "direction" in df.columns:
        df["direction_enc"] = df["direction"].map({"右": 0, "左": 1, "直線": 2}).fillna(-1).astype(int)
    return df


def _add_horse_features(df: pd.DataFrame, history_dict: dict) -> pd.DataFrame:
    """馬個別の過去成績から特徴量を生成。"""
    records = []
    for _, row in df.iterrows():
        horse_id = row.get("horse_id", "")
        hist = history_dict.get(horse_id)
        feats = _calc_horse_history_features(hist, row)
        records.append(feats)

    hist_df = pd.DataFrame(records, index=df.index)
    return pd.concat([df, hist_df], axis=1)


def _calc_horse_history_features(hist: Optional[pd.DataFrame], current_row: pd.Series) -> dict:
    """馬の過去成績からスカラー特徴量を返す。"""
    feats: dict = {}

    if hist is None or hist.empty:
        # 過去データなし → 平均的な値で埋める
        feats.update({
            "h_n_races": 0,
            "h_win_rate_all": 0.0,
            "h_top3_rate_all": 0.0,
            "h_win_rate_5": 0.0,
            "h_top3_rate_5": 0.0,
            "h_avg_order_5": 8.0,
            "h_avg_last3f_5": 38.0,
            "h_best_last3f": 40.0,
            "h_avg_time_deviation": 0.0,
            "h_days_since_last": 60,
            "h_course_win_rate": 0.0,
            "h_dist_win_rate": 0.0,
            "h_weight_change": 0,
            "h_consecutive_wins": 0,
            "h_consecutive_losses": 0,
        })
        return feats

    # 全成績
    total = len(hist)
    feats["h_n_races"] = total

    orders = pd.to_numeric(hist.get("order_hist", pd.Series()), errors="coerce").dropna()
    feats["h_win_rate_all"] = (orders == 1).sum() / total if total else 0
    feats["h_top3_rate_all"] = (orders <= 3).sum() / total if total else 0

    # 直近5走
    hist5 = hist.head(5)
    orders5 = pd.to_numeric(hist5.get("order_hist", pd.Series()), errors="coerce").dropna()
    n5 = len(orders5)
    feats["h_win_rate_5"] = (orders5 == 1).sum() / n5 if n5 else 0
    feats["h_top3_rate_5"] = (orders5 <= 3).sum() / n5 if n5 else 0
    feats["h_avg_order_5"] = float(orders5.mean()) if n5 else 8.0

    # 上がり3F
    last3f = pd.to_numeric(hist5.get("last3f_hist", pd.Series()), errors="coerce").dropna()
    feats["h_avg_last3f_5"] = float(last3f.mean()) if len(last3f) else 38.0
    all_last3f = pd.to_numeric(hist.get("last3f_hist", pd.Series()), errors="coerce").dropna()
    feats["h_best_last3f"] = float(all_last3f.min()) if len(all_last3f) else 40.0

    # タイム偏差 (平均から何秒速いか)
    times = pd.to_numeric(hist5.get("time_hist", pd.Series()), errors="coerce").dropna()
    feats["h_avg_time_deviation"] = 0.0
    if len(times) >= 2:
        feats["h_avg_time_deviation"] = float(times.mean() - times.iloc[0]) if times.iloc[0] else 0.0

    # レース間隔 (直近2走間隔)
    feats["h_days_since_last"] = _calc_days_since_last(hist)

    # コース適性 (芝/ダート別)
    course_col = hist.get("course_type_hist", pd.Series())
    current_course = current_row.get("course_type", "")
    if current_course and not course_col.empty:
        mask = course_col == current_course
        same_course = hist[mask]
        if len(same_course) > 0:
            sc_orders = pd.to_numeric(same_course.get("order_hist", pd.Series()), errors="coerce").dropna()
            feats["h_course_win_rate"] = float((sc_orders == 1).sum() / len(sc_orders)) if len(sc_orders) else 0.0
        else:
            feats["h_course_win_rate"] = 0.0
    else:
        feats["h_course_win_rate"] = feats["h_win_rate_all"]

    # 距離適性 (±200m以内)
    current_dist = current_row.get("distance", 0)
    if current_dist and "distance_hist" in hist.columns:
        dist_col = pd.to_numeric(hist["distance_hist"], errors="coerce")
        mask_dist = (dist_col - current_dist).abs() <= 200
        same_dist = hist[mask_dist]
        if len(same_dist) > 0:
            sd_orders = pd.to_numeric(same_dist.get("order_hist", pd.Series()), errors="coerce").dropna()
            feats["h_dist_win_rate"] = float((sd_orders == 1).sum() / len(sd_orders)) if len(sd_orders) else 0.0
        else:
            feats["h_dist_win_rate"] = 0.0
    else:
        feats["h_dist_win_rate"] = feats["h_win_rate_all"]

    # 体重変化
    hw_diff = current_row.get("horse_weight_diff")
    feats["h_weight_change"] = int(hw_diff) if hw_diff is not None else 0

    # 連勝/連敗数
    streak_win, streak_loss = 0, 0
    for o in orders:
        if o == 1:
            streak_win += 1
            streak_loss = 0
        else:
            streak_loss += 1
            streak_win = 0
    # 直近から数える
    feats["h_consecutive_wins"] = 0
    feats["h_consecutive_losses"] = 0
    for o in orders:
        if o == 1:
            feats["h_consecutive_wins"] += 1
            break
        else:
            feats["h_consecutive_losses"] += 1

    return feats


def _calc_days_since_last(hist: pd.DataFrame) -> int:
    """最後のレースから今日まで (概算)。"""
    if "date" not in hist.columns or hist.empty:
        return 60
    try:
        last_date = pd.to_datetime(hist["date"].iloc[0], errors="coerce")
        if pd.isna(last_date):
            return 60
        today = pd.Timestamp.today()
        return int((today - last_date).days)
    except Exception:
        return 60


def _add_race_features(df: pd.DataFrame) -> pd.DataFrame:
    """レース固有の特徴量。"""
    if "head_count" in df.columns:
        df["head_count"] = pd.to_numeric(df["head_count"], errors="coerce").fillna(10)
    if "distance" in df.columns:
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce").fillna(1600)
        # 距離カテゴリ (短距離/マイル/中距離/長距離)
        df["dist_category"] = pd.cut(
            df["distance"],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=[0, 1, 2, 3],
        ).astype(float)
    if "frame_no" in df.columns:
        df["frame_no"] = pd.to_numeric(df["frame_no"], errors="coerce").fillna(4)
    if "horse_no" in df.columns:
        df["horse_no"] = pd.to_numeric(df["horse_no"], errors="coerce").fillna(8)
    if "weight_carried" in df.columns:
        df["weight_carried"] = pd.to_numeric(df["weight_carried"], errors="coerce").fillna(55)
    return df


def _add_odds_features(df: pd.DataFrame) -> pd.DataFrame:
    """オッズ関連の特徴量。"""
    if "odds" in df.columns:
        df["odds"] = pd.to_numeric(df["odds"], errors="coerce")
        df["implied_prob"] = 1.0 / df["odds"].clip(lower=1.01)
        df["log_odds"] = np.log(df["odds"].clip(lower=1.01))
    if "fav_rank" in df.columns:
        df["fav_rank"] = pd.to_numeric(df["fav_rank"], errors="coerce").fillna(9)
        df["is_favorite"] = (df["fav_rank"] == 1).astype(int)
        df["is_top3_fav"] = (df["fav_rank"] <= 3).astype(int)
    return df


def _add_relative_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    レース内の相対特徴量。
    同一レース内での偏差・ランク特徴を追加する。
    """
    # オッズ偏差 (同レース内での標準化)
    if "odds" in df.columns and "race_id" in df.columns:
        df["odds_z"] = df.groupby("race_id")["odds"].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-6)
        )
    # 馬番の外枠/内枠フラグ
    if "horse_no" in df.columns and "head_count" in df.columns:
        df["inner_gate"] = (df["horse_no"] <= df["head_count"] / 3).astype(int)
        df["outer_gate"] = (df["horse_no"] > df["head_count"] * 2 / 3).astype(int)

    # 上がり3F レース内相対
    if "h_avg_last3f_5" in df.columns and "race_id" in df.columns:
        df["last3f_z"] = df.groupby("race_id")["h_avg_last3f_5"].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-6)
        )
    return df


# ---------------------------------------------------------------------------
# 学習用データセット構築
# ---------------------------------------------------------------------------

def build_training_dataset(race_df: pd.DataFrame, history_dict: dict) -> tuple[pd.DataFrame, pd.Series]:
    """
    レース結果 DataFrame から学習用 (X, y) を生成する。
    y = 1 (1着) / 0 (2着以下)
    """
    feat_df = build_features(race_df, history_dict)

    # ターゲット: 単勝的中
    if "order" in feat_df.columns:
        y = (pd.to_numeric(feat_df["order"], errors="coerce") == 1).astype(int)
    else:
        raise ValueError("'order' column not found in race_df")

    feature_cols = _get_feature_columns(feat_df)
    X = feat_df[feature_cols].copy()
    X = X.apply(pd.to_numeric, errors="coerce").fillna(0)

    return X, y


def _get_feature_columns(df: pd.DataFrame) -> list[str]:
    """学習に使う特徴量カラムを返す。"""
    use_cols = [
        # レース情報
        "course_type_enc", "track_cond_enc", "distance", "dist_category",
        "direction_enc", "head_count",
        # 馬番・枠番
        "frame_no", "horse_no", "inner_gate", "outer_gate",
        # 斤量・馬体重
        "weight_carried", "horse_weight", "horse_weight_diff",
        # 個体特徴
        "sex_enc", "age",
        # 過去成績
        "h_n_races", "h_win_rate_all", "h_top3_rate_all",
        "h_win_rate_5", "h_top3_rate_5", "h_avg_order_5",
        "h_avg_last3f_5", "h_best_last3f", "h_avg_time_deviation",
        "h_days_since_last", "h_course_win_rate", "h_dist_win_rate",
        "h_weight_change", "h_consecutive_wins", "h_consecutive_losses",
        # オッズ
        "odds", "implied_prob", "log_odds", "fav_rank",
        "is_favorite", "is_top3_fav", "odds_z",
        # 相対特徴
        "last3f_z",
    ]
    return [c for c in use_cols if c in df.columns]


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _safe_int(text) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", str(text)))
    except Exception:
        return None

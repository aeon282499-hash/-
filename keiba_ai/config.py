"""
競馬AI 設定ファイル
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
LOG_DIR = BASE_DIR / "logs"

for d in [DATA_DIR, MODEL_DIR, LOG_DIR]:
    d.mkdir(exist_ok=True)

# ---- スクレイピング設定 ----
NETKEIBA_BASE = "https://db.netkeiba.com"
REQUEST_INTERVAL = 2.0          # リクエスト間隔(秒) ※サーバー負荷軽減
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---- 場コード (JRA 中央競馬) ----
JRA_VENUES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

# ---- 場コード (NAR 地方競馬) ----
NAR_VENUES = {
    "30": "門別", "31": "北見", "32": "岩見沢", "33": "帯広",
    "34": "旭川", "35": "盛岡", "36": "水沢", "37": "上山",
    "38": "三条", "39": "足利", "40": "宇都宮", "41": "高崎",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋", "50": "園田",
    "51": "姫路", "54": "福山", "55": "高知", "56": "佐賀",
    "57": "荒尾", "58": "中津",
}

# ---- モデル設定 ----
LIGHTGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "boosting_type": "gbdt",
    "num_leaves": 63,
    "max_depth": -1,
    "learning_rate": 0.05,
    "n_estimators": 1000,
    "min_child_samples": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "random_state": 42,
    "verbose": -1,
}

# ---- ベッティング設定 ----
KELLY_FRACTION = 0.25           # フラクショナルKelly (0.25 = 1/4 Kelly)
MIN_EXPECTED_VALUE = 1.10       # 最低期待値(EV) ≥ 1.10 のみ賭ける
MAX_BET_RATIO = 0.05            # 1レース最大賭け金割合 (資金の5%)
MIN_BET_UNIT = 100              # 最小賭け金 (円)
WIN_ODDS_MIN = 2.0              # 最低単勝オッズ (1.9倍以下は見送り)
WIN_ODDS_MAX = 50.0             # 最高単勝オッズ (穴狙い過ぎ防止)

# ---- バックテスト設定 ----
INITIAL_BANKROLL = 100_000      # 初期資金 (10万円)
BET_TYPE = "win"                # "win" (単勝) or "place" (複勝)

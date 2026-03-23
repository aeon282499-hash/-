"""
screener.py — 銘柄選定・売買シグナルロジック
================================================

【戦略：厳選ボラティリティ + 極端乖離フィルター（AND条件）】

以下の条件を"すべて同時に満たす"銘柄だけを抽出する。
いずれか1つでも欠けた銘柄は除外するため、シグナル数は少なくなるが確実性が高い。

条件①  RSI(14) が極端ゾーン
        RSI < RSI_BUY_MAX  → 買い候補
        RSI > RSI_SELL_MIN → 売り候補（空売り）

条件②  25日移動平均乖離率が大きく偏離
        乖離率 < DEV_BUY_MAX%  → 買い候補（下方乖離）
        乖離率 > DEV_SELL_MIN% → 売り候補（上方乖離）

条件③  前日の値幅が ATR(20) の RANGE_MULT 倍以上
        → 相場に"勢い"がある日の翌日を狙う

条件④  前日の出来高が 20日平均出来高の VOL_MULT 倍以上
        → 機関投資家等の大口参加を確認

条件⑤  方向の一致
        RSI が売られすぎ ＆ 乖離率が下方 → BUY
        RSI が買われすぎ ＆ 乖離率が上方 → SELL
        方向が食い違う場合はスキップ

────────────────────────────────────────────────
カスタマイズ方法
  閾値定数（以下の「=== 閾値設定 ===」ブロック）を変えるだけで
  ロジック全体が変わる。
  独自の指標を追加したい場合は judge_signal() 内に追記してください。
────────────────────────────────────────────────
"""

import yfinance as yf
import pandas as pd
import numpy as np

# ================================================================
# 対象銘柄ユニバース（日経225から流動性上位）
#   コード末尾の ".T" = 東証銘柄として yfinance が認識する
#   銘柄を追加・削除したい場合はこのリストを編集してください
# ================================================================
UNIVERSE: list[tuple[str, str]] = [
    ("7203.T", "トヨタ自動車"),
    ("9984.T", "ソフトバンクグループ"),
    ("6758.T", "ソニーグループ"),
    ("9983.T", "ファーストリテイリング"),
    ("6861.T", "キーエンス"),
    ("6098.T", "リクルートホールディングス"),
    ("4063.T", "信越化学工業"),
    ("8035.T", "東京エレクトロン"),
    ("9433.T", "KDDI"),
    ("8306.T", "三菱UFJフィナンシャル・グループ"),
    ("6954.T", "ファナック"),
    ("6367.T", "ダイキン工業"),
    ("7974.T", "任天堂"),
    ("8316.T", "三井住友フィナンシャルグループ"),
    ("4568.T", "第一三共"),
    ("4519.T", "中外製薬"),
    ("6902.T", "デンソー"),
    ("7267.T", "本田技研工業"),
    ("6981.T", "村田製作所"),
    ("9020.T", "東日本旅客鉄道"),
    ("2914.T", "日本たばこ産業"),
    ("8411.T", "みずほフィナンシャルグループ"),
    ("6501.T", "日立製作所"),
    ("6503.T", "三菱電機"),
    ("9022.T", "東海旅客鉄道"),
    ("4502.T", "武田薬品工業"),
    ("7011.T", "三菱重工業"),
    ("5401.T", "日本製鉄"),
    ("8058.T", "三菱商事"),
    ("8031.T", "三井物産"),
]

# ================================================================
# === 閾値設定（ここを変えるだけでロジックが変わる）===
# ================================================================

RSI_PERIOD    = 14     # RSI 算出期間（日）
MA_PERIOD     = 25     # 移動平均 算出期間（日）
ATR_PERIOD    = 20     # ATR 算出期間（日）
LOOKBACK_DAYS = 80     # yfinance から取得する過去日数

# 条件① RSI 閾値
RSI_BUY_MAX   = 30     # RSI がこの値 "以下" → 買い候補（売られ過ぎ）
RSI_SELL_MIN  = 70     # RSI がこの値 "以上" → 売り候補（買われ過ぎ）

# 条件② 移動平均乖離率 閾値（%）
DEV_BUY_MAX   = -5.0   # 乖離率がこの値 "以下" → 買い候補
DEV_SELL_MIN  = +5.0   # 乖離率がこの値 "以上" → 売り候補

# 条件③ 前日値幅 / ATR 閾値
RANGE_MULT    = 1.5    # 前日の値幅が ATR の何倍以上か

# 条件④ 前日出来高 / 20日平均出来高 閾値
VOL_MULT      = 1.5    # 前日出来高が 20日平均の何倍以上か

# 最大抽出銘柄数
MAX_SIGNALS   = 10


# ================================================================
# テクニカル指標 計算関数
# ================================================================

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> float | None:
    """
    RSI（Relative Strength Index）を計算して最新値を返す。

    Parameters
    ----------
    close  : 終値の時系列 Series
    period : RSI 算出期間（デフォルト 14）

    Returns
    -------
    float | None : RSI 値（0〜100）。データ不足の場合は None。
    """
    if len(close) < period + 1:
        return None
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    rsi      = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)


def calc_ma_deviation(close: pd.Series, period: int = MA_PERIOD) -> float | None:
    """
    移動平均乖離率（%）を計算して最新値を返す。

    乖離率 = (最新終値 − MA) / MA × 100
      正 → MA 上方乖離（買われ過ぎ傾向）
      負 → MA 下方乖離（売られ過ぎ傾向）

    Returns
    -------
    float | None : 乖離率(%)。データ不足の場合は None。
    """
    if len(close) < period:
        return None
    ma       = float(close.rolling(period).mean().iloc[-1])
    latest   = float(close.iloc[-1])
    return round((latest - ma) / ma * 100, 2)


def calc_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float | None:
    """
    前日の値幅 ÷ ATR(atr_period) を返す（ボラティリティ比率）。

    値が大きいほど前日の動きが平常より大きかったことを示す。
    True Range = max(H-L, |H-prev_C|, |L-prev_C|) の移動平均が ATR。

    Returns
    -------
    float | None : 比率。データ不足の場合は None。
    """
    if len(df) < atr_period + 2:
        return None
    high       = df["High"]
    low        = df["Low"]
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr        = float(tr.rolling(atr_period).mean().iloc[-2])   # 前日時点のATR
    prev_range = float(high.iloc[-2]) - float(low.iloc[-2])      # 前日の値幅
    if atr == 0:
        return None
    return round(prev_range / atr, 2)


def calc_volume_ratio(df: pd.DataFrame, period: int = 20) -> float | None:
    """
    前日出来高 ÷ 直近 period 日の平均出来高 を返す。

    Returns
    -------
    float | None : 出来高比率。データ不足の場合は None。
    """
    vol = df["Volume"].dropna()
    if len(vol) < period + 1:
        return None
    avg_vol  = float(vol.iloc[-(period + 1):-1].mean())
    prev_vol = float(vol.iloc[-2])
    if avg_vol == 0:
        return None
    return round(prev_vol / avg_vol, 2)


# ================================================================
# シグナル判定関数（カスタマイズのメイン）
# ================================================================

def judge_signal(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """
    1銘柄のシグナルを判定して結果 dict または None を返す。

    AND 条件（全部満たした場合のみシグナル）:
      ① RSI が極端ゾーン
      ② 乖離率が閾値を超えた方向
      ③ 前日値幅 / ATR >= RANGE_MULT
      ④ 前日出来高 / 20日平均 >= VOL_MULT
      ⑤ ①と②の方向（BUY/SELL）が一致

    ──────────────────────────────────────────
    独自ロジックを追加したい場合:
      1. 新しい指標計算関数を上部に追加する
      2. この関数内の「条件チェック」ブロックに条件を追記し、
         pass_all フラグに AND で追加する
    ──────────────────────────────────────────

    Returns
    -------
    dict | None
      {
        "ticker"    : str,   銘柄コード
        "name"      : str,   銘柄名
        "direction" : str,   "BUY" or "SELL"
        "rsi"       : float,
        "deviation" : float, 乖離率(%)
        "range_ratio": float,
        "vol_ratio" : float,
        "reason"    : list[str],  選定理由の箇条書き
      }
    """
    close = df["Close"].dropna()
    if len(close) < MA_PERIOD + 5:
        return None

    # ── 指標を計算 ──────────────────────────────────
    rsi          = calc_rsi(close)
    deviation    = calc_ma_deviation(close)
    range_ratio  = calc_range_ratio(df)
    vol_ratio    = calc_volume_ratio(df)

    if any(v is None for v in [rsi, deviation, range_ratio, vol_ratio]):
        return None

    # ── 条件①：RSI 方向 ─────────────────────────────
    if rsi <= RSI_BUY_MAX:
        rsi_dir = "BUY"
    elif rsi >= RSI_SELL_MIN:
        rsi_dir = "SELL"
    else:
        return None   # RSI が中立ゾーン → 除外

    # ── 条件②：乖離率 方向 ──────────────────────────
    if deviation <= DEV_BUY_MAX:
        dev_dir = "BUY"
    elif deviation >= DEV_SELL_MIN:
        dev_dir = "SELL"
    else:
        return None   # 乖離率が閾値未満 → 除外

    # ── 条件⑤：方向の一致 ───────────────────────────
    if rsi_dir != dev_dir:
        return None   # 買われ過ぎ RSI ＋ 下方乖離など矛盾 → 除外

    direction = rsi_dir

    # ── 条件③：ボラティリティ ───────────────────────
    if range_ratio < RANGE_MULT:
        return None   # 前日の動きが小さすぎる → 除外

    # ── 条件④：出来高 ───────────────────────────────
    if vol_ratio < VOL_MULT:
        return None   # 出来高が平常並み → 除外

    # ── 全条件クリア：選定理由を組み立てる ─────────────
    if direction == "BUY":
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{RSI_BUY_MAX}以下：売られ過ぎ）",
            f"{MA_PERIOD}MA乖離率 = {deviation:+.1f}%（{DEV_BUY_MAX}%以下：下方乖離）",
            f"前日値幅/ATR = {range_ratio}（{RANGE_MULT}倍超：高ボラ確認）",
            f"前日出来高 = 平均の {vol_ratio}倍（{VOL_MULT}倍超：大口参加確認）",
        ]
    else:
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{RSI_SELL_MIN}以上：買われ過ぎ）",
            f"{MA_PERIOD}MA乖離率 = {deviation:+.1f}%（{DEV_SELL_MIN}%以上：上方乖離）",
            f"前日値幅/ATR = {range_ratio}（{RANGE_MULT}倍超：高ボラ確認）",
            f"前日出来高 = 平均の {vol_ratio}倍（{VOL_MULT}倍超：大口参加確認）",
        ]

    return {
        "ticker":      ticker,
        "name":        name,
        "direction":   direction,
        "rsi":         rsi,
        "deviation":   deviation,
        "range_ratio": range_ratio,
        "vol_ratio":   vol_ratio,
        "reason":      reason,
    }


# ================================================================
# メインスクリーニング関数
# ================================================================

def run_screener() -> list[dict]:
    """
    UNIVERSE 全銘柄をスクリーニングして、条件を満たした銘柄リストを返す。
    シグナルが 0 件の場合は空リスト [] を返す（上限は MAX_SIGNALS 件）。

    Returns
    -------
    list[dict] : judge_signal() が返す dict のリスト
    """
    print(f"[screener] {len(UNIVERSE)} 銘柄をスクリーニング中...")
    signals: list[dict] = []

    for ticker, name in UNIVERSE:
        try:
            df = yf.download(
                ticker,
                period=f"{LOOKBACK_DAYS}d",
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            # MultiIndex 対応（yfinance 0.2.x 以降）
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < MA_PERIOD + 10:
                print(f"  [{ticker}] データ不足 → スキップ")
                continue

            result = judge_signal(ticker, name, df)
            if result:
                signals.append(result)
                print(f"  [{ticker}] {name} → {result['direction']} ✅")
            else:
                print(f"  [{ticker}] {name} → 条件未達 ✗")

        except Exception as e:
            print(f"  [{ticker}] エラー: {e}")

        if len(signals) >= MAX_SIGNALS:
            print(f"[screener] 上限 {MAX_SIGNALS} 件に達したため終了")
            break

    print(f"[screener] 結果: {len(signals)} 銘柄がシグナル条件を満たしました")
    return signals

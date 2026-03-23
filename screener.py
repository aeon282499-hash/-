"""
screener.py — 銘柄選定・売買シグナルロジック
================================================

【対象】東証プライム市場 全銘柄（JPXの公式リストから自動取得）
【戦略】厳選ボラティリティ + 極端乖離フィルター（AND条件）

条件①  RSI(14) が極端ゾーン
        RSI < RSI_BUY_MAX  → 買い候補
        RSI > RSI_SELL_MIN → 売り候補

条件②  25日移動平均乖離率が大きく偏離
        乖離率 < DEV_BUY_MAX%  → 買い候補
        乖離率 > DEV_SELL_MIN% → 売り候補

条件③  前日の値幅が ATR(20) の RANGE_MULT 倍以上

条件④  前日の出来高が 20日平均出来高の VOL_MULT 倍以上

条件⑤  ①と②の方向が一致

────────────────────────────────────────────────
カスタマイズ方法
  閾値定数（以下の「=== 閾値設定 ===」ブロック）を変えるだけで
  ロジック全体が変わる。独自指標は judge_signal() 内に追記。
────────────────────────────────────────────────
"""

import io
import time
import requests
import yfinance as yf
import pandas as pd
import numpy as np

# ================================================================
# === 閾値設定（ここを変えるだけでロジックが変わる）===
# ================================================================

RSI_PERIOD    = 14
MA_PERIOD     = 25
ATR_PERIOD    = 20
LOOKBACK_DAYS = 80    # yfinance から取得する過去日数

RSI_BUY_MAX   = 32    # RSI がこの値以下 → 買い候補
RSI_SELL_MIN  = 68    # RSI がこの値以上 → 売り候補
DEV_BUY_MAX   = -4.0  # 乖離率がこの値(%)以下 → 買い候補
DEV_SELL_MIN  = +4.0  # 乖離率がこの値(%)以上 → 売り候補
RANGE_MULT    = 1.3   # 前日値幅が ATR の何倍以上か
VOL_MULT      = 2.0   # 前日出来高が 20日平均の何倍以上か
MAX_SIGNALS   = 10    # 最大抽出銘柄数

# バッチダウンロードの分割サイズ（大きすぎるとタイムアウト）
BATCH_SIZE    = 100


# ================================================================
# 東証プライム銘柄リスト取得（JPX公式Excelから自動取得）
# ================================================================

def fetch_tse_universe() -> list[tuple[str, str]]:
    """
    JPX（日本取引所グループ）の公式Excelファイルから
    東証プライム市場の全銘柄コードと銘柄名を取得する。

    取得元: https://www.jpx.co.jp/markets/statistics-equities/misc/
    戻り値: [("1234.T", "銘柄名"), ...] のリスト
    """
    url = (
        "https://www.jpx.co.jp/markets/statistics-equities/misc/"
        "tvdivq0000001vg2-att/data_j.xls"
    )
    print("[universe] JPXから銘柄リストを取得中...")
    try:
        resp = requests.get(url, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))

        # プライム市場のみ抽出
        prime = df[df["市場・商品区分"] == "プライム（内国株式）"].copy()
        prime["ticker"] = prime["コード"].astype(str).str.zfill(4) + ".T"
        universe = list(zip(prime["ticker"], prime["銘柄名"]))
        print(f"[universe] 取得完了: {len(universe)} 銘柄（東証プライム）")
        return universe

    except Exception as e:
        print(f"[universe] JPX取得失敗: {e}")
        print("[universe] フォールバック: 組み込みリストを使用します")
        return _fallback_universe()


def _fallback_universe() -> list[tuple[str, str]]:
    """JPX取得失敗時のフォールバック（日経225採用銘柄 代表30銘柄）"""
    return [
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
# テクニカル指標 計算関数
# ================================================================

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> float | None:
    """RSI(period日) の最新値を返す。データ不足時は None。"""
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
    """移動平均乖離率(%) の最新値を返す。データ不足時は None。"""
    if len(close) < period:
        return None
    ma     = float(close.rolling(period).mean().iloc[-1])
    latest = float(close.iloc[-1])
    return round((latest - ma) / ma * 100, 2)


def calc_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float | None:
    """前日の値幅 / ATR(atr_period) を返す。データ不足時は None。"""
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
    atr        = float(tr.rolling(atr_period).mean().iloc[-2])
    prev_range = float(high.iloc[-2]) - float(low.iloc[-2])
    if atr == 0:
        return None
    return round(prev_range / atr, 2)


def calc_volume_ratio(df: pd.DataFrame, period: int = 20) -> float | None:
    """前日出来高 / 直近 period 日の平均出来高 を返す。データ不足時は None。"""
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
    1銘柄のシグナルを判定。全AND条件を満たした場合のみ dict を返す。

    ──────────────────────────────────────────
    独自ロジックを追加したい場合:
      1. 新しい指標計算関数を上部に追加する
      2. この関数内に条件を追記して pass_all に AND で加える
    ──────────────────────────────────────────
    """
    close = df["Close"].dropna()
    if len(close) < MA_PERIOD + 5:
        return None

    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)

    if any(v is None for v in [rsi, deviation, range_ratio, vol_ratio]):
        return None

    # 条件① RSI
    if rsi <= RSI_BUY_MAX:      rsi_dir = "BUY"
    elif rsi >= RSI_SELL_MIN:   rsi_dir = "SELL"
    else:                       return None

    # 条件② 乖離率
    if deviation <= DEV_BUY_MAX:     dev_dir = "BUY"
    elif deviation >= DEV_SELL_MIN:  dev_dir = "SELL"
    else:                            return None

    # 条件⑤ 方向の一致
    if rsi_dir != dev_dir:      return None

    # 条件③ ボラティリティ
    if range_ratio < RANGE_MULT: return None

    # 条件④ 出来高
    if vol_ratio < VOL_MULT:     return None

    direction = rsi_dir
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
# バッチダウンロード（高速化）
# ================================================================

def _batch_download(tickers: list[str], period: str) -> dict[str, pd.DataFrame]:
    """
    複数銘柄を BATCH_SIZE 件ずつ一括ダウンロードして
    {ticker: DataFrame} の dict を返す。
    """
    result = {}
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for idx, batch in enumerate(batches):
        print(f"  [batch {idx+1}/{len(batches)}] {len(batch)} 銘柄をダウンロード中...")
        try:
            raw = yf.download(
                batch,
                period=period,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
            )
            for ticker in batch:
                try:
                    if len(batch) == 1:
                        df = raw.copy()
                    else:
                        df = raw[ticker].copy()
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    if not df.empty:
                        result[ticker] = df
                except Exception:
                    pass
        except Exception as e:
            print(f"  [batch {idx+1}] ダウンロードエラー: {e}")
        time.sleep(0.5)  # レート制限対策

    return result


# ================================================================
# メインスクリーニング関数
# ================================================================

def run_screener() -> list[dict]:
    """
    東証プライム全銘柄をスクリーニングし、条件を満たした銘柄リストを返す。
    シグナルが 0 件の場合は空リスト [] を返す（上限は MAX_SIGNALS 件）。
    """
    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[screener] {len(universe)} 銘柄のデータを一括取得中...")
    data = _batch_download(tickers, period=f"{LOOKBACK_DAYS}d")
    print(f"[screener] {len(data)} 銘柄のデータ取得完了。シグナル判定中...")

    signals: list[dict] = []
    for ticker, df in data.items():
        if len(signals) >= MAX_SIGNALS:
            break
        name   = name_map.get(ticker, ticker)
        result = judge_signal(ticker, name, df)
        if result:
            signals.append(result)
            print(f"  ✅ [{ticker}] {name} → {result['direction']}")

    print(f"[screener] 結果: {len(signals)} 銘柄がシグナル条件を満たしました")
    return signals

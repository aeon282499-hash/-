"""
screener.py — 銘柄選定・売買シグナルロジック
================================================

【戦略】RSI(14)モメンタム順張り＋過熱空売り
  日経225採用銘柄（売買代金30億円以上）に絞り、
  強いトレンドに乗る順張り買いと過熱銘柄の寄り天空売りを狙う。

■ シグナル判定フロー
  ─ 前日（T-1）データで判定 ────────────────────────
  ① RSI(14)        買い: 60〜79 / 売り: 80以上
  ② 25MA乖離率     買い: +2.0%以上 / 売り: +5.0%以上
  ③ ボラ/出来高   [OR条件] 値幅≧ATR×1.5 OR 出来高≧平均×1.5
  ④ 流動性        売買代金≧30億円（150万決済でスリッページゼロ）
  ⑤ マクロフィルター 米国市場の前日騰落で買い/売りをバイアス調整

  ─ 最終選定 ─────────────────────────────────────
  ⑥ 上記通過銘柄を流動性（売買代金）降順にソートし最大3銘柄を選出
"""

import io
import os
import ssl
import time
import requests
import yfinance as yf

ssl._create_default_https_context = ssl._create_unverified_context

def _make_session():
    try:
        from curl_cffi import requests as cfr
        return cfr.Session(verify=False)
    except ImportError:
        pass
    try:
        import requests as req
        s = req.Session()
        s.verify = False
        return s
    except Exception:
        return None

_SESSION = _make_session()
import pandas as pd
import numpy as np

# ================================================================
# === 閾値設定 ===
# ================================================================

RSI_PERIOD     = 14           # モメンタム計測 RSI(14)
MA_DEV_PERIOD  = 25           # 乖離率用 25日移動平均
ATR_PERIOD     = 14           # ATR 算出期間
VOL_AVG_PERIOD = 20           # 平均出来高の算出期間
LOOKBACK_DAYS  = 80           # 取得する過去日数（25MA+ATR計算用）

# ── 条件①②: 買い（順張りモメンタム）───────────────────
RSI_BUY_MIN    = 60           # RSI(14) 60以上（強い上昇モメンタム）
RSI_BUY_MAX    = 79           # RSI(14) 79以下（過熱前）
DEV_BUY_MIN    = 2.0          # 25MA乖離率 +2%以上（上昇トレンド確認）

# ── 条件①②: 売り（過熱からの寄り天崩れ）──────────────
RSI_SELL_MIN   = 80           # RSI(14) 80以上（異常過熱）
DEV_SELL_MIN   = 5.0          # 25MA乖離率 +5%以上（極端な上方乖離）

# ── 条件③: ボラ OR 出来高 ────────────────────────────
RANGE_MULT     = 1.5          # 前日値幅 ≧ ATR × 1.5
VOL_MULT       = 1.5          # 前日出来高 ≧ 平均 × 1.5

# ── 条件④: 流動性（最重要）───────────────────────────
TURNOVER_MIN   = 3_000_000_000  # 前日売買代金 ≧ 30億円

MAX_SIGNALS    = 3            # 最大3銘柄
BATCH_SIZE     = 100


# ================================================================
# 日経225ユニバース（売買代金上位・API負荷軽減）
# ================================================================

def fetch_tse_universe() -> list[tuple[str, str]]:
    """日経225採用銘柄を返す（JPXから取得失敗時はフォールバック）。"""
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
        target_markets = [
            "プライム（内国株式）",
            "スタンダード（内国株式）",
            "グロース（内国株式）",
        ]
        filtered = df[df["市場・商品区分"].isin(target_markets)].copy()
        filtered["ticker"] = filtered["コード"].astype(str).str.zfill(4) + ".T"
        universe = list(zip(filtered["ticker"], filtered["銘柄名"]))
        print(f"[universe] 取得完了: {len(universe)} 銘柄")
        return universe
    except Exception as e:
        print(f"[universe] JPX取得失敗: {e} → 日経225フォールバック使用")
        return _nikkei225_universe()


def _nikkei225_universe() -> list[tuple[str, str]]:
    """日経225採用銘柄のフォールバックリスト。"""
    return [
        ("7203.T","トヨタ自動車"),("9984.T","ソフトバンクグループ"),
        ("6758.T","ソニーグループ"),("9983.T","ファーストリテイリング"),
        ("6861.T","キーエンス"),("6098.T","リクルートHD"),
        ("4063.T","信越化学工業"),("8035.T","東京エレクトロン"),
        ("9433.T","KDDI"),("8306.T","三菱UFJ FG"),
        ("6954.T","ファナック"),("6367.T","ダイキン工業"),
        ("7974.T","任天堂"),("8316.T","三井住友FG"),
        ("4568.T","第一三共"),("4519.T","中外製薬"),
        ("6902.T","デンソー"),("7267.T","本田技研工業"),
        ("6981.T","村田製作所"),("9020.T","東日本旅客鉄道"),
        ("2914.T","日本たばこ産業"),("8411.T","みずほFG"),
        ("6501.T","日立製作所"),("6503.T","三菱電機"),
        ("9022.T","東海旅客鉄道"),("4502.T","武田薬品工業"),
        ("7011.T","三菱重工業"),("5401.T","日本製鉄"),
        ("8058.T","三菱商事"),("8031.T","三井物産"),
        ("8001.T","伊藤忠商事"),("8002.T","丸紅"),
        ("9432.T","日本電信電話"),("6645.T","オムロン"),
        ("4523.T","エーザイ"),("4151.T","協和キリン"),
        ("8725.T","MS&ADインシュランス"),("8750.T","第一生命HD"),
        ("7751.T","キヤノン"),("7733.T","オリンパス"),
        ("6702.T","富士通"),("6701.T","日本電気"),
        ("9984.T","ソフトバンクグループ"),("7832.T","バンダイナムコHD"),
        ("4704.T","トレンドマイクロ"),("9766.T","コナミグループ"),
        ("7309.T","シマノ"),("6869.T","シスメックス"),
        ("7741.T","HOYA"),("4578.T","大塚HD"),
        ("9613.T","NTTデータグループ"),("4661.T","オリエンタルランド"),
        ("6762.T","TDK"),("6971.T","京セラ"),
        ("5108.T","ブリヂストン"),("7270.T","SUBARU"),
        ("7201.T","日産自動車"),("7261.T","マツダ"),
        ("7269.T","スズキ"),("6301.T","小松製作所"),
        ("6326.T","クボタ"),("6472.T","NTN"),
        ("5713.T","住友金属鉱山"),("5714.T","DOWAホールディングス"),
        ("3382.T","セブン&アイHD"),("8267.T","イオン"),
        ("2502.T","アサヒグループHD"),("2503.T","キリンHD"),
        ("2801.T","キッコーマン"),("2802.T","味の素"),
        ("3086.T","Jフロントリテイリング"),("3092.T","ZOZO"),
        ("9602.T","東宝"),("9681.T","東京ドーム"),
        ("8830.T","住友不動産"),("8801.T","三井不動産"),
        ("8802.T","菱地所"),("3289.T","東急不動産HD"),
        ("9301.T","三菱倉庫"),("9107.T","川崎汽船"),
        ("9101.T","日本郵船"),("9104.T","商船三井"),
        ("1925.T","大和ハウス工業"),("1928.T","積水ハウス"),
        ("1801.T","大成建設"),("1802.T","大林組"),
        ("1803.T","清水建設"),("1812.T","鹿島建設"),
        ("4021.T","日産化学"),("4041.T","日本曹達"),
        ("4183.T","三井化学"),("4188.T","三菱ケミカルグループ"),
        ("4208.T","UBEグループ"),("4452.T","花王"),
        ("4911.T","資生堂"),("4901.T","富士フイルムHD"),
        ("7731.T","ニコン"),("7752.T","リコー"),
        ("6724.T","セイコーエプソン"),("6952.T","カシオ計算機"),
        ("9531.T","東京ガス"),("9532.T","大阪ガス"),
        ("9501.T","東京電力HD"),("9502.T","中部電力"),
        ("9503.T","関西電力"),("4543.T","テルモ"),
        ("4530.T","久光製薬"),("4507.T","塩野義製薬"),
        ("2432.T","DeNA"),("3659.T","ネクソン"),
        ("4689.T","LINEヤフー"),("4755.T","楽天グループ"),
        ("3697.T","SHIFT"),("4751.T","サイバーエージェント"),
    ]


# ================================================================
# マクロ環境取得
# ================================================================

def fetch_macro() -> dict:
    """前日の米国市場（NYダウ・ナスダック）騰落率を取得する。"""
    result = {"dow": None, "nasdaq": None, "bias": "neutral"}
    macro_tickers = {"dow": "^DJI", "nasdaq": "^IXIC"}
    for key, ticker in macro_tickers.items():
        try:
            kwargs = dict(period="3d", interval="1d",
                          auto_adjust=True, progress=False)
            if _SESSION:
                kwargs["session"] = _SESSION
            df = yf.download(ticker, **kwargs)
            if len(df) >= 2:
                prev = float(df["Close"].iloc[-2])
                last = float(df["Close"].iloc[-1])
                result[key] = round((last - prev) / prev * 100, 2)
        except Exception:
            pass

    dow = result.get("dow") or 0
    nas = result.get("nasdaq") or 0
    if dow < -1.0 and nas < -1.0:
        result["bias"] = "bearish"   # 米国大幅安 → 買い見送り
    elif dow > 1.0 and nas > 1.0:
        result["bias"] = "bullish"   # 米国大幅高 → 売り見送り
    else:
        result["bias"] = "neutral"

    dow_str = f"{dow:+.1f}%" if result["dow"] is not None else "取得不可"
    nas_str = f"{nas:+.1f}%" if result["nasdaq"] is not None else "取得不可"
    print(f"[macro] NYダウ前日比: {dow_str}  ナスダック前日比: {nas_str}  → バイアス: {result['bias']}")
    return result


# ================================================================
# テクニカル指標 計算関数
# ================================================================

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> float | None:
    if len(close) < period + 1:
        return None
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return round(float(val), 2) if pd.notna(val) else None


def calc_ma_deviation(close: pd.Series, period: int = MA_DEV_PERIOD) -> float | None:
    """25MA乖離率(%) = (最新値 - MA) / MA × 100"""
    if len(close) < period:
        return None
    ma = float(close.rolling(period).mean().iloc[-1])
    if ma == 0:
        return None
    return round((float(close.iloc[-1]) - ma) / ma * 100, 2)


def calc_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float | None:
    if len(df) < atr_period + 2:
        return None
    high, low = df["High"], df["Low"]
    prev_close = df["Close"].shift(1)
    tr  = pd.concat([high - low,
                     (high - prev_close).abs(),
                     (low  - prev_close).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(atr_period).mean().iloc[-2])
    if atr == 0:
        return None
    prev_range = float(high.iloc[-2]) - float(low.iloc[-2])
    return round(prev_range / atr, 2)


def calc_volume_ratio(df: pd.DataFrame,
                      period: int = VOL_AVG_PERIOD) -> float | None:
    vol = df["Volume"].dropna()
    if len(vol) < period + 2:
        return None
    avg_vol  = float(vol.iloc[-(period + 2):-2].mean())
    prev_vol = float(vol.iloc[-2])
    if avg_vol == 0:
        return None
    return round(prev_vol / avg_vol, 2)


def calc_turnover(df: pd.DataFrame) -> float | None:
    if len(df) < 2:
        return None
    prev_close  = float(df["Close"].iloc[-2])
    prev_volume = float(df["Volume"].iloc[-2])
    if prev_volume == 0:
        return None
    return prev_close * prev_volume


def calc_trend(close: pd.Series, period: int = 200) -> str | None:
    if len(close) < period:
        return None
    ma  = float(close.rolling(period).mean().iloc[-1])
    cur = float(close.iloc[-1])
    if ma == 0:
        return None
    return "UP" if cur > ma else "DOWN"


# ================================================================
# シグナル判定（前日データ ①〜④）
# ================================================================

def judge_signal_pre(
    ticker: str,
    name: str,
    df: pd.DataFrame,
) -> dict | None:
    """
    RSI(14)モメンタム戦略でシグナル判定。
    買い: RSI(14)=60〜79 かつ 25MA乖離率≧+2%
    売り: RSI(14)≧80   かつ 25MA乖離率≧+5%
    """
    close = df["Close"].dropna()
    if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD) + 5:
        return None

    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)
    turnover    = calc_turnover(df)

    if any(v is None for v in [rsi, deviation, turnover]):
        return None

    # ── 条件①②: 方向判定 ────────────────────────────
    if RSI_BUY_MIN <= rsi <= RSI_BUY_MAX and deviation >= DEV_BUY_MIN:
        direction = "BUY"
    elif rsi >= RSI_SELL_MIN and deviation >= DEV_SELL_MIN:
        direction = "SELL"
    else:
        return None

    # ── 条件③: ボラ OR 出来高 ────────────────────────
    range_ok = (range_ratio is not None) and (range_ratio >= RANGE_MULT)
    vol_ok   = (vol_ratio   is not None) and (vol_ratio   >= VOL_MULT)
    if not (range_ok or vol_ok):
        return None

    # ── 条件④: 流動性 ────────────────────────────────
    if turnover < TURNOVER_MIN:
        return None

    cond3_str = []
    if range_ok: cond3_str.append(f"値幅/ATR={range_ratio:.1f}（≧{RANGE_MULT}）")
    if vol_ok:   cond3_str.append(f"出来高比={vol_ratio:.1f}（≧{VOL_MULT}）")

    prev_close_price = float(close.iloc[-1])
    if direction == "BUY":
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{RSI_BUY_MIN}〜{RSI_BUY_MAX}：強い上昇モメンタム）",
            f"25MA乖離率 = {deviation:+.1f}%（≧+{DEV_BUY_MIN}%：上昇トレンド確認）",
            "③ " + " / ".join(cond3_str),
            f"売買代金 = {turnover/1e8:.0f}億円（流動性十分）",
        ]
    else:
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（≧{RSI_SELL_MIN}：異常過熱）",
            f"25MA乖離率 = {deviation:+.1f}%（≧+{DEV_SELL_MIN}%：極端な上方乖離）",
            "③ " + " / ".join(cond3_str),
            f"売買代金 = {turnover/1e8:.0f}億円（空売り流動性十分）",
        ]

    return {
        "ticker":      ticker,
        "name":        name,
        "direction":   direction,
        "rsi":         rsi,
        "deviation":   deviation,
        "range_ratio": range_ratio,
        "vol_ratio":   vol_ratio,
        "turnover":    turnover,
        "prev_close":  prev_close_price,
        "reason":      reason,
    }


# 後方互換
judge_signal = judge_signal_pre


def check_gap_entry(signal: dict, today_open: float, gap_max_pct: float = 5.0) -> bool:
    if not today_open or np.isnan(today_open) or today_open <= 0:
        return False
    prev_close = signal["prev_close"]
    if prev_close <= 0:
        return False
    gap_pct = (today_open / prev_close - 1) * 100
    if signal["direction"] == "BUY":
        return -gap_max_pct <= gap_pct < 0
    else:
        return 0 < gap_pct <= gap_max_pct


# ================================================================
# バッチダウンロード
# ================================================================

def batch_download(
    tickers: list[str],
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, pd.DataFrame]:
    result  = {}
    batches = [tickers[i:i + BATCH_SIZE]
               for i in range(0, len(tickers), BATCH_SIZE)]
    failed: list[list[str]] = []

    def _run_batch(batch: list[str]) -> dict[str, pd.DataFrame]:
        kwargs = dict(interval="1d", auto_adjust=True,
                      progress=False, group_by="ticker")
        if period:
            kwargs["period"] = period
        else:
            kwargs["start"] = start
            kwargs["end"]   = end
        if _SESSION is not None:
            kwargs["session"] = _SESSION
        try:
            raw = yf.download(batch, **kwargs)
        except Exception:
            return {}
        out = {}
        for ticker in batch:
            try:
                df = raw[ticker].copy() if len(batch) > 1 else raw.copy()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if not df.empty:
                    out[ticker] = df
            except Exception:
                pass
        return out

    for idx, batch in enumerate(batches):
        print(f"  [batch {idx+1}/{len(batches)}] {len(batch)} 銘柄...")
        got = _run_batch(batch)
        if got:
            result.update(got)
        else:
            failed.append(batch)
        time.sleep(1.5)

    if failed:
        print(f"  [retry] {len(failed)}バッチ失敗。60秒待機後リトライ...")
        time.sleep(60)
        for batch in failed:
            got = _run_batch(batch)
            if got:
                result.update(got)
            time.sleep(2)

    return result


# ================================================================
# メインスクリーニング関数
# ================================================================

def run_screener() -> tuple[list[dict], dict]:
    """
    スクリーニングを実行し、(signals, macro) を返す。
    signals: 最大3銘柄のシグナルリスト
    macro: NYダウ・ナスダックの騰落率と地合いバイアス
    """
    # ── マクロ環境取得 ────────────────────────────────
    macro = fetch_macro()

    # ── 銘柄リスト取得 ────────────────────────────────
    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[screener] {len(universe)} 銘柄のデータを取得中...")
    data = batch_download(tickers, period=f"{LOOKBACK_DAYS}d")
    print(f"[screener] {len(data)} 銘柄取得完了。シグナル判定中...")

    # 当日の途中データを除外
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")
    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    candidates: list[dict] = []

    for ticker, df in data.items():
        close = df["Close"].dropna()
        if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD) + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_pre(ticker, name, df)
        if result:
            candidates.append(result)
            print(f"  ✅ [{ticker}] {name} → {result['direction']} "
                  f"RSI={result['rsi']} 乖離={result['deviation']:+.1f}% "
                  f"売買代金={result['turnover']/1e8:.0f}億")

    # ── マクロバイアスで候補を絞り込む ──────────────────
    bias = macro.get("bias", "neutral")
    if bias == "bearish":
        candidates = [c for c in candidates if c["direction"] == "SELL"]
        print(f"[screener] 地合い悪化（米国株安）→ 買いシグナルを破棄、売りのみ残す")
    elif bias == "bullish":
        candidates = [c for c in candidates if c["direction"] == "BUY"]
        print(f"[screener] 地合い良好（米国株高）→ 売りシグナルを破棄、買いのみ残す")

    # ── 流動性降順ソート → 上位MAX_SIGNALS銘柄 ──────────
    candidates.sort(key=lambda x: x["turnover"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener] 候補{len(candidates)}銘柄 → 最終選定{len(signals)}銘柄")
    return signals, macro

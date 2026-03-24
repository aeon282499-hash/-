"""
screener.py — 銘柄選定・売買シグナルロジック
================================================

【戦略】超短期パニック・リバーサル（逆張り）
  2〜3日の極端な行き過ぎからの自律反発を狙う。

■ シグナル判定フロー
  ─ 前日（T-1）データで判定 ────────────────────────
  ① RSI(2)        買い≦RSI_BUY_MAX / 売り≧RSI_SELL_MIN
  ② 5MA乖離率     買い≦DEV_BUY_MAX% / 売り≧DEV_SELL_MIN%
  ③ ボラ/出来高   [OR条件] 値幅≧ATR×RANGE_MULT OR 出来高≧平均×VOL_MULT
  ④ 流動性        売買代金≧TURNOVER_MIN円
  ⑤ 方向一致      ①と②が同じBUY/SELL

  ─ 当日（T）始値で最終判定 ──────────────────────────
  ⑥ ギャップ      BUY: 始値＜前日終値（ギャップダウン）
                   SELL: 始値＞前日終値（ギャップアップ）
                   ±GAP_MAX_PCT%超の特大ギャップは見送り

────────────────────────────────────────────────
カスタマイズ方法:
  以下「=== 閾値設定 ===」の定数を変更するだけでロジックが変わる。
  optimize.py でこれらの最適値を自動探索できる。
────────────────────────────────────────────────
"""

import io
import os
import ssl
import time
import requests
import yfinance as yf

# ── SSL証明書エラー回避（ユーザー名に日本語が含まれる環境向け）──
# certifi のパスに日本語が含まれると curl が証明書を読めないため、
# SSL検証を無効化したカスタムセッションを使用する
ssl._create_default_https_context = ssl._create_unverified_context

def _make_session():
    """SSL検証を無効化したHTTPセッションを返す。"""
    try:
        # yfinance 0.2.40+ は curl_cffi を使用
        from curl_cffi import requests as cfr
        return cfr.Session(verify=False)
    except ImportError:
        pass
    try:
        # フォールバック: requests ライブラリ
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
# === 閾値設定（optimize.py で自動最適化される）===
# ================================================================

RSI_PERIOD     = 2            # 超短期オシレーター RSI(2)
MA_PERIOD      = 200          # トレンドフィルター用 長期MA
MA_SHORT       = 5            # 短期乖離率用 MA
ATR_PERIOD     = 14           # ATR 算出期間
VOL_AVG_PERIOD = 20           # 平均出来高の算出期間
LOOKBACK_DAYS  = 60           # 取得する過去日数（RSI/5MA/ATRに必要な最小限）

# ── 条件①: RSI(2) 閾値 ──────────────────────────────
RSI_BUY_MAX    = 25           # RSI(2) がこの値以下 → 買い候補
RSI_SELL_MIN   = 75           # RSI(2) がこの値以上 → 売り候補

# ── 条件②: 短期MA乖離率 閾値 ────────────────────────
DEV_BUY_MAX    = -1.5         # 5MA乖離率(%)がこの値以下 → 買い候補
DEV_SELL_MIN   = +1.5         # 5MA乖離率(%)がこの値以上 → 売り候補

# ── 条件③: ボラ OR 出来高（どちらか一方でOK）──────────
RANGE_MULT     = 1.0          # 前日値幅 ≧ ATR × この値
VOL_MULT       = 1.2          # 前日出来高 ≧ 平均 × この値

# ── 条件④: 流動性 ────────────────────────────────────
TURNOVER_MIN   = 500_000_000  # 前日売買代金 ≧ 5億円

# ── 条件⑥: ギャップ 閾値 ────────────────────────────
GAP_MAX_PCT    = 5.0          # ±この%超の特大ギャップは見送り

# ── 条件⑦: 200MAトレンドフィルター（勝率向上の核心）──
# True にすると「トレンド方向への逆張りのみ」に絞り込む
# 買い: 株価が200MA より上（上昇トレンド中の押し目買い）
# 売り: 株価が200MA より下（下降トレンド中の戻り売り）
USE_TREND_FILTER = False  # 一時的に無効化してシグナル数を確認

MAX_SIGNALS    = 999          # 上限なし（条件を満たした銘柄をすべて出す）
BATCH_SIZE     = 100          # バッチダウンロードの分割サイズ


# ================================================================
# 東証全銘柄リスト取得（JPX公式Excelから自動取得）
# ================================================================

def fetch_tse_universe() -> list[tuple[str, str]]:
    """JPXからプライム・スタンダード・グロース全銘柄を取得する。"""
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
        print(f"[universe] JPX取得失敗: {e} → フォールバックリストを使用")
        return _fallback_universe()


def _fallback_universe() -> list[tuple[str, str]]:
    return [
        ("7203.T","トヨタ自動車"),("9984.T","ソフトバンクグループ"),
        ("6758.T","ソニーグループ"),("9983.T","ファーストリテイリング"),
        ("6861.T","キーエンス"),("6098.T","リクルートホールディングス"),
        ("4063.T","信越化学工業"),("8035.T","東京エレクトロン"),
        ("9433.T","KDDI"),("8306.T","三菱UFJフィナンシャル・グループ"),
        ("6954.T","ファナック"),("6367.T","ダイキン工業"),
        ("7974.T","任天堂"),("8316.T","三井住友フィナンシャルグループ"),
        ("4568.T","第一三共"),("4519.T","中外製薬"),
        ("6902.T","デンソー"),("7267.T","本田技研工業"),
        ("6981.T","村田製作所"),("9020.T","東日本旅客鉄道"),
        ("2914.T","日本たばこ産業"),("8411.T","みずほフィナンシャルグループ"),
        ("6501.T","日立製作所"),("6503.T","三菱電機"),
        ("9022.T","東海旅客鉄道"),("4502.T","武田薬品工業"),
        ("7011.T","三菱重工業"),("5401.T","日本製鉄"),
        ("8058.T","三菱商事"),("8031.T","三井物産"),
    ]


# ================================================================
# テクニカル指標 計算関数（スカラー値返し・1銘柄ずつ用）
# ================================================================

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> float | None:
    """RSI(period日)の最新値を返す。データ不足・NaNは None。"""
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


def calc_ma_deviation(close: pd.Series, period: int = MA_SHORT) -> float | None:
    """短期MA乖離率(%) = (最新値 - MA) / MA × 100。MA=0 は None。"""
    if len(close) < period:
        return None
    ma = float(close.rolling(period).mean().iloc[-1])
    if ma == 0:
        return None
    return round((float(close.iloc[-1]) - ma) / ma * 100, 2)


def calc_trend(close: pd.Series, period: int = MA_PERIOD) -> str | None:
    """
    200MA トレンド判定。
    株価 > 200MA → "UP"（上昇トレンド）
    株価 < 200MA → "DOWN"（下降トレンド）
    データ不足  → None（フィルターをスキップ）
    """
    if len(close) < period:
        return None  # データ不足の場合はフィルターしない
    ma  = float(close.rolling(period).mean().iloc[-1])
    cur = float(close.iloc[-1])
    if ma == 0:
        return None
    return "UP" if cur > ma else "DOWN"


def calc_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float | None:
    """前日の値幅 / ATR(atr_period)。iloc[-2]=前日 を使用。ATR=0 は None。"""
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
    """前日出来高 / 直近 period 日の平均出来高。ゼロ除算は None。"""
    vol = df["Volume"].dropna()
    if len(vol) < period + 2:
        return None
    avg_vol  = float(vol.iloc[-(period + 2):-2].mean())
    prev_vol = float(vol.iloc[-2])
    if avg_vol == 0:
        return None
    return round(prev_vol / avg_vol, 2)


def calc_turnover(df: pd.DataFrame) -> float | None:
    """前日売買代金 = 前日終値 × 前日出来高。出来高0（ストップ等）は None。"""
    if len(df) < 2:
        return None
    prev_close  = float(df["Close"].iloc[-2])
    prev_volume = float(df["Volume"].iloc[-2])
    if prev_volume == 0:
        return None
    return prev_close * prev_volume


# ================================================================
# 前日データ（①〜⑤）のシグナル判定
# ================================================================

def judge_signal_pre(
    ticker: str,
    name: str,
    df: pd.DataFrame,
    *,
    rsi_buy: float   = RSI_BUY_MAX,
    rsi_sell: float  = RSI_SELL_MIN,
    dev_buy: float   = DEV_BUY_MAX,
    dev_sell: float  = DEV_SELL_MIN,
    range_mult: float = RANGE_MULT,
    vol_mult: float  = VOL_MULT,
    turnover_min: float = TURNOVER_MIN,
) -> dict | None:
    """
    前日（T-1）のデータで条件①〜⑤を判定する。
    全条件クリアで dict を返す。1つでも不合格なら None。

    パラメータをキーワード引数で渡せるので optimize.py でのグリッドサーチに使用可。

    ルックアヘッドバイアス防止:
      df の最終行（iloc[-1]）が「前日」のデータになっていること。
      当日データは一切参照しない。
    """
    close = df["Close"].dropna()
    # MA_SHORT（5日）と ATR_PERIOD（14日）が最低限必要
    # MA_PERIOD（200日）は calc_trend 内で不足時に None を返してスキップするので除外
    if len(close) < max(MA_SHORT, ATR_PERIOD) + 5:
        return None

    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    trend       = calc_trend(close)       # 200MAトレンド方向
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)
    turnover    = calc_turnover(df)

    # いずれかが計算不能なら除外
    if any(v is None for v in [rsi, deviation, turnover]):
        return None

    # ── 条件① RSI(2) ──────────────────────────────────
    if rsi <= rsi_buy:      rsi_dir = "BUY"
    elif rsi >= rsi_sell:   rsi_dir = "SELL"
    else:                   return None

    # ── 条件② 5MA乖離率 ───────────────────────────────
    if deviation <= dev_buy:     dev_dir = "BUY"
    elif deviation >= dev_sell:  dev_dir = "SELL"
    else:                        return None

    # ── 条件⑤ 方向の一致 ─────────────────────────────
    if rsi_dir != dev_dir:
        return None

    direction = rsi_dir

    # ── 条件⑦ 200MAトレンドフィルター（勝率向上の核心）─
    # 上昇トレンド中の押し目買い / 下降トレンド中の戻り売り のみ許可
    # データ不足で trend=None の場合はスキップしない（小型株対応）
    if USE_TREND_FILTER and trend is not None:
        if direction == "BUY"  and trend != "UP":   return None
        if direction == "SELL" and trend != "DOWN":  return None

    # ── 条件③ ボラティリティ OR 出来高（どちらか一方でOK）─
    range_ok = (range_ratio is not None) and (range_ratio >= range_mult)
    vol_ok   = (vol_ratio   is not None) and (vol_ratio   >= vol_mult)
    if not (range_ok or vol_ok):
        return None   # 両方未達 → 除外

    # ── 条件④ 流動性（売買代金） ─────────────────────
    if turnover < turnover_min:
        return None

    # ── 選定理由の組み立て ─────────────────────────────
    cond3_str = []
    if range_ok: cond3_str.append(f"値幅/ATR={range_ratio}（≧{range_mult}）")
    if vol_ok:   cond3_str.append(f"出来高比={vol_ratio}（≧{vol_mult}）")

    prev_close_price = float(close.iloc[-1])
    if direction == "BUY":
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{rsi_buy}以下：超短期売られ過ぎ）",
            f"{MA_PERIOD}MA乖離率 = {deviation:+.1f}%（下方乖離）",
            "③ " + " / ".join(cond3_str),
            f"売買代金 = {turnover/1e8:.1f}億円",
        ]
    else:
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{rsi_sell}以上：超短期買われ過ぎ）",
            f"{MA_PERIOD}MA乖離率 = {deviation:+.1f}%（上方乖離）",
            "③ " + " / ".join(cond3_str),
            f"売買代金 = {turnover/1e8:.1f}億円",
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


# 後方互換エイリアス
judge_signal = judge_signal_pre


def check_gap_entry(
    signal: dict,
    today_open: float,
    *,
    gap_max_pct: float = GAP_MAX_PCT,
) -> bool:
    """
    条件⑥: 寄り付きギャップ判定（当日の始値のみ参照）。

    BUY : 始値＜前日終値（ギャップダウン）、かつ gap > -gap_max_pct%
    SELL: 始値＞前日終値（ギャップアップ）、かつ gap < +gap_max_pct%
    特大ギャップ（ストップ高/安相当）は見送り。

    当日の高値・安値・終値は絶対に参照しない（ルックアヘッド厳禁）。
    """
    if not today_open or np.isnan(today_open) or today_open <= 0:
        return False

    prev_close = signal["prev_close"]
    if prev_close <= 0:
        return False

    gap_pct = (today_open / prev_close - 1) * 100

    if signal["direction"] == "BUY":
        # ギャップダウン（gap_pct < 0）かつ特大ギャップ除外
        return -gap_max_pct <= gap_pct < 0
    else:
        # ギャップアップ（gap_pct > 0）かつ特大ギャップ除外
        return 0 < gap_pct <= gap_max_pct


# ================================================================
# バッチダウンロード（高速化）
# ================================================================

def batch_download(
    tickers: list[str],
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, pd.DataFrame]:
    """複数銘柄を BATCH_SIZE 件ずつ一括ダウンロード。"""
    result  = {}
    batches = [tickers[i:i + BATCH_SIZE]
               for i in range(0, len(tickers), BATCH_SIZE)]

    failed: list[list[str]] = []

    def _run_batch(batch: list[str]) -> dict[str, pd.DataFrame]:
        """1バッチをダウンロードして {ticker: df} を返す。"""
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

    # ── レートリミット分をリトライ ────────────────────────
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
# メインスクリーニング関数（毎朝8:30実行 / 条件①〜⑤）
# ================================================================

def run_screener() -> list[dict]:
    """
    東証全銘柄をスクリーニングし、条件①〜⑤を満たした銘柄を返す。
    条件⑥（ギャップ判定）は9:00の始値確認後に手動で最終判断。
    """
    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[screener] {len(universe)} 銘柄のデータを取得中...")
    data = batch_download(tickers, period=f"{LOOKBACK_DAYS}d")
    print(f"[screener] {len(data)} 銘柄取得完了。シグナル判定中...")

    # 市場開場中に実行した場合、今日の途中データが混入するため除外する
    # （判定は前日終値までのデータで行う）
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")
    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    signals: list[dict] = []
    cnt = {"total": 0, "data_ok": 0, "rsi": 0, "dev": 0, "vol": 0, "turn": 0, "signal": 0}

    for ticker, df in data.items():
        if len(signals) >= MAX_SIGNALS:
            break
        cnt["total"] += 1
        close = df["Close"].dropna()
        if len(close) < max(MA_SHORT, ATR_PERIOD) + 5:
            continue
        cnt["data_ok"] += 1

        rsi = calc_rsi(close)
        dev = calc_ma_deviation(close)
        if rsi is not None and (rsi <= RSI_BUY_MAX or rsi >= RSI_SELL_MIN):
            cnt["rsi"] += 1
        if dev is not None and (dev <= DEV_BUY_MAX or dev >= DEV_SELL_MIN):
            cnt["dev"] += 1

        name   = name_map.get(ticker, ticker)
        result = judge_signal_pre(ticker, name, df)
        if result:
            signals.append(result)
            cnt["signal"] += 1
            print(f"  ✅ [{ticker}] {name} → {result['direction']}")

    print(f"[screener] 処理銘柄:{cnt['total']} データOK:{cnt['data_ok']} "
          f"RSI条件①:{cnt['rsi']} 乖離条件②:{cnt['dev']} シグナル:{cnt['signal']}")
    return signals

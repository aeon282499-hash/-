"""
screener.py — 銘柄選定・売買シグナルロジック
================================================

【戦略】RSI(14)モメンタム順張り＋過熱空売り
  日本株データ: J-Quants API（JPX公式）でレートリミットなし
  マクロデータ: yfinance（米国市場指数のみ）

■ シグナル判定フロー
  ① RSI(14)      買い: 60〜79 / 売り: 80以上
  ② 25MA乖離率   買い: +2.0%以上 / 売り: +5.0%以上
  ③ ボラ/出来高  値幅≧ATR×1.5 OR 出来高≧平均×1.5
  ④ 流動性       売買代金≧30億円
  ⑤ マクロ       米国市場騰落でバイアス調整
  ⑥ 最終選定     流動性降順で最大3銘柄
"""

import os
import ssl
import time

import requests
import yfinance as yf
import pandas as pd
import numpy as np

ssl._create_default_https_context = ssl._create_unverified_context

# ================================================================
# === 閾値設定 ===
# ================================================================

RSI_PERIOD     = 14
MA_DEV_PERIOD  = 25
ATR_PERIOD     = 14
VOL_AVG_PERIOD = 20
LOOKBACK_DAYS  = 60   # 営業日数（J-Quantsから取得する日数）

RSI_BUY_MAX    = 45     # RSIがこの値以下 → 買い候補（売られすぎ）
DEV_BUY_MAX    = -1.5  # 乖離率がこの値(%)以下 → 買い候補（下がりすぎ）

RSI_SELL_MIN   = 65    # RSIがこの値以上 → 売り候補（買われすぎ）
DEV_SELL_MIN   = 2.5   # 乖離率がこの値(%)以上 → 売り候補（上がりすぎ）

RANGE_MULT     = 1.5
VOL_MULT       = 2.0
TURNOVER_MIN   = 2_000_000_000   # 20億円
ATR_VOL_CAP    = 2.5             # ATR/終値(%)がこれを超える高ボラ銘柄は除外

MAX_SIGNALS    = 8
BATCH_SIZE     = 100   # yfinanceフォールバック用

_JQUANTS_BASE  = "https://api.jquants.com/v2"


# ================================================================
# J-Quants API 認証・データ取得（V2 APIキー方式）
# ================================================================

def _jquants_id_token() -> str:
    """J-Quants V2 APIキーを返す（旧トークン方式は廃止）。"""
    key = (os.getenv("JQUANTS_API_KEY", "") or
           os.getenv("JQUANTS_REFRESH_TOKEN", "")).strip()
    if not key:
        raise ValueError("JQUANTS_API_KEY が未設定です")
    return key


def _jquants_get(path: str, token: str, params: dict | None = None) -> dict:
    resp = requests.get(
        f"{_JQUANTS_BASE}{path}",
        headers={"x-api-key": token},
        params=params or {},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_earnings_tickers(days: int = 2) -> set[str]:
    """直近N日以内に決算発表した銘柄のtickerセットを返す。取得失敗時は空セット。"""
    from datetime import date as _date, timedelta
    try:
        token = _jquants_id_token()
        today = _date.today()
        result: set[str] = set()
        for i in range(days + 1):
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            data = _jquants_get("/fins/announcement", token, {"date": d})
            for item in data.get("announcement", []):
                code = str(item.get("Code", ""))[:4]
                if code:
                    result.add(code + ".T")
        if result:
            print(f"[screener] 決算除外銘柄: {len(result)}件")
        return result
    except Exception as e:
        print(f"[screener] 決算カレンダー取得失敗（フィルターOFF）: {e}")
        return set()


def fetch_tse_universe(token: str | None = None) -> list[tuple[str, str]]:
    """J-Quants /listed/info から上場銘柄を取得する。"""
    if token is None:
        try:
            token = _jquants_id_token()
        except Exception as e:
            print(f"[universe] J-Quants認証失敗: {e} → フォールバック使用")
            return _nikkei225_universe()
    try:
        data  = _jquants_get("/listed/info", token)
        items = data.get("info", [])
        target_keywords = ["プライム", "スタンダード", "グロース"]
        universe = []
        for item in items:
            market = item.get("MarketCodeName", "")
            if any(k in market for k in target_keywords):
                code   = str(item.get("Code", ""))[:4]
                name   = item.get("CompanyName", code)
                ticker = code + ".T"
                universe.append((ticker, name))
        print(f"[universe] J-Quants: {len(universe)} 銘柄取得完了")
        return universe if universe else _nikkei225_universe()
    except Exception as e:
        print(f"[universe] J-Quants取得失敗: {e} → フォールバック使用")
        return _nikkei225_universe()


def batch_download_jquants(
    token: str,
    lookback_trading_days: int = LOOKBACK_DAYS,
    start: str | None = None,
    end: str | None = None,
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    J-Quants APIから全銘柄の日足OHLCVを取得し {ticker: DataFrame} を返す。

    start/end を指定した場合はその期間、省略時は lookback_trading_days 営業日分。
    tickers は互換性のために残しているが使用しない（日付ベース一括取得）。
    """
    import jpholiday
    from datetime import date as _date, timedelta, datetime

    trading_days: list[str] = []
    if start and end:
        cur  = datetime.strptime(start, "%Y-%m-%d").date()
        end_ = datetime.strptime(end,   "%Y-%m-%d").date()
        while cur <= end_:
            if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                trading_days.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
    else:
        cur = _date.today() - timedelta(days=1)
        while len(trading_days) < lookback_trading_days:
            if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                trading_days.append(cur.strftime("%Y-%m-%d"))
            cur -= timedelta(days=1)
        trading_days.reverse()

    print(f"[jquants] {trading_days[0]} 〜 {trading_days[-1]}"
          f"（{len(trading_days)} 営業日）を取得中...")

    all_records: list[dict] = []
    for i, date_str in enumerate(trading_days):
        pagination_key = None
        while True:
            params: dict = {"date": date_str}
            if pagination_key:
                params["pagination_key"] = pagination_key
            try:
                data       = _jquants_get("/equities/bars/daily", token, params)
                records    = data.get("data", [])
                all_records.extend(records)
                pagination_key = data.get("pagination_key")
                if not pagination_key:
                    break
                time.sleep(1.2)  # ページネーション間も待機
            except Exception as e:
                if "429" in str(e):
                    print(f"  [jquants] レート制限 → 5分待機してリトライ...")
                    time.sleep(300)
                    continue
                print(f"  [jquants] {date_str} 取得失敗: {e}")
                break
        if (i + 1) % 10 == 0:
            print(f"  [jquants] {i+1}/{len(trading_days)} 日完了...")
        time.sleep(1.2)  # 1分60リクエスト制限対応（1.2秒 = 最大50req/分）

    if not all_records:
        print("[jquants] データ取得件数: 0")
        return {}

    print(f"[jquants] 合計 {len(all_records):,} レコード取得完了。DataFrame作成中...")

    df_all = pd.DataFrame(all_records)
    required = {"Code", "Date", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"}
    if not required.issubset(df_all.columns):
        missing = required - set(df_all.columns)
        print(f"[jquants] カラム不足: {missing}")
        return {}

    df_all["Date"] = pd.to_datetime(df_all["Date"])
    df_all = df_all.rename(columns={
        "AdjO": "Open", "AdjH": "High", "AdjL": "Low",
        "AdjC": "Close", "AdjVo": "Volume",
    })

    result: dict[str, pd.DataFrame] = {}
    for code, grp in df_all.groupby("Code"):
        base   = str(code)[:4]
        ticker = base + ".T"
        sub    = grp[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        sub    = sub.set_index("Date").sort_index()
        sub    = sub.apply(pd.to_numeric, errors="coerce")
        sub    = sub.dropna(subset=["Close"])
        if not sub.empty:
            result[ticker] = sub

    print(f"[jquants] {len(result)} 銘柄分のDataFrame作成完了")
    return result


# ================================================================
# stooq によるデータ取得（メイン・認証不要）
# ================================================================

def batch_download_stooq(
    tickers: list[str],
    lookback_days: int = 90,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    stooq.com から直接 CSV で日足データを取得する。
    ticker は '.T' 形式を受け取り、stooq の '.jp' 形式に変換する。
    start/end は "YYYY-MM-DD" 形式。省略時は今日から lookback_days 日前〜今日。
    """
    from datetime import date as _date, timedelta
    if end is None:
        end_date = _date.today()
    else:
        end_date = _date.fromisoformat(end)
    if start is None:
        start_date = end_date - timedelta(days=lookback_days)
    else:
        start_date = _date.fromisoformat(start)
    d1 = start_date.strftime("%Y%m%d")
    d2 = end_date.strftime("%Y%m%d")

    result: dict[str, pd.DataFrame] = {}
    failed: list[str] = []

    for i, ticker in enumerate(tickers):
        code  = ticker.replace(".T", "")
        # インデックス（^始まり）はそのまま、株式は.jpを付ける
        stooq = code if code.startswith("^") else f"{code}.jp"
        url   = f"https://stooq.com/q/d/l/?s={stooq}&d1={d1}&d2={d2}&i=d"
        try:
            r  = requests.get(url, timeout=15, verify=False)
            r.raise_for_status()
            import io
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty or "Close" not in df.columns:
                failed.append(ticker)
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date").sort_index()
            df = df.apply(pd.to_numeric, errors="coerce")
            df = df.dropna(subset=["Close"])
            if not df.empty:
                result[ticker] = df
        except Exception:
            failed.append(ticker)
        if (i + 1) % 10 == 0:
            print(f"  [stooq] {i+1}/{len(tickers)} 銘柄取得済み...")
        time.sleep(0.3)

    if failed:
        print(f"  [stooq] 取得失敗: {len(failed)} 銘柄")
    print(f"[stooq] {len(result)} 銘柄取得完了")
    return result


# ================================================================
# yfinance フォールバック（バックテスト用・手動デバッグ用）
# ================================================================

def _make_yf_session():
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

_SESSION = _make_yf_session()


def batch_download(
    tickers: list[str],
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, pd.DataFrame]:
    """yfinanceによるバッチダウンロード（バックテスト・フォールバック用）。"""
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
# マクロ環境取得（Alpha Vantage で米国市場のみ）
# ================================================================

def _fetch_av_daily_return(symbol: str, api_key: str) -> float | None:
    """Alpha Vantage TIME_SERIES_DAILY から前日比騰落率(%)を返す。"""
    url = "https://www.alphavantage.co/query"
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",
        "apikey":     api_key,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("Time Series (Daily)", {})
        dates = sorted(data.keys(), reverse=True)
        if len(dates) < 2:
            return None
        last = float(data[dates[0]]["4. close"])
        prev = float(data[dates[1]]["4. close"])
        return round((last - prev) / prev * 100, 2)
    except Exception:
        return None


def fetch_macro() -> dict:
    """前日の米国市場（NYダウ・ナスダック）騰落率を取得する。"""
    result = {"dow": None, "nasdaq": None, "bias": "neutral"}
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    if api_key:
        result["dow"]    = _fetch_av_daily_return("DJI",  api_key)
        result["nasdaq"] = _fetch_av_daily_return("IXIC", api_key)
    else:
        print("[macro] ALPHA_VANTAGE_API_KEY 未設定 → マクロ取得スキップ")

    dow = result.get("dow") or 0
    nas = result.get("nasdaq") or 0
    if dow < -1.0 and nas < -1.0:
        result["bias"] = "bearish"
    elif dow > 1.0 and nas > 1.0:
        result["bias"] = "bullish"
    else:
        result["bias"] = "neutral"

    dow_str = f"{dow:+.1f}%" if result["dow"] is not None else "取得不可"
    nas_str = f"{nas:+.1f}%" if result["nasdaq"] is not None else "取得不可"
    print(f"[macro] NYダウ: {dow_str}  ナスダック: {nas_str}  → {result['bias']}")
    return result


# ================================================================
# テクニカル指標
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
    if len(close) < period:
        return None
    ma = float(close.rolling(period).mean().iloc[-1])
    if ma == 0:
        return None
    return round((float(close.iloc[-1]) - ma) / ma * 100, 2)


def calc_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float | None:
    if len(df) < atr_period + 2:
        return None
    high, low  = df["High"], df["Low"]
    prev_close = df["Close"].shift(1)
    tr  = pd.concat([high - low,
                     (high - prev_close).abs(),
                     (low  - prev_close).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(atr_period).mean().iloc[-2])
    if atr == 0:
        return None
    return round((float(high.iloc[-2]) - float(low.iloc[-2])) / atr, 2)


def calc_volume_ratio(df: pd.DataFrame, period: int = VOL_AVG_PERIOD) -> float | None:
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


def calc_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> float | None:
    """ATR（平均真の値幅・絶対値）を返す。"""
    if len(df) < period + 1:
        return None
    high, low  = df["High"], df["Low"]
    prev_close = df["Close"].shift(1)
    tr  = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = float(tr.rolling(period).mean().iloc[-1])
    return atr if atr > 0 else None


def calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> tuple:
    """ボリンジャーバンドの上限・下限を返す。"""
    if len(close) < period:
        return None, None
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = float((sma + std_dev * std).iloc[-1])
    lower = float((sma - std_dev * std).iloc[-1])
    return upper, lower


def calc_trend(close: pd.Series, period: int = 200) -> str | None:
    if len(close) < period:
        return None
    ma  = float(close.rolling(period).mean().iloc[-1])
    cur = float(close.iloc[-1])
    return "UP" if cur > ma else "DOWN" if ma != 0 else None


# ================================================================
# シグナル判定
# ================================================================

def judge_signal_pre(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """逆張り平均回帰戦略でシグナル判定（RSI + 乖離率 + ボリンジャーバンド）。"""
    close = df["Close"].dropna()
    if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD, 20) + 5:
        return None

    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)
    turnover    = calc_turnover(df)
    bb_upper, bb_lower = calc_bollinger(close, std_dev=1.5)

    if any(v is None for v in [rsi, deviation, turnover]):
        return None

    last_close = float(close.iloc[-1])
    last_open  = float(df["Open"].iloc[-1]) if "Open" in df.columns else None

    # ── 高ボラ除外（ATR/終値 > ATR_VOL_CAP%）─────────────
    atr = calc_atr(df)
    if atr is not None and last_close > 0:
        if (atr / last_close * 100) > ATR_VOL_CAP:
            return None

    # ── ①②③ 方向判定（逆張り + ボリンジャーバンド）─────
    if (rsi <= RSI_BUY_MAX and deviation <= DEV_BUY_MAX
            and bb_lower is not None and last_close < bb_lower):
        direction = "BUY"
    elif (rsi >= RSI_SELL_MIN and deviation >= DEV_SELL_MIN
            and bb_upper is not None and last_close > bb_upper):
        direction = "SELL"
    else:
        return None

    # ── 確認足フィルター（反転方向の足が出ていること）────
    if last_open is not None and last_open > 0:
        if direction == "BUY"  and last_close <= last_open:
            return None   # 陰線 = まだ売り圧力が続いている → スキップ
        if direction == "SELL" and last_close >= last_open:
            return None   # 陽線 = まだ買い圧力が続いている → スキップ

    # ── ④ ボラ OR 出来高 ──────────────────────────────
    range_ok = (range_ratio is not None) and (range_ratio >= RANGE_MULT)
    vol_ok   = (vol_ratio   is not None) and (vol_ratio   >= VOL_MULT)
    if not (range_ok or vol_ok):
        return None

    # ── ⑤ 流動性 ──────────────────────────────────────
    if turnover < TURNOVER_MIN:
        return None

    cond4 = []
    if range_ok: cond4.append(f"値幅/ATR={range_ratio:.1f}（≧{RANGE_MULT}）")
    if vol_ok:   cond4.append(f"出来高比={vol_ratio:.1f}（≧{VOL_MULT}）")

    if direction == "BUY":
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（≦{RSI_BUY_MAX}：売られすぎ → 反発狙い）",
            f"25MA乖離率 = {deviation:+.1f}%（≦{DEV_BUY_MAX}%：下がりすぎ）",
            f"BB下限(-1.5σ) = {bb_lower:.0f}（終値{last_close:.0f}が下抜け）",
            "④ " + " / ".join(cond4),
            f"売買代金 = {turnover/1e8:.0f}億円",
        ]
    else:
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（≧{RSI_SELL_MIN}：買われすぎ → 反落狙い）",
            f"25MA乖離率 = {deviation:+.1f}%（≧+{DEV_SELL_MIN}%：上がりすぎ）",
            f"BB上限(+1.5σ) = {bb_upper:.0f}（終値{last_close:.0f}が上抜け）",
            "④ " + " / ".join(cond4),
            f"売買代金 = {turnover/1e8:.0f}億円",
        ]

    return {
        "ticker":      ticker,
        "name":        name,
        "direction":   direction,
        "rsi":         rsi,
        "deviation":   deviation,
        "bb_upper":    bb_upper,
        "bb_lower":    bb_lower,
        "range_ratio": range_ratio,
        "vol_ratio":   vol_ratio,
        "turnover":    turnover,
        "prev_close":  last_close,
        "reason":      reason,
    }


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
# 東証プライム銘柄一覧（JPX公開データから動的取得）
# ================================================================

def fetch_tse_prime_universe() -> list[tuple[str, str]]:
    """JPX公開Excelから東証プライム内国株を取得する。失敗時はフォールバック。"""
    url = ("https://www.jpx.co.jp/markets/statistics-equities/misc/"
           "tvdivq0000001vg2-att/data_j.xls")
    try:
        import io as _io
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        df = pd.read_excel(_io.BytesIO(r.content), dtype=str)
        df.columns = ["date","code","name","market",
                      "s33c","s33","s17c","s17","szc","sz"]
        prime = df[
            df["market"].str.contains("プライム", na=False) &
            ~df["market"].str.contains("外国", na=False)
        ].copy()
        result = []
        for _, row in prime.iterrows():
            code   = str(row["code"]).strip().zfill(4)[:4]
            name   = str(row["name"]).strip()
            ticker = code + ".T"
            result.append((ticker, name))
        print(f"[universe] JPXプライム: {len(result)} 銘柄取得完了")
        return result if result else _nikkei225_universe()
    except Exception as e:
        print(f"[universe] JPX取得失敗: {e} → フォールバック使用")
        return _nikkei225_universe()


# ================================================================
# フォールバック: 日経225ユニバース
# ================================================================

def _nikkei225_universe() -> list[tuple[str, str]]:
    return [
        # === 電気機器・精密 ===
        ("6758.T","ソニーグループ"),("6861.T","キーエンス"),
        ("8035.T","東京エレクトロン"),("6954.T","ファナック"),
        ("6367.T","ダイキン工業"),("6981.T","村田製作所"),
        ("6702.T","富士通"),("7741.T","HOYA"),("6762.T","TDK"),
        ("6971.T","京セラ"),("7733.T","オリンパス"),
        ("6503.T","三菱電機"),("6501.T","日立製作所"),
        ("6902.T","デンソー"),("6723.T","ルネサスエレクトロニクス"),
        ("6857.T","アドバンテスト"),("7735.T","SCREEN HD"),
        ("6594.T","日本電産（ニデック）"),("6645.T","オムロン"),
        ("6752.T","パナソニックHD"),("6753.T","シャープ"),
        ("6770.T","アルプスアルパイン"),("6806.T","ホシデン"),
        ("6841.T","横河電機"),("6963.T","ローム"),
        ("7012.T","川崎重工業"),("7013.T","IHI"),
        ("6268.T","ナブテスコ"),("6273.T","SMC"),
        # === 自動車・輸送機器 ===
        ("7203.T","トヨタ自動車"),("7267.T","本田技研工業"),
        ("7270.T","SUBARU"),("7201.T","日産自動車"),
        ("7011.T","三菱重工業"),("7261.T","マツダ"),
        ("7269.T","スズキ"),("7272.T","ヤマハ発動機"),
        ("5108.T","ブリヂストン"),("7296.T","エフ・シー・シー"),
        # === 情報通信・サービス ===
        ("9984.T","ソフトバンクグループ"),("9983.T","ファーストリテイリング"),
        ("6098.T","リクルートHD"),("9433.T","KDDI"),
        ("9432.T","日本電信電話"),("4689.T","LINEヤフー"),
        ("4755.T","楽天グループ"),("3659.T","ネクソン"),
        ("4751.T","サイバーエージェント"),("4307.T","野村総合研究所"),
        ("4704.T","トレンドマイクロ"),("3673.T","ブロードリーフ"),
        ("9613.T","NTTデータグループ"),("4324.T","電通グループ"),
        ("2432.T","ディー・エヌ・エー"),("3765.T","ガンホー"),
        # === 金融 ===
        ("8306.T","三菱UFJ FG"),("8316.T","三井住友FG"),
        ("8411.T","みずほFG"),("8750.T","第一生命HD"),
        ("8725.T","MS&AD"),("8766.T","東京海上HD"),
        ("8601.T","大和証券グループ"),("8604.T","野村HD"),
        ("8630.T","SOMPOホールディングス"),
        # === 商社・卸売 ===
        ("8058.T","三菱商事"),("8031.T","三井物産"),
        ("8001.T","伊藤忠商事"),("8002.T","丸紅"),
        ("8053.T","住友商事"),("9803.T","三菱食品"),
        # === 医薬品・医療 ===
        ("4568.T","第一三共"),("4519.T","中外製薬"),
        ("4502.T","武田薬品工業"),("4543.T","テルモ"),
        ("4507.T","塩野義製薬"),("4578.T","大塚HD"),
        ("4523.T","エーザイ"),("4536.T","参天製薬"),
        ("4901.T","富士フイルムHD"),("4063.T","信越化学工業"),
        # === 素材・化学 ===
        ("5401.T","日本製鉄"),("4452.T","花王"),
        ("4911.T","資生堂"),("3407.T","旭化成"),
        ("4183.T","三井化学"),("4188.T","三菱ケミカルグループ"),
        ("4208.T","UBE"),("5802.T","住友電気工業"),
        ("5713.T","住友金属鉱山"),("5214.T","日本電気硝子"),
        # === 不動産・建設 ===
        ("8830.T","住友不動産"),("8801.T","三井不動産"),
        ("8802.T","三菱地所"),("1925.T","大和ハウス工業"),
        ("1928.T","積水ハウス"),("1808.T","長谷工コーポレーション"),
        ("1801.T","大成建設"),("1802.T","大林組"),
        ("1803.T","清水建設"),("1812.T","鹿島建設"),
        # === 小売・食品・飲料 ===
        ("3382.T","セブン&アイHD"),("8267.T","イオン"),
        ("2502.T","アサヒグループHD"),("2503.T","キリンHD"),
        ("2914.T","日本たばこ産業"),("2801.T","キッコーマン"),
        ("2802.T","味の素"),("2871.T","ニチレイ"),
        ("2282.T","日本ハム"),("2269.T","明治HD"),
        ("3086.T","J.フロント リテイリング"),("3099.T","三越伊勢丹HD"),
        ("8233.T","高島屋"),("2651.T","ローソン"),
        # === 運輸・インフラ ===
        ("9020.T","東日本旅客鉄道"),("9022.T","東海旅客鉄道"),
        ("9101.T","日本郵船"),("9104.T","商船三井"),
        ("9107.T","川崎汽船"),("9531.T","東京ガス"),
        ("9532.T","大阪ガス"),("9001.T","東武鉄道"),
        ("9005.T","東急"),("9007.T","小田急電鉄"),
        ("9008.T","京王電鉄"),("9009.T","京成電鉄"),
        ("9021.T","西日本旅客鉄道"),("9048.T","名古屋鉄道"),
        ("9064.T","ヤマトHD"),("9147.T","NIPPON EXPRESSHDG"),
        # === 機械・重工 ===
        ("6301.T","小松製作所"),("6326.T","クボタ"),
        ("7751.T","キヤノン"),("7974.T","任天堂"),
        ("6502.T","東芝"),("6113.T","アマダ"),
        ("6201.T","豊田自動織機"),("6361.T","荏原製作所"),
        ("6376.T","日機装"),("7004.T","日立造船"),
    ]


# ================================================================
# メインスクリーニング関数
# ================================================================

def run_screener() -> tuple[list[dict], dict]:
    """スクリーニングを実行し (signals, macro) を返す。"""

    # ── マクロ環境（US市場） ──────────────────────────
    macro = fetch_macro()

    # ── ユニバース取得（東証プライム全銘柄）──────────
    universe = fetch_tse_prime_universe()
    name_map = {t: n for t, n in universe}
    tickers  = [t for t, _ in universe]
    print(f"[screener] ユニバース: {len(tickers)} 銘柄")

    # ── 日足データ取得（yfinance）─────────────────────
    data = batch_download(tickers, period="6mo")
    if not data:
        print("[screener] データ取得失敗 → シグナルなし")
        return [], macro

    # ── 当日データを除外（市場開場中対応）────────────
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")
    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    # ── 決算発表銘柄の除外リスト取得 ─────────────────
    earnings_exclude = fetch_earnings_tickers(days=2)

    # ── 日経225データ取得（市場フィルター用）─────────
    nk_above_ma25 = None  # True=25MA以上, False=以下, None=取得不可
    try:
        kwargs_nk = dict(period="60d", interval="1d", auto_adjust=True, progress=False)
        if _SESSION:
            kwargs_nk["session"] = _SESSION
        nk_raw = yf.download("^N225", **kwargs_nk)
        if len(nk_raw) >= 25:
            nk_close = float(nk_raw["Close"].iloc[-1])
            nk_ma25  = float(nk_raw["Close"].rolling(25).mean().iloc[-1])
            nk_above_ma25 = (nk_close >= nk_ma25)
            direction_str = "25MA以上↑" if nk_above_ma25 else "25MA以下↓"
            print(f"[screener] 日経225: {nk_close:,.0f}円 / 25MA: {nk_ma25:,.0f}円 → {direction_str}")
    except Exception as e:
        print(f"[screener] 日経225取得失敗: {e} → 市場フィルターOFF")

    # ── シグナル判定 ──────────────────────────────────
    candidates: list[dict] = []
    for ticker, df in data.items():
        close = df["Close"].dropna()
        if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD) + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_pre(ticker, name, df)
        if result:
            # 決算除外フィルター
            if ticker in earnings_exclude:
                print(f"  [SKIP] {ticker} 直近決算発表のため除外")
                continue
            # 日経フィルター: 25MA割れのときはBUYシグナルを出さない
            if result["direction"] == "BUY" and nk_above_ma25 is False:
                continue
            candidates.append(result)
            print(f"  [HIT] [{ticker}] {name} -> {result['direction']} "
                  f"RSI={result['rsi']} deviation={result['deviation']:+.1f}% "
                  f"turnover={result['turnover']/1e8:.0f}oku")

    # ── マクロバイアス（参考表示のみ・シグナル絞り込みは行わない）──
    bias = macro.get("bias", "neutral")
    print(f"[screener] マクロバイアス: {bias}（参考）")

    # ── 既存ポジションの銘柄を除外 ───────────────────
    import json as _json
    try:
        with open("positions.json", encoding="utf-8") as _f:
            _positions = _json.load(_f)
        open_tickers = {p["ticker"] for p in _positions if p.get("status") in ("pending", "open")}
        if open_tickers:
            before = len(candidates)
            candidates = [c for c in candidates if c["ticker"] not in open_tickers]
            print(f"[screener] 保有中銘柄を除外: {before - len(candidates)}件 {open_tickers}")
    except Exception:
        pass

    # ── 流動性降順ソート → 上位MAX_SIGNALS銘柄 ──────
    candidates.sort(key=lambda x: x["turnover"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro

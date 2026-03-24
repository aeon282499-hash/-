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

RSI_BUY_MIN    = 60
RSI_BUY_MAX    = 79
DEV_BUY_MIN    = 2.0

RSI_SELL_MIN   = 80
DEV_SELL_MIN   = 5.0

RANGE_MULT     = 1.5
VOL_MULT       = 1.5
TURNOVER_MIN   = 3_000_000_000   # 30億円

MAX_SIGNALS    = 3
BATCH_SIZE     = 100   # yfinanceフォールバック用

_JQUANTS_BASE  = "https://api.jquants.com/v1"


# ================================================================
# J-Quants API 認証・データ取得
# ================================================================

def _jquants_id_token() -> str:
    """Refresh Token → ID Token を取得する。"""
    refresh = os.getenv("JQUANTS_REFRESH_TOKEN", "").strip()
    if not refresh:
        raise ValueError("JQUANTS_REFRESH_TOKEN が未設定です")
    resp = requests.post(
        f"{_JQUANTS_BASE}/token/auth_refresh",
        params={"refreshToken": refresh},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("idToken")
    if not token:
        raise ValueError(f"idToken取得失敗: {resp.text[:200]}")
    return token


def _jquants_get(path: str, token: str, params: dict | None = None) -> dict:
    resp = requests.get(
        f"{_JQUANTS_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


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
) -> dict[str, pd.DataFrame]:
    """
    J-Quants APIから全銘柄の日足OHLCVを取得し {ticker: DataFrame} を返す。
    日付ごとに全銘柄を一括取得するため API コール数 = 営業日数のみ。
    """
    import jpholiday
    from datetime import date as _date, timedelta

    # 取得する営業日リストを生成
    trading_days: list[str] = []
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
                data       = _jquants_get("/prices/daily_quotes", token, params)
                records    = data.get("daily_quotes", [])
                all_records.extend(records)
                pagination_key = data.get("pagination_key")
                if not pagination_key:
                    break
            except Exception as e:
                print(f"  [jquants] {date_str} 取得失敗: {e}")
                break
        if (i + 1) % 10 == 0:
            print(f"  [jquants] {i+1}/{len(trading_days)} 日完了...")
        time.sleep(0.2)

    if not all_records:
        print("[jquants] データ取得件数: 0")
        return {}

    print(f"[jquants] 合計 {len(all_records):,} レコード取得完了。DataFrame作成中...")

    df_all = pd.DataFrame(all_records)
    required = {"Code", "Date", "Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(df_all.columns):
        missing = required - set(df_all.columns)
        print(f"[jquants] カラム不足: {missing}")
        return {}

    df_all["Date"] = pd.to_datetime(df_all["Date"])

    # 修正後終値があれば使用する（株式分割対応）
    for col in ["Open", "High", "Low", "Close"]:
        adj_col = f"Adjustment{col}"
        if adj_col in df_all.columns:
            df_all[col] = df_all[adj_col]

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
# マクロ環境取得（yfinance で米国市場のみ）
# ================================================================

def fetch_macro() -> dict:
    """前日の米国市場（NYダウ・ナスダック）騰落率を取得する。"""
    result = {"dow": None, "nasdaq": None, "bias": "neutral"}
    for key, ticker in {"dow": "^DJI", "nasdaq": "^IXIC"}.items():
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
    """RSI(14)モメンタム戦略でシグナル判定。"""
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

    # ── ①② 方向判定 ─────────────────────────────────
    if RSI_BUY_MIN <= rsi <= RSI_BUY_MAX and deviation >= DEV_BUY_MIN:
        direction = "BUY"
    elif rsi >= RSI_SELL_MIN and deviation >= DEV_SELL_MIN:
        direction = "SELL"
    else:
        return None

    # ── ③ ボラ OR 出来高 ──────────────────────────────
    range_ok = (range_ratio is not None) and (range_ratio >= RANGE_MULT)
    vol_ok   = (vol_ratio   is not None) and (vol_ratio   >= VOL_MULT)
    if not (range_ok or vol_ok):
        return None

    # ── ④ 流動性 ──────────────────────────────────────
    if turnover < TURNOVER_MIN:
        return None

    cond3 = []
    if range_ok: cond3.append(f"値幅/ATR={range_ratio:.1f}（≧{RANGE_MULT}）")
    if vol_ok:   cond3.append(f"出来高比={vol_ratio:.1f}（≧{VOL_MULT}）")

    if direction == "BUY":
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（{RSI_BUY_MIN}〜{RSI_BUY_MAX}：強い上昇モメンタム）",
            f"25MA乖離率 = {deviation:+.1f}%（≧+{DEV_BUY_MIN}%：上昇トレンド確認）",
            "③ " + " / ".join(cond3),
            f"売買代金 = {turnover/1e8:.0f}億円",
        ]
    else:
        reason = [
            f"RSI({RSI_PERIOD}) = {rsi}（≧{RSI_SELL_MIN}：異常過熱）",
            f"25MA乖離率 = {deviation:+.1f}%（≧+{DEV_SELL_MIN}%：極端な上方乖離）",
            "③ " + " / ".join(cond3),
            f"売買代金 = {turnover/1e8:.0f}億円",
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
        "prev_close":  float(close.iloc[-1]),
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
# フォールバック: 日経225ユニバース
# ================================================================

def _nikkei225_universe() -> list[tuple[str, str]]:
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
        ("9432.T","日本電信電話"),("7751.T","キヤノン"),
        ("7733.T","オリンパス"),("6702.T","富士通"),
        ("4901.T","富士フイルムHD"),("7741.T","HOYA"),
        ("4578.T","大塚HD"),("6762.T","TDK"),
        ("6971.T","京セラ"),("5108.T","ブリヂストン"),
        ("7270.T","SUBARU"),("7201.T","日産自動車"),
        ("6301.T","小松製作所"),("6326.T","クボタ"),
        ("3382.T","セブン&アイHD"),("8267.T","イオン"),
        ("2502.T","アサヒグループHD"),("2503.T","キリンHD"),
        ("8830.T","住友不動産"),("8801.T","三井不動産"),
        ("8802.T","三菱地所"),("9101.T","日本郵船"),
        ("9104.T","商船三井"),("9107.T","川崎汽船"),
        ("1925.T","大和ハウス工業"),("1928.T","積水ハウス"),
        ("4452.T","花王"),("4911.T","資生堂"),
        ("4543.T","テルモ"),("4507.T","塩野義製薬"),
        ("8750.T","第一生命HD"),("8725.T","MS&AD"),
        ("9531.T","東京ガス"),("9532.T","大阪ガス"),
        ("4689.T","LINEヤフー"),("4755.T","楽天グループ"),
        ("3659.T","ネクソン"),("4751.T","サイバーエージェント"),
    ]


# ================================================================
# メインスクリーニング関数
# ================================================================

def run_screener() -> tuple[list[dict], dict]:
    """スクリーニングを実行し (signals, macro) を返す。"""

    # ── マクロ環境（US市場） ──────────────────────────
    macro = fetch_macro()

    # ── J-Quants 認証 ─────────────────────────────────
    try:
        token = _jquants_id_token()
        print("[jquants] 認証成功")
    except Exception as e:
        print(f"[jquants] 認証失敗: {e}")
        return [], macro

    # ── ユニバース取得 ────────────────────────────────
    universe = fetch_tse_universe(token)
    name_map = {t: n for t, n in universe}

    # ── 日足データ取得（J-Quants）──────────────────────
    data = batch_download_jquants(token, lookback_trading_days=LOOKBACK_DAYS)
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

    # ── シグナル判定 ──────────────────────────────────
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

    # ── マクロバイアスで絞り込み ──────────────────────
    bias = macro.get("bias", "neutral")
    if bias == "bearish":
        candidates = [c for c in candidates if c["direction"] == "SELL"]
        print("[screener] 米国株安 → 買いシグナル破棄")
    elif bias == "bullish":
        candidates = [c for c in candidates if c["direction"] == "BUY"]
        print("[screener] 米国株高 → 売りシグナル破棄")

    # ── 流動性降順ソート → 上位MAX_SIGNALS銘柄 ──────
    candidates.sort(key=lambda x: x["turnover"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro

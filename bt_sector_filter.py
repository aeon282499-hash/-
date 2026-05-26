"""
bt_sector_filter.py — Phase 1: セクター流入フィルタBT（フィルタ on/off 比較）

bt_buy_grid_fast.py のロジックをベースに、現状本番ロジック（固定パラメータ）で
セクターフィルタ（東証33業種・5日平均リターン上位 1/3）の有無を比較する。

ベースライン期待値（memoryより・TPグリッドBT 2026-05-24）:
  TP=5.0 / RSI<=45 / DEV<=-1.5 / VOL>=2.0 / ATR<=3.0 / TO>=20億 → PF1.24 / +640% / 3097件

セクター判定は trade_date の「前営業日」ランキングを使用 (look-ahead bias 防止)。
"""
import math
import pickle
import time
from datetime import datetime as dt, timedelta
from pathlib import Path

import jpholiday
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from screener import is_etf_ticker
from sector_filter import fetch_sector33_map, build_sector_ranking, is_ticker_in_top_sector

CACHE_FILE = Path("jquants_cache.pkl")
START = "2022-01-01"
END = "2026-05-02"

# 現状本番設定（screener.py と整合）
MAX_HOLD = 3
STOP_LOSS = 3.0
TAKE_PROFIT = 5.0
MAX_SIGNALS = 5
RANGE_MULT = 1.5
RSI_MAX = 45
DEV_MAX = -1.5
VOL_MULT = 2.0
ATR_CAP = 3.0  # 買い: 3.0
TURNOVER_MIN = 2_000_000_000  # 20億

RSI_PERIOD = 14
MA_DEV_PERIOD = 25
ATR_PERIOD = 14
VOL_AVG_PERIOD = 20

# セクターフィルタ設定
SECTOR_WINDOW = 5
SECTOR_TOP_FRAC = 1 / 3


def precompute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = (100 - 100 / (1 + rs)).round(2)

    ma25 = close.rolling(MA_DEV_PERIOD).mean()
    df["DEV"] = ((close - ma25) / ma25 * 100).round(2)

    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    atr = tr.rolling(ATR_PERIOD).mean()
    df["ATR_PCT"] = atr / close * 100

    range_today = (high - low) / atr
    df["RANGE_RATIO_PREV"] = range_today.shift(1)

    vol_avg_prev20 = vol.shift(2).rolling(VOL_AVG_PERIOD).mean()
    df["VOL_RATIO_PREV"] = vol.shift(1) / vol_avg_prev20

    df["TURNOVER_PREV"] = close.shift(1) * vol.shift(1)
    df["DateStr"] = df.index.strftime("%Y-%m-%d")
    return df


def build_long_df(all_data: dict) -> pd.DataFrame:
    rows = []
    for ticker, df in all_data.items():
        if len(df) < 50:
            continue
        if is_etf_ticker(ticker, ticker):
            continue
        d = precompute_indicators(df)
        d["Ticker"] = ticker
        rows.append(
            d[
                [
                    "DateStr", "Ticker", "Open", "High", "Low", "Close", "Volume",
                    "RSI", "DEV", "ATR_PCT", "RANGE_RATIO_PREV", "VOL_RATIO_PREV",
                    "TURNOVER_PREV",
                ]
            ]
        )
    return pd.concat(rows, ignore_index=True)


def buy_score(rsi: float, dev: float, turnover: float) -> float:
    rsi_score = 1.0 / (1.0 + ((rsi - 38.0) / 8.0) ** 2)
    dev_score = 1.0 / (1.0 + ((dev + 3.0) / 2.0) ** 2)
    turn_score = math.log10(max(turnover, 1) / 1e9 + 1.0) / 3.0
    return rsi_score * 0.30 + dev_score * 0.30 + turn_score * 0.40


def trading_days_between(start: str, end: str) -> list[str]:
    cur, end_d = dt.strptime(start, "%Y-%m-%d").date(), dt.strptime(end, "%Y-%m-%d").date()
    out = []
    while cur <= end_d:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def simulate(
    df_long: pd.DataFrame,
    all_data: dict,
    all_trading_days: list[str],
    *,
    sector_filter_on: bool,
    sector_map: dict[str, str],
    sector_ranking: dict[str, set[str]],
) -> list[dict]:
    """シミュレーション本体。sector_filter_on=True ならセクター上位 1/3 のみ通過。"""
    base = df_long[
        (df_long["RSI"] <= RSI_MAX)
        & (df_long["DEV"] <= DEV_MAX)
        & (df_long["ATR_PCT"] <= ATR_CAP)
        & (df_long["TURNOVER_PREV"] >= TURNOVER_MIN)
        & (
            (df_long["RANGE_RATIO_PREV"] >= RANGE_MULT)
            | (df_long["VOL_RATIO_PREV"] >= VOL_MULT)
        )
    ].copy()

    base["score"] = base.apply(
        lambda r: buy_score(r["RSI"], r["DEV"], r["TURNOVER_PREV"]), axis=1
    )
    base = base.sort_values(["DateStr", "score"], ascending=[True, False])

    # セクターフィルタ: sig_date の翌営業日が entry_date なので、
    # シグナル判定そのものは sig_date 時点で行われる。
    # 「sig_date の前営業日ランキング」を使うと、判定時に既に確定している直近5日リターンで判断できる。
    if sector_filter_on:
        # 各 sig_date における通過チェック
        def _pass(row) -> bool:
            return is_ticker_in_top_sector(
                row["Ticker"], row["DateStr"], sector_map, sector_ranking, all_trading_days
            )

        mask = base.apply(_pass, axis=1)
        base = base[mask]

    candidates_by_sig: dict[str, list[str]] = {}
    for sig_date, grp in base.groupby("DateStr"):
        candidates_by_sig[sig_date] = list(grp["Ticker"].head(MAX_SIGNALS))

    trades = []
    open_until: dict[str, str] = {}
    atd_index = {d: i for i, d in enumerate(all_trading_days)}

    for sig_date in sorted(candidates_by_sig.keys()):
        if sig_date not in atd_index:
            continue
        i = atd_index[sig_date]
        if i + 1 >= len(all_trading_days):
            continue
        trade_date = all_trading_days[i + 1]
        if trade_date < START or trade_date > END:
            continue

        for ticker in candidates_by_sig[sig_date]:
            if ticker in open_until and open_until[ticker] >= trade_date:
                continue
            full_df = all_data.get(ticker)
            if full_df is None:
                continue
            entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == trade_date]
            if entry_rows.empty:
                continue
            entry_open = float(entry_rows["Open"].iloc[0])
            if entry_open <= 0 or np.isnan(entry_open):
                continue

            stop_price = entry_open * (1 - STOP_LOSS / 100)
            tp_price = entry_open * (1 + TAKE_PROFIT / 100)

            pnl_pct = exit_date = exit_type = None
            for hold_day in range(1, MAX_HOLD + 1):
                day_idx = atd_index[trade_date] + (hold_day - 1)
                if day_idx >= len(all_trading_days):
                    break
                check_date = all_trading_days[day_idx]
                day_rows = full_df[full_df.index.strftime("%Y-%m-%d") == check_date]
                if day_rows.empty:
                    continue
                day_open = float(day_rows["Open"].iloc[0])
                day_high = float(day_rows["High"].iloc[0])
                day_low = float(day_rows["Low"].iloc[0])
                day_close = float(day_rows["Close"].iloc[0])
                if any(v <= 0 or np.isnan(v) for v in [day_open, day_high, day_low, day_close]):
                    continue
                if day_low <= stop_price:
                    pnl_pct, exit_date, exit_type = -STOP_LOSS, check_date, "STOP"
                    break
                if day_high >= tp_price:
                    pnl_pct, exit_date, exit_type = +TAKE_PROFIT, check_date, "TP"
                    break
                pre = full_df.loc[full_df.index.strftime("%Y-%m-%d") <= check_date]
                cl = pre["Close"].dropna()
                if len(cl) >= RSI_PERIOD + 1:
                    delta = cl.diff()
                    g = delta.clip(lower=0)
                    l = -delta.clip(upper=0)
                    ag = g.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
                    al = l.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
                    rs = ag / al.replace(0, np.nan)
                    rsi_now = (100 - 100 / (1 + rs)).iloc[-1]
                    rsi_exit = pd.notna(rsi_now) and rsi_now >= 50
                else:
                    rsi_exit = False
                if rsi_exit or hold_day == MAX_HOLD:
                    pnl_pct = (day_close - entry_open) / entry_open * 100
                    exit_date = check_date
                    exit_type = "RSI" if rsi_exit else "MAXHOLD"
                    break
            if pnl_pct is None:
                continue
            trades.append(
                {
                    "signal_date": sig_date,
                    "entry_date": trade_date,
                    "exit_date": exit_date,
                    "ticker": ticker,
                    "pnl_pct": pnl_pct,
                    "exit_type": exit_type,
                }
            )
            open_until[ticker] = exit_date

    return trades


def summarize(trades: list[dict], label: str) -> dict:
    if not trades:
        return {"label": label, "count": 0, "wr": 0.0, "pf": 0.0, "cum": 0.0,
                "avg": 0.0, "maxdd": 0.0}
    n = len(trades)
    pnls = [t["pnl_pct"] for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    gw = sum(p for p in pnls if p > 0)
    gl = -sum(p for p in pnls if p < 0)
    pf = gw / gl if gl > 0 else float("inf")
    # 累積%とDD
    df = pd.DataFrame(trades).sort_values("entry_date")
    cum = df["pnl_pct"].cumsum()
    maxdd = (cum - cum.cummax()).min()
    return {
        "label": label,
        "count": n,
        "wr": wins / n * 100,
        "pf": pf,
        "cum": sum(pnls),
        "avg": sum(pnls) / n,
        "maxdd": maxdd,
    }


def yearly_breakdown(trades: list[dict]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()
    df = pd.DataFrame(trades)
    df["year"] = df["exit_date"].str[:4]
    out_rows = []
    for yr, g in df.groupby("year"):
        pnls = g["pnl_pct"].tolist()
        wins = sum(1 for p in pnls if p > 0)
        gw = sum(p for p in pnls if p > 0)
        gl = -sum(p for p in pnls if p < 0)
        pf = gw / gl if gl > 0 else float("inf")
        out_rows.append(
            {
                "year": yr,
                "count": len(g),
                "win_rate": wins / len(g) * 100,
                "pf": pf,
                "cum_pct": sum(pnls),
            }
        )
    return pd.DataFrame(out_rows).sort_values("year").reset_index(drop=True)


def main():
    if not CACHE_FILE.exists():
        print(f"[bt_sector] {CACHE_FILE} がありません。cache_jquants.py を先に実行してください")
        return

    print(f"[bt_sector] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    print(f"[bt_sector] all_data: {len(all_data)} 銘柄  ({time.time()-t0:.0f}s)")

    print(f"[bt_sector] セクターマップ取得...")
    sector_map = fetch_sector33_map()

    print(f"[bt_sector] セクターランキング構築...")
    sector_ranking, sw_df = build_sector_ranking(
        all_data, sector_map, window=SECTOR_WINDOW, top_frac=SECTOR_TOP_FRAC
    )

    print(f"[bt_sector] 指標事前計算...")
    t_pre = time.time()
    df_long = build_long_df(all_data)
    print(f"[bt_sector] long DF: {len(df_long):,} 行 ({time.time()-t_pre:.0f}s)")

    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    print(f"\n[bt_sector] ===== Pass A: フィルタOFF (ベースライン) =====")
    t1 = time.time()
    trades_off = simulate(
        df_long, all_data, all_trading_days,
        sector_filter_on=False, sector_map=sector_map, sector_ranking=sector_ranking,
    )
    print(f"[bt_sector] OFF 完了: {len(trades_off)}件 ({time.time()-t1:.0f}s)")

    print(f"\n[bt_sector] ===== Pass B: フィルタON (33業種・5日平均・上位1/3) =====")
    t2 = time.time()
    trades_on = simulate(
        df_long, all_data, all_trading_days,
        sector_filter_on=True, sector_map=sector_map, sector_ranking=sector_ranking,
    )
    print(f"[bt_sector] ON 完了: {len(trades_on)}件 ({time.time()-t2:.0f}s)")

    s_off = summarize(trades_off, "フィルタなし")
    s_on = summarize(trades_on, "フィルタあり")

    print(f"\n{'='*72}")
    print(f"  Phase 1 セクターフィルタ比較 ({START} 〜 {END})")
    print(f"{'='*72}")
    header = f"  {'pattern':<14} {'件数':>6} {'勝率':>7} {'PF':>6} {'累積%':>9} {'平均':>8} {'MaxDD':>9}"
    print(header)
    print(f"  {'-'*70}")
    for s in (s_off, s_on):
        print(
            f"  {s['label']:<14} {s['count']:>6} {s['wr']:>6.1f}% {s['pf']:>6.2f} "
            f"{s['cum']:>+8.1f}% {s['avg']:>+7.3f}% {s['maxdd']:>+8.1f}%"
        )
    print(f"  {'-'*70}")
    if s_off["count"] > 0:
        diff_pf = s_on["pf"] - s_off["pf"]
        diff_cum = s_on["cum"] - s_off["cum"]
        ret_filter = s_on["count"] / s_off["count"] * 100 if s_off["count"] else 0
        print(f"  Δ PF: {diff_pf:+.3f} / Δ 累積%: {diff_cum:+.1f}pt / 残存率: {ret_filter:.1f}%")
    print(f"{'='*72}\n")

    # 年別比較
    print(f"{'='*72}")
    print(f"  年別比較")
    print(f"{'='*72}")
    yr_off = yearly_breakdown(trades_off).rename(
        columns={"count": "off_n", "win_rate": "off_wr", "pf": "off_pf", "cum_pct": "off_cum"}
    )
    yr_on = yearly_breakdown(trades_on).rename(
        columns={"count": "on_n", "win_rate": "on_wr", "pf": "on_pf", "cum_pct": "on_cum"}
    )
    if not yr_off.empty and not yr_on.empty:
        merged = yr_off.merge(yr_on, on="year", how="outer").fillna(0)
        print(merged.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # 保存
    pd.DataFrame(trades_off).to_csv("bt_sector_off.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(trades_on).to_csv("bt_sector_on.csv", index=False, encoding="utf-8-sig")
    print(f"\n[bt_sector] CSV: bt_sector_off.csv / bt_sector_on.csv")
    print(f"[bt_sector] 総経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()

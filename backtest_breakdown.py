"""
backtest_breakdown.py - 売り側ブレイクダウン専用バックテスト
============================================================

【戦略】 信用売り（空売り）
  エントリー: 前日大引け後シグナル確認 → 上位銘柄を翌日寄成売り
  エグジット: 同日大引け成買い戻し（1日完結、損切り・利確なし）

【シグナル条件】 以下3条件を全て満たす銘柄
  A: 当日終値が過去20日の最安値を更新（終値 <= 過去20日の最安値）
  B: 当日出来高が20日平均出来高の VOL_RATIO_MIN 倍以上
  C: 当日終値が日中レンジの下位30%以内
     (Close - Low) / (High - Low) <= 1 - CLOSE_RANGE_MIN

【選択】 シグナル銘柄を出来高比率の高い順に最大 MAX_CONCURRENT 銘柄

【ポジション管理】
  - 1銘柄あたり 100万円
  - 同時保有 最大3銘柄
  - 初期資金 300万円

【フィルター】
  - 日経225が25日MA割れの日のみ実行（地合い：下落トレンド）
  - 流動性: 平均出来高 < 10万株 を除外
  - 価格: 300円未満を除外
  - ボラ: ATR/終値 > 3% を除外

【データ】
  - jquants_cache.pkl からキャッシュデータを読み込み（SSL回避）

【コスト】
  - 往復0.2%

【使い方】
  python backtest_breakdown.py 2023-01-01 2025-12-31
"""

import sys
import pickle
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np

from screener import calc_atr

# ============== 資金管理 ==============
INITIAL_CAPITAL    = 3_000_000
POSITION_SIZE      = 1_000_000
MAX_CONCURRENT     = 3
COMMISSION_RATE    = 0.002

# ============== フィルター ==============
MIN_PRICE          = 300
MIN_AVG_VOLUME     = 100_000
ATR_VOL_CAP        = 3.0
USE_MARKET_FILTER  = True   # True: 日経25MA割れの日のみ売り
INVERT_MARKET      = True   # SELL側は下げトレンドで実行（25MA割れ）

# ============== ブレイクダウン条件 ==============
BREAKDOWN_DAYS     = 20
VOL_RATIO_MIN      = 1.5
CLOSE_RANGE_MAX    = 0.3   # 終値が日中レンジ下位30%以内
LOOKBACK_DAYS      = 60

CACHE_FILE = Path("jquants_cache.pkl")


def get_trading_days(start: str, end: str) -> list[str]:
    days = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def calc_breakdown_signal(pre_df):
    """ブレイクダウン条件判定（売り）。"""
    if len(pre_df) < BREAKDOWN_DAYS + 1:
        return None

    close  = pre_df["Close"]
    volume = pre_df["Volume"]
    high   = pre_df["High"]
    low    = pre_df["Low"]

    last_close  = float(close.iloc[-1])
    last_high   = float(high.iloc[-1])
    last_low    = float(low.iloc[-1])
    last_volume = float(volume.iloc[-1])

    if last_close < MIN_PRICE:
        return None

    vol_avg_20 = float(volume.iloc[:-1].tail(BREAKDOWN_DAYS).mean())
    if vol_avg_20 < MIN_AVG_VOLUME:
        return None

    atr = calc_atr(pre_df)
    if atr is None or last_close == 0:
        return None
    atr_pct = atr / last_close * 100
    if atr_pct > ATR_VOL_CAP:
        return None

    # 条件A: 当日終値が過去20日の最安値を割る
    low_20 = float(low.iloc[:-1].tail(BREAKDOWN_DAYS).min())
    cond_a = last_close <= low_20

    # 条件B: 当日出来高が20日平均の VOL_RATIO_MIN 倍以上
    vol_ratio = last_volume / vol_avg_20 if vol_avg_20 > 0 else 0.0
    cond_b = vol_ratio >= VOL_RATIO_MIN

    # 条件C: 終値が日中レンジの下位30%以内
    day_range = last_high - last_low
    if day_range == 0:
        cond_c = False
    else:
        close_position = (last_close - last_low) / day_range
        cond_c = close_position <= CLOSE_RANGE_MAX

    if not (cond_a and cond_b and cond_c):
        return None

    return {
        "vol_ratio": vol_ratio,
        "atr_pct":   atr_pct,
        "low_20":    low_20,
    }


def load_cache():
    if not CACHE_FILE.exists():
        raise FileNotFoundError(f"{CACHE_FILE} が見つかりません")
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache.get("all_data", {})
    name_map = cache.get("name_map", {})
    print(f"[Cache] {len(all_data)} 銘柄を読み込み（{cache.get('start')} 〜 {cache.get('end')}）")
    return all_data, name_map


def run_backtest(start, end):
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  ブレイクダウン(売り) BT 期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"  初期資金: {INITIAL_CAPITAL:,}円")
    print(f"  1銘柄サイズ: {POSITION_SIZE:,}円 x 最大{MAX_CONCURRENT}銘柄")
    print(f"  往復コスト: {COMMISSION_RATE*100:.2f}%")
    print(f"  戦略: 寄成売り → 引成買戻し（1日完結）")
    print(f"{'='*60}\n")

    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    fetch_end   = (datetime.strptime(end,   "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")

    all_data, name_map = load_cache()

    nk_df = all_data.get("1321.T")
    if nk_df is not None and len(nk_df) > 25:
        nk_df = nk_df.copy()
        nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
        print(f"[BD] 日経ETF(1321.T) {len(nk_df)}日分")
    else:
        nk_df = None
        print("[BD] 日経25日MAデータなし → 地合いフィルターOFF")

    all_trading_days = get_trading_days(fetch_start, fetch_end)

    capital = INITIAL_CAPITAL
    trades = []
    equity_curve = []

    for trade_date in trading_days:
        # 地合いチェック（SELL側は25MA割れの日に実行）
        market_ok = True
        if USE_MARKET_FILTER and nk_df is not None:
            nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") < trade_date]
            if len(nk_rows) >= 25:
                nk_close = float(nk_rows["Close"].iloc[-1])
                nk_ma25  = float(nk_rows["MA25"].iloc[-1])
                if not np.isnan(nk_ma25):
                    if INVERT_MARKET:
                        market_ok = (nk_close < nk_ma25)
                    else:
                        market_ok = (nk_close >= nk_ma25)

        if not market_ok:
            equity_curve.append({"date": trade_date, "capital": capital, "trades": 0, "day_pnl": 0, "market": "NG"})
            continue

        if trade_date not in all_trading_days:
            continue
        idx = all_trading_days.index(trade_date)
        if idx + 1 >= len(all_trading_days):
            continue
        entry_date = all_trading_days[idx + 1]

        candidates = []
        for ticker, full_df in all_data.items():
            try:
                pre_df = full_df[full_df.index.strftime("%Y-%m-%d") <= trade_date].copy()
                if len(pre_df) < BREAKDOWN_DAYS + 1:
                    continue
                if pre_df.index[-1].strftime("%Y-%m-%d") != trade_date:
                    continue

                signal = calc_breakdown_signal(pre_df)
                if signal is None:
                    continue

                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue
                entry_open  = float(entry_rows["Open"].iloc[0])
                entry_close = float(entry_rows["Close"].iloc[0])
                if entry_open <= 0 or entry_close <= 0 or np.isnan(entry_open) or np.isnan(entry_close):
                    continue

                candidates.append({
                    "ticker":      ticker,
                    "name":        name_map.get(ticker, ticker),
                    "vol_ratio":   signal["vol_ratio"],
                    "entry_open":  entry_open,
                    "entry_close": entry_close,
                    "atr_pct":     signal["atr_pct"],
                    "low_20":      signal["low_20"],
                })
            except Exception:
                continue

        candidates.sort(key=lambda x: -x["vol_ratio"])
        picks = candidates[:MAX_CONCURRENT]

        day_trades_pnl = 0
        day_trade_count = 0
        for pick in picks:
            if capital < POSITION_SIZE:
                break

            entry_open  = pick["entry_open"]
            entry_close = pick["entry_close"]
            # 売り: 寄りで売って引けで買い戻す → (open - close)/open が利益
            gross_return = (entry_open - entry_close) / entry_open
            net_return   = gross_return - COMMISSION_RATE

            position_pnl = POSITION_SIZE * net_return
            capital += position_pnl
            day_trades_pnl += position_pnl
            day_trade_count += 1

            # gap_pct: 翌朝寄りが20日安値からどれだけ下げてるか（負値ほど大きいギャップダウン）
            gap_pct = (entry_open - pick["low_20"]) / pick["low_20"] * 100

            trades.append({
                "signal_date":   trade_date,
                "entry_date":    entry_date,
                "ticker":        pick["ticker"],
                "name":          pick["name"],
                "vol_ratio":     round(pick["vol_ratio"], 2),
                "gap_pct":       round(gap_pct, 2),
                "entry_open":    round(entry_open, 1),
                "entry_close":   round(entry_close, 1),
                "gross_ret_%":   round(gross_return * 100, 3),
                "net_ret_%":     round(net_return * 100, 3),
                "pnl_jpy":       round(position_pnl),
                "capital_after": round(capital),
                "win":           net_return > 0,
                "atr_pct":       round(pick["atr_pct"], 2),
                "low_20":        round(pick["low_20"], 1),
            })

        equity_curve.append({
            "date":    entry_date,
            "capital": round(capital),
            "trades":  day_trade_count,
            "day_pnl": round(day_trades_pnl),
            "market":  "OK",
        })

    _print_results(trades, equity_curve, start, end)


def _print_results(trades, equity_curve, start, end):
    print(f"\n{'='*60}")
    print(f"  BT結果 ({start} 〜 {end})  [ブレイクダウン(売り)]")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件")
        return

    df_trades = pd.DataFrame(trades)
    df_equity = pd.DataFrame(equity_curve)

    total       = len(df_trades)
    wins        = df_trades["win"].sum()
    losses      = total - wins
    win_rate    = wins / total * 100
    avg_pnl_pct = df_trades["net_ret_%"].mean()
    total_pnl   = df_trades["pnl_jpy"].sum()

    gross_profit = df_trades[df_trades["win"]]["pnl_jpy"].sum()
    gross_loss   = abs(df_trades[~df_trades["win"]]["pnl_jpy"].sum())
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    equity_series = df_equity["capital"]
    peak = equity_series.cummax()
    drawdown_pct = (equity_series - peak) / peak * 100
    max_dd_pct = drawdown_pct.min()

    final_capital = equity_series.iloc[-1]
    total_return_pct = (final_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100

    print(f"\n  最終資金 : {final_capital:,}円 ({total_return_pct:+.2f}%)")
    print(f"  最大DD   : {max_dd_pct:+.2f}%")
    print(f"  取引数   : {total} (勝{wins}/負{losses})  勝率{win_rate:.1f}%")
    print(f"  平均損益 : {avg_pnl_pct:+.3f}%")
    print(f"  PF       : {pf:.2f}")
    print(f"  総損益   : {total_pnl:+,.0f}円")

    print(f"\n  年別:")
    df_trades["year"] = df_trades["entry_date"].str[:4]
    for yr in sorted(df_trades["year"].unique()):
        sub = df_trades[df_trades["year"] == yr]
        n   = len(sub)
        wr  = sub["win"].sum() / n * 100
        avg = sub["net_ret_%"].mean()
        gp  = sub[sub["win"]]["pnl_jpy"].sum()
        gl  = abs(sub[~sub["win"]]["pnl_jpy"].sum())
        pf_y = gp / gl if gl > 0 else float("inf")
        pnl_y = sub["pnl_jpy"].sum()
        print(f"    {yr}: n={n} wr={wr:.1f}% avg={avg:+.3f}% PF={pf_y:.2f} pnl={pnl_y:+,.0f}")

    out_trades = f"breakdown_trades_{start}_{end}.csv"
    df_trades.to_csv(out_trades, index=False, encoding="utf-8-sig")
    print(f"\n  保存: {out_trades}")

    out_equity = f"breakdown_equity_{start}_{end}.csv"
    df_equity.to_csv(out_equity, index=False, encoding="utf-8-sig")
    print(f"  保存: {out_equity}\n")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) >= 2:
        s, e = args[0], args[1]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e}")

    run_backtest(s, e)

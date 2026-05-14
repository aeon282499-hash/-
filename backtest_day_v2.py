"""
backtest_day_v2.py - データドリブン改良版デイトレBT
======================================================

【戦略】
  エントリー: 前日大引け後シグナル確認 → 翌朝寄り値でギャップ条件チェック → 寄成買い
  エグジット: 同日大引け成売り（1日完結、損切り・利確なし）

【シグナル条件】 以下4条件を全て満たす銘柄
  A: 当日終値が直近20日高値を更新（終値 >= 過去20日の最高値）
  B: 当日出来高が20日平均出来高の VOL_RATIO_MIN 倍以上
  C: 当日終値が日中レンジの上位30%以内
     (Close - Low) / (High - Low) >= 0.7
  D: 翌朝の寄り値が 20日高値 × (1 + GAP_MIN/100) 以上（翌朝判定）

【選択】 全条件通過銘柄を出来高比率の高い順に最大 MAX_CONCURRENT 銘柄

【ポジション管理】
  - 1銘柄あたり 100万円
  - 同時保有 最大3銘柄
  - 初期資金 300万円

【フィルター】
  - 日経225が25日MA割れの日は全スキップ（地合い）
  - 流動性: 平均出来高 < 10万株 を除外
  - 価格: 300円未満を除外
  - ボラ: ATR/終値 > 3% を除外

【コスト】
  - 往復0.2%

【使い方】
  python backtest_day_v2.py 2023-01-01 2025-12-31
"""

import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np

from screener import (
    fetch_tse_universe,
    batch_download_jquants,
    _jquants_id_token,
    calc_atr,
    LOOKBACK_DAYS,
)

# ============== 資金管理 ==============
INITIAL_CAPITAL    = 3_000_000
POSITION_SIZE      = 1_000_000
MAX_CONCURRENT     = 3
COMMISSION_RATE    = 0.002

# ============== フィルター ==============
MIN_PRICE          = 300
MIN_AVG_VOLUME     = 100_000
ATR_VOL_CAP        = 3.0
USE_MARKET_FILTER  = True

# ============== ブレイクアウト条件 ==============
BREAKOUT_DAYS      = 20
VOL_RATIO_MIN      = 10.0   # 出来高比 10倍以上（強エッジ）
CLOSE_RANGE_MIN    = 0.7
GAP_MIN            = 5.0    # 翌朝寄り値が20日高値+5%以上
GAP_MAX            = 20.0   # 20%超は過熱で除外


def get_trading_days(start: str, end: str) -> list[str]:
    days = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def calc_breakout_signal(pre_df):
    """当日データを含む前日大引けまでのデータからブレイクアウト条件を判定。"""
    if len(pre_df) < BREAKOUT_DAYS + 1:
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

    vol_avg_20 = float(volume.iloc[:-1].tail(BREAKOUT_DAYS).mean())
    if vol_avg_20 < MIN_AVG_VOLUME:
        return None

    atr = calc_atr(pre_df)
    if atr is None or last_close == 0:
        return None
    atr_pct = atr / last_close * 100
    if atr_pct > ATR_VOL_CAP:
        return None

    # 条件A: 当日終値が過去20日の最高値を更新
    high_20 = float(high.iloc[:-1].tail(BREAKOUT_DAYS).max())
    cond_a = last_close >= high_20

    # 条件B: 当日出来高が20日平均の1.5倍以上
    vol_ratio = last_volume / vol_avg_20 if vol_avg_20 > 0 else 0.0
    cond_b = vol_ratio >= VOL_RATIO_MIN

    # 条件C: 終値が日中レンジの上位30%以内
    day_range = last_high - last_low
    if day_range == 0:
        cond_c = False
    else:
        close_position = (last_close - last_low) / day_range
        cond_c = close_position >= CLOSE_RANGE_MIN

    if not (cond_a and cond_b and cond_c):
        return None

    return {
        "vol_ratio": vol_ratio,
        "atr_pct":   atr_pct,
        "high_20":   high_20,
    }


def run_backtest(start, end):
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  デイトレv2 バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"  初期資金: {INITIAL_CAPITAL:,}円")
    print(f"  1銘柄サイズ: {POSITION_SIZE:,}円 × 最大{MAX_CONCURRENT}銘柄")
    print(f"  往復コスト: {COMMISSION_RATE*100:.2f}%")
    print(f"  フィルター: VOL_RATIO>={VOL_RATIO_MIN} / GAP {GAP_MIN}-{GAP_MAX}%")
    print(f"  戦略: 寄成買い → 引成売り（1日完結）")
    print(f"{'='*60}\n")

    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    fetch_end   = (datetime.strptime(end,   "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")

    token    = _jquants_id_token()
    universe = fetch_tse_universe(token)
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[BO] データ取得中: {fetch_start} 〜 {fetch_end} ...")
    all_data = batch_download_jquants(token, start=fetch_start, end=fetch_end, tickers=tickers)
    print(f"[BO] J-Quants: {len(all_data)} 銘柄のデータ取得完了\n")

    nk_df = all_data.get("1321.T")
    if nk_df is not None and len(nk_df) > 25:
        nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
        print(f"[BO] 日経ETF(1321.T) 取得完了({len(nk_df)}日分)")
    else:
        nk_df = None
        print("[BO] 日経25日MAデータ取得失敗 → 地合いフィルターOFF")

    all_trading_days = get_trading_days(fetch_start, fetch_end)

    capital = INITIAL_CAPITAL
    trades = []
    equity_curve = []

    for trade_date in trading_days:
        # 地合いチェック
        market_ok = True
        if USE_MARKET_FILTER and nk_df is not None:
            nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") < trade_date]
            if len(nk_rows) >= 25:
                nk_close = float(nk_rows["Close"].iloc[-1])
                nk_ma25  = float(nk_rows["MA25"].iloc[-1])
                if not np.isnan(nk_ma25):
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

        # 全銘柄でブレイクアウト判定
        candidates = []
        for ticker, full_df in all_data.items():
            try:
                pre_df = full_df[full_df.index.strftime("%Y-%m-%d") <= trade_date].copy()
                if len(pre_df) < BREAKOUT_DAYS + 1:
                    continue
                if pre_df.index[-1].strftime("%Y-%m-%d") != trade_date:
                    continue

                signal = calc_breakout_signal(pre_df)
                if signal is None:
                    continue

                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue
                entry_open  = float(entry_rows["Open"].iloc[0])
                entry_close = float(entry_rows["Close"].iloc[0])
                if entry_open <= 0 or entry_close <= 0 or np.isnan(entry_open) or np.isnan(entry_close):
                    continue

                # D: 翌朝寄り値ギャップフィルター（20日高値+GAP_MIN%以上、GAP_MAX以下で過熱除外）
                gap_pct = (entry_open - signal["high_20"]) / signal["high_20"] * 100
                if gap_pct < GAP_MIN or gap_pct > GAP_MAX:
                    continue

                candidates.append({
                    "ticker":      ticker,
                    "name":        name_map.get(ticker, ticker),
                    "vol_ratio":   signal["vol_ratio"],
                    "entry_open":  entry_open,
                    "entry_close": entry_close,
                    "atr_pct":     signal["atr_pct"],
                    "high_20":     signal["high_20"],
                    "gap_pct":     gap_pct,
                })
            except Exception:
                continue

        # 出来高比率の高い順に上位3銘柄
        candidates.sort(key=lambda x: -x["vol_ratio"])
        picks = candidates[:MAX_CONCURRENT]

        day_trades_pnl = 0
        day_trade_count = 0
        for pick in picks:
            if capital < POSITION_SIZE:
                break

            entry_open  = pick["entry_open"]
            entry_close = pick["entry_close"]
            gross_return = (entry_close - entry_open) / entry_open
            net_return   = gross_return - COMMISSION_RATE

            position_pnl = POSITION_SIZE * net_return
            capital += position_pnl
            day_trades_pnl += position_pnl
            day_trade_count += 1

            trades.append({
                "signal_date":   trade_date,
                "entry_date":    entry_date,
                "ticker":        pick["ticker"],
                "name":          pick["name"],
                "vol_ratio":     round(pick["vol_ratio"], 2),
                "gap_pct":       round(pick["gap_pct"], 2),
                "entry_open":    round(entry_open, 1),
                "entry_close":   round(entry_close, 1),
                "gross_ret_%":   round(gross_return * 100, 3),
                "net_ret_%":     round(net_return * 100, 3),
                "pnl_jpy":       round(position_pnl),
                "capital_after": round(capital),
                "win":           net_return > 0,
                "atr_pct":       round(pick["atr_pct"], 2),
                "high_20":       round(pick["high_20"], 1),
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
    print(f"  バックテスト結果 ({start} 〜 {end})  [ブレイクアウト戦略]")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件(条件を満たす銘柄なし)")
        print(f"{'='*60}\n")
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
    max_dd_jpy = (equity_series - peak).min()

    final_capital = equity_series.iloc[-1]
    total_return_pct = (final_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100

    days_elapsed = (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
    years = days_elapsed / 365.25
    cagr = ((final_capital / INITIAL_CAPITAL) ** (1 / years) - 1) * 100 if years > 0 else 0

    df_equity["daily_ret"] = df_equity["capital"].pct_change()
    daily_std  = df_equity["daily_ret"].std()
    daily_mean = df_equity["daily_ret"].mean()
    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std and daily_std > 0 else 0

    print(f"\n  【資金推移】")
    print(f"    初期資金           : {INITIAL_CAPITAL:>12,} 円")
    print(f"    最終資金           : {final_capital:>12,} 円")
    print(f"    累積リターン       : {total_return_pct:>+12.2f} %")
    print(f"    年率リターン(CAGR) : {cagr:>+12.2f} %")
    print(f"    最大DD (資金ベース): {max_dd_pct:>+12.2f} % ({max_dd_jpy:>+,.0f} 円)")
    print(f"    シャープレシオ     : {sharpe:>12.2f}")

    print(f"\n  【取引統計】")
    print(f"    総取引回数  : {total} 件 (勝{wins}/負{losses})")
    print(f"    勝率        : {win_rate:.1f} %")
    print(f"    平均損益    : {avg_pnl_pct:+.3f} % (1取引あたり)")
    print(f"    PF          : {pf:.2f}")
    print(f"    取引総損益  : {total_pnl:+,.0f} 円")

    print(f"\n  {'='*56}")
    print(f"  年別パフォーマンス")
    print(f"  {'='*56}")
    print(f"  {'年':>4}  {'取引数':>6}  {'勝率':>6}  {'平均損益':>8}  {'PF':>5}  {'年間損益':>14}")
    print(f"  {'-'*56}")

    df_trades["year"] = df_trades["entry_date"].str[:4]
    for yr in sorted(df_trades["year"].unique()):
        sub = df_trades[df_trades["year"] == yr]
        n   = len(sub)
        wr  = sub["win"].sum() / n * 100
        avg = sub["net_ret_%"].mean()
        gp  = sub[sub["win"]]["pnl_jpy"].sum()
        gl  = abs(sub[~sub["win"]]["pnl_jpy"].sum())
        pf_y  = gp / gl if gl > 0 else float("inf")
        pnl_y = sub["pnl_jpy"].sum()
        print(f"  {yr}  {n:>6}  {wr:>5.1f}%  {avg:>+7.3f}%  {pf_y:>5.2f}  {pnl_y:>+14,.0f}")

    out_trades = f"day_v2_trades_{start}_{end}.csv"
    df_trades.to_csv(out_trades, index=False, encoding="utf-8-sig")
    print(f"\n  取引履歴を {out_trades} に保存しました")

    out_equity = f"day_v2_equity_{start}_{end}.csv"
    df_equity.to_csv(out_equity, index=False, encoding="utf-8-sig")
    print(f"  資金推移を {out_equity} に保存しました")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) >= 2:
        s, e = args[0], args[1]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_backtest(s, e)

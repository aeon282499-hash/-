"""
backtest_jquants.py — 寄り引け逆張り戦略バックテスト（J-Quants のみ・全銘柄）
============================================================================
戦略: 米株急落翌日に RSI 売られすぎ銘柄を寄りで買い → 引けで決済

フィルター（前日データで判定）:
  ① 1655.T 前日リターン <= -1.0%（米株が売られた日のみ）
  ② ボラ(ATR/終値) < 2.5%（高ボラ銘柄除外）
  ③ RSI(14) <= 30（売られすぎ）
  ④ 売買代金 >= 30億円（流動性確保）
  ⑤ ストップ: Open -2.0%（日中安値が到達した場合）

使い方:
  python backtest_jquants.py                    # 直近1年
  python backtest_jquants.py 2024-01-01 2025-12-31
"""

import sys, time
from datetime import datetime, timedelta, date

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from screener import (
    _jquants_id_token, batch_download_jquants,
    fetch_tse_prime_universe, calc_rsi, calc_atr,
)

# ── パラメーター ──────────────────────────────────────────────────────────
SP500_BUY_MAX  = -1.0    # 1655.T 前日リターン <= この値なら買い候補
VOL_CAP        = 2.5     # ATR/終値(%) 上限（高ボラ除外）
RSI_BUY_MAX    = 30      # RSI 上限（売られすぎゾーン）
TURNOVER_MIN   = 3_000_000_000  # 売買代金下限（30億円）
STOP_PCT       = 2.0     # ストップロス %
RSI_PERIOD     = 14

SP500_PROXY  = "1655.T"  # iShares Core S&P500 ETF（東証）
NIKKEI_PROXY = "1321.T"  # 日経225 ETF（東証）


def build_macro(sp_df: pd.DataFrame) -> pd.DataFrame:
    """1655.T から S&P500 代替リターンを計算。"""
    df = sp_df.copy()
    df.index = pd.to_datetime(df.index).normalize()
    df["sp500_ret"] = df["Close"].pct_change() * 100
    return df


def run_backtest(start_str: str = None, end_str: str = None) -> None:
    today = date.today()
    if end_str   is None: end_str   = today.strftime("%Y-%m-%d")
    if start_str is None: start_str = (today - timedelta(days=365)).strftime("%Y-%m-%d")

    fetch_start = (datetime.strptime(start_str, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  寄り引け逆張りバックテスト（J-Quants / 東証プライム全銘柄）")
    print(f"  期間: {start_str} ~ {end_str}")
    print(f"  S&P500代替: {SP500_PROXY}  閾値: <={SP500_BUY_MAX}%")
    print(f"  フィルター: ATR<={VOL_CAP}% / RSI<={RSI_BUY_MAX} / 売買代金>={TURNOVER_MIN//1e8:.0f}億")
    print(f"  ストップ: -{STOP_PCT}%")
    print(f"{'='*60}\n")

    # ── 銘柄リスト取得 ────────────────────────────────────
    print("[1/2] 銘柄リスト & データ取得中...")
    universe = fetch_tse_prime_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    # プロキシETF を追加
    for proxy in [SP500_PROXY, NIKKEI_PROXY]:
        if proxy not in tickers:
            tickers.append(proxy)

    print(f"  ユニバース: {len(universe)} 銘柄（プロキシETF含む {len(tickers)} 件）")

    token      = _jquants_id_token()
    price_data = batch_download_jquants(token, start=fetch_start, end=end_str, tickers=tickers)
    print(f"  価格データ取得完了: {len(price_data)} 銘柄\n")

    # ── マクロ指標構築 ────────────────────────────────────
    sp_raw = price_data.get(SP500_PROXY)
    if sp_raw is not None and len(sp_raw) >= 5:
        macro_df = build_macro(sp_raw)
        print(f"[2/2] {SP500_PROXY} マクロ指標構築完了（{len(macro_df)} 日）\n")
    else:
        macro_df = pd.DataFrame()
        print(f"[2/2] WARNING: {SP500_PROXY} データなし → マクロフィルター無効\n")

    # ── バックテストループ ────────────────────────────────
    print("バックテスト実行中...\n")
    trades = []

    for ticker, full_df in price_data.items():
        if ticker in (SP500_PROXY, NIKKEI_PROXY):
            continue
        if full_df is None or len(full_df) < RSI_PERIOD + 5:
            continue

        full_df = full_df.copy()
        full_df.index = pd.to_datetime(full_df.index).normalize()

        in_range = full_df[full_df.index.strftime("%Y-%m-%d") >= start_str]
        if len(in_range) < 2:
            continue

        for signal_date in in_range.index[:-1]:
            pos = full_df.index.get_loc(signal_date)
            if pos + 1 >= len(full_df):
                continue

            signal_row = full_df.iloc[pos]
            entry_row  = full_df.iloc[pos + 1]
            entry_date = full_df.index[pos + 1]

            # ─ ① マクロフィルター（1655.T）────────────────
            sp500_ret = 0.0
            if not macro_df.empty:
                past = macro_df[macro_df.index <= signal_date]
                if len(past) == 0:
                    continue
                sp500_ret = float(past["sp500_ret"].iloc[-1])
                if np.isnan(sp500_ret) or sp500_ret > SP500_BUY_MAX:
                    continue  # 米株が下がっていない日はスキップ

            # ─ ② ボラフィルター（ATR/終値）────────────────
            hist = full_df.iloc[max(0, pos - 20): pos + 1]
            atr  = calc_atr(hist)
            last_close = float(signal_row["Close"])
            if atr is None or last_close <= 0:
                continue
            if (atr / last_close * 100) > VOL_CAP:
                continue

            # ─ ③ RSI フィルター ─────────────────────────
            close_slice = full_df["Close"].iloc[:pos + 1].dropna()
            rsi = calc_rsi(close_slice)
            if rsi is None or rsi > RSI_BUY_MAX:
                continue

            # ─ ④ 売買代金フィルター ─────────────────────
            turnover = last_close * float(signal_row.get("Volume", 0))
            if turnover < TURNOVER_MIN:
                continue

            # ─ 損益計算 ──────────────────────────────────
            try:
                entry_open  = float(entry_row["Open"])
                entry_high  = float(entry_row["High"])
                entry_low   = float(entry_row["Low"])
                entry_close = float(entry_row["Close"])
            except Exception:
                continue

            if any(np.isnan(v) or v <= 0 for v in [entry_open, entry_high, entry_low, entry_close]):
                continue

            stop_px = entry_open * (1 - STOP_PCT / 100)
            if entry_low <= stop_px:
                pnl_pct, exit_type = -STOP_PCT, "STOP"
            else:
                pnl_pct   = (entry_close - entry_open) / entry_open * 100
                exit_type = "CLOSE"

            trades.append({
                "signal_date": signal_date.strftime("%Y-%m-%d"),
                "entry_date":  entry_date.strftime("%Y-%m-%d"),
                "ticker":      ticker,
                "name":        name_map.get(ticker, ticker),
                "rsi":         round(rsi, 1),
                "sp500_ret":   round(sp500_ret, 2),
                "atr_pct":     round(atr / last_close * 100, 2),
                "entry_open":  round(entry_open, 1),
                "pnl_pct":     round(pnl_pct, 3),
                "exit_type":   exit_type,
                "win":         pnl_pct > 0,
            })

    _print_and_save(trades, start_str, end_str)


def _print_and_save(trades: list, start: str, end: str) -> None:
    print(f"\n{'='*60}")
    print(f"  バックテスト結果 ({start} ~ {end})")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件（条件を満たす取引なし）")
        print(f"{'='*60}\n")
        return

    df      = pd.DataFrame(trades).sort_values("entry_date").reset_index(drop=True)
    total   = len(df)
    wins    = int(df["win"].sum())
    losses  = total - wins
    wr      = wins / total * 100
    avg_pnl = df["pnl_pct"].mean()
    avg_win = df[df["win"]]["pnl_pct"].mean()  if wins   > 0 else 0.0
    avg_los = df[~df["win"]]["pnl_pct"].mean() if losses > 0 else 0.0
    pf      = (df[df["win"]]["pnl_pct"].sum() / abs(df[~df["win"]]["pnl_pct"].sum())
               if losses > 0 and df[~df["win"]]["pnl_pct"].sum() != 0 else float("inf"))

    cumret   = (1 + df["pnl_pct"] / 100).cumprod()
    max_dd   = float(((cumret - cumret.cummax()) / cumret.cummax() * 100).min())

    print(f"  取引回数        : {total} 件")
    print(f"  勝ち            : {wins} 件")
    print(f"  負け            : {losses} 件")
    print(f"  勝率            : {wr:.1f}%")
    print(f"  平均損益        : {avg_pnl:+.3f}%")
    print(f"  平均利益        : {avg_win:+.3f}%")
    print(f"  平均損失        : {avg_los:+.3f}%")
    print(f"  プロフィットF   : {pf:.2f}")
    print(f"  最大ドローダウン: {max_dd:.2f}%")

    print(f"\n  -- エグジット種別 --")
    for etype in ["STOP", "CLOSE"]:
        sub = df[df["exit_type"] == etype]
        if len(sub):
            wr_e = sub["win"].sum() / len(sub) * 100
            print(f"  [{etype:5s}] {len(sub):4d}件 / 勝率{wr_e:5.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    print(f"\n  -- 上位5件（利益）--")
    for _, r in df.nlargest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"RSI={r['rsi']} -> {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n  -- 下位5件（損失）--")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"RSI={r['rsi']} -> {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n{'='*60}\n")

    # グラフ
    try:
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        fig.suptitle(
            f"Open-to-Close Mean Reversion  ({start} ~ {end})\n"
            f"Trades:{total}  WinRate:{wr:.1f}%  PF:{pf:.2f}  MaxDD:{max_dd:.1f}%",
            fontsize=11
        )
        ax1 = axes[0]
        ax1.plot(range(len(cumret)), cumret.values, color="steelblue", linewidth=1.5)
        ax1.fill_between(range(len(cumret)), cumret.values, 1.0,
                         where=(cumret.values < 1.0), color="salmon", alpha=0.4)
        ax1.axhline(y=1.0, color="gray", linestyle="--", linewidth=0.8)
        ax1.set_ylabel("Cumulative Return (x)")
        ax1.set_title("Cumulative P&L")
        ax1.grid(True, alpha=0.3)

        ax2 = axes[1]
        colors = ["#2ecc71" if w else "#e74c3c" for w in df["win"]]
        ax2.bar(range(len(df)), df["pnl_pct"].values, color=colors, alpha=0.8, width=0.8)
        ax2.axhline(y=0, color="black", linewidth=0.8)
        ax2.set_ylabel("P&L (%)")
        ax2.set_title("Per-trade P&L")
        ax2.grid(True, alpha=0.3, axis="y")

        plt.tight_layout()
        plt.savefig("jquants_backtest.png", dpi=150, bbox_inches="tight")
        print(f"  グラフ保存: jquants_backtest.png")
        plt.close()
    except Exception as e:
        print(f"  グラフ保存失敗: {e}")

    out_csv = f"jquants_backtest_{start}_{end}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"  CSV 保存: {out_csv}\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        run_backtest(sys.argv[1], sys.argv[2])
    else:
        run_backtest()

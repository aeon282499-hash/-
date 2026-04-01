"""
backtest_jquants.py — 寄り引け逆張り戦略バックテスト（J-Quants のみ）
============================================================================
マクロデータも J-Quants で取得（IP ブロック回避）:
  S&P500 代替 → 1655.T（iShares S&P500 ETF、東証上場）
  VIX    代替 → 1655.T の ATR(14) / 終値 % で実現ボラを計算

使い方:
  python backtest_jquants.py                    # 直近2年
  python backtest_jquants.py 2024-01-01 2025-12-31
"""

import os, sys, time, requests
from datetime import datetime, timedelta, date

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from screener import _jquants_id_token, batch_download_jquants, calc_rsi, calc_atr

# ── パラメーター ──────────────────────────────────────────────────────────
VOL_LIMIT      = 2.5    # 1655.T の ATR/終値(%) がこれ以上 → 高ボラ → ノートレード（VIX25相当）
SP500_BUY_MAX  = -1.0   # 1655.T 前日リターン ≤ この値なら買い候補
SP500_SELL_MIN = +1.0   # 1655.T 前日リターン ≥ この値なら売り候補
RSI_BUY_MAX    = 30
RSI_SELL_MIN   = 70
ROE_MIN        = 8.0    # %
PBR_MAX        = 1.5
PER_SELL_MIN   = 50.0
STOP_PCT       = 2.0    # ストップロス %

SP500_PROXY  = "1655.T"   # iShares Core S&P500 ETF（東証）
NIKKEI_PROXY = "1321.T"   # NEXT FUNDS 日経225連動型 ETF（東証）

# ── ユニバース（TOPIX100 流動性上位30銘柄）────────────────────────────────
UNIVERSE = [
    "7203.T",  # トヨタ自動車
    "6758.T",  # ソニーグループ
    "8306.T",  # 三菱UFJフィナンシャル・グループ
    "6861.T",  # キーエンス
    "9432.T",  # 日本電信電話
    "8035.T",  # 東京エレクトロン
    "6098.T",  # リクルートホールディングス
    "7741.T",  # HOYA
    "6367.T",  # ダイキン工業
    "9433.T",  # KDDI
    "4063.T",  # 信越化学工業
    "8316.T",  # 三井住友フィナンシャルグループ
    "7267.T",  # 本田技研工業
    "9983.T",  # ファーストリテイリング
    "8411.T",  # みずほフィナンシャルグループ
    "6954.T",  # ファナック
    "4519.T",  # 中外製薬
    "7974.T",  # 任天堂
    "6902.T",  # デンソー
    "4568.T",  # 第一三共
    "3382.T",  # セブン&アイ・ホールディングス
    "6501.T",  # 日立製作所
    "6326.T",  # クボタ
    "4502.T",  # 武田薬品工業
    "2914.T",  # 日本たばこ産業
    "9022.T",  # 東日本旅客鉄道
    "6762.T",  # TDK
    "8031.T",  # 三井物産
    "5108.T",  # ブリヂストン
    "4307.T",  # 野村総合研究所
]

# データ取得時はプロキシETFも一緒に取得する
_FETCH_TICKERS = list(dict.fromkeys([SP500_PROXY, NIKKEI_PROXY] + UNIVERSE))


# ─────────────────────────────────────────────────────────────────────────
# データ取得ヘルパー
# ─────────────────────────────────────────────────────────────────────────

def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def fetch_fins_statements(token: str, tickers: list) -> dict:
    """J-Quants fins/statements を銘柄ごとに取得。ROE/PBR/PER 計算の素材。"""
    headers = _auth(token)
    result  = {}
    for ticker in tickers:
        code = ticker.replace(".T", "")
        for _ in range(2):
            try:
                r = requests.get(
                    f"https://api.jquants.com/v1/fins/statements?code={code}",
                    headers=headers, timeout=30
                )
                if r.status_code == 429:
                    print(f"  [fins] レート制限 → 60秒待機...")
                    time.sleep(60)
                    continue
                if r.status_code != 200:
                    break
                rows = r.json().get("statements", [])
                if not rows:
                    break
                df = pd.DataFrame(rows)
                df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"])
                for col in ["Profit", "Equity", "BookValuePerShare", "EarningsPerShare"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                result[ticker] = df.sort_values("DisclosedDate").reset_index(drop=True)
                break
            except Exception as e:
                print(f"  [fins] {ticker}: {e}")
                break
        time.sleep(0.3)
    return result


def get_latest_fin(fin_df, as_of: str) -> dict | None:
    """as_of 日付以前の最新財務行を返す（ルックアヘッド防止）。"""
    if fin_df is None or len(fin_df) == 0:
        return None
    past = fin_df[fin_df["DisclosedDate"].dt.strftime("%Y-%m-%d") <= as_of]
    return past.iloc[-1].to_dict() if len(past) > 0 else None


def calc_fundamentals(fin_row: dict, close_px: float) -> dict:
    """ROE・PBR・PER・EPS を返す（計算不可なら None）。"""
    def _f(v):
        try:
            x = float(v)
            return x if not np.isnan(x) else None
        except Exception:
            return None

    profit = _f(fin_row.get("Profit"))
    equity = _f(fin_row.get("Equity"))
    bvps   = _f(fin_row.get("BookValuePerShare"))
    eps    = _f(fin_row.get("EarningsPerShare"))

    roe = (profit / equity * 100) if profit is not None and equity and equity > 0 else None
    pbr = (close_px / bvps)       if bvps   is not None and bvps   > 0            else None
    per = (close_px / eps)        if eps    is not None and eps    > 0            else None

    return {"roe": roe, "pbr": pbr, "per": per, "eps": eps}


def build_sp500_proxy(sp_df: pd.DataFrame) -> pd.DataFrame:
    """
    1655.T から S&P500 代替マクロ指標を計算。
      sp500_ret  : 前日比リターン(%)
      vol_pct    : ATR(14) / 終値(%)  ← VIX の代替
    """
    df = sp_df.copy()
    df.index = pd.to_datetime(df.index).normalize()
    df["sp500_ret"] = df["Close"].pct_change() * 100
    # ATR(14) を計算して終値で正規化
    atr_list = []
    for i in range(len(df)):
        atr_val = calc_atr(df.iloc[max(0, i - 20): i + 1])
        atr_list.append(atr_val)
    df["atr"] = atr_list
    df["vol_pct"] = df["atr"] / df["Close"] * 100
    return df


# ─────────────────────────────────────────────────────────────────────────
# バックテスト本体
# ─────────────────────────────────────────────────────────────────────────

def run_backtest(start_str: str = None, end_str: str = None) -> None:
    today = date.today()
    if end_str   is None: end_str   = today.strftime("%Y-%m-%d")
    if start_str is None: start_str = (today - timedelta(days=730)).strftime("%Y-%m-%d")

    fetch_start = (datetime.strptime(start_str, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  寄り引け逆張りバックテスト（J-Quants のみ）")
    print(f"  期間: {start_str} 〜 {end_str}")
    print(f"  ユニバース: {len(UNIVERSE)} 銘柄")
    print(f"  S&P500代替: {SP500_PROXY}  VIX代替: ATR/終値%")
    print(f"  フィルター: VOL<{VOL_LIMIT}% / "
          f"SP500<={SP500_BUY_MAX}%->BUY / SP500>={SP500_SELL_MIN}%->SELL")
    print(f"  RSI: BUY<={RSI_BUY_MAX} / SELL>={RSI_SELL_MIN}")
    print(f"  Fundamental: ROE>={ROE_MIN}% & PBR<={PBR_MAX} / PER>={PER_SELL_MIN}or赤字")
    print(f"  ストップ: -{STOP_PCT}%（日中高安が到達した場合）")
    print(f"{'='*60}\n")

    # ── 1. 価格データ（プロキシETF含む全銘柄）────────────
    print("[1/2] J-Quants 価格データ取得中...")
    token      = _jquants_id_token()
    price_data = batch_download_jquants(token, start=fetch_start, end=end_str,
                                        tickers=_FETCH_TICKERS)
    print(f"  取得完了: {len(price_data)} 銘柄\n")

    # S&P500 代替マクロ指標を構築
    sp_raw = price_data.get(SP500_PROXY)
    if sp_raw is None or len(sp_raw) < 20:
        print(f"  WARNING: {SP500_PROXY} データなし → マクロフィルター無効")
        macro_df = pd.DataFrame()
    else:
        macro_df = build_sp500_proxy(sp_raw)
        print(f"  {SP500_PROXY} マクロ指標構築完了（{len(macro_df)}日）\n")

    # ── 2. 財務データ ─────────────────────────────────────
    print("[2/2] J-Quants 財務データ取得中...")
    fin_data = fetch_fins_statements(token, UNIVERSE)
    print(f"  取得完了: {len(fin_data)} 銘柄\n")

    # ── バックテストループ ────────────────────────────────
    print("バックテスト実行中...\n")
    trades = []

    for ticker in UNIVERSE:
        price_df = price_data.get(ticker)
        fin_df   = fin_data.get(ticker)
        if price_df is None or len(price_df) < 20:
            continue

        price_df = price_df.copy()
        price_df.index = pd.to_datetime(price_df.index).normalize()

        in_range = price_df[price_df.index.strftime("%Y-%m-%d") >= start_str]
        if len(in_range) < 2:
            continue

        closes = price_df["Close"].dropna()

        for signal_date in in_range.index[:-1]:
            pos = price_df.index.get_loc(signal_date)
            if pos + 1 >= len(price_df):
                continue

            signal_row = price_df.iloc[pos]
            entry_row  = price_df.iloc[pos + 1]
            entry_date = price_df.index[pos + 1]
            sig_str    = signal_date.strftime("%Y-%m-%d")

            # ─ マクロフィルター（1655.T ベース）─────────
            sp500_ret = 0.0
            vol_pct   = 0.0
            if not macro_df.empty:
                past_mac = macro_df[macro_df.index <= signal_date]
                if len(past_mac) == 0:
                    continue
                mac_row   = past_mac.iloc[-1]
                sp500_ret = float(mac_row["sp500_ret"]) if not np.isnan(mac_row["sp500_ret"]) else 0.0
                vol_pct   = float(mac_row["vol_pct"])   if not np.isnan(mac_row["vol_pct"])   else 0.0
                if vol_pct >= VOL_LIMIT:
                    continue  # 高ボラ → ノートレード

            # ─ RSI ──────────────────────────────────────
            close_slice = closes[closes.index <= signal_date]
            rsi = calc_rsi(close_slice)
            if rsi is None:
                continue

            # ─ ファンダメンタルズ ─────────────────────────
            fin_row = get_latest_fin(fin_df, sig_str)
            if fin_row is None:
                continue
            fund = calc_fundamentals(fin_row, float(signal_row["Close"]))

            # ─ シグナル判定 ───────────────────────────────
            direction = None

            if (sp500_ret <= SP500_BUY_MAX and
                    rsi <= RSI_BUY_MAX and
                    fund["roe"] is not None and fund["roe"] >= ROE_MIN and
                    fund["pbr"] is not None and fund["pbr"] <= PBR_MAX):
                direction = "BUY"

            elif (sp500_ret >= SP500_SELL_MIN and
                      rsi >= RSI_SELL_MIN and
                      ((fund["per"] is not None and fund["per"] >= PER_SELL_MIN) or
                       (fund["eps"] is not None and fund["eps"] < 0))):
                direction = "SELL"

            if direction is None:
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

            if direction == "BUY":
                stop_px = entry_open * (1 - STOP_PCT / 100)
                if entry_low <= stop_px:
                    pnl_pct, exit_type = -STOP_PCT, "STOP"
                else:
                    pnl_pct   = (entry_close - entry_open) / entry_open * 100
                    exit_type = "CLOSE"
            else:
                stop_px = entry_open * (1 + STOP_PCT / 100)
                if entry_high >= stop_px:
                    pnl_pct, exit_type = -STOP_PCT, "STOP"
                else:
                    pnl_pct   = (entry_open - entry_close) / entry_open * 100
                    exit_type = "CLOSE"

            trades.append({
                "signal_date": sig_str,
                "entry_date":  entry_date.strftime("%Y-%m-%d"),
                "ticker":      ticker,
                "direction":   direction,
                "rsi":         round(rsi, 1),
                "sp500_ret":   round(sp500_ret, 2),
                "vol_pct":     round(vol_pct, 2),
                "roe":         round(fund["roe"], 1) if fund["roe"] is not None else None,
                "pbr":         round(fund["pbr"], 2) if fund["pbr"] is not None else None,
                "per":         round(fund["per"], 1) if fund["per"] is not None else None,
                "entry_open":  round(entry_open, 1),
                "pnl_pct":     round(pnl_pct, 3),
                "exit_type":   exit_type,
                "win":         pnl_pct > 0,
            })

    _print_and_save(trades, start_str, end_str)


# ─────────────────────────────────────────────────────────────────────────
# 結果表示 & グラフ保存
# ─────────────────────────────────────────────────────────────────────────

def _print_and_save(trades: list, start: str, end: str) -> None:
    print(f"\n{'='*60}")
    print(f"  バックテスト結果 ({start} 〜 {end})")
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

    cumret      = (1 + df["pnl_pct"] / 100).cumprod()
    rolling_max = cumret.cummax()
    drawdown    = (cumret - rolling_max) / rolling_max * 100
    max_dd      = float(drawdown.min())

    print(f"  取引回数        : {total} 件")
    print(f"  勝ち            : {wins} 件")
    print(f"  負け            : {losses} 件")
    print(f"  勝率            : {wr:.1f}%")
    print(f"  平均損益        : {avg_pnl:+.3f}%")
    print(f"  平均利益        : {avg_win:+.3f}%")
    print(f"  平均損失        : {avg_los:+.3f}%")
    print(f"  プロフィットF   : {pf:.2f}")
    print(f"  最大ドローダウン: {max_dd:.2f}%")

    print(f"\n  ── エグジット種別 ──────────────────────")
    for etype in ["STOP", "CLOSE"]:
        sub = df[df["exit_type"] == etype]
        if len(sub):
            wr_e = sub["win"].sum() / len(sub) * 100
            print(f"  [{etype:5s}] {len(sub):4d}件 / 勝率{wr_e:5.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    for d in ["BUY", "SELL"]:
        sub = df[df["direction"] == d]
        if len(sub):
            wr_d = sub["win"].sum() / len(sub) * 100
            print(f"\n  [{d}] {len(sub)}件 / 勝率{wr_d:.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    print(f"\n  ── 上位5件（利益）──────────────")
    for _, r in df.nlargest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['ticker']} {r['direction']} "
              f"RSI={r['rsi']} → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n  ── 下位5件（損失）──────────────")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['ticker']} {r['direction']} "
              f"RSI={r['rsi']} → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n{'='*60}\n")

    # グラフ
    try:
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        fig.suptitle(
            f"Open-to-Close Mean Reversion Backtest  ({start} ~ {end})\n"
            f"Trades:{total}  WinRate:{wr:.1f}%  PF:{pf:.2f}  MaxDD:{max_dd:.1f}%",
            fontsize=11
        )
        ax1 = axes[0]
        ax1.plot(range(len(cumret)), cumret.values, color="steelblue", linewidth=1.5)
        ax1.fill_between(range(len(cumret)), cumret.values, 1.0,
                         where=(cumret.values < 1.0), color="salmon", alpha=0.4)
        ax1.axhline(y=1.0, color="gray", linestyle="--", linewidth=0.8)
        ax1.set_ylabel("Cumulative Return (x)")
        ax1.set_title("Cumulative P&L (equal weight per trade)")
        ax1.grid(True, alpha=0.3)

        ax2 = axes[1]
        colors = ["#2ecc71" if w else "#e74c3c" for w in df["win"]]
        ax2.bar(range(len(df)), df["pnl_pct"].values, color=colors, alpha=0.8, width=0.8)
        ax2.axhline(y=0, color="black", linewidth=0.8)
        ax2.set_ylabel("P&L (%)")
        ax2.set_title("Per-trade P&L")
        ax2.grid(True, alpha=0.3, axis="y")

        plt.tight_layout()
        out_png = "jquants_backtest.png"
        plt.savefig(out_png, dpi=150, bbox_inches="tight")
        print(f"  グラフ保存: {out_png}")
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

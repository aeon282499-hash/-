"""
optimize.py — パラメータ自動最適化（グリッドサーチ）
=====================================================

使い方:
  python optimize.py 2024-01-01 2024-12-31

処理フロー:
  1. 指定期間の全銘柄データをまとめてダウンロード（1回だけ）
  2. 各銘柄・各日付の指標値をベクトル化して事前計算（高速化）
  3. グリッドサーチでパラメータを変えながらフィルタリング
  4. 勝率70%以上かつ取引件数が多い最良パラメータを報告

ルックアヘッドバイアス防止:
  - 条件①〜⑤の判定には当日（T）の前日（T-1）までのデータのみ使用
  - 条件⑥（ギャップ）は当日の「始値のみ」を参照
  - エグジット計算にのみ当日の終値を使用
"""

import sys
import time
import itertools
from datetime import datetime, timedelta

import jpholiday
import yfinance as yf
import pandas as pd
import numpy as np

from screener import (
    fetch_tse_universe,
    batch_download,
    RSI_PERIOD, MA_PERIOD, ATR_PERIOD, VOL_AVG_PERIOD,
    LOOKBACK_DAYS, TURNOVER_MIN,
)


# ================================================================
# グリッドサーチ対象パラメータの探索範囲
# ここを変えると探索範囲が変わる
# ================================================================
PARAM_GRID = {
    "rsi_buy":    [5, 8, 10, 12, 15],        # RSI(2) 買い閾値
    "rsi_sell":   [85, 88, 90, 92, 95],      # RSI(2) 売り閾値
    "dev_buy":    [-1.5, -2.0, -2.5, -3.0],  # 5MA乖離率 買い閾値(%)
    "dev_sell":   [+1.5, +2.0, +2.5, +3.0],  # 5MA乖離率 売り閾値(%)
    "gap_max":    [3.0, 5.0],                 # 特大ギャップ除外(%)
}
MIN_TRADES  = 20    # 最低取引件数（これ未満のパラメータは評価しない）
TARGET_WR   = 0.70  # 目標勝率


def get_trading_days(start: str, end: str) -> list[str]:
    days, cur = [], datetime.strptime(start, "%Y-%m-%d").date()
    end_      = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


# ================================================================
# ベクトル化された指標計算（1銘柄・全日付を一括処理）
# ================================================================

def _calc_rsi_series(close: pd.Series, period: int) -> pd.Series:
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _calc_dev_series(close: pd.Series, period: int) -> pd.Series:
    ma = close.rolling(period).mean()
    return (close - ma) / ma.replace(0, np.nan) * 100


def _calc_range_ratio_series(df: pd.DataFrame, atr_period: int) -> pd.Series:
    high, low   = df["High"], df["Low"]
    prev_close  = df["Close"].shift(1)
    tr = pd.concat([high - low,
                    (high - prev_close).abs(),
                    (low  - prev_close).abs()], axis=1).max(axis=1)
    atr         = tr.rolling(atr_period).mean()
    daily_range = high - low
    return daily_range / atr.replace(0, np.nan)


def _calc_vol_ratio_series(df: pd.DataFrame, period: int) -> pd.Series:
    vol     = df["Volume"].replace(0, np.nan)
    avg_vol = vol.rolling(period + 1).mean().shift(1)  # period日平均（前日含まず）
    return vol / avg_vol.replace(0, np.nan)


def precompute_all(
    all_data: dict[str, pd.DataFrame],
    trading_days: list[str],
    name_map: dict[str, str],
) -> pd.DataFrame:
    """
    全銘柄・全日付の指標値を事前計算してひとつの DataFrame にまとめる。
    グリッドサーチではこの DataFrame にフィルタを適用するだけでよい（高速）。

    各行は「1銘柄 × 1取引日」を表す。
    カラム:
      date, ticker, name, direction_candidate,
      rsi2, dev5, range_ratio, vol_ratio, turnover,
      gap_pct, pnl_buy_pct, pnl_sell_pct
    """
    print("[optimize] 指標の事前計算中...")
    records = []
    td_set  = set(trading_days)

    for ticker, df in all_data.items():
        try:
            if len(df) < ATR_PERIOD + 10:
                continue

            # ── 全日付を通じてベクトル計算 ──────────────────
            # 各指標は「その日の値」。
            # シグナル判定に使うのは「前日の値」= shift(1) で取得。
            rsi_s    = _calc_rsi_series(df["Close"], RSI_PERIOD)
            dev_s    = _calc_dev_series(df["Close"], MA_PERIOD)
            rr_s     = _calc_range_ratio_series(df, ATR_PERIOD)
            vr_s     = _calc_vol_ratio_series(df, VOL_AVG_PERIOD)
            turn_s   = df["Close"] * df["Volume"]   # 当日の売買代金

            # 前日の値に揃える（シグナル判定はT-1の値を使う）
            prev_rsi   = rsi_s.shift(1)
            prev_dev   = dev_s.shift(1)
            prev_rr    = rr_s.shift(1)
            prev_vr    = vr_s.shift(1)
            prev_turn  = turn_s.shift(1)
            prev_close = df["Close"].shift(1)

            # 当日の始値・終値（エントリー/エグジット用）
            today_open  = df["Open"]
            today_close = df["Close"]

            # ギャップ率（条件⑥用）= (当日始値 / 前日終値 - 1) × 100
            gap_s = (today_open / prev_close.replace(0, np.nan) - 1) * 100

            # 損益率（始値→終値）
            pnl_buy_s  = (today_close - today_open) / today_open.replace(0, np.nan) * 100
            pnl_sell_s = (today_open - today_close) / today_open.replace(0, np.nan) * 100

            # ── 取引日のみ抽出 ────────────────────────────
            # タイムゾーン情報があれば除去してから日付文字列に変換
            raw_index = df.index
            if hasattr(raw_index, "tz") and raw_index.tz is not None:
                raw_index = raw_index.tz_localize(None)
            date_strs = raw_index.strftime("%Y-%m-%d")
            mask      = pd.Index(date_strs).isin(td_set)
            idx       = df.index[mask]

            for i in idx:
                d = i.strftime("%Y-%m-%d")
                try:
                    rv  = float(prev_rsi[i])
                    dv  = float(prev_dev[i])
                    rrv = float(prev_rr[i])  if pd.notna(prev_rr[i])  else np.nan
                    vrv = float(prev_vr[i])  if pd.notna(prev_vr[i])  else np.nan
                    tv  = float(prev_turn[i])
                    pcv = float(prev_close[i])
                    gv  = float(gap_s[i])
                    pb  = float(pnl_buy_s[i])
                    ps  = float(pnl_sell_s[i])
                    ov  = float(today_open[i])

                    # NaN や異常値チェック
                    if any(np.isnan(v) for v in [rv, dv, tv, pcv, gv, pb, ps, ov]):
                        continue
                    if ov <= 0 or pcv <= 0:
                        continue

                    records.append({
                        "date":         d,
                        "ticker":       ticker,
                        "name":         name_map.get(ticker, ticker),
                        "rsi2":         rv,
                        "dev5":         dv,
                        "range_ratio":  rrv,
                        "vol_ratio":    vrv,
                        "turnover":     tv,
                        "gap_pct":      gv,
                        "pnl_buy_pct":  pb,
                        "pnl_sell_pct": ps,
                        "today_open":   ov,
                        "prev_close":   pcv,
                    })
                except Exception:
                    continue

        except Exception as e:
            continue

    df_all = pd.DataFrame(records)
    print(f"[optimize] 事前計算完了: {len(df_all):,} 行")
    return df_all


# ================================================================
# グリッドサーチ本体
# ================================================================

def grid_search(df_all: pd.DataFrame) -> tuple[dict, dict]:
    """
    PARAM_GRID の全組み合わせをテストして最良パラメータを返す。

    Returns
    -------
    best_params : dict  最良パラメータ
    best_stats  : dict  その際の勝率・取引件数・PF
    """
    keys   = list(PARAM_GRID.keys())
    values = list(PARAM_GRID.values())
    combos = list(itertools.product(*values))
    total  = len(combos)
    print(f"\n[optimize] グリッドサーチ開始: {total} 通りの組み合わせ")

    best_params = None
    best_stats  = {"win_rate": 0, "trades": 0, "pf": 0}
    best_score  = 0

    results_log = []

    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        rsi_buy  = params["rsi_buy"]
        rsi_sell = params["rsi_sell"]
        dev_buy  = params["dev_buy"]
        dev_sell = params["dev_sell"]
        gap_max  = params["gap_max"]

        # ── 条件①② RSI と乖離率でシグナル方向を決定 ──────
        buy_mask  = (df_all["rsi2"] <= rsi_buy)  & (df_all["dev5"] <= dev_buy)
        sell_mask = (df_all["rsi2"] >= rsi_sell) & (df_all["dev5"] >= dev_sell)
        dir_mask  = buy_mask | sell_mask
        df_f      = df_all[dir_mask].copy()

        if df_f.empty:
            continue

        # ── 条件③ ボラ OR 出来高 ──────────────────────────
        range_ok = df_f["range_ratio"] >= 1.0   # RANGE_MULT 固定（OR条件の一方）
        vol_ok   = df_f["vol_ratio"]   >= 1.2   # VOL_MULT 固定
        df_f     = df_f[range_ok | vol_ok]

        # ── 条件④ 流動性 ──────────────────────────────────
        df_f = df_f[df_f["turnover"] >= TURNOVER_MIN]

        if df_f.empty:
            continue

        # ── 売買方向を列に追加 ────────────────────────────
        df_f = df_f.copy()
        df_f["direction"] = np.where(
            (df_f["rsi2"] <= rsi_buy) & (df_f["dev5"] <= dev_buy), "BUY", "SELL"
        )
        # 方向が不一致（RSIはBUYだが乖離はSELL等）は除外
        rsi_buy_m  = df_f["rsi2"] <= rsi_buy
        dev_buy_m  = df_f["dev5"] <= dev_buy
        rsi_sell_m = df_f["rsi2"] >= rsi_sell
        dev_sell_m = df_f["dev5"] >= dev_sell
        consistent = (rsi_buy_m & dev_buy_m) | (rsi_sell_m & dev_sell_m)
        df_f = df_f[consistent]

        # ── 条件⑥ ギャップ判定 ────────────────────────────
        buy_gap  = (df_f["direction"] == "BUY")  & (df_f["gap_pct"] < 0)  & (df_f["gap_pct"] >= -gap_max)
        sell_gap = (df_f["direction"] == "SELL") & (df_f["gap_pct"] > 0)  & (df_f["gap_pct"] <= gap_max)
        df_f     = df_f[buy_gap | sell_gap]

        if len(df_f) < MIN_TRADES:
            continue

        # ── 損益計算 ──────────────────────────────────────
        df_f = df_f.copy()
        df_f["pnl"] = np.where(
            df_f["direction"] == "BUY",
            df_f["pnl_buy_pct"],
            df_f["pnl_sell_pct"],
        )
        df_f["win"] = df_f["pnl"] > 0

        win_rate = df_f["win"].mean()
        trades   = len(df_f)
        wins_sum = df_f.loc[df_f["win"], "pnl"].sum()
        loss_sum = df_f.loc[~df_f["win"], "pnl"].abs().sum()
        pf       = wins_sum / loss_sum if loss_sum > 0 else float("inf")

        results_log.append({
            **params,
            "win_rate": win_rate,
            "trades":   trades,
            "pf":       round(pf, 2),
        })

        # 勝率70%以上で取引件数を最大化するスコア
        if win_rate >= TARGET_WR:
            score = win_rate * trades
            if score > best_score:
                best_score  = score
                best_params = params
                best_stats  = {
                    "win_rate": win_rate,
                    "trades":   trades,
                    "pf":       round(pf, 2),
                    "avg_pnl":  round(df_f["pnl"].mean(), 3),
                }

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{total} 完了...")

    # ── 70%未達の場合は最高勝率の結果を返す ─────────────
    if best_params is None:
        print(f"\n[optimize] 勝率{TARGET_WR*100:.0f}%以上の組み合わせが見つからなかったため、"
              f"最高勝率の結果を返します。")
        if results_log:
            results_log.sort(key=lambda x: x["win_rate"], reverse=True)
            best_row    = results_log[0]
            best_params = {k: best_row[k] for k in keys}
            best_stats  = {k: best_row[k]
                           for k in ["win_rate", "trades", "pf"]}

    return best_params, best_stats


# ================================================================
# 最良パラメータの詳細表示
# ================================================================

def print_trade_detail(df_all: pd.DataFrame, best_params: dict) -> None:
    """最良パラメータで実際のトレード一覧を表示する。"""
    rsi_buy  = best_params["rsi_buy"]
    rsi_sell = best_params["rsi_sell"]
    dev_buy  = best_params["dev_buy"]
    dev_sell = best_params["dev_sell"]
    gap_max  = best_params["gap_max"]

    buy_mask  = (df_all["rsi2"] <= rsi_buy)  & (df_all["dev5"] <= dev_buy)
    sell_mask = (df_all["rsi2"] >= rsi_sell) & (df_all["dev5"] >= dev_sell)
    df_f      = df_all[buy_mask | sell_mask].copy()

    range_ok = df_f["range_ratio"] >= 1.0
    vol_ok   = df_f["vol_ratio"]   >= 1.2
    df_f     = df_f[range_ok | vol_ok]
    df_f     = df_f[df_f["turnover"] >= TURNOVER_MIN]

    df_f["direction"] = np.where(buy_mask.reindex(df_f.index, fill_value=False),
                                 "BUY", "SELL")
    buy_gap  = (df_f["direction"] == "BUY")  & (df_f["gap_pct"] < 0)  & (df_f["gap_pct"] >= -gap_max)
    sell_gap = (df_f["direction"] == "SELL") & (df_f["gap_pct"] > 0)  & (df_f["gap_pct"] <= gap_max)
    df_f     = df_f[buy_gap | sell_gap]
    df_f["pnl"] = np.where(df_f["direction"] == "BUY",
                            df_f["pnl_buy_pct"], df_f["pnl_sell_pct"])
    df_f["win"] = df_f["pnl"] > 0

    print("\n  ── 上位10件（利益）────────────────────")
    for _, r in df_f.nlargest(10, "pnl").iterrows():
        icon = "✅" if r["win"] else "❌"
        print(f"    {icon} {r['date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl']:+.2f}%")

    print("\n  ── 下位10件（損失）────────────────────")
    for _, r in df_f.nsmallest(10, "pnl").iterrows():
        icon = "✅" if r["win"] else "❌"
        print(f"    {icon} {r['date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl']:+.2f}%")

    out = f"optimize_result.csv"
    df_f.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n  詳細を {out} に保存しました。")


# ================================================================
# メイン
# ================================================================

def main(start: str, end: str) -> None:
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  パラメータ最適化: {start} 〜 {end}  ({len(trading_days)}営業日)")
    print(f"{'='*60}\n")

    # ── データ取得 ─────────────────────────────────────
    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    data_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 10)
                  ).strftime("%Y-%m-%d")
    data_end   = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=3)
                  ).strftime("%Y-%m-%d")

    print(f"[optimize] {len(universe)} 銘柄のデータを取得中...")
    all_data = batch_download(tickers, start=data_start, end=data_end)

    # ── 事前計算 ───────────────────────────────────────
    df_all = precompute_all(all_data, trading_days, name_map)
    if df_all.empty:
        print("[optimize] データが空です。期間を確認してください。")
        return

    # ── グリッドサーチ ─────────────────────────────────
    best_params, best_stats = grid_search(df_all)

    # ── 結果表示 ───────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  最適化結果")
    print(f"{'='*60}")
    if best_params:
        print(f"  最良パラメータ:")
        for k, v in best_params.items():
            print(f"    {k:12s} = {v}")
        print(f"\n  成績:")
        print(f"    勝率           = {best_stats['win_rate']*100:.1f}%")
        print(f"    取引回数       = {best_stats['trades']} 件")
        print(f"    PF             = {best_stats['pf']}")
        if "avg_pnl" in best_stats:
            print(f"    平均損益       = {best_stats['avg_pnl']:+.3f}%")
        print(f"\n  screener.py への反映方法:")
        print(f"    RSI_BUY_MAX  = {best_params['rsi_buy']}")
        print(f"    RSI_SELL_MIN = {best_params['rsi_sell']}")
        print(f"    DEV_BUY_MAX  = {best_params['dev_buy']}")
        print(f"    DEV_SELL_MIN = {best_params['dev_sell']}")
        print(f"    GAP_MAX_PCT  = {best_params['gap_max']}")
        print_trade_detail(df_all, best_params)
    else:
        print("  条件を満たす組み合わせが見つかりませんでした。")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s, e = sys.argv[1], sys.argv[2]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] デフォルト期間: {s} 〜 {e}")
    main(s, e)

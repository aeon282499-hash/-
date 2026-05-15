"""
backtest_sell_extreme.py - SELL極みBT（高速ベクトル化版）
==========================================================

【戦略】
  「買われすぎた極限」で翌日寄り売り→引け買い戻し（信用売り）。
  特徴量を1回だけベクトル計算し、後でフィルター組合せを試す。

【出力】
  sell_extreme_features_<start>_<end>.csv に全候補のフィーチャーを記録
  → 別スクリプト or REPL で filter 組合せ探索

【特徴量】
  daily_gain    : 前日比%
  cum_gain_3d   : 3日連騰の累積%
  cum_gain_5d   : 5日連騰の累積%
  vol_ratio     : 出来高 / 20日平均
  ma25_dev      : 終値と25MAの乖離%
  rsi14         : 14日RSI
  close_pos     : (close - low) / (high - low)
  next_open     : 翌日始値
  next_close    : 翌日終値
  ret_pct       : SELL利益% = (next_open - next_close) / next_open * 100 - 0.2%

【フィルター（候補抽出のための緩い条件）】
  価格 >= 300 / 平均出来高 >= 10万 / ATR/終値 <= 5%
  daily_gain >= 3% OR cum_gain_3d >= 7% OR ma25_dev >= 10% OR rsi >= 75
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

MIN_PRICE        = 300
MIN_AVG_VOLUME   = 100_000
ATR_VOL_CAP      = 5.0
COMMISSION_RATE  = 0.002

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


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """1銘柄の全特徴量をベクトル計算。"""
    close = df["Close"]
    high  = df["High"]
    low   = df["Low"]
    vol   = df["Volume"]

    out = pd.DataFrame(index=df.index)
    out["close"] = close
    out["high"]  = high
    out["low"]   = low
    out["vol"]   = vol

    # 前日比%
    out["daily_gain"] = close.pct_change() * 100

    # 3日・5日連騰累積%
    out["cum_gain_3d"] = (close / close.shift(3) - 1) * 100
    out["cum_gain_5d"] = (close / close.shift(5) - 1) * 100

    # 出来高比（20日平均、シグナル日含めず=>shift1で過去20日）
    vol_avg_20 = vol.shift(1).rolling(20).mean()
    out["vol_ratio"] = vol / vol_avg_20.replace(0, np.nan)

    # 25MA乖離%
    ma25 = close.rolling(25).mean()
    out["ma25_dev"] = (close / ma25 - 1) * 100

    # RSI14
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    out["rsi14"] = 100 - (100 / (1 + rs))

    # 終値位置
    day_range = high - low
    out["close_pos"] = (close - low) / day_range.replace(0, np.nan)

    # ATR/終値%
    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    out["atr_pct"] = atr / close * 100

    # 翌日 open/close（shift -1で翌日値を持ってくる）
    out["next_open"]  = df["Open"].shift(-1)
    out["next_close"] = df["Close"].shift(-1)

    # 平均出来高（20日）
    out["avg_vol"] = vol.rolling(20).mean()

    return out


def run_backtest(start: str, end: str):
    print(f"\n{'='*60}")
    print(f"  SELL極みBT（高速版）期間: {start} 〜 {end}")
    print(f"{'='*60}\n")

    print("[load] cache 読込中...")
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    name_map = cache.get("name_map", {})
    print(f"[load] {len(all_data)} 銘柄")

    all_rows = []
    start_date = pd.Timestamp(start)
    end_date   = pd.Timestamp(end)

    n_processed = 0
    for ticker, df in all_data.items():
        if df is None or len(df) < 50:
            continue

        # 特徴量計算（インデックス=日付）
        feats = compute_features(df)

        # 期間絞り込み
        mask = (feats.index >= start_date) & (feats.index <= end_date)
        sub = feats[mask].copy()

        # 基本フィルター
        sub = sub[
            (sub["close"] >= MIN_PRICE)
            & (sub["avg_vol"] >= MIN_AVG_VOLUME)
            & (sub["atr_pct"] <= ATR_VOL_CAP)
            & sub["next_open"].notna()
            & sub["next_close"].notna()
        ]

        # 候補抽出: 4つの「極み」条件のいずれか
        cand_mask = (
            (sub["daily_gain"] >= 3.0)
            | (sub["cum_gain_3d"] >= 7.0)
            | (sub["ma25_dev"] >= 10.0)
            | (sub["rsi14"] >= 75)
        )
        sub = sub[cand_mask]
        if len(sub) == 0:
            continue

        # SELL シミュ: 翌日寄り売り→引け買戻し
        sub["ret_pct"] = (sub["next_open"] - sub["next_close"]) / sub["next_open"] * 100 - COMMISSION_RATE * 100

        sub["ticker"] = ticker
        sub["name"]   = name_map.get(ticker, ticker)
        sub["signal_date"] = sub.index.strftime("%Y-%m-%d")
        sub["entry_date"]  = (sub.index + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        all_rows.append(sub)
        n_processed += 1
        if n_processed % 500 == 0:
            print(f"  [{n_processed}/{len(all_data)}] 銘柄処理済 / 候補累計 {sum(len(r) for r in all_rows)} 件")

    print(f"[done] {n_processed} 銘柄処理完了")
    if not all_rows:
        print("候補なし")
        return

    df_all = pd.concat(all_rows, ignore_index=True)
    print(f"[done] 候補総数: {len(df_all)} 件")

    keep_cols = [
        "signal_date","entry_date","ticker","name","close",
        "daily_gain","cum_gain_3d","cum_gain_5d","vol_ratio","ma25_dev","rsi14","close_pos","atr_pct",
        "next_open","next_close","ret_pct",
    ]
    df_all = df_all[keep_cols]
    df_all["win"] = df_all["ret_pct"] > 0

    out = f"sell_extreme_features_{start}_{end}.csv"
    df_all.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[save] {out}")

    # 全候補での参考統計
    print(f"\n=== 候補全件 (フィルター無し) ===")
    print(f"  n={len(df_all)} 勝率={df_all['win'].mean()*100:.1f}% 平均={df_all['ret_pct'].mean():+.3f}%")
    gp = df_all[df_all['win']]['ret_pct'].sum()
    gl = abs(df_all[~df_all['win']]['ret_pct'].sum())
    print(f"  PF={gp/gl if gl else 0:.2f}")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) >= 2:
        s, e = args[0], args[1]
    else:
        s = "2023-01-01"
        e = "2025-12-31"
        print(f"[info] デフォルト期間: {s} 〜 {e}")
    run_backtest(s, e)

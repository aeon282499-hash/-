# -*- coding: utf-8 -*-
"""_sell_yose_check.py — 「買いは寄指なのに売りは成行」の非対称は正しいかの実測(2026-07-16)。
現行SELL設定(60/4/3/below_ma25)の全トレードにエントリー日ギャップを付与し、
(a) ギャップ方向別の成績 (b) SELL版寄指(指値=prev_close×0.99・寄りが指値未満=安寄りは
見送り)を適用した場合の成績 を出す。n=124の検出力の限界込みで判断材料にする。
"""
import pickle

import numpy as np
import pandas as pd

import bt_sell_grid_fast as B

B.END = "2026-07-09"

cache = pickle.load(open("jquants_cache.pkl", "rb"))
all_data = {tk: df for tk, df in cache["all_data"].items()
            if (cache["name_map"].get(tk) is not None or tk == "1321.T")}
nk_df = all_data["1321.T"].copy()
nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
df_long = B.build_long_df(all_data)
all_td = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})
td = B.trading_days_between(B.START, B.END)

trades = B.simulate(df_long, all_data, all_td, td, nk_df,
                    {"rsi_min": 60, "dev_min": 4.0, "day_change_min": 3.0}, "below_ma25")
print(f"[base] 現行SELL n={len(trades)}")

rows = []
for t in trades:
    df = all_data[t["ticker"]]
    ds = df.index.strftime("%Y-%m-%d")
    sig_rows = df[ds == t["signal_date"]]
    ent_rows = df[ds == t["entry_date"]]
    if sig_rows.empty or ent_rows.empty:
        continue
    pc = float(sig_rows["Close"].iloc[0])
    eo = float(ent_rows["Open"].iloc[0])
    rows.append({**t, "prev_close": pc, "entry_open": eo,
                 "gap": (eo - pc) / pc * 100})
T = pd.DataFrame(rows)


def stat(label, g):
    p = g["pnl_pct"]
    n = len(p)
    if n == 0:
        print(f"  {label:<34} 0件")
        return
    gw = p[p > 0].sum(); gl = -p[p < 0].sum()
    pf = gw / gl if gl > 0 else float("inf")
    print(f"  {label:<34} {n:>4}件 勝率{(p>0).mean()*100:>5.1f}% "
          f"平均{p.mean():+.2f}% PF{pf:>5.2f} 累積{p.sum():+.1f}%")


print("\n■ エントリー日ギャップ方向別（空売りは高く寄るほど有利のはず）")
stat("全体", T)
stat("ギャップアップ寄り(gap>+1%)", T[T["gap"] > 1])
stat("ほぼ変わらず(±1%)", T[(T["gap"] >= -1) & (T["gap"] <= 1)])
stat("ギャップダウン寄り(gap<-1%)", T[T["gap"] < -1])

print("\n■ SELL版寄指シミュレーション（指値=prev_close×0.99・寄り<指値は見送り）")
lim = T["entry_open"] >= T["prev_close"] * 0.99
stat("約定分(寄り≥指値)", T[lim])
stat("見送り分(安寄り<-1%相当)", T[~lim])
print(f"  → 見送り率 {(~lim).mean()*100:.1f}%")

print("\n■ 同・年別（約定分のみ vs 現行全部）")
T["year"] = pd.to_datetime(T["entry_date"]).dt.year
for y, g in T.groupby("year"):
    p_all = g["pnl_pct"]; p_lim = g.loc[lim[g.index], "pnl_pct"]
    print(f"  {y}: 現行 {len(p_all):>3}件 {p_all.sum():+6.1f}%  →  "
          f"寄指 {len(p_lim):>3}件 {p_lim.sum():+6.1f}%")

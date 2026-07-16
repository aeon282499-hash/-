# -*- coding: utf-8 -*-
"""_sell_grid_recheck.py — 「SELL現行設定(60/4/3/below_ma25)が一番か」最新データで再検証(2026-07-16)。
bt_sell_grid_fast の simulate をそのまま使い END を 2026-07-09 に更新して108パターン再実行。
オリジナルとの差分: ETF/REIT除外(name_mapなし銘柄スキップ=本番universe相当)を追加。
出力: 全108のCSV + 現行設定と上位案の年別内訳。
"""
import pickle
import time

import pandas as pd

import bt_sell_grid_fast as B

B.END = "2026-07-09"  # 旧グリッドは〜2026-05-02。関税ショック(2026-03/04)後まで延長

cache = pickle.load(open("jquants_cache.pkl", "rb"))
all_data = cache["all_data"]
name_map = cache["name_map"]
all_data = {tk: df for tk, df in all_data.items()
            if (name_map.get(tk) is not None or tk == "1321.T")}
print(f"[recheck] 銘柄数(ETF等除外後): {len(all_data)} / END={B.END}")

nk_df = all_data["1321.T"].copy()
nk_df["MA25"] = nk_df["Close"].rolling(25).mean()

t0 = time.time()
df_long = B.build_long_df(all_data)
print(f"[recheck] long format {len(df_long):,}行 / {time.time()-t0:.0f}s")

all_trading_days = sorted({d for df in all_data.values()
                           for d in df.index.strftime("%Y-%m-%d")})
trading_days = B.trading_days_between(B.START, B.END)

results = []
trades_by_key = {}
i = 0
for r in [60, 65, 70]:
    for d in [3.0, 4.0, 5.0]:
        for cch in [3.0, 4.0, 5.0]:
            for m in ["none", "below_ma25", "above_ma25", "below_ma5"]:
                i += 1
                params = {"rsi_min": r, "dev_min": d, "day_change_min": cch}
                trades = B.simulate(df_long, all_data, all_trading_days,
                                    trading_days, nk_df, params, m)
                s = B.summarize(trades, B.START, B.END)
                s.update({"RSI": r, "DEV": d, "DCH": cch, "MACRO": m})
                results.append(s)
                trades_by_key[(r, d, cch, m)] = trades
                print(f"[{i:3d}/108] RSI{r} DEV{d:.0f} DCH{cch:.0f} {m:<11} "
                      f"n={s['count']:>4} wr={s['win_rate']:>5.1f}% PF={s['pf']:>5.2f} "
                      f"cum={s['cum']:>+8.1f}%", flush=True)

R = pd.DataFrame(results)
R.to_csv("_sell_grid_recheck.csv", index=False, encoding="utf-8-sig")

print("\n=== 月1件以上 / PF降順 Top 12 ===")
top = R[R["month_avg"] >= 1.0].sort_values("pf", ascending=False).head(12)
print(top[["RSI", "DEV", "DCH", "MACRO", "count", "month_avg", "win_rate", "pf", "cum"]]
      .to_string(index=False))
print("\n=== 累積%降順 Top 8 ===")
print(R.sort_values("cum", ascending=False).head(8)
      [["RSI", "DEV", "DCH", "MACRO", "count", "month_avg", "win_rate", "pf", "cum"]]
      .to_string(index=False))

cur = R[(R.RSI == 60) & (R.DEV == 4.0) & (R.DCH == 3.0) & (R.MACRO == "below_ma25")].iloc[0]
print(f"\n=== 現行設定(60/4/3/below_ma25) ===")
print(f"  n={cur['count']} wr={cur['win_rate']:.1f}% PF={cur['pf']:.2f} cum={cur['cum']:+.1f}%")
rank_pf = (R[R["month_avg"] >= 1.0]["pf"] > cur["pf"]).sum() + 1
rank_cum = (R["cum"] > cur["cum"]).sum() + 1
print(f"  順位: PF {rank_pf}位(月1件以上内) / 累積 {rank_cum}位(全108)")


def yearly(key, label):
    trs = trades_by_key.get(key, [])
    print(f"\n--- 年別: {label} (n={len(trs)}) ---")
    T = pd.DataFrame(trs)
    if T.empty:
        print("  トレードなし")
        return
    T["year"] = pd.to_datetime(T["entry_date"]).dt.year
    for y, g in T.groupby("year"):
        p = g["pnl_pct"]
        gw = p[p > 0].sum(); gl = -p[p < 0].sum()
        pf = gw / gl if gl > 0 else float("inf")
        print(f"  {y}: {len(g):>3}件 勝率{(p>0).mean()*100:>5.1f}% "
              f"平均{p.mean():+.2f}% PF{pf:>5.2f} 累積{p.sum():+.1f}%")


yearly((60, 4.0, 3.0, "below_ma25"), "現行設定")
yearly((60, 4.0, 3.0, "none"), "現行条件でゲートなし")
best_pf = top.iloc[0]
yearly((int(best_pf["RSI"]), float(best_pf["DEV"]), float(best_pf["DCH"]),
        str(best_pf["MACRO"])), f"PF首位 {best_pf['RSI']:.0f}/{best_pf['DEV']:.0f}/"
       f"{best_pf['DCH']:.0f}/{best_pf['MACRO']}")
best_cum = R.sort_values("cum", ascending=False).iloc[0]
yearly((int(best_cum["RSI"]), float(best_cum["DEV"]), float(best_cum["DCH"]),
        str(best_cum["MACRO"])), f"累積首位 {best_cum['RSI']:.0f}/{best_cum['DEV']:.0f}/"
       f"{best_cum['DCH']:.0f}/{best_cum['MACRO']}")
print("\n[done]")

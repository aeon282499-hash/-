# -*- coding: utf-8 -*-
"""_bt_fibo_rankonly_0920.py — 本人「銘柄選定が悪い。朝一1〜2分の値上がりランキングにいる銘柄だけに絞ったら？」（2026-09-20）
ランキングの再現: 9:01〜9:02の値上がりランキング ≒ 寄り付きの値上がり率（公式始値÷前日終値）の日内順位。
  ＋ 9:15時点の値上がり率（9:00-9:15の高値÷前日終値）の順位も併記。母集団=5分足キャッシュ1,162銘柄（代金10億+）。
戦略: ①フィボ押し目買い 23.6のみ（損切り=当日安値割れ・利確+2%／+3%／引け）②本人仕様の買い下がりA 1:2:3（1段下損切り・+5%）
      ③比較: 9:30空売り→引け ④参考: 寄り値で買って引け（地合い）。
実行: python -X utf8 _bt_fibo_rankonly_0920.py > _log_fibo_rankonly_0920.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, pickle, time, sys
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read()
hdr = src.split("t0 = time.time(); C = pickle.load")[0]          # 定数・tick
exec(hdr)
sim_src = src.split("def simulate(e, alloc, target, stop_mode):")[1].split("\n\n\nrows = []")[0]
exec("def simulate(e, alloc, target, stop_mode):" + sim_src)      # 同じエンジン
TPV["引け"] = 9.99
t0 = time.time()
import os
CACHE = "_rankonly_cache_0920.pkl"
if os.path.exists(CACHE):
    D, bars_all = pickle.load(open(CACHE, "rb")); print(f"[cache] {CACHE} 読込 {len(D):,}銘柄日", flush=True)
else:
    C = pickle.load(open("_intraday_cache_5m.pkl", "rb")); A = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
    recs = []; bars_all = {}
    for tk, df in C.items():
        dd = A.get(tk)
        if dd is None: continue
        dd = dd[dd.Close > 0]; pc = dd["Close"].shift(1)
        dt = pd.to_datetime(df["dt"]).dt.tz_localize(None)
        g_all = df.assign(day=dt.dt.date, hm=dt.dt.strftime("%H:%M")).sort_values("dt")
        for day, g in g_all.groupby("day"):
            ts = pd.Timestamp(day)
            if ts not in dd.index or len(g) < 40: continue
            p = pc.get(ts, np.nan); o = float(dd.loc[ts, "Open"]); c_off = float(dd.loc[ts, "Close"])
            if not (p > 0) or o <= 0: continue
            g = g[g.hm != "09:00"].reset_index(drop=True); f15 = g[g.hm <= "09:15"]
            if len(f15) < 2: continue
            h15 = float(f15.h.max()); l15 = min(o, float(f15.l.min()))
            recs.append(dict(tk=tk, day=str(day), gap=(o / p - 1) * 100, rise15=(h15 / p - 1) * 100, rise_o=(h15 / o - 1) * 100, o=o, c=c_off, h15=h15, l15=l15))
            bars_all[(tk, str(day))] = g[["hm", "o", "h", "l", "c"]]
    D = pd.DataFrame(recs)
    D["rk_gap"] = D.groupby("day").gap.rank(ascending=False, method="first"); D["rk_15"] = D.groupby("day").rise15.rank(ascending=False, method="first")
    keep = D[(D.rk_gap <= 30) | (D.rk_15 <= 30) | (D.rise_o >= 5) | (D.gap >= 3)]
    bars_all = {(r.tk, r.day): bars_all[(r.tk, r.day)] for r in keep.itertuples()}
    pickle.dump((D, bars_all), open(CACHE, "wb")); print(f"[cache] 保存 {len(bars_all):,}銘柄日の5分足", flush=True)
print(f"[all] 銘柄日 {len(D):,}・{D.day.nunique()}日・母集団 日平均{len(D)/D.day.nunique():.0f}銘柄（{time.time()-t0:.0f}s）", flush=True)
print(f"  寄りギャップ順位≤10の下限 中央値 +{D[D.rk_gap <= 10].groupby('day').gap.min().median():.1f}% ／ ≤20 +{D[D.rk_gap <= 20].groupby('day').gap.min().median():.1f}% ／ ≤30 +{D[D.rk_gap <= 30].groupby('day').gap.min().median():.1f}%")
COST = 0.1
def evaluate(sub, label):
    rows = []
    for r in sub.itertuples():
        e = dict(tk=r.tk, day=r.day, o=r.o, c=r.c, h15=r.h15, l15=r.l15, bars=bars_all[(r.tk, r.day)]); b = e["bars"]
        out = dict(tk=r.tk, day=r.day, gap=r.gap, drift=(r.c / r.o - 1) * 100 - COST)
        hm = b.hm.to_numpy(); cl = b.c.to_numpy(float); i30 = np.where(hm == "09:30")[0]
        out["short930"] = ((cl[i30[0]] - r.c) / cl[i30[0]] * 100 - COST) if len(i30) and cl[i30[0]] > 0 else np.nan
        for nm, alloc, tg, sm in (("E+2%", ALLOC["E 23.6のみ"], "+2%", "安値割れ"), ("E+3%", ALLOC["E 23.6のみ"], "+3%", "安値割れ"), ("E引け", ALLOC["E 23.6のみ"], "引け", "安値割れ"),
                                  ("A+5%", ALLOC["A 23.6/38.2/50=1:2:3"], "+5%", "1段下"), ("A引け", ALLOC["A 23.6/38.2/50=1:2:3"], "引け", "安値割れ")):
            s = simulate(e, alloc, tg, sm)
            out[nm + "_R"] = s["R"] if s else np.nan; out[nm + "_fill"] = (s["n_fill"] > 0) if s else False; out[nm + "_win"] = (1.0 if s["pnl_yen"] > 0 else 0.0) if s and s["n_fill"] > 0 else np.nan
        rows.append(out)
    R = pd.DataFrame(rows)
    def cell(nm):
        f = R[R[nm + "_fill"]]; return f"{R[nm + '_R'].mean():>+6.2f}R({f[nm + '_win'].mean()*100 if len(f) else 0:>3.0f}%/{len(f)/max(len(R),1)*100:>3.0f}%)"
    print(f"  {label:<26} n{len(R):>5}({len(R)/max(R.day.nunique(),1):>4.1f}本/日) 寄→引{R.drift.mean():>+5.2f}%({(R.drift > 0).mean()*100:>3.0f}%) | " + " ".join(f"{nm}{cell(nm)}" for nm in ("E+2%", "E+3%", "E引け", "A+5%", "A引け"))
          + f" | 9:30売り{R.short930.mean():>+5.2f}%({(R.short930 > 0).mean()*100:>3.0f}%)", flush=True)
print("\n■ 母集団別（R=3万円リスク・( )内=約定ありの勝率/約定率・9:30売りは件あたり%）", flush=True)
evaluate(D[D.rise_o >= 5], "参考: 9:15までに寄り比+5%(前回)")
for n in (5, 10, 20, 30):
    evaluate(D[D.rk_gap <= n], f"寄りギャップ順位 ≤{n}")
for n in (10, 20):
    evaluate(D[(D.rk_gap <= n) & (D.gap >= 5)], f"寄りギャップ順位 ≤{n} ×gap≥5%")
for n in (10, 20, 30):
    evaluate(D[D.rk_15 <= n], f"9:15値上がり順位 ≤{n}")
evaluate(D[(D.rk_gap <= 20) & (D.rise_o >= 3)], "ギャップ順位≤20 ×寄り後さらに+3%")
evaluate(D[(D.rk_gap > 30) & (D.gap >= 3)], "参考: ランキング外(順位>30)でgap≥3%")
print("\n■ ギャップ順位≤20 の内訳: ギャップの大きさ別（E+2%／9:30売り）", flush=True)
S = D[D.rk_gap <= 20]
for lo, hi in ((-99, 3), (3, 6), (6, 10), (10, 999)):
    q = S[(S.gap >= lo) & (S.gap < hi)]
    if len(q): evaluate(q, f"  順位≤20 gap {lo}〜{hi}%")
print(f"\n[done] {time.time()-t0:.0f}s")

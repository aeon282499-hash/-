# -*- coding: utf-8 -*-
"""_bt_surge_short_nanpin_0920.py — 「噴いた銘柄を9:30に空売り→引け」の頑健性と、売り側ナンピン（担がれたら売り増し）の検証（2026-09-20）
対象: 寄り後15分で+5%噴いた581銘柄日（Yahoo5分足60日×立花公式値）。往復コスト0.1%。
A 頑健性: 月別・上位3日/上位3%玉除去・貸借○(現在の貸借リスト)のみ・噴き別
B ナンピン: 9:30に100万相当を売り→建値+2%/+3%/+5%で同額を売り増し（足の高値が水準+2ティック以上で約定）→引け。損切りなし／平均+7%で買戻し
実行: python -X utf8 _bt_surge_short_nanpin_0920.py > _log_surge_short_nanpin_0920.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, sys, os
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read(); exec(src.split("rows = []\nfor e in events:")[0])
ev2 = [e for e in events if e["rise_o"] >= 5]; COST = 0.1
def pf(x): x = np.asarray(x, float); g = x[x > 0].sum(); l = -x[x <= 0].sum(); return g / l if l else np.inf
def tick(px):
    for lim, t in ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3e6, 1000)):
        if px <= lim: return t
    return 5000
# 貸借○（現在のリスト・J-Quants）
iss = {}
try:
    import daytrade_paper as dp
    iss = dp.fetch_iss_map(dp._jq_token())
except Exception as ex:
    print(f"[iss] 取得失敗 {ex}")
def base_rows(t_entry):
    rows = []
    for e in ev2:
        b = e["bars"]; hm = b.hm.to_numpy(); cl = b.c.to_numpy(float); H = b.h.to_numpy(float)
        idx = np.where(hm == t_entry)[0]
        if len(idx) == 0: continue
        k = idx[0]; p = cl[k]
        if not (p > 0): continue
        rows.append(dict(tk=e["tk"], day=e["day"], ym=e["day"][:7], rise_o=e["rise_o"], iss=iss.get(e["tk"][:4]) == "2",
                         ret=(p - e["c"]) / p * 100 - COST, k=k, p=p))
    return pd.DataFrame(rows)
print(f"[T2] {len(ev2)}銘柄日・貸借リスト{len(iss)}銘柄", flush=True)
for t_entry in ("09:20", "09:30"):
    R = base_rows(t_entry)
    print(f"\n■ A {t_entry}空売り→引け（n{len(R)}）: 平均{R.ret.mean():+.2f}% 中央値{R.ret.median():+.2f}% 勝率{(R.ret > 0).mean()*100:.0f}% PF{pf(R.ret):.2f}")
    d = R.groupby("day").ret.mean().sort_values(ascending=False)
    R3 = R[~R.day.isin(d.index[:3])]; print(f"  上位3日除去: 平均{R3.ret.mean():+.2f}% PF{pf(R3.ret):.2f} 勝率{(R3.ret > 0).mean()*100:.0f}% ／ 上位3%玉除去: 平均{R[R.ret < R.ret.quantile(.97)].ret.mean():+.2f}% PF{pf(R[R.ret < R.ret.quantile(.97)].ret):.2f}")
    for ym, q in R.groupby("ym"): print(f"  {ym}: n{len(q):>3} 平均{q.ret.mean():+.2f}% 勝率{(q.ret > 0).mean()*100:.0f}% PF{pf(q.ret):.2f} 日別勝ち{(q.groupby('day').ret.mean() > 0).sum()}/{q.day.nunique()}")
    for lab, m in (("貸借○(制度で売れる)", R.iss), ("貸借✕(ハイカラ在庫が要る)", ~R.iss)):
        q = R[m]; print(f"  {lab}: n{len(q):>3} 平均{q.ret.mean():+.2f}% 勝率{(q.ret > 0).mean()*100:.0f}% PF{pf(q.ret):.2f}")
    for lo, hi in ((5, 8), (8, 12), (12, 999)):
        q = R[(R.rise_o >= lo) & (R.rise_o < hi)]; print(f"  噴き+{lo}〜{hi}%: n{len(q):>3} 平均{q.ret.mean():+.2f}% 勝率{(q.ret > 0).mean()*100:.0f}% PF{pf(q.ret):.2f}")
# ── B 売り側ナンピン（9:30建て） ──
print("\n■ B 9:30空売り＋担がれたら同額を売り増し（100万相当×2まで・引け買戻し・円=100万あたり）", flush=True)
def nanpin(add_pct, stop_pct):
    rows = []
    for e in ev2:
        b = e["bars"]; hm = b.hm.to_numpy(); cl = b.c.to_numpy(float); H = b.h.to_numpy(float); n = len(b)
        idx = np.where(hm == "09:30")[0]
        if len(idx) == 0: continue
        k = idx[0]; p = cl[k]; c_off = e["c"]
        if not (p > 0): continue
        legs = [(p, 1.0)]; add_px = p * (1 + add_pct / 100) if add_pct else None; exit_px = None
        for j in range(k + 1, n):
            if hm[j] > "14:30" and add_px: add_px = None
            if add_px and H[j] >= add_px + 2 * tick(add_px):
                legs.append((add_px, 1.0)); add_px = None
            if stop_pct:
                avg = sum(px * w for px, w in legs) / sum(w for _, w in legs); sp = avg * (1 + stop_pct / 100)
                if H[j] >= sp: exit_px = sp * 1.002; break
        if exit_px is None: exit_px = c_off
        w = sum(w_ for _, w_ in legs); avg = sum(px * w_ for px, w_ in legs) / w
        yen = (avg - exit_px) / avg * w * 1_000_000 - COST / 100 * w * 1_000_000
        rows.append(dict(day=e["day"], yen=yen, legs=len(legs), stopped=exit_px != c_off))
    return pd.DataFrame(rows)
print(f"  {'設定':<28}{'n':>5}{'売り増し発生':>8}{'平均円/回':>10}{'勝率':>6}{'PF':>6}{'合計万':>8}{'最悪日万':>8}{'損切り率':>8}")
for add in (None, 2, 3, 5):
    for stop in (None, 7):
        R = nanpin(add, stop); d = R.groupby("day").yen.sum()
        print(f"  {'売り増しなし' if add is None else f'+{add}%で売り増し':<14}{'損切りなし' if stop is None else '平均+7%損切':<14}{len(R):>5}{(R.legs > 1).mean()*100:>7.0f}%{R.yen.mean():>+10,.0f}{(R.yen > 0).mean()*100:>5.0f}%{pf(R.yen):>6.2f}{R.yen.sum()/1e4:>+8.0f}{d.min()/1e4:>+8.1f}{R.stopped.mean()*100:>7.0f}%")
print("\n[done]")

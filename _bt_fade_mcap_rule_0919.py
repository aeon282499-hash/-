# -*- coding: utf-8 -*-
"""_bt_fade_mcap_rule_0919.py — 時価総額ルールを本番向けの形に落とす（2026-09-19・10年 2017-26・J-Quants valuation）
 A 絶対しきい値で大型を除外（>300/500/700/1000/1500/2000億）
 B 帯別サイズ（正規化なし・実額）: 小型≤300億 ①130/②65・中 100/50・大型≥1000億 50/25 or 0
 C 除外＋傾斜の重ね
 D 年別（現行 vs 候補）・12か月ローリング・最大同時建玉
実行: python -X utf8 _bt_fade_mcap_rule_0919.py > _log_fade_mcap_rule_0919.txt"""
from __future__ import annotations
import pickle, time
import numpy as np, pandas as pd
from _bt_fade_26y_lib import load, base_mask, rank, settle, stats, S1, S2, ERAS, pf

t0 = time.time()
D = load("20y"); P = D[base_mask(D)].copy().reset_index(drop=True); P["sig"] = P.sig.astype(str); P["ent"] = P.ent.astype(str); n = len(P)
SIGD = pd.to_datetime(P.sig); Y17 = (P.y >= 2017).to_numpy()
VAL = pickle.load(open("_valuation_10y.pkl", "rb")); VC = VAL["cols"]; VD = VAL["data"]; VB = np.datetime64(VAL["base"])
sday = (SIGD.to_numpy().astype("datetime64[D]") - VB).astype(float); tick4 = P.ticker.astype(str).str[:4].to_numpy()
j = VC.index("MktCap"); MCAP = np.full(n, np.nan)
for i in np.where(Y17)[0]:
    m = VD.get(tick4[i])
    if m is None or len(m) == 0: continue
    k = np.searchsorted(m[:, 0], sday[i], side="left") - 1
    if k >= 0 and sday[i] - m[k, 0] <= 10: MCAP[i] = m[k, j] * 1e6
P["mcap"] = MCAP; okm = Y17 & np.isfinite(MCAP)
P10 = P[Y17].copy()


def risk(b):
    s = b.groupby("ent").yen.sum().sort_index(); s.index = pd.to_datetime(s.index)
    cum = s.cumsum(); dd = (cum - cum.cummax()).min() / 1e4
    starts = pd.date_range(cum.index.min().to_period("M").to_timestamp(), cum.index.max() - pd.DateOffset(months=12), freq="MS"); mins = []
    for st in starts:
        base = cum[cum.index < st].iloc[-1] if (cum.index < st).any() else 0.0
        w = cum[(cum.index >= st) & (cum.index < st + pd.DateOffset(months=12))]; mins.append((w.min() - base) if len(w) else 0.0)
    mins = np.array(mins); return dict(dd=dd, w12=mins.min() / 1e4, b30=(mins < -3e5).mean() * 100, b50=(mins < -5e5).mean() * 100)
def ex20(b): day = b.groupby("ent").yen.sum(); return (b.yen.sum() - day.nlargest(20).sum()) / 1e4
def settle_sz(d, s1_col, s2_col):
    d = d[d.rk <= 2].copy(); size = np.where(d.rk.to_numpy() == 1, d[s1_col].to_numpy(float), d[s2_col].to_numpy(float))
    d["size"] = size; d["sh"] = (size / d.px // 100 * 100).astype(int); d = d[d.sh > 0].copy(); d["yen"] = d.pnl / 100 * d.sh * d.o1; return d
def run10(mask=None, s1=None, s2=None):
    Q = P10 if mask is None else P10[mask]; d = rank(Q)
    if s1 is None: return settle(d, (S1, S2), 2)
    return settle_sz(d, s1, s2)
def line(lab, b, base=None):
    s = stats(b); r = risk(b); x20 = ex20(b); mx = b.groupby("ent")["size"].sum().max() / 1e4; avg1 = b[b.rk == 1]["size"].mean() / 1e4
    j_ = ""
    if base is not None:
        j_ = " ★" if (s["e3"] >= base["e3"] and s["e4"] >= base["e4"] and s["total"] > base["total"] and s["PF"] >= base["PF"] and x20 > base["ex20"]) else ""
    print(f"  {lab:<40} n{s['n']:>5} 勝率{s['win%']:>5.1f}% PF{s['PF']:.2f} 10年{s['total']/1e4:>+6,.0f}万 | 17-21{s['e3']/1e4:>+6,.0f} 22-26{s['e4']/1e4:>+6,.0f} | 勝年{s['win_years']}/{s['n_years']} 最悪年{s['worst_year']/1e4:>+4,.0f} 最悪月{s['worst_month']/1e4:>+4,.0f} 最悪日{s['worst_day']/1e4:>+4,.0f} 除20日{x20:>+6,.0f} | DD{r['dd']:>+5,.0f} 12か月最悪{r['w12']:>+4,.0f} -30万割れ{r['b30']:>3.0f}% | ①平均{avg1:.0f}万 最大同時{mx:.0f}万" + j_, flush=True)
    s["ex20"] = x20; s.update(r); return s
def band_cols(small_max, large_min, s_small, s_mid, s_large, s2_ratio=0.5):
    mc = P10.mcap.to_numpy(); fin = np.isfinite(mc)
    s1 = np.where(fin & (mc <= small_max), s_small, np.where(fin & (mc >= large_min), s_large, s_mid)).astype(float)
    P10["_s1"] = s1; P10["_s2"] = s1 * s2_ratio; return "_s1", "_s2"

print("■ 土台（現行 ①100/②50・2017-26）", flush=True); B = line("現行", run10())
mc = P10.mcap.to_numpy(); fin = np.isfinite(mc)
print("\n■ A 絶対しきい値で大型を除外（不明は残す）", flush=True)
for th in (3e10, 5e10, 7e10, 1e11, 1.5e11, 2e11, 3e11, 5e11):
    m = ~(fin & (mc > th)); line(f"時価総額>{th/1e8:,.0f}億を除外（除外{(fin & (mc > th)).mean()*100:.0f}%）", run10(m), B)
print("\n■ B 帯別サイズ（実額・①/②=2:1）", flush=True)
for lab, args in (
    ("≤300億130 / 中100 / ≥1000億50", (3e10, 1e11, 130e4, 100e4, 50e4)),
    ("≤300億130 / 中100 / ≥1000億0(撃たない)", (3e10, 1e11, 130e4, 100e4, 0)),
    ("≤300億100 / 中100 / ≥1000億50", (3e10, 1e11, 100e4, 100e4, 50e4)),
    ("≤300億100 / 中100 / ≥700億50", (3e10, 7e10, 100e4, 100e4, 50e4)),
    ("≤300億100 / 中80 / ≥700億50", (3e10, 7e10, 100e4, 80e4, 50e4)),
    ("≤200億130 / 中100 / ≥700億50", (2e10, 7e10, 130e4, 100e4, 50e4)),
    ("≤300億120 / 中90 / ≥1000億50", (3e10, 1e11, 120e4, 90e4, 50e4)),
    ("≤500億100 / ≥500億50", (5e10, 5e10, 100e4, 100e4, 50e4)),
    ("≤1000億100 / ≥1000億50", (1e11, 1e11, 100e4, 100e4, 50e4)),
    ("≤1000億100 / ≥1000億30", (1e11, 1e11, 100e4, 100e4, 30e4)),
):
    s1c, s2c = band_cols(*args); line(lab, run10(None, s1c, s2c), B)
print("\n■ C 除外＋傾斜", flush=True)
for th in (7e10, 1e11):
    m = ~(fin & (mc > th)); s1c, s2c = band_cols(3e10, th, 130e4, 100e4, 0); line(f">{th/1e8:,.0f}億除外 ＋ ≤300億130", run10(m, s1c, s2c), B)
print("\n■ D 年別（確定損益万円・現行 / >1000億除外 / ≤300億130・中100・≥1000億50）", flush=True)
b1 = run10(); b2 = run10(~(fin & (mc > 1e11))); s1c, s2c = band_cols(3e10, 1e11, 130e4, 100e4, 50e4); b3 = run10(None, s1c, s2c)
ys = sorted(set(b1.y))
for bb, nm in ((b1, "現行"), (b2, ">1000億除外"), (b3, "帯別130/100/50")):
    yy = bb.groupby("y").yen.sum() / 1e4; print(f"  {nm:<16} " + " ".join(f"{y}:{yy.get(y, 0):+.0f}" for y in ys))
print("\n■ 参考: 時価総額帯ごとの候補の質（2017-26・件あたり%）", flush=True)
for lo, hi, nm in ((0, 1e10, "<100億"), (1e10, 3e10, "100〜300億"), (3e10, 7e10, "300〜700億"), (7e10, 1e11, "700〜1000億"), (1e11, 3e11, "1000〜3000億"), (3e11, 1e99, ">3000億")):
    q = P10[fin & (mc >= lo) & (mc < hi)]
    print(f"    {nm:<12} n={len(q):>5} 件あたり{q.pnl.mean():>+6.2f}% 勝率{(q.pnl>0).mean()*100:>5.1f}% | 17-21{q[q.y<=2021].pnl.mean():>+6.2f}(n{len(q[q.y<=2021])}) 22-26{q[q.y>=2022].pnl.mean():>+6.2f}(n{len(q[q.y>=2022])})")
print(f"\n[done] {time.time()-t0:.0f}s")

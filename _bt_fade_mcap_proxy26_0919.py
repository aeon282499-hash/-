# -*- coding: utf-8 -*-
"""_bt_fade_mcap_proxy26_0919.py — 時価総額「上位1/3除外」(10年★候補)の26年代理検証（2026-09-19）
10年で 時価総額上位1/3(>733億)の急騰玉は件あたり-0.12%/勝率52% → 除外で+1,507→+1,600万/PF1.54→1.70/DD-86→-60。
時価総額は10年しか無い(J-Quants valuation 2016-09〜)ので、立花26年で使える代理＝平均売買代金(vol_avg×px・20日平均出来高×株価)で
 ①代理と時価総額の相関(2017-26) ②年内三分位の上位除外を4時代で ③絶対帯(平均代金>30/50/100億除外) ④並び順(小さい代金優先) ⑤噪音床
実行: python -X utf8 _bt_fade_mcap_proxy26_0919.py > _log_fade_mcap_proxy26_0919.txt"""
from __future__ import annotations
import pickle, time
import numpy as np, pandas as pd
from _bt_fade_26y_lib import load, base_mask, rank, settle, stats, S1, S2, ERAS, pf

t0 = time.time()
D = load("20y"); P = D[base_mask(D)].copy().reset_index(drop=True); P["sig"] = P.sig.astype(str); P["ent"] = P.ent.astype(str); n = len(P)
SIGD = pd.to_datetime(P.sig); Y17 = (P.y >= 2017).to_numpy()
P["avg_tov"] = P.vol_avg * P.px          # 20日平均出来高×株価 ≈ 平時の日次売買代金
# 時価総額（10年）を結合して代理との相関を見る
VAL = pickle.load(open("_valuation_10y.pkl", "rb")); VC = VAL["cols"]; VD = VAL["data"]; VB = np.datetime64(VAL["base"])
sday = (SIGD.to_numpy().astype("datetime64[D]") - VB).astype(float); tick4 = P.ticker.astype(str).str[:4].to_numpy()
j = VC.index("MktCap"); MCAP = np.full(n, np.nan)
for i in np.where(Y17)[0]:
    m = VD.get(tick4[i])
    if m is None or len(m) == 0: continue
    k = np.searchsorted(m[:, 0], sday[i], side="left") - 1
    if k >= 0 and sday[i] - m[k, 0] <= 10: MCAP[i] = m[k, j] * 1e6
P["mcap"] = MCAP
ok = Y17 & np.isfinite(MCAP) & (P.avg_tov.to_numpy() > 0)
r = np.corrcoef(np.log(MCAP[ok]), np.log(P.avg_tov.to_numpy()[ok]))[0, 1]
q1m, q2m = np.nanquantile(MCAP[ok], [1/3, 2/3]); big = ok & (MCAP > q2m)
print(f"[proxy] 2017-26 候補{ok.sum()}件: log時価総額 vs log平均代金 相関 r={r:.3f}・時価総額上位1/3(>{q2m/1e8:,.0f}億)の平均代金 中央値{np.median(P.avg_tov.to_numpy()[big])/1e8:,.1f}億 / それ以外 {np.median(P.avg_tov.to_numpy()[ok & ~big])/1e8:,.1f}億", flush=True)
# 代理の「年内三分位」（候補プール内・その年の分布で上位1/3）→ 時価総額上位1/3との一致率
P["tov_q"] = P.groupby("y").avg_tov.rank(pct=True)
hi_proxy = (P.tov_q > 2/3).to_numpy()
agree = (hi_proxy[ok] == big[ok]).mean(); print(f"[proxy] 年内三分位(平均代金上位1/3) と 時価総額上位1/3 の一致率 {agree*100:.0f}%・代理上位のうち時価総額も上位 {(big[ok & hi_proxy]).mean()*100:.0f}%", flush=True)


def risk(b, y0=2017):
    s = b[b.y >= y0].groupby("ent").yen.sum().sort_index(); s.index = pd.to_datetime(s.index)
    if len(s) == 0: return dict(dd=np.nan, w12=np.nan)
    cum = s.cumsum(); dd = (cum - cum.cummax()).min() / 1e4
    starts = pd.date_range(cum.index.min().to_period("M").to_timestamp(), cum.index.max() - pd.DateOffset(months=12), freq="MS"); mins = []
    for st in starts:
        base = cum[cum.index < st].iloc[-1] if (cum.index < st).any() else 0.0
        w = cum[(cum.index >= st) & (cum.index < st + pd.DateOffset(months=12))]; mins.append((w.min() - base) if len(w) else 0.0)
    return dict(dd=dd, w12=min(mins) / 1e4 if mins else np.nan)
def ex20(b): day = b.groupby("ent").yen.sum(); return (b.yen.sum() - day.nlargest(20).sum()) / 1e4
def run_b(mask=None, cols=("dev", "atr"), weights=None, asc=None):
    Q = P if mask is None else P[mask]; return settle(rank(Q, cols, weights, asc), (S1, S2), 2)
def line26(lab, b, base=None, noise=None):
    s = stats(b); rr = risk(b); x20 = ex20(b); j = ""
    if base is not None:
        ok_ = (s["total"] > base["total"] and sum(s[nm] >= base[nm] for _, _, nm in ERAS) >= 3 and s["e4"] >= base["e4"] * 0.95 and s["y10"] > base["y10"] and x20 > base["ex20"] and (noise is None or s["total"] / 1e4 > noise))
        j = " ★" if ok_ else ("  (26年○)" if s["total"] > base["total"] and s["y10"] > base["y10"] else "")
    print(f"  {lab:<40} n{s['n']:>5} 勝率{s['win%']:>5.1f}% PF{s['PF']:.2f} 26年{s['total']/1e4:>+7,.0f}万 | " + "/".join(f"{s[nm]/1e4:>+6,.0f}" for _, _, nm in ERAS)
          + f" PF " + "/".join(f"{s[nm+'_pf']:.2f}" for _, _, nm in ERAS) + f" | 10年{s['y10']/1e4:>+6,.0f} 勝年{s['win_years']}/{s['n_years']} 最悪年{s['worst_year']/1e4:>+5,.0f} 最悪月{s['worst_month']/1e4:>+4,.0f} 除20日{x20:>+7,.0f} | 10年DD{rr['dd']:>+5,.0f} 12か月最悪{rr['w12']:>+4,.0f}" + j, flush=True)
    s["ex20"] = x20; s.update(rr); return s
def pm(lab, m):
    q = P[m]; parts = " ".join(f"{nm}{q[(q.y >= a) & (q.y <= c)].pnl.mean():+.2f}%(n{len(q[(q.y >= a) & (q.y <= c)])})" for a, c, nm in ERAS)
    print(f"    {lab:<32} n={len(q):>5} 件あたり{q.pnl.mean():>+6.2f}% 勝率{(q.pnl>0).mean()*100:>5.1f}% | {parts}", flush=True)
rng = np.random.default_rng(1)
def noise26(frac, seeds=30):
    k = int(n * frac); tots = []
    for _ in range(seeds):
        m = np.ones(n, dtype=bool); m[rng.choice(n, size=k, replace=False)] = False; tots.append(stats(run_b(m))["total"] / 1e4)
    return float(np.quantile(tots, 0.99)), float(np.std(tots))
def excl26(lab, m):
    frac = 1 - m.mean(); nf, sd = noise26(frac) if 0 < frac < 0.9 else (None, 0)
    s = line26(lab, run_b(m), B26, nf); print(f"      （除外率{frac*100:.0f}%・同率ランダム除外の99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）", flush=True); return s

print("\n■ 土台", flush=True); B26 = line26("現行 ①100/②50", run_b())
print("\n■ 参考: 10年の時価総額上位1/3除外（本体スクリプトの再現）", flush=True)
line26("時価総額上位1/3除外(2017-26のみ適用)", run_b(~big), B26)
print("\n■ 代理=平均代金（vol_avg×px）の層別（候補・26年）", flush=True)
tq = P.tov_q.to_numpy()
pm("年内 下位1/3", tq <= 1/3); pm("年内 中位", (tq > 1/3) & (tq <= 2/3)); pm("年内 上位1/3", tq > 2/3)
at = P.avg_tov.to_numpy()
for lo, hi, nm in ((0, 5e8, "<5億"), (5e8, 1e9, "5〜10億"), (1e9, 3e9, "10〜30億"), (3e9, 1e10, "30〜100億"), (1e10, 1e99, ">100億")):
    pm(f"平均代金{nm}", (at >= lo) & (at < hi))
print("\n■ 除外テスト（26年・4時代）", flush=True)
excl26("平均代金 年内上位1/3を除外", tq <= 2/3)
excl26("平均代金 年内上位1/4を除外", tq <= 3/4)
excl26("平均代金 年内上位1/2を除外", tq <= 1/2)
excl26("平均代金>100億を除外", at <= 1e10)
excl26("平均代金>50億を除外", at <= 5e9)
excl26("平均代金>30億を除外", at <= 3e9)
excl26("平均代金>20億を除外", at <= 2e9)
# 全銘柄横断の順位（その日の全上場銘柄の中での平均代金%点）は無いので、候補内の年内順位と絶対帯で代用
print("\n■ 並び順（現行mix + w×平均代金の日内順位・小さい方を優先）", flush=True)
for w in (0.3, 0.6, 1.0):
    line26(f"小型(平均代金小)優先 w{w}", run_b(cols=("dev", "atr", "avg_tov"), weights=[1, 1, 2 * w], asc=[False, False, True]), B26)
for w in (0.3, 0.6):
    line26(f"大型優先 w{w}(逆向き確認)", run_b(cols=("dev", "atr", "avg_tov"), weights=[1, 1, 2 * w], asc=[False, False, False]), B26)
print("\n■ 10年側: 代理で同じ除外をした時の10年（時価総額除外との比較）", flush=True)
for lab, m in (("平均代金 年内上位1/3除外", tq <= 2/3), ("平均代金>50億除外", at <= 5e9), ("時価総額上位1/3除外", ~big)):
    b = run_b(m); b = b[b.y >= 2017]; s = stats(b); rr = risk(b)
    print(f"  {lab:<32} n{s['n']:>5} 勝率{s['win%']:>5.1f}% PF{s['PF']:.2f} 10年{s['total']/1e4:>+6,.0f}万 | 17-21{s['e3']/1e4:>+6,.0f} 22-26{s['e4']/1e4:>+6,.0f} | 最悪年{s['worst_year']/1e4:>+4,.0f} 最悪月{s['worst_month']/1e4:>+4,.0f} DD{rr['dd']:>+5,.0f} 12か月最悪{rr['w12']:>+4,.0f}", flush=True)
print("\n■ 年別（現行 vs 平均代金上位1/3除外・確定損益万円）", flush=True)
b1 = run_b(); b2 = run_b(tq <= 2/3)
y1 = b1.groupby("y").yen.sum() / 1e4; y2 = b2.groupby("y").yen.sum() / 1e4
print("  " + " ".join(f"{y}:{y1.get(y, 0):+.0f}/{y2.get(y, 0):+.0f}" for y in sorted(set(y1.index) | set(y2.index))))
print(f"\n[done] {time.time()-t0:.0f}s")

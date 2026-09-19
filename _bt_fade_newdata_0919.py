# -*- coding: utf-8 -*-
"""_bt_fade_newdata_0919.py — 売りフェード「新データで銘柄選定を変えられるか」（2026-09-19 本人「SBI/J-Q/立花のデータが揃った・銘柄選定かえたりしよう・①100/②50は確証バイアスかも」）
土台: _fade_pool_26y.pkl（立花26年・load("20y")・現行入口）・選定=乖離+ATRの百分位平均・①100万/②50万・寄成→引成。
 A J-Quants バリュエーション（/equities/valuation・2016-09〜・時価総額/PBR/PER/ROE/EPS/予想EPS）× フェード 10年（2017-26）
   除外・帯・並び順への混ぜ込み・サイズ傾斜。噪音床=同率ランダム除外30シードの99%点／乱数特徴量の並び順。
 B 業種相対の急騰（26年・立花全銘柄→33業種の等加重日次リターン）: 業種ぐるみ vs 単独急騰・並び順・除外（キュー[F10]）
 C ATR正規化サイズ（26年・平均100万に正規化・キュー[F14]）
 D ①玉 vs ②玉の質（時代別・円あたり）と 現金50万前提のサイズ表（DD・最悪月・12か月ローリング最悪）
判定: 10年物=17-21/22-26とも土台以上・PF・最悪年・噪音床超え。26年物=26年計・4時代中3・22-26の95%・10年・上位20日除去・噪音床。
実行: python -X utf8 _bt_fade_newdata_0919.py > _log_fade_newdata_0919.txt"""
from __future__ import annotations
import json, pickle, time
import numpy as np, pandas as pd
from _bt_fade_26y_lib import load, base_mask, rank, settle, stats, S1, S2, ERAS, pf, quint

t0 = time.time()
D = load("20y"); P = D[base_mask(D)].copy().reset_index(drop=True); P["sig"] = P.sig.astype(str); P["ent"] = P.ent.astype(str); n = len(P)
print(f"[pool] 候補{n:,} {P.sig.min()}〜{P.sig.max()} 1日平均{n/P.sig.nunique():.1f}件", flush=True)
SIGD = pd.to_datetime(P.sig); ENTD = pd.to_datetime(P.ent)
Y17 = (P.y >= 2017).to_numpy()


def risk(b, y0=2017):
    """確定損益(建て日付け)の日次→DD・12か月ローリング最悪（各月初起点）・最悪月"""
    s = b[b.y >= y0].groupby("ent").yen.sum().sort_index(); s.index = pd.to_datetime(s.index)
    if len(s) == 0: return dict(dd=np.nan, w12=np.nan, b30=np.nan, b50=np.nan)
    cum = s.cumsum(); dd = (cum - cum.cummax()).min() / 1e4
    starts = pd.date_range(cum.index.min().to_period("M").to_timestamp(), cum.index.max() - pd.DateOffset(months=12), freq="MS")
    mins = []
    for st in starts:
        base = cum[cum.index < st].iloc[-1] if (cum.index < st).any() else 0.0
        w = cum[(cum.index >= st) & (cum.index < st + pd.DateOffset(months=12))]
        mins.append((w.min() - base) if len(w) else 0.0)
    mins = np.array(mins)
    return dict(dd=dd, w12=mins.min() / 1e4, b30=(mins < -3e5).mean() * 100, b50=(mins < -5e5).mean() * 100)


def ex20(b):
    day = b.groupby("ent").yen.sum(); return (b.yen.sum() - day.nlargest(20).sum()) / 1e4


def run_b(mask=None, sizes=(S1, S2), cols=("dev", "atr"), weights=None, asc=None, multcol=None):
    Q = P if mask is None else P[mask]
    d = rank(Q, cols, weights, asc)
    if multcol is None:
        return settle(d, sizes, 2)
    d = d[d.rk <= 2].copy(); m = d[multcol].to_numpy(float); m = np.where(np.isfinite(m), m, 1.0)
    m = m / np.nanmean(m[(d.rk == 1).to_numpy()])            # #1玉の平均倍率=1（平均100万に正規化）
    size = d.rk.map({1: sizes[0], 2: sizes[1]}).astype(float).to_numpy() * m
    d["size"] = size; d["sh"] = (size / d.px // 100 * 100).astype(int); d = d[d.sh > 0].copy(); d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def line26(lab, b, base=None, noise=None):
    s = stats(b); r = risk(b); x20 = ex20(b)
    j = ""
    if base is not None:
        ok = (s["total"] > base["total"] and sum(s[nm] >= base[nm] for _, _, nm in ERAS) >= 3 and s["e4"] >= base["e4"] * 0.95
              and s["y10"] > base["y10"] and x20 > base["ex20"] and (noise is None or s["total"] / 1e4 > noise))
        j = " ★" if ok else ("  (26年○)" if s["total"] > base["total"] and s["y10"] > base["y10"] else "")
    print(f"  {lab:<36} n{s['n']:>5} 勝率{s['win%']:>5.1f}% PF{s['PF']:.2f} 26年{s['total']/1e4:>+7,.0f}万 | "
          + "/".join(f"{s[nm]/1e4:>+6,.0f}" for _, _, nm in ERAS)
          + f" | 10年{s['y10']/1e4:>+6,.0f} 勝年{s['win_years']}/{s['n_years']} 最悪年{s['worst_year']/1e4:>+5,.0f} 最悪月{s['worst_month']/1e4:>+4,.0f} 除20日{x20:>+7,.0f} | 10年DD{r['dd']:>+5,.0f} 12か月最悪{r['w12']:>+4,.0f}" + j, flush=True)
    s["ex20"] = x20; s.update(r); return s


def line10(lab, b, base=None, noise=None):
    """10年（2017-26）評価: 17-21/22-26・PF・最悪年"""
    b = b[b.y >= 2017]; s = stats(b); r = risk(b); x20 = ex20(b)
    j = ""
    if base is not None:
        ok = (s["e3"] >= base["e3"] and s["e4"] >= base["e4"] and s["total"] > base["total"] and s["PF"] >= base["PF"]
              and s["worst_year"] >= base["worst_year"] - 1e4 and x20 > base["ex20"] and (noise is None or s["total"] / 1e4 > noise))
        j = " ★" if ok else ""
    print(f"  {lab:<36} n{s['n']:>5} 勝率{s['win%']:>5.1f}% PF{s['PF']:.2f} 10年{s['total']/1e4:>+6,.0f}万 | 17-21{s['e3']/1e4:>+6,.0f} 22-26{s['e4']/1e4:>+6,.0f} | 勝年{s['win_years']}/{s['n_years']} 最悪年{s['worst_year']/1e4:>+4,.0f} 最悪月{s['worst_month']/1e4:>+4,.0f} 除20日{x20:>+6,.0f} DD{r['dd']:>+5,.0f} 12か月最悪{r['w12']:>+4,.0f}" + j, flush=True)
    s["ex20"] = x20; s.update(r); return s


def pm(lab, m, y0=2017):
    q = P[m & (P.y >= y0).to_numpy()]
    e3 = q[(q.y >= 2017) & (q.y <= 2021)].pnl.mean(); e4 = q[q.y >= 2022].pnl.mean()
    print(f"    {lab:<30} n={len(q):>5} 件あたり{q.pnl.mean():>+6.2f}% 勝率{(q.pnl>0).mean()*100:>5.1f}% | 17-21{e3:>+6.2f} 22-26{e4:>+6.2f}", flush=True)


print("\n■ 土台（現行 ①100/②50・26年）", flush=True)
B26 = line26("現行", run_b()); B10 = line10("現行(10年)", run_b())
b0 = run_b()
print("\n■ D ①玉 vs ②玉（時代別 件あたり%／円あたり）", flush=True)
for rk_ in (1, 2):
    q = b0[b0.rk == rk_]
    parts = " ".join(f"{nm}{q[(q.y >= a) & (q.y <= c)].pnl.mean():+.2f}%(n{len(q[(q.y >= a) & (q.y <= c)])})" for a, c, nm in ERAS)
    print(f"  #{rk_} 件あたり{q.pnl.mean():+.3f}% 勝率{(q.pnl>0).mean()*100:.1f}% PF{pf(q.yen):.2f} 円{q.yen.sum()/1e4:+,.0f}万 円/投入万{q.yen.sum()/q['size'].sum()*1e4:+.1f}円 | {parts}")
print("  ── サイズ表（選定不変・26年/10年・DDと12か月最悪は2017-26・現金50万の受け皿で見る）", flush=True)
for s1, s2 in ((100, 50), (100, 0), (130, 50), (100, 100), (70, 35), (60, 30), (50, 25), (50, 50), (40, 20)):
    line26(f"①{s1}/②{s2}", run_b(sizes=(s1 * 10_000, s2 * 10_000)))

# ═══════════════ A バリュエーション × フェード（10年） ═══════════════
t1 = time.time()
VAL = pickle.load(open("_valuation_10y.pkl", "rb")); VC = VAL["cols"]; VD = VAL["data"]; VB = np.datetime64(VAL["base"])
sday = (SIGD.to_numpy().astype("datetime64[D]") - VB).astype(float); tick4 = P.ticker.astype(str).str[:4].to_numpy()
def vcol(name):
    j = VC.index(name); x = np.full(n, np.nan)
    for i in np.where(Y17)[0]:
        m = VD.get(tick4[i])
        if m is None or len(m) == 0: continue
        k = np.searchsorted(m[:, 0], sday[i], side="left") - 1     # day < シグナル日（前日以前の最新＝18:50に既知）
        if k >= 0 and sday[i] - m[k, 0] <= 10: x[i] = m[k, j]
    return x
MCAP = vcol("MktCap") * 1e6; PBR = vcol("PBR"); PER = vcol("PER"); FPER = vcol("FwdPER"); ROE = vcol("ROE"); FROE = vcol("FwdROE"); EPS = vcol("EPS"); FEPS = vcol("FwdEPS")
for k_, v_ in (("mcap", MCAP), ("pbr", PBR), ("per", PER), ("roe", ROE)): P[k_] = v_
known = Y17 & np.isfinite(MCAP)
print(f"\n■ A バリュエーション（結合 {known.sum()}/{Y17.sum()}件・{time.time()-t1:.0f}s・時価総額 中央値{np.nanmedian(MCAP[known])/1e8:,.0f}億・PBR中央値{np.nanmedian(PBR[known]):.2f}）", flush=True)
rng = np.random.default_rng(0)
def noise10(frac, seeds=30):
    idx = np.where(Y17)[0]; k = int(len(idx) * frac); tots = []
    for _ in range(seeds):
        m = np.ones(n, dtype=bool); m[rng.choice(idx, size=k, replace=False)] = False
        tots.append(stats(run_b(m)[lambda b: b.y >= 2017])["total"] / 1e4)
    return float(np.quantile(tots, 0.99)), float(np.std(tots))
def excl_test(lab, m):
    frac = 1 - (Y17 & m).sum() / Y17.sum()
    nf, sd = noise10(frac) if 0 < frac < 0.9 else (None, 0)
    line10(lab, run_b(m), B10, nf); print(f"      （除外率{frac*100:.0f}%・同率ランダム除外の99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）", flush=True)
def tercile_block(nm, x, extra=()):
    ok = Y17 & np.isfinite(x); q1, q2 = np.nanquantile(x[ok], [1/3, 2/3])
    lo, mid, hi = ok & (x <= q1), ok & (x > q1) & (x <= q2), ok & (x > q2)
    print(f"\n  ▶ {nm}: 既知{ok.sum()}件 三分位境界 {q1:,.3g}/{q2:,.3g}", flush=True)
    pm("下位1/3", lo); pm("中位", mid); pm("上位1/3", hi); pm("不明", Y17 & ~ok)
    for lab, m in [(f"{nm} 上位1/3を除外", ~hi), (f"{nm} 下位1/3を除外", ~lo), (f"{nm} 中位のみ", ~lo & ~hi), (f"{nm} 上位1/3のみ", hi | ~ok), (f"{nm} 下位1/3のみ", lo | ~ok)] + list(extra):
        excl_test(lab, m)
okm = Y17 & np.isfinite(MCAP)
tercile_block("時価総額", MCAP, extra=(("時価総額<300億を除外", ~(okm & (MCAP < 3e10))), ("時価総額<1000億を除外", ~(okm & (MCAP < 1e11))),
                                       ("時価総額>3000億を除外", ~(okm & (MCAP > 3e11))), ("時価総額>1兆を除外", ~(okm & (MCAP > 1e12))), ("300〜3000億のみ", okm & (MCAP >= 3e10) & (MCAP <= 3e11))))
okp = Y17 & np.isfinite(PBR) & (PBR > 0)
tercile_block("PBR", np.where(PBR > 0, PBR, np.nan), extra=(("PBR≤1を除外", ~(okp & (PBR <= 1))), ("PBR≥3を除外", ~(okp & (PBR >= 3))), ("PBR≥5を除外", ~(okp & (PBR >= 5)))))
loss = Y17 & np.isfinite(EPS) & (EPS <= 0)
print(f"\n  ▶ 赤字(EPS≤0) {loss.sum()}件 / 黒字 {(Y17 & np.isfinite(EPS) & (EPS > 0)).sum()}件", flush=True); pm("赤字", loss); pm("黒字", Y17 & np.isfinite(EPS) & (EPS > 0))
excl_test("赤字を除外", ~loss); excl_test("赤字のみ", loss | ~np.isfinite(EPS))
tercile_block("PER(黒字のみ)", np.where(PER > 0, PER, np.nan))
tercile_block("予想PER(黒字のみ)", np.where(FPER > 0, FPER, np.nan))
tercile_block("ROE", ROE, extra=(("ROE≤0を除外", ~(Y17 & np.isfinite(ROE) & (ROE <= 0))),))
tercile_block("予想ROE", FROE)
g = np.where(np.isfinite(EPS) & np.isfinite(FEPS) & (EPS > 0), FEPS / EPS - 1, np.nan)
tercile_block("予想EPS成長", g, extra=(("減益予想を除外", ~(Y17 & np.isfinite(g) & (g < 0))),))

print("\n  ▶ 並び順に混ぜる（現行mix + w×百分位・10年）", flush=True)
for lab, col, asc in (("小型優先", "mcap", True), ("大型優先", "mcap", False), ("低PBR優先", "pbr", True), ("高PBR優先", "pbr", False), ("高ROE優先", "roe", False), ("低ROE優先", "roe", True)):
    for w in (0.3, 0.6):
        line10(f"{lab} w{w}", run_b(cols=("dev", "atr", col), weights=[1, 1, 2 * w], asc=[False, False, asc]), B10)
rnd = []
for s_ in range(20):
    P["_r"] = rng.random(n); rnd.append(stats(run_b(cols=("dev", "atr", "_r"), weights=[1, 1, 1.2], asc=[False, False, True])[lambda b: b.y >= 2017])["total"] / 1e4)
print(f"      乱数特徴量を w0.6 で混ぜた10年: 平均{np.mean(rnd):+,.0f}万 sd{np.std(rnd):,.0f} 99%点{np.quantile(rnd, 0.99):+,.0f}（現行{B10['total']/1e4:+,.0f}）", flush=True)
print("\n  ▶ サイズ傾斜（選定不変・#1平均100万に正規化・10年）", flush=True)
q1m, q2m = np.nanquantile(MCAP[okm], [1/3, 2/3])
P["_m_small"] = np.where(okm & (MCAP <= q1m), 1.3, np.where(okm & (MCAP > q2m), 0.7, 1.0)); P["_m_large"] = np.where(okm & (MCAP <= q1m), 0.7, np.where(okm & (MCAP > q2m), 1.3, 1.0))
line10("小型1.3/大型0.7", run_b(multcol="_m_small"), B10); line10("小型0.7/大型1.3", run_b(multcol="_m_large"), B10)

# ═══════════════ B 業種相対の急騰（26年） ═══════════════
t2 = time.time()
SEC = json.load(open("sector33_map.json", encoding="utf-8"))
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
W = pd.DataFrame({tk: df["Close"].astype(float).replace(0, np.nan) for tk, df in ALL.items()}); del ALL
W = W.sort_index(); R = W.pct_change() * 100; secs = pd.Series({tk: SEC.get(tk, "__u") for tk in W.columns})
valid = secs[secs != "__u"].index; R = R[valid]; secs = secs[valid]
grp = R.T.groupby(secs.values)
SEC_RET = grp.mean().T; SEC_N = R.notna().T.groupby(secs.values).sum().T; SEC_UP3 = (R >= 3).T.groupby(secs.values).sum().T / SEC_N
SEC_RET = SEC_RET.where(SEC_N >= 5); SEC_UP3 = SEC_UP3.where(SEC_N >= 5)
SEC_IDX = (1 + SEC_RET.fillna(0) / 100).cumprod(); SEC_RET5 = (SEC_IDX / SEC_IDX.shift(5) - 1) * 100
del R, W
psec = P.ticker.map(SEC).fillna("__u"); sidx = SIGD.to_numpy()
def lookup(M):
    out = np.full(n, np.nan); pos = M.index.get_indexer(sidx); cols = M.columns.get_indexer(psec)
    ok = (pos >= 0) & (cols >= 0); out[ok] = M.to_numpy()[pos[ok], cols[ok]]; return out
P["sec_ret0"] = lookup(SEC_RET); P["sec_up3"] = lookup(SEC_UP3); P["sec_ret5"] = lookup(SEC_RET5); P["rel"] = P.gain - P.sec_ret0
print(f"\n■ B 業種相対の急騰（26年・{time.time()-t2:.0f}s・業種付与{np.isfinite(P.sec_ret0).mean()*100:.0f}%）", flush=True)
for c in ("sec_ret0", "sec_up3", "sec_ret5", "rel"):
    quint(b0.join(P[[c]], how="left") if c not in b0.columns else b0, c, label={"sec_ret0": "業種の当日リターン%", "sec_up3": "業種内+3%以上の比率", "sec_ret5": "業種5日リターン%", "rel": "銘柄−業種(当日)"}[c])
rnd26 = []
def noise26(frac, seeds=30):
    idx = np.arange(n); k = int(n * frac); tots = []
    for _ in range(seeds):
        m = np.ones(n, dtype=bool); m[rng.choice(idx, size=k, replace=False)] = False; tots.append(stats(run_b(m))["total"] / 1e4)
    return float(np.quantile(tots, 0.99)), float(np.std(tots))
def excl26(lab, m):
    frac = 1 - m.mean(); nf, sd = noise26(frac) if 0 < frac < 0.9 else (None, 0)
    line26(lab, run_b(m), B26, nf); print(f"      （除外率{frac*100:.0f}%・同率ランダム除外の99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）", flush=True)
sr = P.sec_ret0.to_numpy(); su = P.sec_up3.to_numpy(); s5 = P.sec_ret5.to_numpy(); rel = P.rel.to_numpy()
fin = np.isfinite(sr)
print("  ▶ 除外／限定", flush=True)
excl26("業種ぐるみ(業種当日≥+2%)を除外", ~(fin & (sr >= 2))); excl26("業種ぐるみ(業種当日≥+3%)を除外", ~(fin & (sr >= 3)))
excl26("業種内+3%以上が25%超を除外", ~(np.isfinite(su) & (su > 0.25))); excl26("単独急騰(業種当日<+1%)のみ", fin & (sr < 1))
excl26("業種当日マイナスのみ", fin & (sr < 0)); excl26("業種5日≥+5%を除外", ~(np.isfinite(s5) & (s5 >= 5))); excl26("業種5日≤-3%を除外", ~(np.isfinite(s5) & (s5 <= -3)))
print("  ▶ 並び順に混ぜる（現行mix + w×百分位）", flush=True)
for lab, col, asc in (("単独度(銘柄−業種)大を優先", "rel", False), ("業種が静かな方を優先", "sec_ret0", True), ("業種ぐるみを優先", "sec_ret0", False), ("業種5日が弱い方を優先", "sec_ret5", True)):
    for w in (0.3, 0.6, 1.0):
        line26(f"{lab} w{w}", run_b(cols=("dev", "atr", col), weights=[1, 1, 2 * w], asc=[False, False, asc]), B26)
for s_ in range(20):
    P["_r"] = rng.random(n); rnd26.append(stats(run_b(cols=("dev", "atr", "_r"), weights=[1, 1, 1.2], asc=[False, False, True]))["total"] / 1e4)
print(f"      乱数特徴量を w0.6 で混ぜた26年: 平均{np.mean(rnd26):+,.0f}万 sd{np.std(rnd26):,.0f} 99%点{np.quantile(rnd26, 0.99):+,.0f}（現行{B26['total']/1e4:+,.0f}）", flush=True)

# ═══════════════ C ATR正規化サイズ（26年） ═══════════════
print("\n■ C ATR正規化サイズ（#1平均100万に正規化・26年）", flush=True)
for lab, clip in (("1/ATR (0.5〜2.0倍)", (0.5, 2.0)), ("1/ATR (0.7〜1.5倍)", (0.7, 1.5))):
    P["_m_atr"] = np.clip(5.0 / P.atr.to_numpy(), *clip); line26(lab, run_b(multcol="_m_atr"), B26)
P["_m_atr"] = np.clip(np.sqrt(5.0 / P.atr.to_numpy()), 0.5, 2.0); line26("1/√ATR", run_b(multcol="_m_atr"), B26)
print(f"\n[done] {time.time()-t0:.0f}s")

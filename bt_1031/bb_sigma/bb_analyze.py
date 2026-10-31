# -*- coding: utf-8 -*-
"""bb_analyze.py — grid_{kind}.pkl を読んで、分割OOS・ランダム比較z・固定時刻比較・上位3日/3銘柄除外・DD を出し、md 断片を書く。
使い方: python -X utf8 bb_analyze.py 5m   → analysis_5m.md / top_5m_*.csv / survivors_5m.csv / grid_5m.csv
"""
from __future__ import annotations
import sys, os, time
import numpy as np, pandas as pd
import bb_engine as B

HERE = os.path.dirname(os.path.abspath(__file__))
N_RAND = 200
MIN_N1, MIN_N2, MIN_N = 100, 50, 150
TOPK = 10
EKEY = ["period", "sigma", "def", "band", "window", "side", "uni", "earn"]


def dirname(band, side):
    if band == "lower" and side == 1: return "逆張り買い(下抜け→買)"
    if band == "upper" and side == -1: return "逆張り売り(上抜け→売)"
    if band == "upper" and side == 1: return "順張り買い(上抜け→買)"
    return "順張り売り(下抜け→売)"


def exit_label(bx, sl): return bx + ("" if sl == 0 else f"+SL{sl:g}%")


def maxdd(r_by_day):
    eq = np.cumsum(r_by_day); peak = np.maximum.accumulate(eq)
    return float((eq - peak).min()) if len(eq) else 0.0


def stats(r):
    r = np.asarray(r, float); n = len(r)
    if n == 0: return dict(n=0, win=np.nan, pf=np.nan, avg=np.nan, sum=0.0)
    gp = r[r > 0].sum(); gl = -r[r < 0].sum()
    return dict(n=n, win=(r > 0).mean() * 100, pf=gp / gl if gl > 0 else np.inf, avg=r.mean(), sum=r.sum())


def fixed_entries(D, groups, w):
    """各銘柄日で時間帯の最初の足(=固定時刻) → 次足始値で建てる"""
    lo, hi = w
    ok = (D.tmin >= lo) & (D.tmin <= hi) & (np.arange(D.n) + 1 < D.row_dend) & np.isin(D.grp, groups)
    idx = np.flatnonzero(ok); g = D.grp[idx]
    return idx[np.r_[True, g[1:] != g[:-1]]]


def evaluate_cell(D, W, r, rng):
    """1セルの詳細: 全期間/前半/後半、後半ランダム200回のz、固定時刻ベンチ(同じ銘柄日 / 土俵の全銘柄日)、上3日・上3銘柄除外、DD"""
    side = int(r.side); sl = None if r.sl == 0 else float(r.sl)
    tr, ctx = D.trades(r.period, r.sigma, r["def"], r.band, r.window, side, r.uni, r.earn == "決算除外")
    col = f"{r['exit']}|{r.sl:g}" if r.sl else f"{r['exit']}|0"
    ret = tr[col].to_numpy(); half = tr.half.to_numpy(); grp = tr.grp.to_numpy()
    ci = B.EXITS.index((r["exit"], sl))
    ma, up, lo_, w, band, _ = ctx
    # ランダム: 同じ銘柄日(後半)・同時間帯・同出口・同向き ×200
    groups2 = grp[half == 2]
    lo, hi = w
    ok = (D.tmin >= lo) & (D.tmin <= hi) & (np.arange(D.n) + 1 < D.row_dend) & np.isin(D.grp, groups2)
    pool = np.flatnonzero(ok); pg = D.grp[pool]
    ravg = []
    for k in range(N_RAND):
        rr = rng.random(len(pool)); order = np.lexsort((rr, pg)); pp = pool[order]; gg = pg[order]
        e = pp[np.r_[True, gg[1:] != gg[:-1]]]
        ep, res = D.simulate(e, ma, up, lo_, band)
        ravg.append(D.returns(e, ep, res, side)[:, ci].mean())
    ravg = np.array(ravg)
    a2 = ret[half == 2].mean(); z = (a2 - ravg.mean()) / ravg.std() if ravg.std() > 0 else np.nan
    # 固定時刻: 同じ銘柄日
    e = fixed_entries(D, grp, w); ep, res = D.simulate(e, ma, up, lo_, band); fx_same = D.returns(e, ep, res, side)[:, ci]
    fx_same_h = D.half[D.grp[e]]
    # 固定時刻: 土俵の全銘柄日(決算条件も同じ)
    gall = np.flatnonzero(D.uni[r.uni] & (~D.g_earn if r.earn == "決算除外" else True))
    e = fixed_entries(D, gall, w); ep, res = D.simulate(e, ma, up, lo_, band); fx_all = D.returns(e, ep, res, side)[:, ci]
    fx_all_h = D.half[D.grp[e]]
    byday = tr.assign(r=ret).groupby("date").r.sum().sort_index(); bytk = tr.assign(r=ret).groupby("tk").r.sum()
    st = stats(ret); st1 = stats(ret[half == 1]); st2 = stats(ret[half == 2])
    return dict(kind=D.kind, dir=dirname(r.band, side), period=int(r.period), sigma=float(r.sigma), dfn=r["def"], window=r.window, exit=exit_label(r["exit"], r.sl), uni=r.uni, earn=r.earn,
                n=st["n"], win=st["win"], pf=st["pf"], avg=st["avg"], sum=st["sum"], dd=maxdd(byday.to_numpy()),
                n1=st1["n"], pf1=st1["pf"], avg1=st1["avg"], n2=st2["n"], pf2=st2["pf"], avg2=st2["avg"],
                rand_mean=ravg.mean(), rand_sd=ravg.std(), rand_p95=np.percentile(ravg, 95), z=z,
                fx_same=fx_same.mean(), fx_same2=fx_same[fx_same_h == 2].mean() if (fx_same_h == 2).any() else np.nan,
                fx_all_n=len(fx_all), fx_all=fx_all.mean(), fx_all_pf=stats(fx_all)["pf"], fx_all2=fx_all[fx_all_h == 2].mean() if (fx_all_h == 2).any() else np.nan,
                ex3d=ret.sum() - byday.nlargest(3).sum(), ex3t=ret.sum() - bytk.nlargest(3).sum(),
                avgc=tr.cost_eod.mean(), ndays=tr.date.nunique(), ntk=tr.tk.nunique(), ravg=ravg)


HDR = "| # | 向き | BB | 乖離 | 時間帯 | 出口 | 土俵 | 決算 | n | 勝率 | PF | 平均% | 合計% | DD | 前半 n/PF/平均 | 後半 n/PF/平均 | ランダム後半 平均±sd | z | 固定時刻 同銘柄日 全/後半 | 固定時刻 土俵全日 n/平均/PF | 上3日除 | 上3銘柄除 | 日数/銘柄数 |"
SEP = "|" + "---|" * 23


def row_md(i, r):
    return (f"| {i} | {r['dir']} | {r['period']}/{r['sigma']:g}σ | {r['dfn']} | {r['window']} | {r['exit']} | {r['uni']} | {r['earn']} | {r['n']} | {r['win']:.1f}% | {r['pf']:.2f} | {r['avg']:+.3f} | {r['sum']:+.0f} | {r['dd']:.0f} | "
            f"{r['n1']}/{r['pf1']:.2f}/{r['avg1']:+.3f} | {r['n2']}/{r['pf2']:.2f}/{r['avg2']:+.3f} | {r['rand_mean']:+.3f}±{r['rand_sd']:.3f} | {r['z']:+.2f} | {r['fx_same']:+.3f}/{r['fx_same2']:+.3f} | {r['fx_all_n']:,}/{r['fx_all']:+.3f}/{r['fx_all_pf']:.2f} | {r['ex3d']:+.0f} | {r['ex3t']:+.0f} | {r['ndays']}/{r['ntk']} |")


def main(kind):
    t0 = time.time()
    g = pd.read_pickle(os.path.join(HERE, f"grid_{kind}.pkl"))
    g["pf"] = g.gp / g.gl; g["avg"] = g["sum"] / g.n; g["win"] = g.nwin / g.n * 100
    g["pf1"] = g.gp1 / g.gl1; g["pf2"] = g.gp2 / g.gl2; g["avg1"] = g.sum1 / g.n1; g["avg2"] = g.sum2 / g.n2; g["avgc"] = g.cost / g.n
    g["dir"] = [dirname(b, s) for b, s in zip(g.band, g.side)]
    g["exit_l"] = [exit_label(b, s) for b, s in zip(g["exit"], g.sl)]
    g.to_csv(os.path.join(HERE, f"grid_{kind}.csv"), index=False, encoding="utf-8-sig")
    D = B.Data(kind); W = D.windows(); rng = np.random.default_rng(20261031)
    L = []; P = L.append
    P(f"## {kind} データ")
    P(f"- {D.summary}")
    P(f"- 前半 = 最初の{D.half_day}営業日、後半 = 残り。時間帯の定義: " + ", ".join(f"{k} {a//60:02d}:{a%60:02d}〜{b//60:02d}:{b%60:02d}(シグナル足の開始時刻)" for k, (a, b) in W.items()))
    P("")
    P(f"## {kind} 総当たりの全体像")
    P(f"- セル数 {len(g):,}（BB {len(B.PERIODS)}期間×{len(B.SIGMAS)}σ×乖離{len(B.DEFS)}定義×バンド2×向き2×時間帯5×出口{len(B.EXITS)}×土俵{len(B.UNIS)}×決算{len(B.EARNS)}）。n≥{MIN_N} のセル {int((g.n>=MIN_N).sum()):,}")
    q = g[g.n >= MIN_N].copy()
    surv_mask = (q.pf1 > 1.15) & (q.pf2 > 1.15) & (q.n1 >= MIN_N1) & (q.n2 >= MIN_N2)
    P(f"- n≥{MIN_N} のうち PF>1: {int((q.pf>1).sum()):,} ({(q.pf>1).mean()*100:.1f}%)、前後半とも PF>1.15 (n1≥{MIN_N1}, n2≥{MIN_N2}): {int(surv_mask.sum()):,} セル（入口セット単位で {q[surv_mask].drop_duplicates(EKEY).shape[0]} 通り）")
    P(f"- 平均コスト(往復0.10%+不利2tick) = {q.avgc.mean():.3f}%/件。土俵別: " + ", ".join(f"{u} {v:.2f}%" for u, v in q.groupby('uni').avgc.mean().items()))
    P("")
    P("### 向き別（n≥150セルの 1件平均% の分布と PF>1 の割合）")
    P("| 向き | セル数 | 平均%の中央値 | 平均%の最大 | PF>1の割合 |"); P("|---|---|---|---|---|")
    for d, s in q.groupby("dir"):
        P(f"| {d} | {len(s):,} | {s.avg.median():+.3f} | {s.avg.max():+.3f} | {(s.pf>1).mean()*100:.1f}% |")
    P("")
    for nm, key in (("土俵", "uni"), ("時間帯", "window"), ("BB期間", "period"), ("σ", "sigma"), ("乖離定義", "def"), ("出口", "exit_l")):
        P(f"### {nm}×向き（PF>1 のセル数 / セル数）")
        pv = q.assign(w=(q.pf > 1)).pivot_table(index=key, columns="dir", values="w", aggfunc=["sum", "count"])
        P(f"| {nm} | " + " | ".join(pv["sum"].columns) + " |"); P("|---" * (len(pv["sum"].columns) + 1) + "|")
        for u in pv.index:
            P(f"| {u} | " + " | ".join(f"{int(pv['sum'].loc[u,c])}/{int(pv['count'].loc[u,c])}" for c in pv["sum"].columns) + " |")
        P("")
    P("### 出口別（U0全・決算除外・n≥150 の 1件平均% の中央値）")
    s = q[(q.uni == "U0_全") & (q.earn == "決算除外")].groupby("exit_l").avg.median().sort_values(ascending=False)
    P("| 出口 | 中央値% |"); P("|---|---|")
    for k, v in s.items(): P(f"| {k} | {v:+.3f} |")
    P("")

    # ---------- 素直な形 ----------
    P(f"## {kind} 素直な形（20本/2σ・終値がバンド外・時間帯 全・出口 引け／MA回帰・U0全）")
    P("| 向き | 決算 | 出口 | n | 勝率 | PF | 平均% | 前半PF | 後半PF | 平均コスト |"); P("|---|---|---|---|---|---|---|---|---|---|")
    s = g[(g.period == 20) & (g.sigma == 2.0) & (g["def"] == "D1_外") & (g.window == "W0_全") & (g.uni == "U0_全") & (g.sl == 0) & (g["exit"].isin(["引け", "MA回帰"]))]
    for _, r in s.sort_values(["dir", "earn", "exit"]).iterrows():
        P(f"| {r.dir} | {r.earn} | {r.exit_l} | {int(r.n):,} | {r.win:.1f}% | {r.pf:.2f} | {r.avg:+.3f} | {r.pf1:.2f} | {r.pf2:.2f} | {r.avgc:.2f}% |")
    P("")
    # ---------- 決算除外の効果 ----------
    P(f"## {kind} 決算除外の効果（n≥150 セルの 1件平均%：除外 − 込み、ペア差の分布）")
    k2 = ["period", "sigma", "def", "band", "window", "side", "exit", "sl", "uni"]
    a = g[g.earn == "決算込"].set_index(k2); b = g[g.earn == "決算除外"].set_index(k2)
    j = a.join(b, lsuffix="_i", rsuffix="_x", how="inner"); j = j[(j.n_i >= MIN_N) & (j.n_x >= MIN_N)]
    dlt = j.avg_x - j.avg_i
    P(f"- ペア数 {len(j):,}・差の中央値 {dlt.median():+.4f}%・平均 {dlt.mean():+.4f}%・除外で改善したセル {(dlt>0).mean()*100:.1f}%")
    jb = j.index.get_level_values("band"); js = j.index.get_level_values("side")
    for d_, s_ in j.groupby([jb, js]):
        P(f"  - {dirname(d_[0], int(d_[1]))}: 差の中央値 {(s_.avg_x - s_.avg_i).median():+.4f}%・除外で改善 {((s_.avg_x - s_.avg_i)>0).mean()*100:.0f}%")
    P("")

    # ---------- 分割OOS top10 ----------
    cand = g[(g.n1 >= MIN_N1) & (g.n2 >= MIN_N2)].copy()
    for mode in ("raw", "dedup"):
        c = cand.sort_values("pf1", ascending=False)
        if mode == "dedup": c = c.drop_duplicates(EKEY)
        top = c.head(TOPK)
        rows = [evaluate_cell(D, W, r, rng) for _, r in top.iterrows()]
        T = pd.DataFrame(rows); M = np.vstack(T.pop("ravg").to_numpy())
        T.to_csv(os.path.join(HERE, f"top_{kind}_{mode}.csv"), index=False, encoding="utf-8-sig")
        agg_act = T.avg2.mean(); agg_rand = M.mean(0); zagg = (agg_act - agg_rand.mean()) / agg_rand.std()
        P(f"## {kind} 分割OOS（前半{D.half_day}日の PF 上位{TOPK}セル{'・同じ入口セット(BB/乖離/時間帯/向き/土俵/決算)は1つに集約' if mode=='dedup' else '（出口違いの重複あり）'} → 後半で確認）")
        P(f"- 候補セル(n1≥{MIN_N1}, n2≥{MIN_N2}): {len(cand):,}。上位{TOPK}の後半平均 = **{agg_act:+.3f}%/件**（後半PF>1 は {int((T.pf2>1).sum())}/{TOPK}）。同じ銘柄日×同時間帯×同出口のランダム建て200回の後半平均 = {agg_rand.mean():+.3f}% ± {agg_rand.std():.3f}（95%点 {np.percentile(agg_rand,95):+.3f}） → 10セル平均の z = {zagg:+.2f}（セルごとの z は表）")
        P("")
        P(HDR); P(SEP)
        for i, r in T.iterrows(): P(row_md(i + 1, r))
        P("")

    # ---------- 生き残り(前後半PF>1.15) ----------
    sv = q[surv_mask].sort_values("pf1", ascending=False).drop_duplicates(EKEY)
    P(f"## {kind} 前後半とも PF>1.15 の入口セット（{len(sv)} 通り・各セットは pf1 最大の出口で代表）→ ランダム比較z・固定時刻比較・上3日/上3銘柄除外")
    if len(sv):
        rows = [evaluate_cell(D, W, r, rng) for _, r in sv.iterrows()]
        S = pd.DataFrame(rows); S.pop("ravg")
        S.to_csv(os.path.join(HERE, f"survivors_{kind}.csv"), index=False, encoding="utf-8-sig")
        ok = (S.z > 2) & (S.ex3d > 0) & (S.ex3t > 0)
        beat_fx = S.avg > S.fx_all
        P(f"- ランダム比 z>2 かつ 上3日・上3銘柄除いてプラス: {int(ok.sum())}/{len(S)}。さらに「同じ土俵の全銘柄日を固定時刻で建てる」より平均が良い: {int((ok & beat_fx).sum())}/{len(S)}")
        P(f"- 向き別の内訳: " + ", ".join(f"{d} {n}" for d, n in S["dir"].value_counts().items()) + "。土俵別: " + ", ".join(f"{d} {n}" for d, n in S["uni"].value_counts().items()))
        P("")
        P(HDR); P(SEP)
        for i, r in S.iterrows(): P(row_md(i + 1, r))
    P("")
    out = os.path.join(HERE, f"analysis_{kind}.md")
    open(out, "w", encoding="utf-8").write("\n".join(L))
    print(f"[analyze {kind}] -> {out} {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main(sys.argv[1])

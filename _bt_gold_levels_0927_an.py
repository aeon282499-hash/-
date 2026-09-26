# -*- coding: utf-8 -*-
"""_bt_gold_levels_0927_an.py — G1 価格水準の集計(事前登録550セル)。_bt_gold_levels_0927.py の出力を読む。
t = 日ごとにまとめたクラスタt。ノイズ床 = 日ごとの符号反転(コスト抜き・全セル共通)300回の最大tの95%点。
実行: python -X utf8 _bt_gold_levels_0927_an.py [xm|duka] > _bt_gold_levels_0927.log
"""
import pandas as pd, numpy as np, sys, time
SRC = sys.argv[1] if len(sys.argv) > 1 else "xm"
COST = 0.593
E = pd.read_pickle(f"_bt_gold_levels_0927_events_{SRC}.pkl")
E["yr"] = E.day.dt.year
E["base"] = E.fam.str.replace(r"偽.*", "", regex=True)
E["plc"] = E.fam.str.contains("偽")
REAL = ["RN5", "RN10", "RN25", "RN50", "RN100", "PD", "PW", "OD", "OL", "ON", "AS"]
KEYS = ["base", "dir", "trade", "hold"]
ndays = int(E.k.max()) + 1


def ctstat(k, x):
    """k=日番号, x=損益 → (件数, 平均, クラスタt)"""
    if len(x) < 30: return len(x), np.nan, np.nan
    S = np.bincount(k, weights=x, minlength=ndays); N = np.bincount(k, minlength=ndays)
    use = N > 0; S = S[use]; N = N[use]; G = len(S)
    m = S.sum() / N.sum()
    se = np.sqrt(((S - m * N) ** 2).sum() * G / max(G - 1, 1)) / N.sum()
    return len(x), m, m / se if se > 0 else np.nan


def cell_stats(g):
    k = g.k.to_numpy(); gr = g.gross.to_numpy(); net = gr - COST; yr = g.yr.to_numpy()
    n, m, t = ctstat(k, net)
    _, mg, tg = ctstat(k, gr)
    _, _, t1 = ctstat(k[yr <= 2020], net[yr <= 2020])
    _, m2, t2 = ctstat(k[yr >= 2021], net[yr >= 2021])
    ys = pd.Series(net).groupby(yr).sum()
    Sd = pd.Series(net).groupby(k).sum()
    drop = set(Sd.nlargest(3).index)
    msk = ~np.isin(k, list(drop))
    _, _, t3 = ctstat(k[msk], net[msk])
    _, _, tc = ctstat(k, gr - COST * 1.5)
    _, mk, tk = ctstat(k, gr - 0.30)
    return dict(n=n, days=len(np.unique(k)), gross=mg, t_gross=tg, net=m, t=t, t1=t1, t2=t2, net2=m2,
                yrs=f"{(ys > 0).sum()}/{len(ys)}", ypos=(ys > 0).mean(), t_top3=t3, t_c15=tc, net_kiwami=mk, t_kiwami=tk,
                hit=(net > 0).mean())


t0 = time.time()
R = E[~E.plc & E.base.isin(REAL)]
rows = []
for key, g in R.groupby(KEYS, sort=False):
    s = cell_stats(g); s.update(dict(zip(KEYS, key))); rows.append(s)
T = pd.DataFrame(rows)
# 偽の水準(対照)の同じセル
P = E[E.plc]
prow = {}
for key, g in P.groupby(KEYS, sort=False):
    k = g.k.to_numpy(); gr = g.gross.to_numpy()
    n, m, t = ctstat(k, gr)
    prow[key] = (n, m, t)
T["plc_gross"] = [prow.get(tuple(r[k] for k in KEYS), (0, np.nan, np.nan))[1] for _, r in T.iterrows()]
T["diff_vs_plc"] = T.gross - T.plc_gross
print(f"セル {len(T)}  ({time.time()-t0:.0f}s)")

# ---- ノイズ床(コスト抜き・日ごとの符号反転・全セル共通の符号)
cells = []
for key, g in R.groupby(KEYS, sort=False):
    k = g.k.to_numpy(); gr = g.gross.to_numpy()
    S = np.bincount(k, weights=gr, minlength=ndays); N = np.bincount(k, minlength=ndays).astype(float)
    cells.append((S, N))
SM = np.vstack([c[0] for c in cells]); NM = np.vstack([c[1] for c in cells])
rng = np.random.default_rng(20260927); mx = []
for b in range(300):
    s = rng.choice([-1.0, 1.0], size=ndays)
    Sb = SM * s
    tot = NM.sum(1); m = Sb.sum(1) / np.maximum(tot, 1)
    G = (NM > 0).sum(1)
    se = np.sqrt(((Sb - m[:, None] * NM) ** 2).sum(1) * G / np.maximum(G - 1, 1)) / np.maximum(tot, 1)
    tt = np.where(se > 0, m / se, 0)
    mx.append(np.nanmax(tt))
FLOOR = float(np.percentile(mx, 95))
print(f"ノイズ床(コスト抜き・最大tの95%点・300回) = {FLOOR:.2f}   ({time.time()-t0:.0f}s)")

T["pass"] = ((T.t_gross > FLOOR) & (T.t > 2) & (T.t1 > 1.5) & (T.t2 > 1.5) & (T.ypos >= 0.7) & (T.t_top3 > 2.5) & (T.t_c15 > 0)).fillna(False)
pd.set_option("display.width", 260); pd.set_option("display.max_columns", 40); pd.set_option("display.float_format", "{:.3f}".format)
cols = ["base", "dir", "trade", "hold", "n", "days", "gross", "t_gross", "plc_gross", "diff_vs_plc", "net", "t", "t1", "t2", "yrs", "t_top3", "t_c15", "net_kiwami", "t_kiwami", "hit"]
print("\n■ コスト抜きtの上位30(合格判定は t_gross>床 かつ 実勢コスト後の全条件)")
print(T.sort_values("t_gross", ascending=False)[cols].head(30).to_string(index=False))
print("\n■ 実勢コスト後(net)tの上位20")
print(T.sort_values("t", ascending=False)[cols].head(20).to_string(index=False))
print("\n■ 水準の種類ごとの最良セル(コスト抜きt)")
print(T.loc[T.groupby("base").t_gross.idxmax()][cols].to_string(index=False))
print(f"\n■ 合格 {int(T['pass'].sum())}/{len(T)}")
if T["pass"].any(): print(T[T["pass"]][cols].to_string(index=False))
# 反転 vs 継続の全体像(キリ番: 本物と偽の差)
print("\n■ キリ番の本物−偽(コスト抜き平均$/oz・保有30分・全方向平均)")
for S in ["RN5", "RN10", "RN25", "RN50", "RN100"]:
    sub = T[(T.base == S) & (T.hold == 30)]
    print("  " + S + ": " + " / ".join(f"{r.dir}{r.trade}: 本物{r.gross:+.3f} 偽{r.plc_gross:+.3f} 差{r.diff_vs_plc:+.3f}" for _, r in sub.iterrows()))
T.to_csv(f"_bt_gold_levels_0927_cells_{SRC}.csv", index=False, encoding="utf-8-sig")

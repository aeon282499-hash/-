# -*- coding: utf-8 -*-
"""_bt_gold_rn_break_0927_b.py — $50キリ番の抜け順張りを「%の効果・今のコスト」で判定（2026-09-27・G1の第3段）
理由: コストは$0.593/oz固定 → 金$1,200時代は0.049%、今($4,285)は0.0138%。同じ%の効果でも昔はコスト負け。
  JP225と同じ作法=「エッジの有無はコスト抜き(%)、儲けは今のコスト」で測る(未来の玉は今の値段で払う)。
事前登録108セル: 水準{全$50, $X00だけ, $X50だけ} × 方向{両方, 下から=買い, 上から=売り} × δ{0.5,1,2} × 保有{5,15,30,60}。
  今のコスト% = 0.593/4285 = 0.01384%(×1.5 も)。ノイズ床=日ごとの符号反転(コスト抜き%)300回の最大tの95%点。
合格: 粗%t>床 & 今コスト後t>2.5 & 前後半(2015-20/2021-26)とも今コスト後t>1.5 & 勝ち年7割 & 上位3日除去t>2.5 & コスト1.5倍でt>0 & 偽の格子より粗%が上(差のt>2)
実行: python -X utf8 _bt_gold_rn_break_0927_b.py [xm|duka]
"""
import pandas as pd, numpy as np, sys
SRC = sys.argv[1] if len(sys.argv) > 1 else "xm"
E = pd.read_pickle(f"_bt_gold_rn_break_0927_{SRC}.pkl")
CNOW = 0.593 / 4285 * 100
ndays = int(E.k.max()) + 1
E["is100"] = (E.grid == "本物") & (np.round(E.lv) % 100 == 0)
E["is50"] = (E.grid == "本物") & ~E["is100"]


def ct(k, x):
    if len(x) < 30: return len(x), np.nan, np.nan
    S = np.bincount(k, weights=x, minlength=ndays); N = np.bincount(k, minlength=ndays)
    u = N > 0; S = S[u]; N = N[u]; G = len(S); m = S.sum() / N.sum()
    se = np.sqrt(((S - m * N) ** 2).sum() * G / max(G - 1, 1)) / N.sum()
    return len(x), m, (m / se if se > 0 else np.nan)


def subset(df, lvl, dr, dl, h, placebo=False):
    g = df[(df.dl == dl) & (df.hold == h)]
    if placebo:
        g = g[g.grid != "本物"]
    else:
        g = g[g.grid == "本物"]
        if lvl == "X00": g = g[g.is100]
        elif lvl == "X50": g = g[g.is50]
    if dr != "両方": g = g[g.dir == dr]
    return g


cells = []
for lvl in ("全$50", "X00", "X50"):
    for dr in ("両方", "下から", "上から"):
        for dl in (0.5, 1.0, 2.0):
            for h in (5, 15, 30, 60):
                cells.append((lvl, dr, dl, h))
rows = []; SM = []; NM = []
for (lvl, dr, dl, h) in cells:
    g = subset(E, lvl, dr, dl, h); p = subset(E, lvl, dr, dl, h, placebo=True)
    k = g.k.to_numpy(); gp = g.gpct.to_numpy(); yr = g.yr.to_numpy()
    net = gp - CNOW
    n, mg, tg = ct(k, gp); _, mn, tn = ct(k, net)
    _, m1, t1 = ct(k[yr <= 2020], net[yr <= 2020]); _, m2, t2 = ct(k[yr >= 2021], net[yr >= 2021])
    ys = pd.Series(net).groupby(yr).sum()
    Sd = pd.Series(net).groupby(k).sum(); drop = set(Sd.nlargest(3).index); msk = ~np.isin(k, list(drop))
    _, _, t3 = ct(k[msk], net[msk]); _, _, tc = ct(k, gp - CNOW * 1.5)
    _, mp, tp = ct(p.k.to_numpy(), p.gpct.to_numpy())
    # 本物−偽 の差のt(独立近似)
    se_g = abs(mg / tg) if tg else np.nan; se_p = abs(mp / tp) if tp else np.nan
    tdiff = (mg - mp) / np.sqrt(se_g ** 2 + se_p ** 2) if np.isfinite(se_g) and np.isfinite(se_p) else np.nan
    rows.append(dict(lvl=lvl, dir=dr, dl=dl, hold=h, n=n, per_yr=n / 11.7, gross_pct=mg, t_gross=tg, net_pct=mn, t=tn, t1=t1, t2=t2,
                     yrs=f"{(ys > 0).sum()}/{len(ys)}", ypos=(ys > 0).mean(), t_top3=t3, t_c15=tc, plc_gross=mp, t_diff=tdiff,
                     usd_at_now=mn / 100 * 4285))
    SM.append(np.bincount(k, weights=gp, minlength=ndays)); NM.append(np.bincount(k, minlength=ndays).astype(float))
T = pd.DataFrame(rows)
SM = np.vstack(SM); NM = np.vstack(NM)
rng = np.random.default_rng(927); mx = []
for b in range(300):
    s = rng.choice([-1.0, 1.0], size=ndays); Sb = SM * s; tot = NM.sum(1); m = Sb.sum(1) / np.maximum(tot, 1); G = (NM > 0).sum(1)
    se = np.sqrt(((Sb - m[:, None] * NM) ** 2).sum(1) * G / np.maximum(G - 1, 1)) / np.maximum(tot, 1)
    mx.append(np.nanmax(np.where(se > 0, m / se, 0)))
FLOOR = float(np.percentile(mx, 95))
T["pass"] = ((T.t_gross > FLOOR) & (T.t > 2.5) & (T.t1 > 1.5) & (T.t2 > 1.5) & (T.ypos >= 0.7) & (T.t_top3 > 2.5) & (T.t_c15 > 0) & (T.t_diff > 2)).fillna(False)
pd.set_option("display.width", 260); pd.set_option("display.max_columns", 30); pd.set_option("display.float_format", "{:.4f}".format)
print(f"[{SRC}] 108セル・ノイズ床(粗%・最大tの95%点) = {FLOOR:.2f}   今のコスト = {CNOW:.4f}%/往復")
cols = ["lvl", "dir", "dl", "hold", "n", "per_yr", "gross_pct", "t_gross", "plc_gross", "t_diff", "net_pct", "usd_at_now", "t", "t1", "t2", "yrs", "t_top3", "t_c15"]
print(T.sort_values("t", ascending=False)[cols].head(30).to_string(index=False))
print(f"\n合格 {int(T['pass'].sum())}/108")
if T["pass"].any(): print(T[T["pass"]][cols].to_string(index=False))
T.to_csv(f"_bt_gold_rn_break_0927_b_{SRC}.csv", index=False, encoding="utf-8-sig")

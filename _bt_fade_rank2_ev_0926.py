# -*- coding: utf-8 -*-
"""_bt_fade_rank2_ev_0926.py — 本人「②ってそもそも要る？期待値ある？」(2026-09-26)
②だけ(50万・前終+1%当日中指値・大型除外・10年)の1玉期待値/t値/年別/時代別、①との相関、②の上乗せ効果。
実行: python -X utf8 _bt_fade_rank2_ev_0926.py > _log_fade_rank2_ev_0926.txt"""
import os
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk <= 2].reset_index(drop=True)
o, h, c, px = d.o1.values, d.h1.values, d.c1.values, d.px.values
L = tickc(px * 1.01); fo = o >= L; fh = ~fo & (h >= L)
d["fill"] = fo | fh; d["pct"] = np.where(fo, (o - c) / o, np.where(fh, (L - c) / L, np.nan)) * 100
d["pct_mkt"] = (o - c) / o * 100
for r in (1, 2):
    x = d[(d.rk == r) & d.fill]; p = x.pct
    print(f"■ {'①' if r==1 else '②'}  候補{(d.rk==r).sum()} 約定{len(x)} 1玉平均{p.mean():+.2f}% 中央値{p.median():+.2f}% 勝率{(p>0).mean()*100:.1f}% PF{pf(p):.2f} t={p.mean()/p.std()*np.sqrt(len(p)):.2f}  (寄成なら{d[d.rk==r].pct_mkt.mean():+.2f}% t={d[d.rk==r].pct_mkt.mean()/d[d.rk==r].pct_mkt.std()*np.sqrt((d.rk==r).sum()):.2f})")
    print("  年別平均%:", x.groupby("y").pct.mean().round(2).to_dict())
    print("  年別玉数:", x.groupby("y").pct.size().to_dict())
    print("  上位10玉を除いた平均: %+.2f%%  最悪5玉: %s" % (p.sort_values().iloc[:-10].mean(), p.nsmallest(5).round(1).tolist()))
x2 = d[(d.rk == 2) & d.fill]
print("\n■ ②のプラセボ: ②の各玉と同じ日の①との差・①と②が同日に揃う日")
both = d[d.fill].pivot_table(index="ent", columns="rk", values="pct")
both = both.dropna(); print(f"  同日両方約定 {len(both)}日  相関 {both[1].corr(both[2]):.2f}  両方負け日率 {((both[1]<0)&(both[2]<0)).mean()*100:.1f}%  片方負け率 {((both[1]<0)^(both[2]<0)).mean()*100:.1f}%")
# ②の日しかない日(①は見送り/①なし)
only2 = d[(d.rk == 2) & d.fill & ~d.ent.isin(d[(d.rk == 1) & d.fill].ent)]
print(f"  ①が約定しない日に②だけ約定: {len(only2)}玉 平均{only2.pct.mean():+.2f}%")
# 時代別(26年、大型除外なし)も
D2 = rank(P[base_mask(P) if False else slice(None)]) if False else None

print("\n■ 長い期間(load('20y')全期間・大型除外なし=時価総額データが2017年以降しか無いため)")
d = rank(P); d = d[d.rk <= 2].reset_index(drop=True)
o, h, c, px = d.o1.values, d.h1.values, d.c1.values, d.px.values
L = tickc(px * 1.01); fo = o >= L; fh = ~fo & (h >= L)
d["fill"] = fo | fh; d["pct"] = np.where(fo, (o - c) / o, np.where(fh, (L - c) / L, np.nan)) * 100
d["era"] = pd.cut(d.y, [0, 2008, 2016, 2021, 2100], labels=["〜08", "09-16", "17-21", "22-26"])
for r in (1, 2):
    x = d[(d.rk == r) & d.fill]; p = x.pct
    print(f" {'①' if r==1 else '②'} {d.y.min()}-{d.y.max()} 約定{len(x)} 平均{p.mean():+.2f}% 勝率{(p>0).mean()*100:.1f}% PF{pf(p):.2f} t={p.mean()/p.std()*np.sqrt(len(p)):.2f} 上位10除去{p.sort_values().iloc[:-10].mean():+.2f}%")
    print("   時代別:", x.groupby("era", observed=True).pct.agg(lambda s: f"{s.mean():+.2f}%/n{len(s)}").to_dict())
    yy = x.groupby("y").pct.mean(); print(f"   プラスの年 {(yy>0).sum()}/{len(yy)}")

# -*- coding: utf-8 -*-
"""chk2: harness(10/28の物差し)を再現し、出口の内訳と楽観仮定の感度を出す（読み取り専用）"""
import os, sys, time, pickle
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project")
sys.path.insert(0, "."); sys.path.insert(0, "_gk1028")
t0 = time.time()
import harness as Hn
E = Hn.E
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo"
R0 = Hn.R0; S0 = Hn.S0
print(f"[基準] 玉{S0['n']} PF{S0['pf']:.2f} 10年{S0['tot']:+.1f}万 DD{S0['dd']:+.1f} 勝率{S0['win']:.1f}  ({time.time()-t0:.0f}s)")
X = R0[(R0.entry >= "2017-01-01")].copy()
print("出口内訳(10年):"); print(X.groupby("typ").agg(n=("yen", "size"), yen_man=("yen", lambda s: s.sum() / 1e4), mean_pct=("gross", "mean")).round(2))
print("年別(万):", (X.groupby(X.entry.dt.year).yen.sum() / 1e4).round(1).to_dict())
print("from_nf 玉", int(X.from_nf.sum()), " 上位20玉の利益(万)", round(X.yen.nlargest(20).sum() / 1e4, 1), " 利益合計(万)", round(X.yen[X.yen > 0].sum() / 1e4, 1))
# 1日の候補数(マスク後)
cnt = pd.Series([int(Hn.BASE[g].sum()) for g in E.by_day], index=E.DAYS)
cnt10 = cnt[cnt.index >= "2017-01-01"]
print("候補数(2017〜)の分布: 0本", (cnt10 == 0).mean().round(3), "1本", (cnt10 == 1).mean().round(3), "2本", (cnt10 == 2).mean().round(3), "3本以上", (cnt10 >= 3).mean().round(3), " 候補ありの日のうち1本だけ", ((cnt10 == 1).sum() / (cnt10 > 0).sum()).round(3))
# ── 感度 ──
def st(R, a="2017-01-01", b="2026-12-31"): return Hn.st(R, a, b)
def line(lab, R):
    s = st(R); print(f"  {lab:<44} 玉{s['n']:>3} PF{s['pf']:4.2f} 10年{s['tot']:+6.1f}万({s['tot']-S0['tot']:+5.1f}) DD{s['dd']:+5.1f} 勝率{s['win']:4.1f}")
    return s
# (a) RSI引け→翌朝寄り
line("RSI出口を翌朝寄りで執行(引け先読みの上限)", E.run(slots=1, size=Hn.SIZE, mask=Hn.BASE, cost_rt=Hn.COST, exit_kw=dict(rsi_next_open=True)))
# (b) 費用
for c in (0.0, 0.13, 0.3, 0.5):
    line(f"往復費用 {c}%", E.run(slots=1, size=Hn.SIZE, mask=Hn.BASE, cost_rt=c))
# (c) 寄り=指値ちょうど
LP = Hn._LP
eq = np.isfinite(LP) & (E.EOPEN == LP)
print("  寄り==指値ちょうど の候補数(全期間・BASE内)", int((eq & Hn.BASE).sum()), " うち選ばれた玉", int(eq[X.i.to_numpy()].sum()))
m2 = Hn.BASE & ~eq
line("寄り==指値ちょうどは不成立扱い", E.run(slots=1, size=Hn.SIZE, mask=m2, cost_rt=Hn.COST))
# (d) 建て日の足が O=H=L=C（商い無し/張り付き）
flat = (E.A_OP[:, 0] == E.A_HI[:, 0]) & (E.A_OP[:, 0] == E.A_LO[:, 0]) & (E.A_OP[:, 0] == E.A_CL[:, 0])
print("  建て日 O=H=L=C の玉(10年)", int(flat[X.i.to_numpy()].sum()))
# (e) 利確: HI ≥ tl → HI > tl(呼値1刻み上) に厳しく
def tick(p): return np.where(p <= 3000, 1, np.where(p <= 5000, 5, 10)).astype(float)
tl = E.EOPEN * 1.05; tl_ceil = np.ceil(tl / tick(tl)) * tick(tl)
i10 = X.i.to_numpy()
tp_rows = X[X.typ == "TP"]
k_tp = E.EXO_ if hasattr(E, "EXO_") else None
PNL, EXO, TYP, XPX = E._rp()
hi_at_exit = E.A_HI[tp_rows.i.to_numpy(), EXO[tp_rows.i.to_numpy()]]
exact = hi_at_exit <= tl_ceil[tp_rows.i.to_numpy()] + 1e-9
print(f"  利確玉{len(tp_rows)} のうち 高値が指値の呼値ちょうど(並び待ちで約定しない可能性) {int(exact.sum())}玉  その利益合計 {tp_rows.yen[exact].sum()/1e4:+.1f}万")
# (f) 損切スリッページ 0.2%/0.5%
stops = X[X.typ.isin(["STOP"])]
print(f"  損切玉{len(stops)} 建玉合計{stops.notional.sum()/1e4:.0f}万 → 滑り0.2%で{-stops.notional.sum()*0.002/1e4:+.1f}万 / 0.5%で{-stops.notional.sum()*0.005/1e4:+.1f}万")
gaps = X[X.typ.isin(["GAP", "初日処分", "RSI翌朝"])]
print(f"  寄り成行で出る玉{len(gaps)} 建玉合計{gaps.notional.sum()/1e4:.0f}万 → 滑り0.1%で{-gaps.notional.sum()*0.001/1e4:+.1f}万")
# (g) 全部まとめて悲観(RSI翌朝+利確ちょうど除外は近似で利確玉の利益をTP→期限扱いにはできないので、滑りだけ合算)
# (h) 建玉額と株数丸め
print(f"  平均建玉 {X.notional.mean()/1e4:.1f}万 / 最小 {X.notional.min()/1e4:.1f}万  (150万に対し)")
# (i) 損益集中: 2017-21 vs 22-26
for a, b in (("2017-01-01", "2021-12-31"), ("2022-01-01", "2026-12-31"), ("2026-01-01", "2026-12-31")):
    s = st(R0, a, b); print(f"  {a[:4]}-{b[:4]} 玉{s['n']} PF{s['pf']:.2f} {s['tot']:+.1f}万 DD{s['dd']:+.1f}")
# (j) 勝ち玉の中央値・負け玉の中央値
print(f"  勝ち玉 中央値{X.pnl[X.pnl>0].median():+.2f}% 平均{X.pnl[X.pnl>0].mean():+.2f}% / 負け玉 中央値{X.pnl[X.pnl<=0].median():+.2f}% 平均{X.pnl[X.pnl<=0].mean():+.2f}%  件平均{X.pnl.mean():+.3f}%")
X.to_pickle(os.path.join(OUT, "R0_10y.pkl"))
pickle.dump(dict(BASE=Hn.BASE, LP=LP), open(os.path.join(OUT, "mask.pkl"), "wb"))
print(f"done {time.time()-t0:.0f}s")

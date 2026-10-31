# -*- coding: utf-8 -*-
"""chk5: (1) 株価≤1万円を『調整後』で判定している先読みの影響を harness 上で測る
   2021-10以降は生値で厳密、以前は AdjFactor の累積(2021-10〜)で割り戻した下限値（分割のみ想定）
   (2) 寄指不成立(miss)の回数 (3) wide キャッシュの DAYS に営業日の欠けが無いか"""
import os, sys, time, pickle
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project"); sys.path.insert(0, "."); sys.path.insert(0, "_gk1028")
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo"
t0 = time.time()
import harness as Hn
E = Hn.E
print(f"[基準] {Hn.S0}  寄指不成立(miss) {Hn.R0.attrs['miss']} 回 / 候補ゼロの夜 {Hn.R0.attrs['empty']}")
X10 = Hn.R0[Hn.R0.entry >= "2017-01-01"]
print(f"  2017〜 玉{len(X10)}  2017〜の不成立回数は run 全体の attrs にしか無いので全期間: miss {Hn.R0.attrs['miss']}  (玉{len(Hn.R0)}) → 不成立率 {Hn.R0.attrs['miss']/(Hn.R0.attrs['miss']+len(Hn.R0))*100:.0f}%")
# ── AdjFactor 累積 ──
B = pickle.load(open("_bars_daily_5y_1024.pkl", "rb"))[["Date", "Code", "AdjFactor"]]
B = B[B.AdjFactor.fillna(1.0) != 1.0]
print("AdjFactor≠1 の行", len(B), " 銘柄", B.Code.nunique())
AF = {}
for code, g in B.groupby("Code"):
    AF[code[:4]] = (g.Date.to_numpy(), g.AdjFactor.to_numpy(dtype=float))
ENT = pd.to_datetime(np.concatenate([E.C.entry.to_numpy(), E.NF.entry.to_numpy()]))
SIG = ENT - pd.Timedelta(days=1)   # 近似(シグナル日はその前営業日・分割の権利落ち日が建て日と一致する稀なケースだけずれる)
RAW_LB = E.PRICE.copy()
for i in np.where(Hn.BASE)[0]:
    a = AF.get(str(E.TICK[i])[:4])
    if a is None: continue
    ds, fs = a
    fut = fs[ds > np.datetime64(SIG[i])]
    if len(fut): RAW_LB[i] = E.PRICE[i] / np.prod(fut)
over = Hn.BASE & (RAW_LB > 10000)
over_i = np.where(over)[0]
print(f"候補のうち当時の株価(下限)が1万円超 = 本番なら弾かれていた候補: {len(over_i)}件  銘柄: {pd.Series([str(E.TICK[i])[:4] for i in over_i]).value_counts().head(12).to_dict()}")
picked = X10[X10.i.isin(over_i)]
print(f"  うち基準BTで選ばれた玉 {len(picked)}玉 損益 {picked.yen.sum()/1e4:+.1f}万  内訳: " + ", ".join(f"{r.ticker[:4]}@{r.entry.date()} {r.yen/1e4:+.1f}" for r in picked.itertuples()))
R = E.run(slots=1, size=Hn.SIZE, mask=Hn.BASE & ~over, cost_rt=Hn.COST); s = Hn.st(R)
print(f"  株価上限を当時の生値で判定(近似): 玉{s['n']} PF{s['pf']:.2f} 10年{s['tot']:+.1f}万({s['tot']-Hn.S0['tot']:+.1f}) DD{s['dd']:+.1f}  前半{Hn.st(R, Hn.A, '2021-12-31')['tot']:+.1f} 後半{Hn.st(R, '2022-01-01', Hn.B)['tot']:+.1f}")
# ── wide キャッシュの DAYS ──
W = pickle.load(open("_bt_gokujo_wide_1029.pkl", "rb")); WD = W["DAYS"]
cal = pickle.load(open(os.path.join(OUT, "cal.pkl"), "rb")); uni = cal["uni"]
u2 = uni[(uni >= WD.min()) & (uni <= WD.max())]
miss_days = u2.difference(WD)
print(f"wide DAYS {len(WD)} ({WD.min().date()}〜{WD.max().date()}) 営業日(主力8銘柄) {len(u2)} → DAYSに無い営業日 {len(miss_days)} {list(miss_days[:8].date)}")
print(f"done {time.time()-t0:.0f}s")

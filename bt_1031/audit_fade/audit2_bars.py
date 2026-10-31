# 監査2: 生の日足(_bars_daily_5y_1024.pkl・10/30まで)で (5)引けS高の翌日寄り買い戻し・IPO直後・出来高比・台帳との照合・遊びの再現。読むだけ。
import os, json, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_fade"
MFP = r"C:\Users\星野\Documents\my-first-project"
L = []
def W(*a):
    s = " ".join(str(x) for x in a); print(s, flush=True); L.append(s)
Z0 = pd.read_pickle(os.path.join(OUT, "_Z0.pkl")); U = pd.read_pickle(os.path.join(OUT, "_U.pkl"))
B = pd.read_pickle(os.path.join(MFP, "_bars_daily_5y_1024.pkl"))
B = B[B.Code.astype(str).str[-1] == "0"].copy(); B["code"] = B.Code.astype(str).str[:4]
for k in ("O", "H", "L", "C", "Vo", "Va", "UL", "AdjFactor"): B[k] = pd.to_numeric(B[k], errors="coerce")
B = B.sort_values(["code", "Date"]).reset_index(drop=True)
days = np.sort(B.Date.unique()); nxt = dict(zip(days[:-1], days[1:]))
B["nday"] = B.Date.map(nxt)
nx = B[["code", "Date", "O", "C", "H", "L", "Vo"]].rename(columns={"Date": "nday", "O": "nO", "C": "nC", "H": "nH", "L": "nL", "Vo": "nVo"})
# ── (5) 売った日が引けS高の玉: 引けで買い戻せず翌日寄りで買い戻した場合 ──
cl = Z0[(Z0.tUL == 1) & (Z0.tC >= Z0.tH)].copy()
cl = cl.merge(B[["code", "Date", "Vo", "nday"]].rename(columns={"Date": "d"}), on=["code", "d"], how="left")
cl = cl.merge(nx.rename(columns={"nday": "d2"}), left_on=["code", "nday"], right_on=["code", "d2"], how="left")
sh = cl.nt / ((cl.tO + cl.tH) / 2)   # 概算株数(建玉÷売値)。2段の平均なので概算
cl["yen_nextopen"] = cl.yen - (cl.nO * 1.001 - cl.tC * 1.001) * (cl.nt / cl.tH)   # 買値を引け→翌日寄りに置き換え(株数≈建玉÷S高値)
cl["yen_nextclose"] = cl.yen - (cl.nC * 1.001 - cl.tC * 1.001) * (cl.nt / cl.tH)
W("(5) 引けS高で終わった玉", len(cl), " BTの損益", f"{cl.yen.sum()/1e4:+.0f}万", " 翌日寄りで買い戻したら", f"{cl.yen_nextopen.sum()/1e4:+.0f}万", " 翌日引けなら", f"{cl.yen_nextclose.sum()/1e4:+.0f}万",
  " 翌日寄りのギャップ中央値", f"{((cl.nO / cl.tC - 1).median())*100:+.1f}%", " 翌日寄りが上の割合", f"{(cl.nO > cl.tC).mean()*100:.0f}%")
W("   全体の合計 +3,100万 → 引けS高玉を翌日寄り買い戻しにすると", f"{(Z0.yen.sum() - cl.yen.sum() + cl.yen_nextopen.sum())/1e4:+,.0f}万")
# 引けの出来高が極端に薄い(比例配分っぽい)日
W("   引けS高玉の T日出来高 中央値", f"{cl.Vo.median():,.0f}株", " 建玉株数÷出来高 中央値", f"{((cl.nt/cl.tH)/cl.Vo).median()*100:.2f}%")
# ── (3) 建玉株数 ÷ T日の出来高 ──
Zv = Z0.merge(B[["code", "Date", "Vo"]].rename(columns={"Date": "d"}), on=["code", "d"], how="left")
ratio = (Zv.nt / Zv.tO.where(Zv.at_open, Zv.C * 1.03)) / Zv.Vo
W("\n(3) 建玉株数÷T日出来高: 中央値", f"{ratio.median()*100:.3f}%", " 95%点", f"{ratio.quantile(0.95)*100:.2f}%", " 最大", f"{ratio.max()*100:.1f}%", " 1%超の本数", int((ratio > 0.01).sum()))
W("   寄り約定の玉で 寄り値 > +12%指値 (2段とも寄りで約定) の本数", int((Z0.at_open & (Z0.tO >= Z0.C.map(lambda p: np.ceil(p * 1.12)))).sum()))
# ── (9) 上場直後 ──
B["first"] = B.groupby("code").Date.transform("min")
fd = B.groupby("code")["first"].first()
Z0["age"] = np.busday_count(Z0.code.map(fd).values.astype("datetime64[D]"), Z0.sig.values.astype("datetime64[D]"))
pf = lambda v: v[v > 0].sum() / -v[v < 0].sum() if (v < 0).any() else np.inf
for a in (1, 5, 20):
    g = Z0[Z0.age <= a]; W(f"(9) 上場から{a}営業日以内にDが来た玉(2021-10-04以降上場のみ判定可): {len(g)}本 損益{g.yen.sum()/1e4:+.0f}万 PF{pf(g.yen.to_numpy()):.2f}")
# ── (11) 台帳との照合: BTの式で 10/28・10/29・10/30 基準の3本と遊びを作る ──
Bm = B[B.Date >= "2026-07-01"].copy()
g = Bm.groupby("code", sort=False)
Bm["fwdF"] = g.AdjFactor.transform(lambda s: s[::-1].cumprod()[::-1].shift(-1).fillna(1.0))
Bm["adjC"] = Bm.C * Bm.fwdF; Bm["ret1"] = Bm.adjC / g.adjC.shift(1) - 1
M = pd.read_pickle(os.path.join(MFP, "_master_1024.pkl")); M["code"] = M.Code.astype(str).str[:4]
etf = set(M.loc[M.S17.astype(str) == "99", "code"])
Bm = Bm[~Bm.code.isin(etf)]
def tk(p): return 1 if p <= 3000 else 5 if p <= 5000 else 10 if p <= 30000 else 50 if p <= 50000 else 100
def lim(c, k): return np.ceil(c * (1 + k) / tk(c)) * tk(c)
led = json.load(open(r"C:\Users\星野\Documents\my-first-project-fade-notify\kabuai\fade_notify_ledger.json", encoding="utf-8"))
for D in ("2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"):
    d = Bm[(Bm.Date == D) & (Bm.C >= 100) & (Bm.ret1 >= 0.12)].copy()
    d["capok"] = [lim(p, 0.03) * 100 <= 1e6 for p in d.C]
    t3 = d[(d.Va >= 3e8) & d.capok].sort_values(["ret1", "code"], ascending=[False, True]).head(3)
    aso = d[(d.Va >= 1e8) & (d.Va < 3e8)].sort_values(["ret1", "code"], ascending=[False, True]).head(1)
    T = nxt.get(pd.Timestamp(D)); tkey = str(T)[:10] if T is not None else None
    W(f"\n(11) 基準日 {D} → 取引日 {tkey}  BT式の3本:", [(c, int(p), f"{r*100:+.1f}%", f"代金{v/1e8:.1f}億", f"指値{lim(p,0.03):.0f}/{lim(p,0.12):.0f}") for c, p, r, v in zip(t3.code, t3.C, t3.ret1, t3.Va)])
    W("      BT式の遊び:", [(c, int(p), f"{r*100:+.1f}%", f"代金{v/1e8:.1f}億", f"指値{lim(p,0.12):.0f}") for c, p, r, v in zip(aso.code, aso.C, aso.ret1, aso.Va)])
    pl = led["plans"].get(tkey or "", {})
    if pl:
        W("      台帳 top3:", [(x["code"], int(x["close"]), f"{x['ret1']*100:+.1f}%") for x in pl.get("top3", {}).get("rows", [])], " 台帳 遊び:", (lambda a: (a["code"], int(a["close"]), a.get("lim"), a.get("shares")) if a else None)(pl.get("asobi")))
    else:
        W("      台帳に", tkey, "の plan なし")
    if T is not None:
        for c in list(t3.code) + list(aso.code):
            r = Bm[(Bm.code == c) & (Bm.Date == T)]
            if len(r):
                r = r.iloc[0]; W(f"      T日 {c}: 寄{r.O:.0f} 高{r.H:.0f} 安{r.L:.0f} 引{r.C:.0f} UL={int(r.UL)} 出来高{r.Vo:,.0f}  前日終値+値幅={int(d[d.code==c].C.iloc[0])}")
# ── 遊びの再現(1〜3億・1日1本・+12%・25万・1tick抜け) と コスト ──
def band(p):
    for l, w in ((100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000)):
        if p < l: return w
    return p * 0.15
A = U[(U.gain >= 12) & (U.Va >= 1e8) & (U.Va < 3e8)].sort_values(["Date", "gain", "code"], ascending=[True, False, True])
A = A.assign(rk=A.groupby("Date").cumcount() + 1); A = A[A.rk == 1]
def play(A, k=0.12, strict=True, cost=0.0, size=250000):
    rows = []
    for r in A.itertuples():
        if r.pin: continue
        Lp = lim(r.C, k); s = r.tO if r.tO >= Lp else (Lp if r.tH >= Lp + (tk(Lp) if strict else 0) else None)
        if s is None: continue
        sh = int(size / Lp // 100 * 100)
        if sh < 100:
            if Lp * 100 > 2 * size: continue
            sh = 100
        y = (s - r.tC * 1.001) * sh - s * sh * cost; rows.append(dict(d=r.nday, yen=y, nt=s * sh, at_open=s == r.tO, touch=(s != r.tO) and (r.tH == Lp), clUL=(r.tUL == 1) & (r.tC >= r.tH), ulpct=band(r.C) / r.C))
    return pd.DataFrame(rows)
def st(Z):
    day = Z.groupby("d").yen.sum().sort_index(); cc = day.cumsum().to_numpy(); dd = (np.maximum.accumulate(np.r_[0, cc])[1:] - cc).max()
    return f"n={len(Z)} 勝率{(Z.yen > 0).mean()*100:.0f}% PF{pf(Z.yen.to_numpy()):.2f} 合計{Z.yen.sum()/1e4:+,.0f}万 DD{dd/1e4:.0f}万"
Pz = play(A); W("\n(遊び) 再現 1tick抜け:", st(Pz), " ← RESULTS_play_k +12%: 338 56% PF2.48 +285万 DD10万")
W("   置いた日", int((~A.pin).sum()), " 寄りで約定(寄りが+12%以上)", f"{Pz.at_open.mean()*100:.0f}%", " 引けS高で終わった本", int(Pz.clUL.sum()), f"損益{Pz[Pz.clUL].yen.sum()/1e4:+.0f}万")
W("   緩め(ちょうど可):", st(play(A, strict=False)))
for c in (0.005, 0.01, 0.02, 0.03): W(f"   コスト{c*100:.1f}%:", st(play(A, cost=c)))
W("   遊びの対象銘柄の株価帯:", pd.cut(A.C, [0, 200, 500, 1000, 3000, 10000]).value_counts().sort_index().to_dict(), " 代金1〜3億の銘柄で25万(数百〜2千株)を+12%で1tick約定の前提")
open(os.path.join(OUT, "audit2_out.txt"), "w", encoding="utf-8").write("\n".join(L) + "\n")

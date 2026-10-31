# 監査4: 追加の検算 (a)遊びの引けS高玉を翌日寄りで買い戻した場合 (b)20年: 寄り=高値で古い値幅表ならS高だった可能性の玉の損益
import os, pickle, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_fade"
MFP = r"C:\Users\星野\Documents\my-first-project"
L = []
def W(*a):
    s = " ".join(str(x) for x in a); print(s, flush=True); L.append(s)
def tk(p): return 1 if p <= 3000 else 5 if p <= 5000 else 10 if p <= 30000 else 50 if p <= 50000 else 100
def lim(c, k): return np.ceil(c * (1 + k) / tk(c)) * tk(c)
pf = lambda v: v[v > 0].sum() / -v[v < 0].sum() if (v < 0).any() else np.inf
# (a) 遊び
U = pd.read_pickle(os.path.join(OUT, "_U.pkl"))
A = U[(U.gain >= 12) & (U.Va >= 1e8) & (U.Va < 3e8)].sort_values(["Date", "gain", "code"], ascending=[True, False, True])
A = A.assign(rk=A.groupby("Date").cumcount() + 1); A = A[(A.rk == 1) & ~A.pin]
B = pd.read_pickle(os.path.join(MFP, "_bars_daily_5y_1024.pkl")); B = B[B.Code.astype(str).str[-1] == "0"].copy(); B["code"] = B.Code.astype(str).str[:4]
B["O"] = pd.to_numeric(B.O, errors="coerce"); B = B.sort_values(["code", "Date"])
days = np.sort(B.Date.unique()); nxt = dict(zip(days[:-1], days[1:]))
nO = B.set_index(["code", "Date"]).O
rows = []
for r in A.itertuples():
    Lp = lim(r.C, 0.12); s = r.tO if r.tO >= Lp else (Lp if r.tH >= Lp + tk(Lp) else None)
    if s is None: continue
    sh = int(250000 / Lp // 100 * 100)
    if sh < 100:
        if Lp * 100 > 5e5: continue
        sh = 100
    clul = (r.tUL == 1) and (r.tC >= r.tH)
    d2 = nxt.get(r.nday); o2 = nO.get((r.code, d2), np.nan) if d2 is not None else np.nan
    y = (s - r.tC * 1.001) * sh
    y2 = (s - o2 * 1.001) * sh if clul and np.isfinite(o2) else y
    rows.append(dict(d=r.nday, yen=y, yen2=y2, clul=clul))
Z = pd.DataFrame(rows)
W("(遊び) n", len(Z), " BT合計", f"{Z.yen.sum()/1e4:+.0f}万", " 引けS高玉", int(Z.clul.sum()), f"({Z.clul.mean()*100:.0f}%)", " その損益", f"{Z[Z.clul].yen.sum()/1e4:+.0f}万",
  " 引けS高玉を翌日寄りで買い戻すと合計", f"{Z.yen2.sum()/1e4:+.0f}万", f"PF{pf(Z.yen.to_numpy()):.2f}→{pf(Z.yen2.to_numpy()):.2f}")
# (b) 20年
UL = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
      (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000), (100000, 15000), (1e12, 30000)]
def limit_w(p): return next(w for ub, w in UL if p < ub)
H = pickle.load(open(os.path.join(MFP, "tachibana_history.pkl"), "rb"))["all_data"]
rows = []
for t, x in H.items():
    if x is None or len(x) < 30: continue
    x = x.dropna(subset=["Close"])
    c = x.Close.to_numpy(float); o = x.Open.to_numpy(float); h = x.High.to_numpy(float); l = x.Low.to_numpy(float); v = x.Volume.to_numpy(float)
    pc = np.r_[np.nan, c[:-1]]; r1 = c / pc - 1
    idx = np.where((r1 >= 0.12) & (c >= 100) & (c * v >= 3e8))[0]; idx = idx[idx + 1 < len(c)]
    ds = x.index.strftime("%Y-%m-%d")
    for i in idx:
        rows.append((ds[i], ds[i + 1], t[:4], r1[i] * 100, c[i], o[i + 1], h[i + 1], l[i + 1], c[i + 1], o[i + 1] >= c[i] + limit_w(c[i])))
del H
F = pd.DataFrame(rows, columns=["sig", "ent", "ticker", "gain", "px", "o1", "h1", "l1", "c1", "pin"])
F = F[(F.o1 > 0) & (np.array([lim(c, 0.03) for c in F.px]) * 100 <= 1e6)]
F = F.sort_values(["sig", "gain", "ticker"], ascending=[True, False, True]); F["rk"] = F.groupby("sig").cumcount() + 1
MA = F[(F.rk <= 3) & ~F.pin].copy(); MA["y"] = MA.ent.str[:4].astype(int)
def one(P, O, Hh, Cl, slip=0.001):
    L3 = lim(P, 0.03); s3 = O if O >= L3 else (L3 if Hh >= L3 else None)
    if s3 is None: return None
    L2 = lim(P, 0.12); h3 = int(250000 / L3 // 100 * 100); h2 = int(250000 / L2 // 100 * 100)
    if h3 < 100 or h2 < 100:
        sh = int(500000 / L3 // 100 * 100); sh = sh if sh >= 100 else 100
        return (s3 - Cl * (1 + slip)) * sh
    s2 = O if O >= L2 else (L2 if Hh >= L2 else None); y = (s3 - Cl * (1 + slip)) * h3
    if s2 is not None: y += (s2 - Cl * (1 + slip)) * h2
    return y
MA["yen"] = [one(r.px, r.o1, r.h1, r.c1) for r in MA.itertuples()]
MA = MA[MA.yen.notna()]
sus = (MA.o1 == MA.h1) & (MA.o1 / MA.px - 1 >= 0.08)
W("(20年) 全部", len(MA), f"{MA.yen.sum()/1e4:+,.0f}万", " 寄り=高値かつ寄り+8%以上(古い値幅表ならS高張り付きだった可能性・2013年以前)", int((sus & (MA.y <= 2013)).sum()),
  f"損益{MA[sus & (MA.y <= 2013)].yen.sum()/1e4:+.0f}万", "  同 2014年以降(表が合っているはずなのに残る=寄りS高でない単なる寄り天)", int((sus & (MA.y >= 2014)).sum()), f"{MA[sus & (MA.y >= 2014)].yen.sum()/1e4:+.0f}万")
W("  寄り+8%以上で寄り=高値の玉の全体: 寄り天で売れた=最良ケースの玉。年代別本数:", (MA[sus].y // 5 * 5).value_counts().sort_index().to_dict())
W("  2013年以前の全玉:", int((MA.y <= 2013).sum()), f"{MA[MA.y <= 2013].yen.sum()/1e4:+,.0f}万", " 2014年以降:", int((MA.y >= 2014).sum()), f"{MA[MA.y >= 2014].yen.sum()/1e4:+,.0f}万")
open(os.path.join(OUT, "audit4_out.txt"), "w", encoding="utf-8").write("\n".join(L) + "\n")

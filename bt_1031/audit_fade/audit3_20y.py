# 監査3: 20年(立花)側。調整済み価格に100株・呼値・値幅表を当てている件、寄りS高判定の取りこぼし、時代別。読むだけ。
import os, pickle, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_fade"
L = []
def W(*a):
    s = " ".join(str(x) for x in a); print(s, flush=True); L.append(s)
UL = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
      (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000), (100000, 15000), (1e12, 30000)]
def limit_w(p): return next(w for ub, w in UL if p < ub)
def tk(p): return 1 if p <= 3000 else 5 if p <= 5000 else 10 if p <= 30000 else 50 if p <= 50000 else 100
def lim(c, k): return np.ceil(c * (1 + k) / tk(c)) * tk(c)
H = pickle.load(open(r"C:\Users\星野\Documents\my-first-project\tachibana_history.pkl", "rb"))["all_data"]
rows = []
for t, x in H.items():
    if x is None or len(x) < 30: continue
    x = x.dropna(subset=["Close"])
    c = x.Close.to_numpy(float); o = x.Open.to_numpy(float); h = x.High.to_numpy(float); l = x.Low.to_numpy(float); v = x.Volume.to_numpy(float)
    pc = np.r_[np.nan, c[:-1]]; r1 = c / pc - 1
    idx = np.where((r1 >= 0.12) & (c >= 100) & (c * v >= 3e8))[0]; idx = idx[idx + 1 < len(c)]
    ds = x.index.strftime("%Y-%m-%d")
    for i in idx:
        rows.append((ds[i], ds[i + 1], t[:4], r1[i] * 100, c[i], o[i + 1], h[i + 1], l[i + 1], c[i + 1], v[i + 1], o[i + 1] >= c[i] + limit_w(c[i]),
                     (x.index[i] - x.index[0]).days))
del H
F = pd.DataFrame(rows, columns=["sig", "ent", "ticker", "gain", "px", "o1", "h1", "l1", "c1", "v1", "pin", "age"])
F = F[F.o1 > 0]
F = F[np.array([lim(c, 0.03) for c in F.px]) * 100 <= 1e6]
F = F.sort_values(["sig", "gain", "ticker"], ascending=[True, False, True]); F["rk"] = F.groupby("sig").cumcount() + 1
MA = F[F.rk <= 3].copy(); MA["y"] = MA.ent.str[:4].astype(int)
W("20年 3本候補", len(MA), " 日数", MA.sig.nunique(), " pin(表で寄りS高)", int(MA.pin.sum()))
flat = (MA.o1 == MA.h1) & (MA.h1 == MA.l1) & (MA.l1 == MA.c1)
W("  T日が O=H=L=C(終日1本値=張り付きの形) の本数", int(flat.sum()), " うち pin 判定されず(値幅表に合わない)", int((flat & ~MA.pin).sum()),
  "  年代別(判定されず):", (MA[flat & ~MA.pin].y // 5 * 5).value_counts().sort_index().to_dict())
# 寄りが高値で、その後下げた日(寄りS高→剥がれ)で、表に合わないもの: o1==h1 かつ o1/px-1 が 寄り≥+8% なのに pin でない
oh = (MA.o1 == MA.h1) & ~MA.pin & (MA.o1 / MA.px - 1 >= 0.08)
W("  寄り=高値・寄り+8%以上・pin でない(古い値幅表ならS高だった可能性):", int(oh.sum()), " 年代別:", (MA[oh].y // 5 * 5).value_counts().sort_index().to_dict())
def one(P, O, H, Cl, slip=0.001):
    L3 = lim(P, 0.03); s3 = O if O >= L3 else (L3 if H >= L3 else None)
    if s3 is None: return None
    L2 = lim(P, 0.12); h3 = int(250000 / L3 // 100 * 100); h2 = int(250000 / L2 // 100 * 100)
    if h3 < 100 or h2 < 100:
        sh = int(500000 / L3 // 100 * 100); sh = sh if sh >= 100 else 100
        return (s3 - Cl * (1 + slip)) * sh, s3 * sh
    s2 = O if O >= L2 else (L2 if H >= L2 else None); y = (s3 - Cl * (1 + slip)) * h3; nt = s3 * h3
    if s2 is not None: y += (s2 - Cl * (1 + slip)) * h2; nt += s2 * h2
    return y, nt
def run(T, pinmask):
    rows = []
    for r, p in zip(T.itertuples(), pinmask):
        if p: continue
        z = one(r.px, r.o1, r.h1, r.c1)
        if z: rows.append(dict(d=r.ent, y=r.y, yen=z[0], nt=z[1], px=r.px, r=z[0] / z[1], flat=(r.o1 == r.h1 == r.l1 == r.c1), age=r.age))
    return pd.DataFrame(rows)
pf = lambda v: v[v > 0].sum() / -v[v < 0].sum()
def st(Z):
    day = Z.groupby("d").yen.sum().sort_index(); cc = day.cumsum().to_numpy(); dd = (np.maximum.accumulate(np.r_[0, cc])[1:] - cc).max()
    return f"n={len(Z)} 勝率{(Z.yen > 0).mean()*100:.0f}% PF{pf(Z.yen.to_numpy()):.2f} 合計{Z.yen.sum()/1e4:+,.0f}万 DD{dd/1e4:.0f}万"
Z = run(MA, MA.pin); W("  再現(代金は当日 c*v 版・vamin は前日代金で 10,272本):", st(Z))
W("  pin を『表』＋『終日1本値』にした場合:", st(run(MA, MA.pin | flat)))
W("  年代別 PF/合計:", {y: f"{pf(g.yen.to_numpy()):.2f}/{g.yen.sum()/1e4:+.0f}万/{len(g)}本" for y, g in Z.groupby(Z.y // 5 * 5)})
# 調整済み価格の問題: 立花は分割調整済み(トヨタ2001年=744円)。株価帯の分布を年代別に
W("  調整後の前日終値の中央値(年代別):", {y: f"{g.px.median():.0f}円" for y, g in Z.groupby(Z.y // 5 * 5)}, " ← 当時の実際の株価(単元1株/1000株時代)とは別物。100株・呼値・値幅表・100円以上は調整後の値に当てている")
W("  上場から60日以内の玉:", int((Z.age <= 60).sum()), f"損益{Z[Z.age <= 60].yen.sum()/1e4:+.0f}万", " (立花は現存銘柄のみ・上場日=データ開始日)")
open(os.path.join(OUT, "audit3_out.txt"), "w", encoding="utf-8").write("\n".join(L) + "\n")

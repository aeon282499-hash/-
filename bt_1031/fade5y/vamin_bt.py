# 今日の3本の「代金3億以上」を下げたらどうなるか（2026-10-30 本人: 夢みつけ隊 +18.9%・代金1.3億が3本に入らなかった件）。検証のみ。
#  ルールは本番どおり: 前日比+12%以上・終値100円以上・100株×(前日終値+3%)≦100万・前日比の大きい順に3本・寄りS高張り付きは見送り(繰り上げなし)
#  撃ち方: +3%で25万＋+12%で25万の2段指値(どちらかの段で100株買えなければ+3%で50万一発・100株は100万まで)→引成・滑り片道0.1%
#  比べる形: (A) 代金の線を X に下げて並べ直す (B) 3億以上で3本並べ、足りない枠だけ X〜3億で埋める (C) X〜3億の帯だけ(全部・順位なし)
import os, sys, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
def tk(p): return 1 if p <= 3000 else 5 if p <= 5000 else 10 if p <= 30000 else 50 if p <= 50000 else 100
def lim(c, k): return np.ceil(c * (1 + k) / tk(c)) * tk(c)
def fill(O, H, Lp): return O if O >= Lp else (Lp if H >= Lp else None)
def one(P, O, H, Cl, slip=0.001):
    L3 = lim(P, 0.03); s3 = fill(O, H, L3)
    if s3 is None: return None
    L2 = lim(P, 0.12); h3 = int(250000 / L3 // 100 * 100); h2 = int(250000 / L2 // 100 * 100)
    if h3 < 100 or h2 < 100:
        sh = int(500000 / L3 // 100 * 100); sh = sh if sh >= 100 else 100
        return (s3 - Cl * (1 + slip)) * sh, s3 * sh
    s2 = fill(O, H, L2); y = (s3 - Cl * (1 + slip)) * h3; nt = s3 * h3
    if s2 is not None: y += (s2 - Cl * (1 + slip)) * h2; nt += s2 * h2
    return y, nt
pf = lambda v: v[v > 0].sum() / -v[v < 0].sum() if (v < 0).any() else np.inf
def run(T):
    rows = []
    for r in T.itertuples():
        if r.pin: continue
        z = one(r.px, r.o, r.h, r.c)
        if z: rows.append({"d": r.d, "yen": z[0], "r": z[0] / z[1], "va": r.va})
    Z = pd.DataFrame(rows, columns=["d", "yen", "r", "va"]); Z["m"] = Z.d.str[:7]; return Z
def st(Z, split):
    if not len(Z): return "| 0 | | | | | | | |"
    day = Z.groupby("d").yen.sum().sort_index(); cc = day.cumsum().to_numpy(); mo = Z.groupby("m").yen.sum()
    dd = (np.maximum.accumulate(np.r_[0, cc])[1:] - cc).max()
    a, b = Z[Z.d < split], Z[Z.d >= split]
    return (f"| {len(Z)} | {(Z.yen > 0).mean()*100:.0f}% | {Z.r.mean()*100:+.2f}% | {pf(Z.yen.to_numpy()):.2f} | {Z.yen.sum()/1e4:+,.0f}万 | {dd/1e4:.0f}万 | "
            f"{mo.min()/1e4:+.0f}万 | {int((mo < 0).sum())}/{len(mo)} | {pf(a.yen.to_numpy()):.2f} / {pf(b.yen.to_numpy()):.2f} | {Z.r.min()*100:+.0f}% |")
HD = "| 形 | 本数 | 勝率 | 平均 | PF | 合計 | 最大DD | 最悪月 | 負け月 | 前半/後半 PF | 最悪1本 |\n|---|---|---|---|---|---|---|---|---|---|---|"
def base(U, vamin):
    T = U[(U.g >= 12) & (U.va >= vamin) & U.capok].sort_values(["d", "g", "code"], ascending=[True, False, True])
    return T.assign(rk=T.groupby("d").cumcount() + 1)
def topn(T, n=3): return T[T.rk <= n]
def fillup(U, vamin):
    A = topn(base(U, 3e8)); B = base(U[U.va < 3e8], vamin)
    room = (3 - A.groupby("d").size()).to_dict()
    B = B[B.rk <= B.d.map(lambda d: room.get(d, 3))]
    return pd.concat([A, B])
XS = [3e8, 2e8, 1e8, 5e7]
def report(U, split, title, L):
    L += [f"\n## {title}", "### (A) 代金の線を下げて並べ直す（3本）", HD]
    for x in XS: L.append(f"| 代金{x/1e8:g}億以上 " + st(run(topn(base(U, x))), split))
    L += ["### (B) 3億以上で3本 → 足りない枠だけ X〜3億で埋める", HD]
    for x in XS[1:]:
        Z = run(fillup(U, x)); L.append(f"| 埋め{x/1e8:g}億〜 " + st(Z, split))
    for x in XS[1:]:
        F = fillup(U, x); F = F[F.va < 3e8]; L.append(f"| うち埋めた玉だけ {x/1e8:g}〜3億 " + st(run(F), split))
    L += ["### (C) 帯ごと（+12%以上・順位なし・全部）", HD]
    for lo, hi in [(3e8, 1e13), (2e8, 3e8), (1e8, 2e8), (5e7, 1e8)]:
        T = U[(U.g >= 12) & (U.va >= lo) & (U.va < hi) & U.capok]
        L.append(f"| {lo/1e8:g}億〜{'' if hi > 1e12 else f'{hi/1e8:g}億'} " + st(run(T), split))

L = ["# 今日の3本: 代金3億の線を下げたら（検証のみ）",
     "本番どおり +12%以上・3本・+3%/+12%の2段指値・引成・滑り0.1%・張り付き見送り。前半/後半は5年=2024年で、20年=2014年で分割。"]
# ── 5年: J-Quants 日足パネル(上場廃止込み・C は Va≥5千万まで入っている) ──
os.chdir(os.path.dirname(HERE))
G = {}; exec(open(os.path.join(HERE, "top3_tune.py"), encoding="utf-8").read().split("L = []; W = L.append")[0], G)
C = G["C"]
U5 = pd.DataFrame({"d": C.nday.dt.strftime("%Y-%m-%d"), "code": C.code, "g": C.gain, "va": C.Va, "px": C.C,
                   "o": C.tO, "h": C.tH, "c": C.tC, "pin": C.pin.astype(bool)})
U5 = U5[C.C >= 100]
U5["capok"] = [lim(p, 0.03) * 100 <= 1e6 for p in U5.px]
report(U5, "2024-01-01", "5年（J-Quants 2021-11〜2026-10・上場廃止込み）", L)
z26 = U5[U5.d >= "2026-01-01"]
L += ["### 2026年だけ (A)", HD]
for x in XS: L.append(f"| 代金{x/1e8:g}億以上 " + st(run(topn(base(z26, x))), "2026-07-01"))
del G, C
# ── 20年: 立花の日足（現存銘柄のみ）を代金5千万から作り直す ──
src = open(os.path.join(HERE, "fade20y.py"), encoding="utf-8").read().split("# ── 友人の①: 終値100円以上")[0]
assert "(c * v >= 3e8)" in src
G = {}; exec(src.replace("(c * v >= 3e8)", "(c * v >= 5e7)"), G)
F = G["F"]
U20 = pd.DataFrame({"d": F.ent, "code": F.ticker, "g": F.gain, "va": F.px * 0, "px": F.px, "o": F.o1, "h": F.h1, "c": F.c1,
                    "pin": F.o1_is_limit.astype(bool)})
del G
# 代金は立花の終値×出来高を作り直す（fade20y は F に代金を持たないので）
import pickle
H = pickle.load(open(os.path.join(os.getcwd(), "tachibana_history.pkl"), "rb"))["all_data"]
va = {}
for t, x in H.items():
    if x is None or len(x) < 30: continue
    x = x.dropna(subset=["Close"]); s = (x.Close * x.Volume).shift(1)
    s.index = x.index.strftime("%Y-%m-%d"); va[t] = s
del H
U20["va"] = [va[t].get(d, np.nan) for t, d in zip(U20.code, U20.d)]
U20 = U20[U20.va.notna()]
U20["capok"] = [lim(p, 0.03) * 100 <= 1e6 for p in U20.px]
report(U20, "2014-01-01", "20年（立花 2001〜2026・現存銘柄のみ）", L)
out = os.path.join(HERE, "RESULTS_vamin.md")
open(out, "w", encoding="utf-8").write("\n".join(L) + "\n")
print("\n".join(L))

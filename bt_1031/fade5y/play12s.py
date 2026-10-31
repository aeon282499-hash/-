import re
src = open("fade5y/vamin_bt.py", encoding="utf-8").read()
head, rest = src.split('L = ["# 今日の3本', 1)
build = rest.split("\n", 2)[2]
build = build.replace('report(U5, "2024-01-01", "5年（J-Quants 2021-11〜2026-10・上場廃止込み）", L)', 'R5()')
build = build.replace('report(U20, "2014-01-01", "20年（立花 2001〜2026・現存銘柄のみ）", L)', 'R20()')
build = build.split("out = os.path.join")[0]
build = re.sub(r'z26 = U5.*?\ndel G, C', 'del G, C', build, flags=re.S)
play = '''
def play_run(T, k, size=250000):
    rows = []
    for r in T.itertuples():
        if r.pin: continue
        Lp = lim(r.px, k); s = r.o if r.o >= Lp else (Lp if r.h >= Lp + (tk(Lp) if STRICT else 0) else None)
        if s is None: continue
        sh = int(size / Lp // 100 * 100)
        if sh < 100:
            if Lp * 100 > 2 * size: continue
            sh = 100
        y = (s - r.c * 1.001) * sh; rows.append({"d": r.d, "yen": y, "r": y / (s * sh), "va": r.va})
    Z = pd.DataFrame(rows, columns=["d", "yen", "r", "va"]); Z["m"] = Z.d.str[:7]; return Z
STRICT = False
def comb(U, Z):
    A = run(topn(base(U, 3e8))); B = pd.concat([A, Z]); day = B.groupby("d").yen.sum().sort_index().cumsum().to_numpy()
    mo = B.groupby(B.d.str[:7]).yen.sum()
    return f"3本だけ +{A.yen.sum()/1e4:,.0f}万 DD{(np.maximum.accumulate(np.r_[0,A.groupby('d').yen.sum().sort_index().cumsum().to_numpy()])[1:]-A.groupby('d').yen.sum().sort_index().cumsum().to_numpy()).max()/1e4:.0f}万 → 3本＋遊び +{B.yen.sum()/1e4:,.0f}万 DD{(np.maximum.accumulate(np.r_[0,day])[1:]-day).max()/1e4:.0f}万 最悪月{mo.min()/1e4:+.0f}万 負け月{int((mo<0).sum())}"
def play_rep(U, split, title):
    global STRICT
    L.append(""); L.append(f"## {title} 遊び(+12%指値だけ・25万)"); L.append(HD)
    for lo, n in [(1e8, 99), (1e8, 1), (5e7, 1)]:
        T = U[(U.g >= 12) & (U.va >= lo) & (U.va < 3e8)].sort_values(["d", "g", "code"], ascending=[True, False, True])
        T = T.assign(rk=T.groupby("d").cumcount() + 1); T = T[T.rk <= n]
        for STRICT in (False, True):
            Z = play_run(T, 0.12)
            L.append(f"| {lo/1e8:g}〜3億{' 1日1本' if n == 1 else ' 全部'}{' 厳しめ(1tick抜け)' if STRICT else ''} " + st(Z, split))
        L.append("  - " + comb(U, Z))
    STRICT = False
    return
def _old(U, split, title):
    L.append(""); L.append(f"## {title}（指値だけ・1本25万・引成）"); L.append(HD)
    for lo, hi, n in [(1e8, 3e8, 99), (5e7, 3e8, 99), (1e8, 3e8, 1), (2e8, 3e8, 99), (3e8, 1e13, 99)]:
        T = U[(U.g >= 12) & (U.va >= lo) & (U.va < hi)].sort_values(["d", "g", "code"], ascending=[True, False, True])
        T = T.assign(rk=T.groupby("d").cumcount() + 1); T = T[T.rk <= n]
        if lo >= 3e8:
            T3 = topn(base(U, 3e8)); T = T[~T.set_index(["d", "code"]).index.isin(T3.set_index(["d", "code"]).index)]
        for k in (0.12, 0.03):
            nm = f"{lo/1e8:g}〜{'' if hi > 1e12 else f'{hi/1e8:g}'}億{'(4位以下)' if lo >= 3e8 else ''}{' 1日1本' if n == 1 else ''} 指値+{k*100:.0f}%"
            Z = play_run(T, k)
            yrs = Z.d.str[:4].nunique() if len(Z) else 1
            L.append(f"| {nm} " + st(Z, split) + f" 年{len(Z)/yrs:.0f}本")
R5 = lambda: play_rep(U5, "2024-01-01", "5年")
R20 = lambda: play_rep(U20, "2014-01-01", "20年")
'''
L = ["# 遊び: 3本の外の+12%以上に指値だけ置く（検証のみ）"]
g = {"L": L, "__file__": __import__("os").path.abspath("fade5y/vamin_bt.py")}
exec(head + play + build, g)
out = "\n".join(g["L"])
print(out); open(r"C:/Users/星野/Downloads/trade_bt/uwick_bt/fade5y/RESULTS_play12_strict.md", "w", encoding="utf-8").write(out + "\n")


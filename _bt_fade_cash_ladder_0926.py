# -*- coding: utf-8 -*-
"""_bt_fade_cash_ladder_0926.py — 現金余力C(30〜60万)から60営業日で15万割れ/ゼロを踏む率をサイズ別に(①②とも前終+1%指値・大型除外・10年)。
実行: python -X utf8 _bt_fade_cash_ladder_0926.py > _log_fade_cash_ladder_0926.txt"""
import os
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk <= 2].reset_index(drop=True)
allday = pd.Series(sorted(set(P10.ent.astype(str))))
o, h, c, px, rk, ent = d.o1.values, d.h1.values, d.c1.values, d.px.values, d.rk.values, d.ent.values
L = tickc(px * 1.01); ps = np.where(o >= L, o - c, np.where(h >= L, L - c, 0))
H = 60; rows = []
for s1, s2 in ((100, 50), (100, 0), (70, 50), (70, 35), (70, 0), (50, 50), (50, 25), (50, 0)):
    size = np.where(rk == 1, s1, s2) * 1e4; sh = np.where(size >= px * 100, size / px // 100 * 100, 0)
    day = pd.Series(ps * sh, index=ent).groupby(level=0).sum().reindex(allday, fill_value=0).values
    cum = np.concatenate([[0], np.cumsum(day)])
    m = np.array([(cum[i + 1:i + 1 + H] - cum[i]).min() for i in range(len(day) - H)])
    e = np.array([cum[i + H] - cum[i] for i in range(len(day) - H)])
    r = {"サイズ": f"①{s1}/②{s2}", "3か月後中央値万": round(np.median(e) / 1e4), "年平均万": round(ps.dot(sh) / 1e4 / 9.67)}
    for C in (30, 35, 40, 45, 50, 55, 60):
        r[f"{C}万:15割れ/ゼロ%"] = f"{(m <= -(C - 15) * 1e4).mean() * 100:.1f}/{(m <= -C * 1e4).mean() * 100:.1f}"
    rows.append(r)
pd.set_option("display.width", 300)
print(pd.DataFrame(rows).to_string(index=False))

# -*- coding: utf-8 -*-
"""_bt_fade_cash35_size_0926.py — 余力35万から60/120営業日で -20万(15万割れ)/-35万(ゼロ)を踏む率をサイズ別に(前終+1%/+0%/寄成・大型除外・10年)。
実行: python -X utf8 _bt_fade_cash35_size_0926.py > _log_fade_cash35_size_0926.txt"""
import os
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk <= 2].reset_index(drop=True)
allday = pd.Series(sorted(set(P10.ent.astype(str))))
o, h, c, px, rk, ent = d.o1.values, d.h1.values, d.c1.values, d.px.values, d.rk.values, d.ent.values
def ps(k):
    if k is None: return o - c
    L = tickc(px * (1 + k / 100)); return np.where(o >= L, o - c, np.where(h >= L, L - c, 0))
rows = []
for s1, s2 in ((100, 0), (100, 50), (70, 0), (50, 0), (50, 50), (30, 0)):
    for lab, k in (("寄成", None), ("+0%", 0), ("+1%", 1)):
        size = np.where(rk == 1, s1, s2) * 1e4; sh = np.where(size >= px * 100, size / px // 100 * 100, 0)
        yen = ps(k) * sh
        day = pd.Series(yen, index=ent).groupby(level=0).sum().reindex(allday, fill_value=0).values
        cum = np.concatenate([[0], np.cumsum(day)]); r = {"①": s1, "②": s2}; r.update(dict( 執行=lab, 年平均万=round(yen.sum() / 1e4 / 9.67, 1)))
        for H in (60, 120):
            m = np.array([(cum[i + 1:i + 1 + H] - cum[i]).min() for i in range(len(day) - H)])
            e = np.array([cum[i + H] - cum[i] for i in range(len(day) - H)])
            r[f"{H}日で15万割れ%"] = round((m <= -2e5).mean() * 100, 1); r[f"{H}日でゼロ%"] = round((m <= -3.5e5).mean() * 100, 1)
            r[f"{H}日後の中央値万"] = round(np.median(e) / 1e4, 1)
        rows.append(r)
pd.set_option("display.width", 250)
print(pd.DataFrame(rows).to_string(index=False))

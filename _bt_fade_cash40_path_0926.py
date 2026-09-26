# -*- coding: utf-8 -*-
"""_bt_fade_cash40_path_0926.py — 本人「②廃止で現金余力40万ならどうなりそう？」(2026-09-26)
①だけ(前終+1%当日中指値・大型除外)を現金余力40万から、余力でサイズを変えるラダーで回す。
ラダー: <40万=①50 / 40-49=①60 / 50-54=①70 / 55万以上=①100。下げは翌営業日から、上げは週末(金曜引け)判定。
比較: ①60固定・①100固定。開始日を全営業日でずらし 60/120/245営業日後の分布と15万割れ/ゼロ/55万到達を集計。
実行: python -X utf8 _bt_fade_cash40_path_0926.py > _log_fade_cash40_path_0926.txt"""
import os
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk == 1].reset_index(drop=True)
L = tickc(d.px.values * 1.01); o, h, c = d.o1.values, d.h1.values, d.c1.values
d["ps"] = np.where(o >= L, o - c, np.where(h >= L, L - c, 0))
days = pd.Series(sorted(set(P10.ent.astype(str)))); di = pd.to_datetime(days)
fri = (di.dt.dayofweek.values == 4) | np.r_[di.dt.isocalendar().week.values[1:] != di.dt.isocalendar().week.values[:-1], True]  # 週の最終営業日
trade = {e: (px, ps) for e, px, ps in zip(d.ent, d.px, d.ps)}
TR = [trade.get(e) for e in days]
def ladder(c):
    return 100 if c >= 55 else 70 if c >= 50 else 60 if c >= 40 else 50
def sim(i0, H, mode, C0=40.0):
    cash = C0; wk = C0; path = []; hit15 = zero = False; reach = None
    for j in range(i0, min(i0 + H, len(days))):
        size = ladder(min(cash, wk)) if mode == "ladder" else mode
        t = TR[j]
        if t is not None:
            px, ps = t; sh = (size * 1e4 / px) // 100 * 100; cash += ps * sh / 1e4
        if cash <= 15: hit15 = True
        if cash <= 0: zero = True
        if reach is None and cash >= 55: reach = j - i0 + 1
        if fri[j]: wk = cash
        path.append(cash)
    return cash, hit15, zero, reach, min(path)
pd.set_option("display.width", 250)
for Y0 in (2017, 2022):
    starts = [i for i in range(len(days)) if di[i].year >= Y0]
    rows = []
    for mode in ("ladder", 60, 100, 50):
        for H in (60, 120, 245):
            ss = [i for i in starts if i + H <= len(days)]
            R = [sim(i, H, mode) for i in ss]
            end = np.array([r[0] for r in R]); rc = [r[3] for r in R]
            rows.append(dict(方式="ラダー" if mode == "ladder" else f"①{mode}固定", 期間=f"{H}日≈{ {60:'3か月',120:'6か月',245:'1年'}[H]}",
                             終わりの余力_中央値=round(np.median(end)), 悪い1割=round(np.percentile(end, 10)), 良い1割=round(np.percentile(end, 90)),
                             最悪=round(end.min()), 増えてる率=round((end > 40).mean() * 100), 十五万割れ率=round(np.mean([r[1] for r in R]) * 100, 1),
                             ゼロ率=round(np.mean([r[2] for r in R]) * 100, 1), 五五万到達率=round(np.mean([x is not None for x in rc]) * 100),
                             到達日数中央値=np.median([x for x in rc if x is not None]) if any(x is not None for x in rc) else None))
    print(f"\n■ 開始日={Y0}年以降の全営業日・余力40万スタート・①だけ(+1%指値)")
    print(pd.DataFrame(rows).to_string(index=False))

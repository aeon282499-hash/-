# -*- coding: utf-8 -*-
"""_bt_fade_limit_k_cash35_0926.py — 本人「結局指値どうするのよ・細かく」(2026-09-26・現金余力35万で①100万だけ撃つ局面)
フェード①だけ100万(大型除外・10年2017-01〜2026-09-01)で、前日終値+k%の当日中指値を k=寄成/0/0.5/1/1.5/2/2.5/3/4/5 で比較。
①寄りで約定(寄値≥指値)②場中で約定(寄値<指値≤高値)③見送り の3区分に分け、区分ごとの件数と1玉平均を出す。
加えて「35万から始めて60営業日以内に-20万(=15万割れ)を踏む確率」と最悪日/最悪5日。約定甘さ=指値を0.3%抜けないと不約定。
実行: python -X utf8 _bt_fade_limit_k_cash35_0926.py > _log_fade_limit_k_cash35_0926.txt"""
import os
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk == 1].copy()
d["sh"] = (1_000_000 / d.px // 100 * 100).astype(int); d = d[d.sh > 0].reset_index(drop=True)
o, h, c, px, sh = d.o1.values, d.h1.values, d.c1.values, d.px.values, d.sh.values
ent = d.ent.values; y = d.y.values
allday = pd.Series(sorted(set(P10.ent.astype(str))))  # 候補が出た営業日(玉のない日も0円で数える)

def run(k, slip=0.0):
    if k is None:
        yen = (o - c) * sh; kind = np.full(len(d), "寄り")
    else:
        L = tickc(px * (1 + k / 100))
        fo = o >= L; fh = ~fo & (h >= L * (1 + slip / 100))
        yen = np.where(fo, o - c, np.where(fh, L - c, 0)) * sh
        kind = np.where(fo, "寄り", np.where(fh, "場中", "見送り"))
    return yen, kind

def path_risk(yen):
    day = pd.Series(yen, index=ent).groupby(level=0).sum().reindex(allday, fill_value=0).values
    cum = np.concatenate([[0], np.cumsum(day)])
    hit = []
    for i in range(len(day) - 60):
        w = cum[i + 1:i + 61] - cum[i]; hit.append(w.min() <= -200_000)
    s5 = pd.Series(day).rolling(5).sum().min()
    eq = np.cumsum(day); dd = (eq - np.maximum.accumulate(eq)).min()
    return np.mean(hit) * 100, day.min() / 1e4, s5 / 1e4, dd / 1e4

rows = []; brk = []
for lab, k, sl in [("寄成", None, 0)] + [(f"+{k}%", k, 0) for k in (0, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5)] + [(f"+{k}% 0.3%抜け", k, 0.3) for k in (0, 1, 2, 3)]:
    yen, kind = run(k, sl); f = kind != "見送り"; t = yen[f]
    hit, wd, w5, dd = path_risk(yen)
    yy = pd.Series(yen, index=y).groupby(level=0).sum() / 1e4
    rows.append(dict(執行=lab, 約定率=round(f.mean() * 100), 玉数=int(f.sum()), 勝率=round((t > 0).mean() * 100, 1), PF=round(pf(pd.Series(t)), 2),
                     一玉平均万=round(t.mean() / 1e4, 2), 十年万=round(yen.sum() / 1e4), 年平均万=round(yen.sum() / 1e4 / 9.67, 1),
                     前半17_21=round(yy[yy.index <= 2021].sum()), 後半22_26=round(yy[yy.index >= 2022].sum()), y2026=round(yy.get(2026, 0)),
                     最悪年=round(yy.min()), DD万=round(dd), 最悪日万=round(wd, 1), 最悪5日万=round(w5, 1),
                     負け10万超日=int((yen < -1e5).sum()), 六十日で15万割れ率=round(hit, 1)))
    if sl == 0 and k is not None:
        for kd in ("寄り", "場中", "見送り"):
            m = kind == kd; y0 = (o - c)[m] * sh[m]
            brk.append(dict(執行=lab, 区分=kd, 件数=int(m.sum()), 実際の一玉万=round(yen[m].mean() / 1e4, 2) if m.any() else 0,
                            寄成なら一玉万=round(y0.mean() / 1e4, 2) if m.any() else 0, 合計差万=round((yen[m] - y0).sum() / 1e4)))
pd.set_option("display.width", 300); pd.set_option("display.max_columns", 30)
print("■ ①100万だけ・大型除外・2017-01〜2026-09-01 (①の候補", len(d), "件)")
print(pd.DataFrame(rows).to_string(index=False))
print("\n■ 区分別: 寄りで約定=寄成と同じ玉 / 場中約定=指値の分だけ高く売れた玉 / 見送り=寄成なら取れた玉")
print(pd.DataFrame(brk).to_string(index=False))

# 寄りの位置(前終比)で見た場中の動き: どこまで担がれるか
g = (o / px - 1) * 100; up = (h / o - 1) * 100; oc = (o - c) / o * 100
bins = [-99, -3, -1, 0, 1, 2, 3, 5, 99]
B = pd.DataFrame(dict(寄り前終比=pd.cut(g, bins), 寄りから高値=up, 寄り引け売り損益=oc))
print("\n■ 寄りの位置(前日終値比%)ごとの件数・寄り→高値の中央値・寄成の1玉損益%")
print(B.groupby("寄り前終比", observed=True).agg(件数=("寄りから高値", "size"), 高値まで中央値=("寄りから高値", "median"),
      高値まで75点=("寄りから高値", lambda s: s.quantile(.75)), 寄成損益平均=("寄り引け売り損益", "mean"), 寄成勝率=("寄り引け売り損益", lambda s: (s > 0).mean() * 100)).round(2).to_string())

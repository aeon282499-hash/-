# -*- coding: utf-8 -*-
"""_bt_masutan_exante_0918.py — 「解除3日前買い」を事前ルール化。
東証の解除基準(株価): 5営業日連続で終値が25日移動平均の±15%以内 → 5日目の引け後に解除発表・翌日実施。
事前シグナル = 規制中に「±15%以内がk日連続」になった引け(k=2なら3日前・k=3なら2日前・k=4なら1日前)。
買い: シグナル翌日の寄り / 売り: (a)3営業日後の終値 (b)解除発表日(=予測5日目)の終値 (c)実施日の終値。
的中率 = 予測した5日目が実際の最終フラグ日と一致するか。"""
import pickle, bisect, numpy as np, pandas as pd
E = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for c in E.code.unique():
    dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(c + ".T")) is not None and len(d)]
    if not dfs: continue
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]
    df = df[df["Close"] > 0]
    cl = df["Close"].astype(float); op = df["Open"].astype(float)
    ma = cl.rolling(25).mean()
    SER[c] = (cl.to_numpy(), op.to_numpy(), (cl / ma - 1).to_numpy() * 100, [d.strftime("%Y-%m-%d") for d in cl.index])
rows = []; hit = []
for r in E.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, op, dev, ds = s
    i0 = bisect.bisect_left(ds, r.start); i1 = bisect.bisect_left(ds, r.end)
    if i1 >= len(ds) or ds[i1] != r.end or i0 >= len(ds) or ds[i0] != r.start: continue
    cnt = 0; fired = {2: False, 3: False, 4: False}
    for i in range(i0 + 1, i1 + 1):            # 実施日(公表翌日)以降
        cnt = cnt + 1 if abs(dev[i]) < 15 else 0
        if cnt == 5:
            hit.append({"yr": r.end[:4], "pred": ds[i], "act": r.end, "diff": i1 - i})
        for k in (2, 3, 4):
            if cnt == k and not fired[k] and i + 1 < len(cl):
                fired[k] = True
                pred5 = i + (5 - k)             # 予測の解除発表日
                buy = op[i + 1]
                if buy <= 0 or pred5 + 1 >= len(cl): continue
                rows.append({"yr": r.end[:4], "k": k, "code": r.code, "sig": ds[i],
                             "a3": (cl[min(i + 3, len(cl) - 1)] / buy - 1) * 100,
                             "b_pred0": (cl[pred5] / buy - 1) * 100,
                             "c_pred1": (cl[pred5 + 1] / buy - 1) * 100,
                             "ok": pred5 == i1, "to_act": i1 - i})
H = pd.DataFrame(hit); R = pd.DataFrame(rows)
print(f"解除日の予測(5日連続±15%以内の5日目=最終フラグ日): 初回到達の的中 {(H.groupby(['pred','act']).size().reset_index().pipe(lambda d: (d.pred==d.act).mean())*100):.0f}%  (到達イベント{len(H)}件・完全一致{(H['diff']==0).sum()}・±1日{(H['diff'].abs()<=1).sum()})")
first = H.sort_values("pred").groupby(["act"]).first()
print(f"エピソード単位: 初めて5日到達した日が実際の最終フラグ日と一致 {(first['diff']==0).mean()*100:.0f}% / ±1日 {(first['diff'].abs()<=1).mean()*100:.0f}% / 実際より早すぎ(規制継続) {(first['diff']>1).mean()*100:.0f}%  n={len(first)}")
def show(R, t):
    print(f"\n== {t} ==  (シグナル翌日寄り買い → 平均% / 中央値 / 上がった割合)")
    for k, lab in ((2, "2日連続=解除3日前"), (3, "3日連続=解除2日前"), (4, "4日連続=解除1日前")):
        g = R[R.k == k]
        if not len(g): continue
        ok = g.ok.mean() * 100
        print(f"  {lab} n={len(g):4d} 的中{ok:3.0f}%  (a)3日後終値 {g.a3.mean():+5.2f}/{g.a3.median():+5.2f}/{(g.a3>0).mean()*100:3.0f}%   (b)予測発表日終値 {g.b_pred0.mean():+5.2f}/{g.b_pred0.median():+5.2f}/{(g.b_pred0>0).mean()*100:3.0f}%   (c)予測実施日終値 {g.c_pred1.mean():+5.2f}/{g.c_pred1.median():+5.2f}/{(g.c_pred1>0).mean()*100:3.0f}%")
        for lab2, gg in (("  的中した玉", g[g.ok]), ("  外れた玉(まだ規制中)", g[~g.ok])):
            if len(gg): print(f"    {lab2:<14} n={len(gg):4d} (a) {gg.a3.mean():+5.2f}/{(gg.a3>0).mean()*100:3.0f}%  (b) {gg.b_pred0.mean():+5.2f}/{(gg.b_pred0>0).mean()*100:3.0f}%  (c) {gg.c_pred1.mean():+5.2f}/{(gg.c_pred1>0).mean()*100:3.0f}%")
show(R, "全期間 2016-2026"); show(R[R.yr.isin(["2025", "2026"])], "2025-26"); show(R[R.yr == "2026"], "2026")
print("\n年別 k=2(3日前)・(a)3日後終値: 平均% / 上がった%")
g = R[R.k == 2].groupby("yr").agg(n=("a3", "size"), a=("a3", "mean"), aw=("a3", lambda x: (x > 0).mean() * 100), ok=("ok", lambda x: x.mean() * 100)).round(1); print(g.to_string())
R.to_csv("_bt_masutan_exante_0918.csv", index=False)
# シリコンスタジオの実際
c = "3907"
if c in SER:
    cl, op, dev, ds = SER[c]; i = bisect.bisect_left(ds, "2026-08-17")
    print("\n3907 25MA乖離(%):"); print(" ".join(f"{ds[j][5:]}:{dev[j]:+.0f}" for j in range(i, min(i + 30, len(ds)))))

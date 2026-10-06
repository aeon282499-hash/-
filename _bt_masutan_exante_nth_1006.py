# -*- coding: utf-8 -*-
"""_bt_masutan_exante_nth_1006.py — 増担保の事前ルール(±15%以内が2日連続→翌寄り買い→3日後終値)を
同じ規制エピソード内で「何回目の点灯か」で分ける（本人「2回目のミナトはどうなる」2026-10-06）。
本番は1回目だけ撃つ。2回目=1回目の後に±15%を外れて(再加速か急落)また2日収まった時。"""
import pickle, bisect, sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding="utf-8")
E = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for c in E.code.unique():
    dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(c + ".T")) is not None and len(d)]
    if not dfs: continue
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]; df = df[df["Close"] > 0]
    cl = df["Close"].astype(float); op = df["Open"].astype(float); vo = df["Volume"].astype(float) if "Volume" in df else cl * 0
    ma = cl.rolling(25).mean(); adv = (cl * vo).rolling(20).mean()
    SER[c] = (cl.to_numpy(), op.to_numpy(), (cl / ma - 1).to_numpy() * 100, [d.strftime("%Y-%m-%d") for d in cl.index], adv.to_numpy())
rows = []
for r in E.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, op, dev, ds, adv = s
    i0 = bisect.bisect_left(ds, r.start); i1 = bisect.bisect_left(ds, r.end)
    if i1 >= len(ds) or ds[i1] != r.end or i0 >= len(ds) or ds[i0] != r.start: continue
    cnt = 0; nth = 0; last_dir = None; prev_fire_i = None
    for i in range(i0 + 1, i1 + 1):
        if abs(dev[i]) < 15: cnt += 1
        else:
            if cnt > 0: last_dir = "上に外れた" if dev[i] >= 15 else "下に外れた"
            cnt = 0
        if cnt == 2 and i + 1 < len(cl):
            nth += 1; buy = op[i + 1]
            if buy <= 0: continue
            j = min(i + 3, len(cl) - 1)
            rows.append({"yr": r.end[:4], "code": r.code, "sig": ds[i], "nth": nth, "how": last_dir if nth > 1 else "-",
                         "gap_days": (i - prev_fire_i) if prev_fire_i is not None else np.nan,
                         "since1": (cl[i] / cl[prev_fire_i] - 1) * 100 if prev_fire_i is not None else np.nan,
                         "a3": (cl[j] / buy - 1) * 100, "adv_oku": adv[i] / 1e8})
            prev_fire_i = i
R = pd.DataFrame(rows); R["net"] = R.a3 - 0.3
def show(X, t):
    if not len(X): print(f"  {t:<28} n=0"); return
    gp = X.net[X.net > 0].sum(); gl = -X.net[X.net <= 0].sum()
    print(f"  {t:<28} n={len(X):>4} 平均{X.net.mean():+.2f}% 中央{X.net.median():+.2f}% 勝率{(X.net>0).mean()*100:.0f}% PF{gp/gl if gl else np.inf:.2f} 最悪{X.net.min():+.1f}%")
print("3日後終値・コスト0.3%後・全部(2016-08〜2026-07)")
for n in (1, 2, 3): show(R[R.nth == n], f"{n}回目")
show(R[R.nth >= 4], "4回目以降")
print("\n2回目を「1回目の後どう外れたか」で分ける")
for h in ("上に外れた", "下に外れた"): show(R[(R.nth == 2) & (R.how == h)], h)
print("\n2回目を「1回目の点灯から株価が上がったか」で分ける")
show(R[(R.nth == 2) & (R.since1 > 0)], "1回目より上で点灯"); show(R[(R.nth == 2) & (R.since1 <= 0)], "1回目より下で点灯")
show(R[(R.nth == 2) & (R.since1 > 10)], "1回目より+10%超")
print("\n代金≥3億(本番の対象)")
for n in (1, 2): show(R[(R.nth == n) & (R.adv_oku >= 3)], f"{n}回目")
show(R[(R.nth == 2) & (R.adv_oku >= 3) & (R.how == "上に外れた")], "2回目・上に外れた")
print("\n年別 2回目(件数/平均)"); print(R[R.nth == 2].groupby("yr").net.agg(["size", "mean"]).round(2).T.to_string())
R.to_csv("_bt_masutan_exante_nth_1006.csv", index=False)

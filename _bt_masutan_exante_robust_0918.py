# -*- coding: utf-8 -*-
"""事前ルール(規制中に25MA±15%以内が2日連続→翌日寄り買い→3日後終値)の頑健性: 上位3玉/3日除去・コスト・低位株・代金・3枠シム年別。"""
import pickle, bisect, numpy as np, pandas as pd
E = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for c in E.code.unique():
    dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(c + ".T")) is not None and len(d)]
    if not dfs: continue
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]; df = df[df["Close"] > 0]
    cl = df["Close"].astype(float); op = df["Open"].astype(float); vo = df["Volume"].astype(float)
    SER[c] = (cl.to_numpy(), op.to_numpy(), (cl / cl.rolling(25).mean() - 1).to_numpy() * 100, (cl * vo).to_numpy(), [d.strftime("%Y-%m-%d") for d in cl.index])
rows = []
for r in E.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, op, dev, tov, ds = s
    i0 = bisect.bisect_left(ds, r.start); i1 = bisect.bisect_left(ds, r.end)
    if i1 >= len(ds) or ds[i1] != r.end or ds[i0] != r.start: continue
    cnt = 0; fired = False
    for i in range(i0 + 1, i1 + 1):
        cnt = cnt + 1 if abs(dev[i]) < 15 else 0
        if cnt == 2 and i + 3 < len(cl) and op[i + 1] > 0:
            buy = op[i + 1]
            rows.append({"code": r.code, "sig": ds[i], "buy_date": ds[i + 1], "yr": ds[i][:4], "px": buy, "tov5": np.nanmean(tov[i - 4:i + 1]) / 1e8,
                         "r3": (cl[i + 3] / buy - 1) * 100, "r1": (cl[i + 1] / buy - 1) * 100, "r2": (cl[i + 2] / buy - 1) * 100,
                         "r_open3": (op[i + 4] / buy - 1) * 100 if i + 4 < len(op) and op[i + 4] > 0 else np.nan,
                         "gap": (buy / cl[i] - 1) * 100, "first": not fired, "dev": dev[i]})
            fired = True
R = pd.DataFrame(rows)
def stat(g, k="r3", cost=0.0):
    v = g[k] - cost; return f"n={len(g):4d} 平均{v.mean():+5.2f}% 中央{v.median():+5.2f}% 勝率{(v > 0).mean() * 100:3.0f}% PF{v[v > 0].sum() / max(1e-9, -v[v <= 0].sum()):.2f}"
F = R[R.first]
print("初回シグナルのみ(前回と同じ)  ", stat(F))
print("全シグナル(再発火含む)        ", stat(R))
print("コスト0.3%控除(往復)          ", stat(F, cost=0.3))
top = F.nlargest(3, "r3").index; print("上位3玉除去                   ", stat(F.drop(top)))
d3 = F.groupby("buy_date").r3.sum().nlargest(3).index; print("上位3日除去                   ", stat(F[~F.buy_date.isin(d3)]))
print("株価<300円除外               ", stat(F[F.px >= 300]))
print("株価<1000円除外              ", stat(F[F.px >= 1000]))
print("5日平均代金<3億除外          ", stat(F[F.tov5 >= 3]))
print("寄りギャップ≥+3%(追いかけ)除外", stat(F[F.gap < 3]))
print("寄りギャップ<0(下寄り)だけ    ", stat(F[F.gap < 0]))
print("前半2016-20                  ", stat(F[F.yr <= "2020"])); print("後半2021-26                  ", stat(F[F.yr >= "2021"]))
print("保有1日                      ", stat(F, "r1")); print("保有2日                      ", stat(F, "r2")); print("3日後の翌寄り売り            ", stat(F.dropna(subset=["r_open3"]), "r_open3"))
print("乖離の位置別(シグナル日の25MA乖離):")
for lo, hi in ((-15, -5), (-5, 5), (5, 15)):
    g = F[(F.dev >= lo) & (F.dev < hi)]; print(f"  {lo:+d}〜{hi:+d}%  ", stat(g))
# 3枠シム(100万/枠・寄り買い→3日後終値・同日複数は代金順・コスト0.3%)
F2 = F[F.tov5 >= 3].sort_values(["buy_date", "tov5"], ascending=[True, False])
SLOT, SIZE, COST = 3, 1_000_000, 0.3
days = sorted(F2.buy_date.unique()); busy = []; pnl = {}; eq = []; cum = 0; peak = 0; dd = 0
for d in days:
    busy = [b for b in busy if b > d]
    for t in F2[F2.buy_date == d].itertuples():
        if len(busy) >= SLOT: break
        sh = int(SIZE / t.px / 100) * 100
        if sh == 0: continue
        y = sh * t.px * (t.r3 - COST) / 100; pnl[t.yr] = pnl.get(t.yr, 0) + y; cum += y; peak = max(peak, cum); dd = min(dd, cum - peak)
        # 3営業日後まで枠を占有(日付文字列比較のため実装は簡便に「buy_date+5暦日」)
        busy.append((pd.Timestamp(d) + pd.Timedelta(days=5)).strftime("%Y-%m-%d"))
print("\n3枠×100万シム(代金3億以上・コスト0.3%・同日は代金順): 年別損益(万円)")
print({k: round(v / 1e4) for k, v in sorted(pnl.items())}, f"合計{cum / 1e4:+.0f}万 最大DD{dd / 1e4:.0f}万 年平均{cum / 1e4 / 10:.0f}万")
R.to_csv("_bt_masutan_exante_robust_0918.csv", index=False)

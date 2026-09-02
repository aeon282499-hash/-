# -*- coding: utf-8 -*-
"""_bt_fade_20y.py — 売りフェードの現行ルールを立花20年日足（2001〜）に当てる（2026-09-03）。

ルール（_bt_fade_pool_v5_100.py / _bt_fade_jsf100.py と同一）:
  シグナル日D: 前日比+7%以上 / 20日代金中央値≥3億 / ATR14%≥5 / 25MA乖離≥+12% / 出来高比(vr)<6 / 当日レンジ>5% /
              平均出来高≥10万株 / 株価×100≤100万 / 貸借銘柄（ISSは2017以降のみ→**2016以前は貸借判定なし**）
  翌日D+1: 寄成で空売り → 引成。並び順=乖離×ATRの百分位平均 → 1番×100万・2番×50万。
  張り付き（o1_locked）は現行と同じく除外しない（ハイカラ在庫前提の集計）。
注意: 立花ユニバースは現存銘柄のみ（生存者バイアス＝過去の急騰株が後に上場廃止していれば抜ける＝売り側には**弱気側**に歪む可能性）。
実行: python -X utf8 _bt_fade_20y.py > _log_fade_20y.txt
"""
from __future__ import annotations

import pickle
import sys

import numpy as np
import pandas as pd

CAP, TOV_MIN, GAIN_FLOOR = 1_000_000, 3e8, 7.0
SIZE1, SIZE2 = 1_000_000, 500_000
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
nm = dict(pickle.load(open("jquants_cache.pkl", "rb"))["name_map"])
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb")); YRS = sorted(ISS)


def iss_ok(tk, y):
    if y < min(YRS):
        return True   # 2016以前は貸借判定不能＝通す（結果は「貸借フィルタ無し」として読む）
    return ISS[min(YRS, key=lambda a: (abs(a - y), a))].get(str(tk)[:4], "?") == "2"


rows = []
for tk, df in ALL.items():
    if nm.get(tk) is None or df is None or len(df) < 40:
        continue
    df = df.dropna(subset=["Close"])
    o = df["Open"].to_numpy(float); c = df["Close"].to_numpy(float)
    h = df["High"].to_numpy(float); l = df["Low"].to_numpy(float); v = df["Volume"].to_numpy(float)
    cs, vs, hs, ls = map(pd.Series, (c, v, h, l))
    tov = (cs * vs).rolling(20).median().to_numpy()
    vma = vs.shift(1).rolling(20).mean().to_numpy()
    pc = cs.shift(1)
    tr = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1)
    atr = (tr.rolling(14).mean() / cs * 100).to_numpy()
    ma25 = cs.rolling(25).mean().to_numpy()
    idx = df.index
    gain_all = np.full(len(c), np.nan); gain_all[1:] = (c[1:] / c[:-1] - 1) * 100
    cand = np.where((gain_all >= GAIN_FLOOR) & np.isfinite(tov) & (tov >= TOV_MIN))[0]
    for t in cand:
        if t < 26 or t + 1 >= len(c) or not (c[t - 1] > 0 and c[t] > 0 and o[t + 1] > 0 and c[t + 1] > 0):
            continue
        if c[t] * 100 > CAP or not (np.isfinite(vma[t]) and vma[t] >= 100_000):
            continue
        y = idx[t + 1].year
        if not iss_ok(tk, y):
            continue
        rows.append({"sig": idx[t].strftime("%Y-%m-%d"), "ent": idx[t + 1].strftime("%Y-%m-%d"), "y": y, "ticker": tk,
                     "gain": gain_all[t], "px": c[t], "o1": o[t + 1], "c1": c[t + 1],
                     "atr": atr[t], "dev": (c[t] / ma25[t] - 1) * 100 if ma25[t] > 0 else 0.0,
                     "vr": v[t] / vma[t], "tov": tov[t], "vol_avg": vma[t], "rng": (h[t] - l[t]) / c[t] * 100})
D = pd.DataFrame(rows).sort_values(["sig", "ticker"])
D["pnl"] = (D.o1 - D.c1) / D.o1 * 100
D["ym"] = D.ent.str[:7]
D.to_csv("_bt_fade_20y_pool.csv", index=False)
G = D[(D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) & (D.rng > 5.0)].copy()
print(f"[pool] gain≥7 {len(D):,}件 → 現行条件 {len(G):,}件 / 期間 {G.sig.min()}〜{G.sig.max()}", flush=True)


def rank(d):
    d = d.copy(); r = None
    for col in ("dev", "atr"):
        x = d.groupby("sig")[col].rank(ascending=False, pct=True); r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable"); d["rk"] = d.groupby("sig").cumcount() + 1
    return d


def settle(d, size):
    d = d.copy(); d["sh"] = (size / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy(); d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def pf(x):
    n = -x[x <= 0].sum(); return x[x > 0].sum() / n if n else float("inf")


R = rank(G)
b1 = settle(R[R.rk == 1], SIZE1); b2 = settle(R[R.rk == 2], SIZE2)
lines = []
def p(s=""):
    print(s); lines.append(s)
p("=" * 120)
p("売りフェード 現行ルール × 26年（1番×100万 / 2番×50万）  ※2016以前は貸借フィルタ無し・ユニバースは現存銘柄")
p("=" * 120)
for label, b in (("1番×100万", b1), ("2番×50万", b2), ("合算", pd.concat([b1, b2]))):
    p(f"\n[{label}]")
    g = b.groupby("y")
    tbl = pd.DataFrame({"n": g.size(), "win%": g.pnl.apply(lambda s: (s > 0).mean() * 100).round(1), "avg%": g.pnl.mean().round(2), "yen": g.yen.sum().round(0)})
    p(tbl.T.to_string())
    for lo, hi, nm_ in ((2001, 2008, "2001-08"), (2009, 2016, "2009-16"), (2017, 2021, "2017-21"), (2022, 2026, "2022-26")):
        s = b[(b.y >= lo) & (b.y <= hi)]
        if len(s):
            yy = s.groupby("y").yen.sum(); ym = s.groupby("ym").yen.sum()
            p(f"  {nm_}: n={len(s):>5} 勝率={(s.pnl>0).mean()*100:.1f}% PF={pf(s.pnl):.2f} 合計={s.yen.sum():>+13,.0f} 勝ち年={int((yy>0).sum())}/{len(yy)} 最悪年={yy.min():>+12,.0f} 最悪月={ym.min():>+10,.0f} 平均%={s.pnl.mean():+.3f}")
    yy = b.groupby("y").yen.sum()
    p(f"  26年: n={len(b)} PF={pf(b.pnl):.2f} 合計={b.yen.sum():+,.0f} 勝ち年={int((yy>0).sum())}/{yy.index.nunique()}")
p("\n[候補レベル] 現行条件の全候補の平均%（選定前）")
G["era"] = pd.cut(G.y, [2000, 2008, 2016, 2021, 2026], labels=["01-08", "09-16", "17-21", "22-26"])
g = G.groupby("era", observed=True).pnl
p(pd.DataFrame({"n": g.size(), "avg%": g.mean().round(3), "win%": g.apply(lambda s: (s > 0).mean() * 100).round(1), "PF": g.apply(pf).round(2)}).to_string())
open("_log_fade_20y.txt", "w", encoding="utf-8").write("\n".join(lines))

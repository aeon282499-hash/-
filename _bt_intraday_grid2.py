# -*- coding: utf-8 -*-
"""_bt_intraday_grid2.py — 日中の「入る時刻×出る時刻」全窓グリッド（2026-08-24）。

本人「必ずあるはずだ勝ち筋が。隅々までチェックして」への回答。
7/26の網羅(_bt_intraday_grand.py)は出口=引け固定だった。ここでは日中の途中で入って
途中で出る全窓を掃く＝「隅々」の最後の空白。

■ 形: 「時刻INに条件Cを満たしていたら方向Dで入り、時刻OUTで返す」
■ 価格: 時刻Xの約定=ラベルXの足の始値（15分足ラベルは開始時刻＝7/26の教訓）。
        CLOSE=最終足の終値（大引け近似）。コスト0.10%/往復。
■ 母集団: 前日代金10億+・前日終値1,000〜5,000円・ショートは貸借○のみ。
■ 条件C: 前日+4%以上 / 前日-4%以下 / 当日GU+3%以上 / 当日GD-3%以下 /
          寄り→INで+3%以上 / 寄り→INで-3%以下 / 無条件
■ 合格バー: ①前後半(日付で2分割)ともPF>1.15 ②n>=150 ③上位3日除去でプラス
            ④隣接する時刻ファミリー(同条件・同方向で合格セル3個以上)＝孤立セルは多重比較の産物とみなす
※データは2026-04-24〜07-24の61営業日=1相場ぶんしかない。ここで受かっても
  「forward検証行き」であって実弾昇格ではない（60日=1相場の家訓）。

実行: python -X utf8 _bt_intraday_grid2.py
"""
from __future__ import annotations

import pickle
import time

import numpy as np
import pandas as pd

TURN_MIN, PX_LO, PX_HI, COST = 1e9, 1000, 5000, 0.10
SLOTS = ["09:00", "09:15", "09:30", "10:00", "10:30", "11:00",
         "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "CLOSE"]
IN_TIMES = SLOTS[:-1]

t0 = time.time()
CACHE: dict = pickle.load(open("_intraday_cache_wide.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
iss = ISS[sorted(ISS)[-1]]

# ── per (ticker,day) の時刻→価格ベクトルを構築 ─────────────────────────
rows = []
for tk, df in CACHE.items():
    d = df
    dates = d["dt"].dt.strftime("%Y-%m-%d").to_numpy()
    hms = d["dt"].dt.strftime("%H:%M").to_numpy()
    o = d["o"].to_numpy(float)
    c = d["c"].to_numpy(float)
    v = d["v"].to_numpy(float)
    uniq, idx = np.unique(dates, return_index=True)
    idx = list(idx) + [len(dates)]
    dcl, dturn = {}, {}
    for k in range(len(uniq)):
        s, e = idx[k], idx[k + 1]
        dcl[uniq[k]] = c[e - 1]
        dturn[uniq[k]] = float((c[s:e] * v[s:e]).sum())
    shortable = iss.get(tk.replace(".T", "").zfill(4)[:4] + "0", iss.get(tk.replace(".T", ""), "?")) == "2"
    for k in range(1, len(uniq)):
        s, e = idx[k], idx[k + 1]
        if e - s < 10:
            continue
        pd_, dt_ = uniq[k - 1], uniq[k]
        pc, pt = dcl[pd_], dturn[pd_]
        ppc = dcl[uniq[k - 2]] if k >= 2 else np.nan
        if not (pt >= TURN_MIN and PX_LO <= pc <= PX_HI):
            continue
        px = {}
        for j in range(s, e):
            hm = hms[j]
            if hm in SLOTS and hm not in px:
                px[hm] = o[j]
        if "09:00" not in px:
            continue
        px["CLOSE"] = c[e - 1]
        pm = (pc / ppc - 1) * 100 if np.isfinite(ppc) and ppc > 0 else np.nan
        gap = (px["09:00"] / pc - 1) * 100
        rows.append((tk, dt_, shortable, pm, gap, px))
print(f"[prep] 対象 {len(rows):,} 銘柄日 / {time.time()-t0:.0f}s", flush=True)

DATES = sorted({r[1] for r in rows})
half = DATES[len(DATES) // 2]


def cond_ok(name, pm, gap, run):
    if name == "前日+4%以上":
        return np.isfinite(pm) and pm >= 4
    if name == "前日-4%以下":
        return np.isfinite(pm) and pm <= -4
    if name == "GU+3%以上":
        return gap >= 3
    if name == "GD-3%以下":
        return gap <= -3
    if name == "寄り→INで+3%":
        return run >= 3
    if name == "寄り→INで-3%":
        return run <= -3
    return True                     # 無条件


CONDS = ["前日+4%以上", "前日-4%以下", "GU+3%以上", "GD-3%以下", "寄り→INで+3%", "寄り→INで-3%", "無条件"]

cells: dict = {}
for tk, dt_, shortable, pm, gap, px in rows:
    for i, tin in enumerate(IN_TIMES):
        pin = px.get(tin)
        if pin is None or not (pin > 0):
            continue
        run = (pin / px["09:00"] - 1) * 100
        okc = [cn for cn in CONDS if cond_ok(cn, pm, gap, run)]
        if not okc:
            continue
        for tout in SLOTS[i + 1:]:
            pout = px.get(tout)
            if pout is None or not (pout > 0):
                continue
            ret_l = (pout / pin - 1) * 100 - COST
            for cn in okc:
                cells.setdefault((cn, tin, tout, "L"), []).append((dt_, ret_l))
                if shortable:
                    cells.setdefault((cn, tin, tout, "S"), []).append((dt_, -(pout / pin - 1) * 100 - COST))
print(f"[grid] セル {len(cells):,} / {time.time()-t0:.0f}s", flush=True)


def stat(v):
    a = np.array([x for _, x in v])
    dts = np.array([d for d, _ in v])
    if len(a) < 150:
        return None
    pf_all = a[a > 0].sum() / -a[a <= 0].sum() if (a <= 0).any() else np.inf
    h1, h2 = a[dts < half], a[dts >= half]
    if len(h1) < 30 or len(h2) < 30:
        return None

    def pf(x):
        n = -x[x <= 0].sum()
        return x[x > 0].sum() / n if n else np.inf
    day = {}
    for d, x in v:
        day[d] = day.get(d, 0) + x
    top3 = sorted(day.values())[-3:]
    ex3 = a.sum() - sum(top3)
    return dict(n=len(a), pf=pf_all, mean=a.mean(), pf1=pf(h1), pf2=pf(h2), ex3=ex3)


passing = {}
for key, v in cells.items():
    st = stat(v)
    if st and st["pf1"] > 1.15 and st["pf2"] > 1.15 and st["ex3"] > 0:
        passing[key] = st

# 隣接時刻ファミリー（同条件・同方向で合格3セル以上）だけ残す
fam: dict = {}
for (cn, tin, tout, d_), st in passing.items():
    fam.setdefault((cn, d_), []).append((tin, tout, st))
print(f"\n=== 一次合格 {len(passing)}セル / {len(cells)}セル ===")
print("（60日=1相場×約{:,}セルの多重比較＝孤立セルは信用しない）".format(len(cells)))
for (cn, d_), lst in sorted(fam.items(), key=lambda kv: -len(kv[1])):
    tag = "買い" if d_ == "L" else "売り"
    mark = "★ファミリー" if len(lst) >= 3 else "（孤立）"
    print(f"\n{cn} × {tag} : 合格{len(lst)}セル {mark}")
    for tin, tout, st in sorted(lst)[:8]:
        print(f"   {tin}→{tout}: n={st['n']} PF{st['pf']:.2f} 平均{st['mean']:+.2f}% "
              f"前半{st['pf1']:.2f}/後半{st['pf2']:.2f} 上位3日除去{st['ex3']:+.0f}%")
print(f"\n[done] {time.time()-t0:.0f}s")

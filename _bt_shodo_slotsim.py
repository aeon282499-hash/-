# -*- coding: utf-8 -*-
"""_bt_shodo_slotsim.py — 初動ブレイク(break60)の実運用形・枠シム（2026-08-24）。

本人「初動ブレイクをシステム化して」。8/11の_bt_shodo_break.py(全件%平均・PF1.22)は
「全部撃てた場合」の数字で、実運用形（枠・玉サイズ・選定・約定可否）は未検証だった。

実運用形: 1玉SIZE円(株数=前日終値基準100株丸め・買えない値がさはスキップ)・枠N・
  シグナル翌日寄り成行買い・SL-15%(安値接触・寄りが下抜けたら寄り値=ギャップスルー現実側)・
  20営業日目の終値で時間決済・保有中は枠を占有・同一銘柄保有中の再シグナルはスキップ・
  コスト0.2%/往復。
約定監査(PEAD事件の家訓): 翌朝の寄りがS高張り付き(前日終値+値幅制限に到達)＝買えない玉を除外。
事前登録の採用バー: ①両半期プラス ②選定順ロバスト(ランダム3種でもプラス) ③張り付き除外後も残存
  ④上位3銘柄除去で残存 ⑤最悪月が拘束(SIZE×枠)対比で受容範囲。
実行: python -X utf8 _bt_shodo_slotsim.py
"""
from __future__ import annotations

import os
import pickle
import random

import numpy as np
import pandas as pd

EVENTS = "_shodo_events60.pkl"
COST = 0.2
HOLD, SL = 20, 15.0

_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400),
        (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000),
        (30000, 5000), (50000, 7000), (70000, 10000)]


def _lim_w(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return w
    return 10000.0


def build() -> pd.DataFrame:
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    tks = set(old["all_data"]) | set(new["all_data"])
    rows = []
    for tk in tks:
        dfs = [d for src in (old["all_data"], new["all_data"])
               if (d := src.get(tk)) is not None and len(d)]
        if not dfs:
            continue
        df = pd.concat(dfs).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        if len(df) < 80:
            continue
        o = df["Open"].astype(float).to_numpy()
        h = df["High"].astype(float).to_numpy()
        l = df["Low"].astype(float).to_numpy()
        c = df["Close"].astype(float).to_numpy()
        v = df["Volume"].astype(float).to_numpy()
        cs = pd.Series(c)
        tov = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        hi60 = cs.rolling(60).max().shift(1).to_numpy()
        v20 = pd.Series(v).rolling(20).mean().shift(1).to_numpy()
        yo = c > o
        dates = df.index
        n = len(df)
        brk = (c > hi60) & np.isfinite(hi60)
        fresh = np.zeros(n, bool)
        last = -10**9
        for i in range(n):
            if brk[i]:
                if i - last > 30:
                    fresh[i] = True
                last = i
        volx = np.where((v20 > 0) & np.isfinite(v20), v / v20, np.nan)
        idx = np.where(fresh & yo & (tov >= 1e8) & (volx >= 1.5) & (np.arange(n) + 1 < n))[0]
        for i in idx:
            e = o[i + 1]
            if not (e > 0) or not np.isfinite(e):
                continue
            pinned = e >= c[i] + _lim_w(c[i]) - 1e-6      # 翌朝S高寄り＝買えない
            stop = e * (1 - SL / 100)
            end = min(i + 1 + HOLD, n - 1)
            pnl, xj = None, end
            for k in range(i + 2, end + 1):
                if o[k] <= stop:                          # ギャップスルー＝寄りで投げ
                    pnl, xj = (o[k] / e - 1) * 100, k
                    break
                if l[k] <= stop:
                    pnl, xj = -SL, k
                    break
            if pnl is None:
                pnl = (c[end] / e - 1) * 100
            rows.append({"tk": tk, "ent": dates[i + 1], "exit": dates[xj], "y": dates[i + 1].year,
                         "px": c[i], "e": e, "volx": float(volx[i]), "tov": float(tov[i]),
                         "gap": (e / c[i] - 1) * 100, "pinned": bool(pinned),
                         "pnl": pnl - COST})
    D = pd.DataFrame(rows).sort_values("ent").reset_index(drop=True)
    D.to_pickle(EVENTS)
    print(f"[build] イベント {len(D):,}件 保存")
    return D


D = pd.read_pickle(EVENTS) if os.path.exists(EVENTS) else build()
print(f"[pool] {len(D):,}件 / 張り付き(買えない玉) {int(D.pinned.sum()):,}件 ({D.pinned.mean()*100:.1f}%)")


def slotsim(d: pd.DataFrame, size: int, slots: int, order, honest=True):
    d = d.copy()
    if honest:
        d = d[~d.pinned]
    d = d[d.px * 100 <= size]                             # 買えない値がさはスキップ
    if order == "random":
        pass                                              # 呼び出し側でシャッフル列を与える
    rows = []
    busy: list = []          # exit dates
    held: dict = {}          # ticker -> exit date
    for ent, g in d.groupby("ent"):
        busy = [x for x in busy if x >= ent]
        for tk in [t for t, x in held.items() if x < ent]:
            del held[tk]
        g = g.sort_values(order, ascending=(order == "gap")) if order != "random" else g
        for _, r in g.iterrows():
            if len(busy) >= slots:
                break
            if r.tk in held:
                continue
            sh = int(size / r.px / 100) * 100
            if sh <= 0:
                continue
            busy.append(r.exit)
            held[r.tk] = r.exit
            rows.append({"y": r.y, "ym": str(r.ent)[:7], "tk": r.tk,
                         "pnl": r.pnl, "yen": r.pnl / 100 * sh * r.e})
    return pd.DataFrame(rows)


def stats(R: pd.DataFrame, cap: int) -> str:
    if not len(R):
        return "0件"
    yy = R.groupby("y").yen.sum()
    mo = R.groupby("ym").yen.sum()
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    top3tk = R.groupby("tk").yen.sum().nlargest(3).sum()
    h1 = yy[yy.index <= 2021].sum(); h2 = yy[yy.index >= 2022].sum()
    return (f"n={len(R):>4} 勝率{(R.pnl>0).mean()*100:>4.1f}% PF{gp/gl if gl else np.inf:>5.2f} "
            f"10年{R.yen.sum():>+11,.0f} 前半{h1:>+10,.0f}/後半{h2:>+10,.0f} "
            f"勝年{int((yy>0).sum())}/{len(yy)} 最悪年{yy.min():>+9,.0f} 最悪月{mo.min():>+9,.0f} "
            f"上位3銘柄除去{R.yen.sum()-top3tk:>+10,.0f}")


print("\n=== 本命構成: 3枠×30万（拘束90万）・選定順の頑健性 ===")
for order, lab in (("volx", "出来高倍率降順"), ("tov", "代金降順"), ("gap", "GU小さい順"), ("tk", "ticker昇順")):
    R = slotsim(D, 300_000, 3, order)
    print(f"  {lab:<10} {stats(R, 900_000)}")
for seed in (1, 2, 3):
    d2 = D.sample(frac=1, random_state=seed).sort_values("ent", kind="stable")
    R = slotsim(d2, 300_000, 3, "random")
    print(f"  ランダム{seed}   {stats(R, 900_000)}")

print("\n=== 張り付き監査（volx順・3枠×30万）＝買えない玉を含めるとどう見えるか ===")
print(f"  除外(正直)  {stats(slotsim(D, 300_000, 3, 'volx', honest=True), 900_000)}")
print(f"  込み(ナイーブ){stats(slotsim(D, 300_000, 3, 'volx', honest=False), 900_000)}")

print("\n=== 枠数・サイズのスイープ（volx順・正直） ===")
for size in (300_000, 500_000):
    for slots in (2, 3, 5):
        R = slotsim(D, size, slots, "volx")
        print(f"  {size//10000}万×{slots}枠(拘束{size*slots//10000}万) {stats(R, size*slots)}")

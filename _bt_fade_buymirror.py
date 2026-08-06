# -*- coding: utf-8 -*-
"""_bt_fade_buymirror.py — 「買いデイトレフェードはなぜ作れない？」への決定版（2026-08-06）。

本人の問い。過去の答え（逆張り買いPF0.94・gapfade LONG全滅・順張り買いPF0.60自殺）は
どれも別の選定ロジックだったので、**今の売りフェードの機械をそのまま鏡像にして**回す:
  前日-7%以上の急落 × 張り付き除外(レンジ>5%) × 出来高6倍未満 × ATR5%以上 × 25MA乖離-12%以下
  → 「一番売られ過ぎ×一番動く」順(乖離昇順+ATR降順)の1番だけ → 100万
併せて反発がどのレッグに住むかを分解:
  ①日中レッグ: 翌日 寄り買い→引け売り（＝買いデイトレフェードそのもの）
  ②夜レッグ:   急落当日 引け買い→翌寄り売り（＝決算持ち越しの一般化・デイトレではない）
  ③24hレッグ:  引け買い→翌引け売り（参考）
買いは貸借不要＝ISSフィルタなし（誰でも建てられる）。プール=_fade_pool_buy100.pkl（再利用可）。
実行: python -X utf8 _bt_fade_buymirror.py
"""
from __future__ import annotations

import os
import pickle

import numpy as np
import pandas as pd

CAP, TOV_MIN, GAIN_CEIL = 1_000_000, 3e8, -5.0
SIZE = 1_000_000
POOL = "_fade_pool_buy100.pkl"

_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400),
        (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000),
        (30000, 5000), (50000, 7000), (70000, 10000)]


def _lim_w(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return w
    return 10000.0


if os.path.exists(POOL):
    D = pd.read_pickle(POOL)
    print(f"[load] 既存プール {len(D):,}件", flush=True)
else:
    print("[build] 急落プール作成中...", flush=True)
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    nm = dict(old["name_map"]); nm.update(new["name_map"])

    def merge(tk):
        dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk))
               if d is not None and len(d)]
        if not dfs:
            return None
        d = pd.concat(dfs).sort_index()
        return d[~d.index.duplicated(keep="last")]

    rows = []
    for tk in set(old["all_data"]) | set(new["all_data"]):
        if nm.get(tk) is None:      # 名前が引けない＝真のETF/ETN
            continue
        df = merge(tk)
        if df is None or len(df) < 40:
            continue
        o = df["Open"].astype(float).to_numpy(); c = df["Close"].astype(float).to_numpy()
        h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy()
        v = df["Volume"].astype(float).to_numpy()
        cs, vs, hs, ls = map(pd.Series, (c, v, h, l))
        tov = (cs * vs).rolling(20).median().to_numpy()
        vma = vs.shift(1).rolling(20).mean().to_numpy()
        pc = cs.shift(1)
        tr = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1)
        atr = (tr.rolling(14).mean() / cs * 100).to_numpy()
        ma25 = cs.rolling(25).mean().to_numpy()
        idx = df.index
        for t in range(26, len(c) - 1):
            if not (c[t - 1] > 0 and c[t] > 0):
                continue
            gain = (c[t] / c[t - 1] - 1) * 100
            if gain > GAIN_CEIL or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
                continue
            if not (o[t + 1] > 0 and c[t + 1] > 0) or c[t] * 100 > CAP:
                continue
            if not (np.isfinite(vma[t]) and vma[t] >= 100_000):
                continue
            w = _lim_w(c[t])
            rows.append({
                "sig": idx[t].strftime("%Y-%m-%d"), "ent": idx[t + 1].strftime("%Y-%m-%d"),
                "y": idx[t + 1].year, "ticker": tk,
                "gain": gain, "px": c[t], "o1": o[t + 1], "c1": c[t + 1],
                "h1": h[t + 1], "l1": l[t + 1],
                "atr": atr[t], "dev": (c[t] / ma25[t] - 1) * 100 if ma25[t] > 0 else 0.0,
                "vr": v[t] / vma[t], "tov": tov[t], "vol_avg": vma[t],
                "rng": (h[t] - l[t]) / c[t] * 100,
                "gu": (o[t + 1] / c[t] - 1) * 100,
                "o1_limit_dn": bool(o[t + 1] <= c[t] - w + 1e-9),
            })
    D = pd.DataFrame(rows).sort_values(["sig", "ticker"])
    D["intra"] = (D.c1 - D.o1) / D.o1 * 100     # 寄り買い→引け売り（買いデイトレフェード）
    D["ovn"] = (D.o1 - D.px) / D.px * 100       # 引け買い→翌寄り売り（夜レッグ）
    D["full"] = (D.c1 - D.px) / D.px * 100      # 引け買い→翌引け売り（24h）
    D.to_pickle(POOL)
    print(f"[save] {POOL} {len(D):,}件 ({D.sig.min()}〜{D.sig.max()})", flush=True)

D["ym"] = D.ent.str[:7]


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def pick1(d, leg="intra"):
    d = d.copy()
    r = (d.groupby("sig")["dev"].rank(ascending=True, pct=True)     # 一番売られ過ぎ
         + d.groupby("sig")["atr"].rank(ascending=False, pct=True)) # ×一番動く
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 1].copy()
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["pnl"] = d[leg]
    ent_px = d.o1 if leg == "intra" else d.px
    d["yen"] = d.pnl / 100 * d.sh * ent_px
    return d


def summ(d, label):
    if len(d) == 0:
        print(f"  {label:<30} 0件"); return
    yy = d.groupby("y").yen.sum()
    ym = d.groupby("ym").yen.sum()
    print(f"  {label:<30}{len(d):>6}玉 勝率{(d.pnl>0).mean()*100:>5.1f}% PF{pf(d.pnl):>5.2f}"
          f" 10年{d.yen.sum():>+13,.0f}円 前半{d[d.y<=2021].yen.sum():>+11,.0f}"
          f" 後半{d[d.y>=2022].yen.sum():>+11,.0f} 勝ち{int((yy>0).sum()):>2}/{yy.index.nunique()}"
          f" 最悪月{ym.min():>+9,.0f}円")


def go(gain=-7.0, atr=5.0, devmax=-12.0, vr=6.0, sticky=5.0):
    return D[(D.gain <= gain) & (D.vr < vr) & (D.atr >= atr) & (D.dev <= devmax)
             & (D.rng > sticky)]


print("\n" + "=" * 132)
print("① 売りフェードの完全鏡像（急落-7%×ATR5%×乖離-12%以下×1番×100万）＝買いデイトレフェード")
print("=" * 132)
summ(pick1(go()), "鏡像・日中(寄り買い→引け売り)")
summ(pick1(go(), leg="ovn"), "同じ玉・夜(引け買い→翌寄り売り)")
summ(pick1(go(), leg="full"), "同じ玉・24h(引け買い→翌引け)")

print("\n" + "=" * 132)
print("② 急落閾値を振る（日中レッグ・鏡像選定）＝閾値の問題ではないことの確認")
print("=" * 132)
for g in (-6, -7, -8, -10, -12, -15):
    summ(pick1(go(gain=g)), f"前日{g}%以下・日中")

print("\n" + "=" * 132)
print("③ フィルタを緩めても救えないか（日中レッグ）")
print("=" * 132)
summ(pick1(go(atr=0, devmax=999)), "ATR/乖離条件なし")
summ(pick1(go(devmax=999)), "ATR5%だけ")
summ(pick1(go(atr=0)), "乖離-12%以下だけ")
summ(pick1(go(sticky=0)), "張り付きS安も入れる")
d = go().copy(); d = d[d.gu <= -1]
summ(pick1(d), "さらに下寄り(GU-1%以下)だけ")

print("\n" + "=" * 132)
print("④ 夜レッグの正体（GO玉全体の素の平均・拾えるなら決算と同型の夜跨ぎ＝デイトレではない）")
print("=" * 132)
g7 = go()
for leg, lab in (("intra", "日中レッグ"), ("ovn", "夜レッグ"), ("full", "24hレッグ")):
    x = g7[leg]
    e1 = g7[g7.y <= 2021][leg].mean(); e2 = g7[g7.y >= 2022][leg].mean()
    print(f"  {lab}: n={len(g7):,} 平均{x.mean():+.3f}%/件 PF{pf(x):.2f}"
          f" 勝率{(x>0).mean()*100:.1f}% 前半{e1:+.3f}%/後半{e2:+.3f}%")
lk = g7[g7.o1_limit_dn]
print(f"  (夜レッグの罠: 翌寄りS安張り付き={len(lk):,}件 平均ovn{lk.ovn.mean():+.2f}%"
      f" ＝夜跨ぎはこのテールを踏む)")

# -*- coding: utf-8 -*-
"""_bt_shodo_dayexit_1115.py — 初動ブレイク(break60)を「翌朝寄りで買い→当日中に利確/損切り/大引け」で回す（2026-11-15）。

本人「初動ブレイク銘柄を当日利確する」。既存BT(_bt_shodo_break / _bt_shodo_slotsim)は全て20日保有・SL-15%で、
当日決済は未検証。判定はアプリ(kabuai/explorer_signals.py break60_recent・v7.6.2)と同一:
  終値が60日高値を30営業日ぶりに初更新 × 陽線 × 出来高20日平均1.5倍以上 × 20日平均代金1億以上
  除外: 当日+20%以上 / 出来高10倍以上 / 60日高値を10%以上抜き
  執行: 翌朝の寄りが前日終値+3%超なら買わない（v7.6.2 執行ルール）・S高寄り張り付きは買えない
出口（すべて当日中）:
  CLOSE   = 大引け
  TPx/SLy = 高値が+x%に届けば利確、安値が-y%に届けば損切り、どちらも無ければ大引け
            日足では同じ日に両方届いた時の順番が分からない → 損切りが先（保守側）として数える。
            ※「両方届いた日」の件数も出す＝多ければ5分足で詰め直す必要あり
対照: 同じ日・同じ流動性条件(代金1億以上)の全銘柄の寄り→引け平均（＝その日の地合い）。シグナルの上乗せ = 本体 − 対照。
コスト: 0.2%/往復。
実行: python -X utf8 _bt_shodo_dayexit_1115.py > _log_shodo_dayexit_1115.txt
"""
from __future__ import annotations

import os
import pickle
import sys

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

EVENTS = "_shodo_dayexit_events.pkl"
COST = 0.2
GAP_MAX = 3.0                       # 翌寄り > 前日終値+3% は買わない
TPS = (1.0, 2.0, 3.0, 5.0)
SLS = (1.0, 2.0, 3.0)
_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400),
        (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000),
        (30000, 5000), (50000, 7000), (70000, 10000)]


def _lim_w(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return w
    return 10000.0


def build() -> tuple[pd.DataFrame, pd.Series]:
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    tks = set(old["all_data"]) | set(new["all_data"])
    rows, base_rows = [], []
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
        dates = df.index
        n = len(df)
        tov = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        # 対照: 代金1億以上の全銘柄・寄り→引け
        ok = (tov >= 1e8) & (o > 0) & np.isfinite(c)
        for i in np.where(ok)[0]:
            base_rows.append((dates[i], (c[i] / o[i] - 1) * 100))
        hi60 = pd.Series(c).rolling(60).max().shift(1).to_numpy()
        v20 = pd.Series(v).rolling(20).mean().shift(1).to_numpy()
        brk = (c > hi60) & np.isfinite(hi60)
        fresh = np.zeros(n, bool)
        last = -10**9
        for i in range(n):
            if brk[i]:
                if i - last > 30:
                    fresh[i] = True
                last = i
        volx = np.where((v20 > 0) & np.isfinite(v20), v / v20, np.nan)
        chg = np.r_[np.nan, c[1:] / c[:-1] - 1] * 100
        over = (c / hi60 - 1) * 100
        hi250 = pd.Series(c).rolling(250).max().shift(1).to_numpy()
        trig = (fresh & (c > o) & (tov >= 1e8) & (volx >= 1.5)
                & (chg < 20) & (volx < 10) & (over < 10) & (np.arange(n) + 1 < n))
        for i in np.where(trig)[0]:
            j = i + 1
            e = o[j]
            if not (e > 0):
                continue
            gap = (e / c[i] - 1) * 100
            rows.append({"tk": tk, "d": dates[j], "y": dates[j].year, "px": c[i], "gap": gap,
                         "pinned": bool(e >= c[i] + _lim_w(c[i]) - 1e-6),
                         "new250": bool(np.isfinite(hi250[i]) and c[i] > hi250[i]),
                         "hi": (h[j] / e - 1) * 100, "lo": (l[j] / e - 1) * 100,
                         "cl": (c[j] / e - 1) * 100, "volx": float(volx[i])})
    D = pd.DataFrame(rows).sort_values("d").reset_index(drop=True)
    B = pd.DataFrame(base_rows, columns=["d", "r"]).groupby("d").r.mean()
    pickle.dump((D, B), open(EVENTS, "wb"))
    print(f"[build] イベント {len(D):,}件 / 対照 {len(B):,}日 保存")
    return D, B


D, B = pickle.load(open(EVENTS, "rb")) if os.path.exists(EVENTS) else build()
D = D[~D.pinned & (D.gap <= GAP_MAX)].copy()
D["base"] = D.d.map(B)
days = D.d.nunique()
print(f"[pool] 買える玉 {len(D):,}件 / シグナルのあった日 {days:,}日 / 1日平均 {len(D)/max(days,1):.1f}本")
per_day = D.groupby("d").size()
print(f"       1日あたり本数: 中央値{per_day.median():.0f} / 90%点{per_day.quantile(.9):.0f} / 最大{per_day.max()}")


def exit_pnl(d: pd.DataFrame, tp: float | None, sl: float | None) -> tuple[pd.Series, int]:
    r = d.cl.copy()
    both = 0
    if tp is not None and sl is not None:
        hit_tp, hit_sl = d.hi >= tp, d.lo <= -sl
        both = int((hit_tp & hit_sl).sum())
        r = np.where(hit_sl, -sl, np.where(hit_tp, tp, d.cl))          # 両方→損切り先（保守）
    elif tp is not None:
        r = np.where(d.hi >= tp, tp, d.cl)
    elif sl is not None:
        r = np.where(d.lo <= -sl, -sl, d.cl)
    return pd.Series(r, index=d.index) - COST, both


def line(lab: str, d: pd.DataFrame, r: pd.Series, both: int = 0) -> str:
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    yy = r.groupby(d.y).mean()
    h1, h2 = r[d.y <= 2021].mean(), r[d.y >= 2022].mean()
    edge = (r + COST - d.base).mean()
    return (f"  {lab:<14} n={len(r):>6,} 平均{r.mean():>+6.2f}% 勝率{(r>0).mean()*100:>4.1f}% "
            f"PF{gp/gl if gl else np.inf:>5.2f} 前半{h1:>+6.2f}/後半{h2:>+6.2f} "
            f"陽性年{int((yy>0).sum())}/{len(yy)} 地合い差{edge:>+6.2f}%"
            + (f" 両方到達{both:,}件({both/len(r)*100:.0f}%)" if both else ""))


for lab, sub in (("全件", D), ("🆕新高値(250日)", D[D.new250]), ("それ以外", D[~D.new250])):
    print(f"\n=== {lab} ===")
    r, _ = exit_pnl(sub, None, None)
    print(line("大引け", sub, r))
    for tp in TPS:
        r, _ = exit_pnl(sub, tp, None)
        print(line(f"利確+{tp:g}%のみ", sub, r))
    for sl in SLS:
        r, _ = exit_pnl(sub, None, sl)
        print(line(f"損切り-{sl:g}%のみ", sub, r))
    for tp in TPS:
        for sl in SLS:
            r, b = exit_pnl(sub, tp, sl)
            print(line(f"+{tp:g}/-{sl:g}", sub, r, b))

print("\n=== 年別（全件・大引け）===")
r, _ = exit_pnl(D, None, None)
for y, g in r.groupby(D.y):
    print(f"  {y}: n={len(g):>5} 平均{g.mean():>+6.2f}% 勝率{(g>0).mean()*100:>4.1f}%")

print("\n=== 寄りのギャップ別（全件・大引け）＝朝どこまで追いかけていいか ===")
for lo_, hi_ in ((-99, -1), (-1, 0), (0, 1), (1, 2), (2, 3)):
    g = D[(D.gap > lo_) & (D.gap <= hi_)]
    if len(g):
        rr, _ = exit_pnl(g, None, None)
        print(line(f"GU {lo_:+}〜{hi_:+}%", g, rr))

print("\n読み方: 平均・PFは本体。地合い差＝同じ日の全銘柄寄り→引け平均との差（コスト前）＝シグナル固有の上乗せ。"
      "\n        両方到達が多い組み合わせは日足では判定不能＝損切り先で数えているので実際はこれ以上の可能性がある。")

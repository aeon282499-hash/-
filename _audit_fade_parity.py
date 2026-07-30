# -*- coding: utf-8 -*-
"""_audit_fade_parity.py — 売りフェード：本番コードとBTの一致監査（2026-07-31）。

本人「BT通りに運用されているか隅々までバグがないかチェックして」。

やること: daytrade_paper.daily_top_fades の**計算式をそのまま**移植した候補生成を書き、
_bt_fade_deep.py（BT側）の定義と1つずつ突き合わせる。差が出た軸は10年で影響額を測る。

突き合わせる軸:
  ①出来高比vrの分母（本番=シグナル日を除く20日 / BT=シグナル日を含む20日）
  ②株価下限300円（本番のみ）
  ③20日平均出来高10万株下限（本番のみ）
  ④ETF除外（BTのみ is_etf_ticker）
  ⑤丸め（本番は dev25を1桁・atr_pctを2桁に丸めてから閾値判定と順位付け）
  ⑥順位付けのタイ処理（本番=sorted+enumerate / BT=pandas rank(pct=True)）
  ⑦株数の基準（本番=前日終値 / BT=翌日始値）

実行: python -X utf8 _audit_fade_parity.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

from screener import is_etf_ticker

SIZE = 500_000
CAND_MIN, GAIN_MIN, TOV_MIN, STICKY_MIN = 5.0, 6.0, 3e8, 0.05
VR_MAX, DEV_MAX, ATR_MIN, DEV_MIN = 6.0, 80.0, 5.0, 12.0

print("[load] 読込中...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
YRS = sorted(ISS)
nm = dict(old["name_map"]); nm.update(new["name_map"])


def iss_for(y):
    return ISS[min(YRS, key=lambda a: (abs(a - y), a))]


def merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


rows = []
for tk in set(old["all_data"]) | set(new["all_data"]):
    name = nm.get(tk)
    if name is None:
        continue
    etf = is_etf_ticker(tk, name)
    df = merge(tk)
    if df is None or len(df) < 40:
        continue
    o = df["Open"].astype(float).to_numpy(); c = df["Close"].astype(float).to_numpy()
    h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy()
    v = df["Volume"].astype(float).to_numpy()
    cs, vs, hs, ls = map(pd.Series, (c, v, h, l))
    tov = (cs * vs).rolling(20).median().to_numpy()          # 両者一致（シグナル日を含む）
    vma_bt = vs.rolling(20).mean().to_numpy()                # BT: シグナル日を含む20日
    vma_live = vs.shift(1).rolling(20).mean().to_numpy()     # 本番: シグナル日を除く20日
    pc = cs.shift(1)
    tr = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1)
    atr = (tr.rolling(14).mean() / cs * 100).to_numpy()
    ma25 = cs.rolling(25).mean().to_numpy()
    idx = df.index
    for t in range(26, len(c) - 1):
        if not (c[t - 1] > 0 and c[t] > 0):
            continue
        gain = (c[t] / c[t - 1] - 1) * 100
        if gain < CAND_MIN or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
            continue
        rng = (h[t] - l[t]) / c[t]
        if rng <= STICKY_MIN:
            continue
        y = idx[t + 1].year
        if iss_for(y).get(str(tk)[:4], "?") != "2":
            continue
        if not (o[t + 1] > 0 and c[t + 1] > 0) or c[t] * 100 > SIZE:
            continue
        dev = (c[t] / ma25[t] - 1) * 100 if ma25[t] > 0 else 0.0
        rows.append({
            "sig": idx[t].strftime("%Y-%m-%d"), "y": y, "ticker": tk, "etf": etf,
            "gain": gain, "px": c[t], "o1": o[t + 1], "c1": c[t + 1],
            "atr": atr[t], "dev": dev,
            "vr_bt": v[t] / vma_bt[t] if vma_bt[t] > 0 else 0.0,
            "vr_live": v[t] / vma_live[t] if vma_live[t] > 0 else 0.0,
            "vol_avg_live": vma_live[t],
            # BT(_bt_fade_deep.py)は保存時点で sh=(CAP/o1//100*100)>0 の玉だけを残すので、
            # 株数0の候補は**ランク付けの前に**プールから消える。本番は _shares_for が
            # max(100,...) で必ず100株以上を返すため、この脱落が起きない＝ここも差の一因。
            "sh_bt": int(SIZE / o[t + 1] // 100 * 100) if o[t + 1] > 0 else 0,
        })
A = pd.DataFrame(rows)
A["pnl"] = (A.o1 - A.c1) / A.o1 * 100
A.to_pickle("_audit_pool.pkl")     # 再実行を速くするため保存（収集に数分かかる）
print(f"[collect] 候補プール {len(A):,}件（{A.sig.min()}〜{A.sig.max()}）\n", flush=True)


def pick(d, mode):
    """mode='bt' はBTの定義、'live' は本番の定義をそのまま再現して上位2本を選ぶ。"""
    d = d.copy()
    if mode == "live":
        d = d[(d.px >= 300) & (d.vol_avg_live >= 100_000)]
        d = d[d.vr_live < VR_MAX]
        d["dv"] = d.dev.round(1)                 # 本番は丸めてから判定・順位付け
        d["atv"] = d.atr.round(2)
    else:
        d = d[~d.etf]
        d = d[d.sh_bt > 0]                       # BTは保存時点でここを落としている
        d = d[d.vr_bt < VR_MAX]
        d["dv"] = d.dev
        d["atv"] = d.atr
    d = d[(d.gain >= GAIN_MIN) & (d.dv < DEV_MAX) & (d.atv >= ATR_MIN) & (d.dv >= DEV_MIN)]
    if mode == "live":
        # sorted+enumerate の整数順位（タイは並び順で決まる）
        d = d.sort_values(["sig", "dv"], ascending=[True, False])
        d["rd"] = d.groupby("sig").cumcount()
        d = d.sort_values(["sig", "atv"], ascending=[True, False])
        d["ra"] = d.groupby("sig").cumcount()
        d["mix"] = (d.rd + d.ra) / 2
    else:
        r = None
        for col in ("dv", "atv"):
            x = d.groupby("sig")[col].rank(ascending=False, pct=True)
            r = x if r is None else r + x
        d["mix"] = r / 2
    d = d.sort_values(["sig", "mix"])
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 2].copy()
    base = d.px if mode == "live" else d.o1     # 株数の基準が違う
    d["sh"] = (SIZE / base // 100 * 100).astype(int)
    d = d[d.sh > 0]
    d["yen"] = (d.o1 - d.c1) * d.sh
    return d


BT, LV = pick(A, "bt"), pick(A, "live")
YEARS = list(range(2016, 2027))


def sm(d):
    yr = d.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    p = d.pnl; loss = -p[p < 0].sum()
    return (f"{len(d):>5}玉 勝率{(p>0).mean()*100:>5.1f}% PF{p[p>0].sum()/loss:>5.2f} "
            f"年{d.yen.sum()/11:>+9,.0f}円 勝ち{int((yr>0).sum()):>2}/11 計{d.yen.sum():>+11,.0f}円")


print("=" * 120)
print("① 総合：BTの定義 vs 本番の定義（同じ10年・同じ執行）")
print("=" * 120)
print(f"  BT側の定義  : {sm(BT)}")
print(f"  本番側の定義: {sm(LV)}")
print(f"  差          : {LV.yen.sum()-BT.yen.sum():+,.0f}円 "
      f"（年{(LV.yen.sum()-BT.yen.sum())/11:+,.0f}円 / {(LV.yen.sum()/BT.yen.sum()-1)*100:+.1f}%）")

bs, ls_ = set(zip(BT.sig, BT.ticker)), set(zip(LV.sig, LV.ticker))
print(f"\n  選ぶ玉の一致: 共通{len(bs & ls_):,}玉 / BTのみ{len(bs - ls_):,}玉 / 本番のみ{len(ls_ - bs):,}玉"
      f"  → 一致率 {len(bs & ls_)/len(bs | ls_)*100:.1f}%")

print("\n" + "=" * 120)
print("② 差分の原因を1つずつ切り分け（BTの定義から本番の定義へ1軸ずつ寄せる）")
print("=" * 120)
steps = [
    ("BT定義（基準）", dict()),
    ("＋ vrの分母を本番式に", dict(vr="live")),
    ("＋ 株価300円下限", dict(vr="live", px300=True)),
    ("＋ 出来高10万株下限", dict(vr="live", px300=True, vol=True)),
    ("＋ ETFを除外しない", dict(vr="live", px300=True, vol=True, etf=False)),
    ("＋ 丸め(dev1桁/atr2桁)", dict(vr="live", px300=True, vol=True, etf=False, rnd=True)),
    ("＋ 順位をenumerate式に", dict(vr="live", px300=True, vol=True, etf=False, rnd=True, enum=True)),
    ("＋ 株数を前日終値基準に", dict(vr="live", px300=True, vol=True, etf=False, rnd=True, enum=True, sh="px")),
    ("＋ 株数0の脱落をなくす", dict(vr="live", px300=True, vol=True, etf=False, rnd=True, enum=True,
                            sh="px", shpool=False)),
]
prev = None
for lab, cfg in steps:
    d = A.copy()
    if not cfg.get("etf", True) is False:
        d = d[~d.etf]
    if not cfg.get("shpool", True) is False:
        d = d[d.sh_bt > 0]
    if cfg.get("px300"):
        d = d[d.px >= 300]
    if cfg.get("vol"):
        d = d[d.vol_avg_live >= 100_000]
    d = d[d["vr_live" if cfg.get("vr") == "live" else "vr_bt"] < VR_MAX]
    d["dv"] = d.dev.round(1) if cfg.get("rnd") else d.dev
    d["atv"] = d.atr.round(2) if cfg.get("rnd") else d.atr
    d = d[(d.gain >= GAIN_MIN) & (d.dv < DEV_MAX) & (d.atv >= ATR_MIN) & (d.dv >= DEV_MIN)]
    if cfg.get("enum"):
        d = d.sort_values(["sig", "dv"], ascending=[True, False]); d["rd"] = d.groupby("sig").cumcount()
        d = d.sort_values(["sig", "atv"], ascending=[True, False]); d["ra"] = d.groupby("sig").cumcount()
        d["mix"] = (d.rd + d.ra) / 2
    else:
        r = None
        for col in ("dv", "atv"):
            x = d.groupby("sig")[col].rank(ascending=False, pct=True)
            r = x if r is None else r + x
        d["mix"] = r / 2
    d = d.sort_values(["sig", "mix"]); d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 2].copy()
    base = d.px if cfg.get("sh") == "px" else d.o1
    d["sh"] = (SIZE / base // 100 * 100).astype(int)
    d = d[d.sh > 0]; d["yen"] = (d.o1 - d.c1) * d.sh
    tot = d.yen.sum()
    delta = f"{tot-prev:+,.0f}円" if prev is not None else "—"
    print(f"  {lab:<24}{len(d):>5}玉 年{tot/11:>+9,.0f}円 計{tot:>+11,.0f}円  この軸の影響 {delta:>13}")
    prev = tot

print("\n" + "=" * 120)
print("③ 出来高比vrの分母だけを取り出して見る")
print("=" * 120)
P = A[(A.gain >= GAIN_MIN) & (A.dev < DEV_MAX) & (A.atr >= ATR_MIN) & (A.dev >= DEV_MIN)]
both = ((P.vr_bt < VR_MAX) & (P.vr_live < VR_MAX)).sum()
only_bt = ((P.vr_bt < VR_MAX) & (P.vr_live >= VR_MAX)).sum()
only_lv = ((P.vr_bt >= VR_MAX) & (P.vr_live < VR_MAX)).sum()
print(f"  条件を満たす候補 {len(P):,}件のうち")
print(f"    両方が通す           {both:>6,}件")
print(f"    BTは通すが本番は弾く {only_bt:>6,}件  ← 本番だけが取り逃す玉")
print(f"    BTは弾くが本番は通す {only_lv:>6,}件")
print(f"  vrの平均: BT {P.vr_bt.mean():.2f}倍 / 本番 {P.vr_live.mean():.2f}倍 "
      f"（本番の方が {P.vr_live.mean()/P.vr_bt.mean():.2f}倍 大きく出る）")
x = P[(P.vr_bt < VR_MAX) & (P.vr_live >= VR_MAX)]
if len(x):
    print(f"  本番だけが弾く{len(x):,}件の実力: 勝率{(x.pnl>0).mean()*100:.1f}% 平均{x.pnl.mean():+.3f}%")

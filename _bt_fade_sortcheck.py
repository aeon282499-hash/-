# -*- coding: utf-8 -*-
"""_bt_fade_sortcheck.py — 並び順「乖離+レンジ」のノイズ判定＋米株プロキシ軸（2026-08-06）。

_bt_fade_rebase100.py で唯一★が付いた「乖離+レンジ」(+73万/10年) を判定する。
判定基準（BT作法）:
  ①土台（50万×2本 vs 100万×1番）で勝者が入れ替わるなら符号反転＝ノイズ
  ②差が少数の日に集中していればまぐれ
  ③同点タイブレーク(ticker昇順→降順)で結論が動くなら順位ノイズ圏
併せて 前日米株(1557.T=S&P500 ETF東京・ex-ante成立) のゲートを測る。
実行: python -X utf8 _bt_fade_sortcheck.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

P100 = pd.read_pickle("_fade_pool_v5_100.pkl")
P50 = pd.read_pickle("_fade_pool_v2.pkl")      # 旧土台プール（50万×2本時代・値がさ5千円）

# 1557.T（S&P500 ETF東京）の前日騰落
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
dfs = [d for d in (old["all_data"].get("1557.T"), new["all_data"].get("1557.T")) if d is not None]
sp = pd.concat(dfs).sort_index()
sp = sp[~sp.index.duplicated(keep="last")]["Close"].astype(float)
us_chg = {d.strftime("%Y-%m-%d"): float(v) for d, v in (sp.pct_change() * 100).items()}
P100 = P100.copy()
P100["us"] = P100.sig.map(us_chg)


def run(P, size, n, sort=("dev", "atr"), tiebreak=True, extra=None,
        gain=None, devmax=None):
    d = P
    if gain is None:
        gain = 7.0 if size == 1_000_000 else 6.0
    if devmax is None:
        devmax = 999.0 if size == 1_000_000 else 80.0   # 旧土台の現行=乖離80%上限あり…7/31採用後は撤廃
    d = d[(d.gain >= gain) & (d.vr < 6.0) & (d.dev < devmax) & (d.atr >= 5.0)
          & (d.dev >= 12.0) & (d.tov >= 3e8) & (d.rng > 5.0) & (d.vol_avg >= 100_000)]
    if extra is not None:
        d = d[extra(d)]
    d = d.copy()
    r = None
    for col in sort:
        x = d.groupby("sig")[col].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / len(sort)
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable", ascending=[True, True, tiebreak])
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= n].copy()
    d["sh"] = (size / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def brief(d, label):
    yr = d.groupby("y").yen.sum()
    p = d.pnl; loss = -p[p < 0].sum()
    pf = p[p > 0].sum() / loss if loss > 0 else np.inf
    print(f"  {label:<32} {len(d):>5}玉 勝率{(p>0).mean()*100:>5.1f}% PF{pf:>5.2f} "
          f"10年{d.yen.sum()/1e4:>+8,.0f}万 勝ち{int((yr>0).sum())}/{yr.index.nunique()}")
    return d.yen.sum()


print("=" * 100)
print("① 土台またぎの安定性（勝者が入れ替わるならノイズ）")
print("=" * 100)
print("[新土台 100万×1番・GO+7%・乖離上限なし]")
a1 = brief(run(P100, 1_000_000, 1, ("dev", "atr")), "乖離+ATR(現行)")
b1 = brief(run(P100, 1_000_000, 1, ("dev", "rng")), "乖離+レンジ")
print("[旧土台 50万×2本・GO+6%・乖離80%上限(7/29当時の現行)]")
a2 = brief(run(P50, 500_000, 2, ("dev", "atr")), "乖離+ATR(当時の現行)")
b2 = brief(run(P50, 500_000, 2, ("dev", "rng")), "乖離+レンジ")
print("[中間検証: 新プールで50万×2本相当（サイズだけ旧）]")
a3 = brief(run(P100, 500_000, 2, ("dev", "atr")), "乖離+ATR")
b3 = brief(run(P100, 500_000, 2, ("dev", "rng")), "乖離+レンジ")
print(f"\n  → 新土台での差 {b1-a1:+,.0f}円 / 旧土台での差 {b2-a2:+,.0f}円 / 中間 {b3-a3:+,.0f}円")

print()
print("=" * 100)
print("② 差の集中度（新土台・乖離+レンジ − 乖離+ATR の日次差）")
print("=" * 100)
da = run(P100, 1_000_000, 1, ("dev", "atr")).set_index("sig")
db = run(P100, 1_000_000, 1, ("dev", "rng")).set_index("sig")
common = da.index.intersection(db.index)
diff_pick = [s for s in common if da.loc[s, "ticker"] != db.loc[s, "ticker"]]
delta = (db.loc[common, "yen"] - da.loc[common, "yen"])
delta_nz = delta[delta != 0].sort_values()
print(f"  選定が異なる日: {len(diff_pick)}/{len(common)}日 ({len(diff_pick)/len(common)*100:.1f}%)")
print(f"  差の総額 {delta.sum():+,.0f}円 / 差が出た日 {len(delta_nz)}日")
top5 = delta_nz.tail(5); bot5 = delta_nz.head(5)
print(f"  乖離+レンジ側に有利な上位5日の寄与 {top5.sum():+,.0f}円 "
      f"({top5.sum()/delta.sum()*100 if delta.sum() else 0:.0f}%)")
for s, v in top5.items():
    print(f"    {s} {da.loc[s,'ticker']}→{db.loc[s,'ticker']} {v:+,.0f}円")
print(f"  不利な下位5日 {bot5.sum():+,.0f}円")
print("  年別の差:")
yd = (db.groupby("y").yen.sum() - da.groupby("y").yen.sum())
for y, v in yd.items():
    print(f"    {y}: {v:+,.0f}円")

print()
print("=" * 100)
print("③ 同点タイブレークの向きで結論が動くか（新土台）")
print("=" * 100)
for tb, lab in ((True, "ticker昇順(本番)"), (False, "ticker降順")):
    x = run(P100, 1_000_000, 1, ("dev", "atr"), tiebreak=tb).yen.sum()
    y = run(P100, 1_000_000, 1, ("dev", "rng"), tiebreak=tb).yen.sum()
    print(f"  {lab}: 乖離+ATR {x/1e4:+,.0f}万 / 乖離+レンジ {y/1e4:+,.0f}万 / 差 {(y-x)/1e4:+,.1f}万")

print()
print("=" * 100)
print("④ 前日米株(1557.T)ゲート（新土台・ex-ante成立）")
print("=" * 100)
base = brief(run(P100, 1_000_000, 1), "現行(ゲートなし)")
for lo, hi, lab in ((-99, 0, "前日米株マイナスの日だけ"), (0, 99, "前日米株プラスの日だけ"),
                    (-1, 99, "-1%以上(暴落夜だけ回避)"), (-2, 99, "-2%以上"),
                    (-99, 2, "+2%未満(爆騰夜だけ回避)")):
    brief(run(P100, 1_000_000, 1, extra=lambda d, lo=lo, hi=hi: (d.us >= lo) & (d.us < hi)),
          lab)
b = run(P100, 1_000_000, 1)
print("\n  [層別] 現行1番玉を前日米株で層別:")
for lo, hi in ((-99, -2), (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 99)):
    s = b[(b.us >= lo) & (b.us < hi)]
    if len(s) < 5:
        continue
    p = s.pnl; loss = -p[p < 0].sum()
    pf = p[p > 0].sum() / loss if loss > 0 else np.inf
    print(f"    米株{lo:+.0f}〜{hi:+.0f}%: {len(s):>4}玉 勝率{(p>0).mean()*100:>5.1f}% "
          f"PF{pf:>5.2f} 計{s.yen.sum()/1e4:>+8,.0f}万")

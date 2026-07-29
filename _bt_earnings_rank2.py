# -*- coding: utf-8 -*-
"""_bt_earnings_rank2.py — 決算: 有望2軸を枠シムで決着させる（2026-07-29）。

_bt_earnings_rank.py の単変量スクリーニングで、**両期間とも同じ向き**に出た軸が2つ:

  ① 同業種の直近決算モメンタム（逆張り）
     同じ33業種で直近N営業日に決算を出した銘柄の平均ギャップが**悪いほど、次が良い**。
     5日窓: 最悪五分位 +0.95%/勝率56.3%（前半+0.51 / 後半+1.75）
     10/20/40日窓でも同じ向き＝窓に対して高原。機構: 同業種の決算が既に売られている
     ＝期待値が下がりきっている → 次の1本がサプライズになりやすい。
  ② 信用倍率（買残÷売残）が高いほど良い
     最高五分位 +0.82%/勝率51.9%（前半+0.83 / 後半+0.82＝両期間ほぼ同値）。
     機構: 買残が厚い＝投げが出切っている/踏み上げ余地。ただしテールも太い（>+8%が11.5%）。

棄却済み: 市場全体モメンタム（窓で符号反転＝ノイズ）/ 枠を絞る（8枠が最良）/
          並び順の単変量総当たり（RSI昇順が頂点）/ 発表時刻（キーが取れず判定不能）。

ここでやること: ①②をフィルタとしてと並び順として枠シムに入れ、両期間で改善するか。
採用条件: 両期間改善 + 近傍が高原 + 機構の説明がつく。
実行: python -X utf8 _bt_earnings_rank2.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

YEARS = list(range(2016, 2027))
SIZE, SLOTS = 1_000_000, 8

E = pd.read_csv("_earnings_events_rich2.csv")
E["d0"] = E["d0"].astype(str)
A = E[(E.rsi <= 45) & (E.runup5 < -3) & (E.tov20 >= 7.5e8)
      & (E.price <= 10_000) & np.isfinite(E.gap)].copy()

# ── 同業種モメンタムを付ける（前日までに確定した情報のみ・d0' < d0）──
ALL = E[np.isfinite(E.gap) & E.sector.notna()][["d0", "sector", "gap"]].sort_values("d0")
days_all = sorted(ALL.d0.unique()); dmap = {d: i for i, d in enumerate(days_all)}
ALL["di"] = ALL.d0.map(dmap)
A["di"] = A.d0.map(dmap)
sec_groups = {s: g.sort_values("di") for s, g in ALL.groupby("sector")}

for win in (5, 10, 20, 40):
    vals = []
    for r in A.itertuples():
        g = sec_groups.get(r.sector)
        if g is None or not np.isfinite(r.di):
            vals.append(np.nan); continue
        m = g[(g.di < r.di) & (g.di >= r.di - win)]
        vals.append(m.gap.mean() if len(m) >= 2 else np.nan)
    A[f"sm{win}"] = vals
print(f"[prep] 候補{len(A):,}件 / 同業種モメンタム測定率 "
      f"{A.sm10.notna().mean()*100:.1f}%(10日窓)", flush=True)


def pf(x):
    l = -x[x < 0].sum()
    return x[x > 0].sum() / l if l > 0 else np.inf


def sim(df, order=("rsi", True), slots=SLOTS, size=SIZE):
    col, asc = order
    d = df[np.isfinite(df.gap) & df[col].notna()].sort_values(["d0", col], ascending=[True, asc])
    days = sorted(d.d0.unique()); di = {x: i for i, x in enumerate(days)}
    busy, held, out = [], {}, []
    for day, g in d.groupby("d0", sort=True):
        i = di[day]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if r.ticker in held:
                continue
            sh = int(size / r.price / 100) * 100
            if sh <= 0:
                continue
            p, sp = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            if not np.isfinite(p):
                continue
            busy.append(i + sp); held[r.ticker] = i + sp
            out.append({"y": r.year, "pnl": p, "yen": p / 100 * sh * r.price})
    if not out:
        return None
    B = pd.DataFrame(out)
    yr = B.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    return dict(n=len(B), tot=B.yen.sum(), win=int((yr > 0).sum()), worst=yr.min(),
                pf=pf(B.pnl), wr=(B.pnl > 0).mean() * 100,
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = sim(A)
print(f"[base] 現行: {BASE['n']}件 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"{BASE['tot']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f}\n")


def row(lab, r):
    if r is None:
        print(f"  {lab:<34}  —"); return
    mk = ("両期間改善" if (r["a"] > BASE["a"] and r["b"] > BASE["b"])
          else ("片側" if r["tot"] > BASE["tot"] else ""))
    print(f"  {lab:<34}{r['n']:>6}{r['wr']:>7.1f}%{r['pf']:>7.2f}{r['tot']:>+12,.0f}円"
          f"{r['win']:>5}/11{r['a']:>+12,.0f}円{r['b']:>+12,.0f}円{mk:>12}")


HDR = f"  {'設定':<34}{'件数':>6}{'勝率':>8}{'PF':>7}{'10年計':>13}{'勝ち年':>7}{'前半':>13}{'後半':>13}{'判定':>13}"

print("=" * 118)
print("① 同業種モメンタムを『並び順』に使う（悪い順＝逆張り）")
print("=" * 118)
print(HDR)
row("現行 RSI昇順", BASE)
for w in (5, 10, 20, 40):
    row(f"同業種モメンタム{w}日 昇順(悪い順)", sim(A, order=(f"sm{w}", True)))
    row(f"同業種モメンタム{w}日 降順(良い順)", sim(A, order=(f"sm{w}", False)))

print("\n" + "=" * 118)
print("② 同業種モメンタムを『フィルタ』に使う（悪いときだけ建てる）")
print("=" * 118)
print(HDR)
row("現行（フィルタなし）", BASE)
for w in (5, 10, 20):
    for q in (0.2, 0.4, 0.6, 0.8):
        thr = A[f"sm{w}"].quantile(q)
        row(f"{w}日モメンタム < {thr:+.2f}%（下位{int(q*100)}%）",
            sim(A[A[f"sm{w}"] < thr]))

print("\n" + "=" * 118)
print("③ 信用倍率を『並び順』『フィルタ』に使う")
print("=" * 118)
print(HDR)
row("現行 RSI昇順", BASE)
row("信用倍率 降順（厚い順）", sim(A, order=("ratio", False)))
for q in (0.5, 0.6, 0.7, 0.8):
    thr = A.ratio.quantile(q)
    row(f"信用倍率 > {thr:.1f}倍（上位{int((1-q)*100)}%）", sim(A[A.ratio > thr]))

print("\n" + "=" * 118)
print("④ 合成: 同業種モメンタム(悪い) × 信用倍率(厚い) の複合スコアで並べる")
print("=" * 118)
print(HDR)
row("現行 RSI昇順", BASE)
for w in (5, 10, 20):
    d = A[A[f"sm{w}"].notna() & A.ratio.notna()].copy()
    d["r_sm"] = d.groupby("d0")[f"sm{w}"].rank(ascending=True, pct=True)     # 悪いほど小
    d["r_rt"] = d.groupby("d0")["ratio"].rank(ascending=False, pct=True)     # 厚いほど小
    d["r_rsi"] = d.groupby("d0")["rsi"].rank(ascending=True, pct=True)
    for nm, expr in (("モメンタム+倍率", d.r_sm + d.r_rt),
                     ("モメンタム+倍率+RSI", d.r_sm + d.r_rt + d.r_rsi),
                     ("モメンタム+RSI", d.r_sm + d.r_rsi)):
        d["mix"] = expr
        row(f"{w}日 {nm}", sim(d, order=("mix", True)))

print("\n" + "=" * 118)
print("⑤ 勝率70%は作れるか（最も強い条件まで絞り込んだときの勝率と件数）")
print("=" * 118)
print(f"  {'条件':<44}{'件数':>7}{'勝率':>8}{'平均gap':>10}{'年あたり':>10}")
combos = [
    ("現行（全候補）", A),
    ("同業種10日モメンタム 下位20%", A[A.sm10 < A.sm10.quantile(0.2)]),
    ("同業種5日モメンタム 下位20%", A[A.sm5 < A.sm5.quantile(0.2)]),
    ("信用倍率 上位20%", A[A.ratio > A.ratio.quantile(0.8)]),
    ("モメンタム下位20% × 倍率上位50%",
     A[(A.sm10 < A.sm10.quantile(0.2)) & (A.ratio > A.ratio.quantile(0.5))]),
    ("モメンタム下位20% × 倍率上位20%",
     A[(A.sm10 < A.sm10.quantile(0.2)) & (A.ratio > A.ratio.quantile(0.8))]),
    ("↑ × RSI≥28", A[(A.sm10 < A.sm10.quantile(0.2)) & (A.ratio > A.ratio.quantile(0.8))
                     & (A.rsi >= 28)]),
    ("↑ × 買残回転<0.3日", A[(A.sm10 < A.sm10.quantile(0.2)) & (A.ratio > A.ratio.quantile(0.8))
                          & (A.rsi >= 28) & (A.days_cover < 0.3)]),
]
for lab, d in combos:
    d = d[np.isfinite(d.gap)]
    if not len(d):
        continue
    yrs = d.year.nunique()
    print(f"  {lab:<44}{len(d):>7}{(d.gap>0).mean()*100:>7.1f}%{d.gap.mean():>+9.2f}%"
          f"{len(d)/max(yrs,1):>9.1f}件")

# -*- coding: utf-8 -*-
"""_audit_volgate.py — 本番実装した「決算ボラゲート≥2.0%」を自己監査する（2026-08-01）。

これは既に本番(main_earnings_hold.py)に入っている＝実弾が動く判断なので、
採用根拠が壊れていないかを叩く。疑うのは次の4点。

  ① 生存バイアス: イベント表の銘柄は「現在の name_map」由来で、上場廃止銘柄が
     抜けている可能性がある。決算ボラの高い（荒い）銘柄ほど、生き残った側だけを
     見ているせいで良く見えていないか。
  ② expanding medianの初期不安定: 実績3回でも値を作っている。実績が薄い時期の
     値が結果を作っていないか（実績n≥8に絞っても効果が残るか）。
  ③ 少数依存: 上位の当たり玉や特定銘柄・特定年を抜いても単調性が残るか。
  ④ 業種の代理変数ではないか: 決算ボラが単に「小型・高ボラ業種」を選んでいるだけなら、
     ATR%や売買代金で代用できるはず。決算ボラ固有の情報があるかを確かめる。

実行: python -X utf8 _audit_volgate.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

R = pd.read_csv("_earnings_rsi_prod.csv")
E = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
g = E.groupby("ticker")["gap"]
E["vol"] = g.transform(lambda s: s.abs().shift(1).expanding(min_periods=3).median())
E["vol_n"] = g.transform(lambda s: s.abs().shift(1).expanding(min_periods=1).count())
E = E.merge(R, on=["ticker", "d0"], how="inner")

F = pd.read_pickle("_fins_history_nodiv.pkl")
F["tk"] = [(str(c)[:4] if len(str(c)) == 5 and str(c).endswith("0") else str(c)) + ".T"
           for c in F.Code]
Q = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)]
kn = set(zip(Q.tk, Q.DiscDate.astype(str)))
E["isq"] = [(t, d) in kn for t, d in zip(E.ticker, E.d0)]

BASE = ((E.rsi_prod <= 55) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8)
        & (E.price <= 10000) & E.isq & E.gap.notna())
C = E[BASE].copy()
C["pnl"] = np.where((C.gap > 8.0) & C.r5.notna(), C.r5, C.gap)
print(f"[data] 本番条件の候補 {len(C):,}件 / 決算ボラ付与 {C.vol.notna().mean()*100:.0f}%")


def band(d, col="vol"):
    """五分位の平均pnlとPFを返す。"""
    x = d[d[col].notna()].copy()
    if len(x) < 200:
        return None
    x["b"] = pd.qcut(x[col].rank(method="first"), 5, labels=False)
    out = []
    for b, gg in x.groupby("b"):
        neg = abs(gg.pnl[gg.pnl <= 0].sum())
        out.append((int(b) + 1, len(gg), gg.pnl.mean(),
                    gg.pnl[gg.pnl > 0].sum() / neg if neg else np.inf))
    return out


def show(rows, tag):
    if rows is None:
        print(f"  {tag}: 件数不足")
        return
    s = "  ".join(f"Q{b}:{m:+.2f}%" for b, n, m, p in rows)
    mono = all(rows[i][2] <= rows[i + 1][2] + 0.15 for i in range(len(rows) - 1))
    print(f"  {tag:<34}{s}   {'単調◎' if mono else '単調でない'}")


print("\n" + "=" * 112)
print("① 生存バイアス — 上場廃止銘柄が抜けていないか / 抜けていたら効果は消えるか")
print("=" * 112)
last = E.groupby("ticker")["d0"].max()
alive = set(last[last >= "2026-01-01"].index)      # 直近も開示がある＝生きている
dead = set(last[last < "2025-01-01"].index)        # 2年以上開示なし＝実質消えた
print(f"  銘柄 {E.ticker.nunique():,} / 直近も開示あり {len(alive):,} / 2年以上なし {len(dead):,}"
      f" ({len(dead)/E.ticker.nunique()*100:.1f}%)")
print(f"  ＝上場廃止銘柄も表に含まれている（{len(dead):,}銘柄）。全滅した銘柄が消えている形ではない。")
show(band(C), "全体")
show(band(C[C.ticker.isin(alive)]), "生き残り銘柄だけ")
show(band(C[~C.ticker.isin(alive)]), "消えた銘柄だけ")

print("\n" + "=" * 112)
print("② expanding medianの初期不安定 — 実績が薄い時期の値が結果を作っていないか")
print("=" * 112)
for lo in (3, 5, 8, 12):
    show(band(C[C.vol_n >= lo]), f"決算ボラの実績が{lo}回以上")

print("\n" + "=" * 112)
print("③ 少数依存 — 当たり玉・銘柄・年を抜いても残るか")
print("=" * 112)
show(band(C), "全体")
top = C.nlargest(20, "pnl").index
show(band(C.drop(top)), "上位20玉を除去")
top50 = C.nlargest(50, "pnl").index
show(band(C.drop(top50)), "上位50玉を除去")
cnt = C.groupby("ticker").size().sort_values(ascending=False)
show(band(C[~C.ticker.isin(cnt.head(20).index)]), "登場回数トップ20銘柄を除去")
for y0, y1, tag in ((2016, 2021, "2016-21のみ"), (2022, 2026, "2022-26のみ")):
    show(band(C[(C.year >= y0) & (C.year <= y1)]), tag)

print("\n" + "=" * 112)
print("④ 決算ボラは «価格のボラ» の言い換えではないか（固有の情報があるか）")
print("=" * 112)
sub = C[C.vol.notna() & C.atr_pct.notna()]
print(f"  決算ボラ と ATR%(価格ボラ) の順位相関: {sub.vol.corr(sub.atr_pct, method='spearman'):+.3f}")
print(f"  決算ボラ と 売買代金 の順位相関  : {sub.vol.corr(sub.tov20, method='spearman'):+.3f}")
print(f"  決算ボラ と 株価 の順位相関      : {sub.vol.corr(sub.price, method='spearman'):+.3f}")
show(band(C, "vol"), "決算ボラで五分位")
show(band(C, "atr_pct"), "ATR%(価格ボラ)で五分位")
print("\n  ATR%を揃えた中で決算ボラがまだ効くか（ATR%の3分位ごとに、決算ボラ上下半分を比較）")
x = C[C.vol.notna() & C.atr_pct.notna()].copy()
x["ab"] = pd.qcut(x.atr_pct.rank(method="first"), 3, labels=False)
print(f"    {'ATR%帯':<12}{'決算ボラ低い半分':>18}{'決算ボラ高い半分':>18}{'差':>10}")
for b, gg in x.groupby("ab"):
    med = gg.vol.median()
    lo, hi = gg[gg.vol < med], gg[gg.vol >= med]
    print(f"    {['低','中','高'][int(b)]:<12}{lo.pnl.mean():>+17.2f}%{hi.pnl.mean():>+17.2f}%"
          f"{hi.pnl.mean()-lo.pnl.mean():>+9.2f}pt")

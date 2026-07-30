# -*- coding: utf-8 -*-
"""_bt_fade_winrate.py — デイトレフェードの勝率を上げる（2026-07-31・本番無変更）。

_bt_fade_smallcap.py で「小型株を除く」は棄却された（代金・株価ともフロアを上げるほど
勝率も総額も下がる）。代わりに勝率が上がったのは **ATR下限** ＝『よく動く銘柄だけ撃つ』。
  ATR5%以上のみ: 勝率57.4%(+2.0pt) / PF1.29 / 年+61.7万 / 勝ち11-11年（現行 55.4% / 1.22 / +59.1万）
ここではその近傍が高原かどうか、両期間で持つか、本数や他フィルタと喧嘩しないかを詰める。
併せて「勝率だけを上げるダイヤル」＝日中の利確（TPのみ・到達順の曖昧さが無い）を重ねて測る。

採用条件: 両期間で総額が落ちない・近傍が高原・勝ち年を減らさない。
実行: python -X utf8 _bt_fade_winrate.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2
YEARS = list(range(2016, 2027))

D0 = pd.read_pickle("_fade_deep.pkl")
D0 = D0[(D0.gain >= 6.0) & (D0.vr < 6.0) & (D0.dev < 80.0)].copy()
D0["ym"] = pd.to_datetime(D0.sig).dt.strftime("%Y-%m")


def build(d, n=NPICK):
    d = d.copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix"])
    d["rank"] = d.groupby("sig").cumcount() + 1
    d = d[d["rank"] <= n].copy()
    d["sh"] = (SIZE / d.o1 // 100 * 100).astype(int)
    return d[(d.sh > 0) & (d.px * 100 <= SIZE)]


def tp_overlay(d, tp=None):
    """日中の利確だけ置く（空売りなので安値が -tp% に触れたら利確）。
    STOPを置かないので『どちらが先か』の曖昧さが無く、日足でも判定できる。"""
    if tp is None:
        return d
    o, l, c = d.o1.to_numpy(), d.l1.to_numpy(), d.c1.to_numpy()
    pnl = np.where(l <= o * (1 - tp / 100), tp, (o - c) / o * 100)
    out = d.copy()
    out["pnl"] = pnl
    out["yen"] = pnl / 100 * out.sh * out.o1
    return out


def stat(d):
    yr = d.groupby("y")["yen"].sum().reindex(YEARS, fill_value=0)
    m = d.groupby("ym")["yen"].sum()
    p = d["pnl"]
    loss = -p[p < 0].sum()
    return dict(n=len(d), days=d.sig.nunique(), wr=(p > 0).mean() * 100,
                pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d.yen.sum(), avg=d.yen.sum() / 11, win=int((yr > 0).sum()),
                worst=yr.min(), wm=m.min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = stat(build(D0))
HDR = (f"  {'設定':<26}{'件数':>6}{'撃つ日':>7}{'勝率':>8}{'PF':>7}{'年平均':>12}{'勝ち年':>7}"
       f"{'最悪年':>11}{'最悪月':>11}{'前半':>12}{'後半':>12}{'判定':>11}")


def row(lab, s):
    mk = ("両期間改善" if (s["a"] > BASE["a"] and s["b"] > BASE["b"])
          else ("総額◎勝率◎" if (s["tot"] > BASE["tot"] and s["wr"] > BASE["wr"])
                else ("勝率のみ" if s["wr"] > BASE["wr"] else "")))
    print(f"  {lab:<26}{s['n']:>6}{s['days']:>7}{s['wr']:>7.1f}%{s['pf']:>7.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>5}/11{s['worst']:>+10,.0f}円{s['wm']:>+10,.0f}円"
          f"{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>13}")


print("=" * 140)
print("① ATR下限スイープ（近傍が高原か）")
print("=" * 140)
print(HDR)
row("現行（下限なし）", BASE)
for a in (3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0):
    row(f"ATR {a}%以上", stat(build(D0[D0.atr >= a])))

print("\n" + "=" * 140)
print("② ATR下限 × 本数")
print("=" * 140)
print(f"  {'ATR下限':<12}" + "".join(f"{f'{n}本':>26}" for n in (1, 2, 3, 4)))
for a in (0, 4.0, 4.5, 5.0, 5.5, 6.0):
    cells = ""
    for n in (1, 2, 3, 4):
        s = stat(build(D0[D0.atr >= a], n=n))
        cells += f"{s['avg']:>+13,.0f}円{s['wr']:>10.1f}%   "
    print(f"  {('なし' if a == 0 else f'{a}%以上'):<12}{cells}")

print("\n" + "=" * 140)
print("③ ATR5%以上 に他フィルタを重ねる")
print("=" * 140)
print(HDR)
row("現行", BASE)
A = D0[D0.atr >= 5.0]
row("ATR5%", stat(build(A)))
row("ATR5% ＋金曜を撃たない", stat(build(A[A.dow != 4])))
row("ATR5% ＋日経±1%以内", stat(build(A[A.nk_chg.abs() <= 1])))
row("ATR5% ＋乖離15%以上", stat(build(A[A.dev >= 15])))
row("ATR5% ＋前日レンジ8%以上", stat(build(A[A.rng >= 8])))
row("ATR5% ＋代金5億以上", stat(build(A[A.tov >= 5e8])))
row("ATR5% ＋金曜除外＋日経±1%", stat(build(A[(A.dow != 4) & (A.nk_chg.abs() <= 1)])))

print("\n" + "=" * 140)
print("④ 勝率ダイヤル＝日中の利確だけ置く（TPのみ＝到達順の曖昧さなし）")
print("=" * 140)
print(HDR)
row("現行（引成まで持つ）", BASE)
B2 = build(D0)
for t in (2, 3, 4, 5, 6, 8):
    row(f"利確 -{t}%", stat(tp_overlay(B2, t)))
print("  ── ATR5%以上と重ねる ──")
A2 = build(A)
row("ATR5%（引成）", stat(A2))
for t in (2, 3, 4, 5, 6, 8):
    row(f"ATR5% ＋利確 -{t}%", stat(tp_overlay(A2, t)))

print("\n" + "=" * 140)
print("⑤ 年別（円）")
print("=" * 140)
CANDS = [("現行", build(D0)), ("ATR4.5%", build(D0[D0.atr >= 4.5])), ("ATR5%", A2),
         ("ATR5.5%", build(D0[D0.atr >= 5.5])),
         ("ATR5%＋金曜除外", build(A[A.dow != 4])),
         ("ATR5%＋利確-5%", tp_overlay(A2, 5))]
print(f"  {'年':>6}" + "".join(f"{lab:>18}" for lab, _ in CANDS))
for y in YEARS:
    line = f"  {y:>6}"
    for _, b in CANDS:
        line += f"{b[b.y == y].yen.sum():>+17,.0f}円"
    print(line)
print(f"  {'計':>6}" + "".join(f"{b.yen.sum():>+17,.0f}円" for _, b in CANDS))
print(f"  {'勝率':>6}" + "".join(f"{(b.pnl > 0).mean() * 100:>17.1f}%" for _, b in CANDS))
print(f"  {'撃つ日':>6}" + "".join(f"{b.sig.nunique():>17,}日" for _, b in CANDS))

print("\n" + "=" * 140)
print("⑥ ATR5%の中身（何を捨てているか）")
print("=" * 140)
kept, drop = D0[D0.atr >= 5.0], D0[D0.atr < 5.0]
for lab, d in (("残す(ATR5%以上)", kept), ("捨てる(ATR5%未満)", drop)):
    print(f"  {lab:<20} 候補{len(d):>6}件  株価中央{d.px.median():>7,.0f}円  "
          f"代金中央{d.tov.median()/1e8:>6.1f}億  前日騰落中央{d.gain.median():>5.1f}%  "
          f"乖離中央{d.dev.median():>5.1f}%")
kb = build(drop)
print(f"\n  参考: 捨てる側だけで組んだ場合 → 勝率{(kb.pnl>0).mean()*100:.1f}% / "
      f"年{kb.yen.sum()/11:+,.0f}円 / 前半{kb[kb.y<=2021].yen.sum():+,.0f} 後半{kb[kb.y>=2022].yen.sum():+,.0f}")

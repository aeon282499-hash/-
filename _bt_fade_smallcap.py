# -*- coding: utf-8 -*-
"""_bt_fade_smallcap.py — デイトレフェード「小型株を除くと勝率が上がるか」（2026-07-31・本番無変更）。

現行（2c6de79）: 前日+6%以上 × 出来高6倍未満 × 25MA乖離80%未満 × 貸借○ × 20日代金中央値3億以上
                 → 乖離+ATRの順位平均で上位2本を寄付成行で空売り → 引成買戻し
                 1玉50万・10年 勝率55.4% / PF1.22 / 年+59.1万 / 勝ち11-11年

本人の要望「もう少し勝率が欲しい。小型株を除くと結果がいいとかないか」。
J-Quantsキャッシュに発行済株式数が無いので、**小型＝薄い**の代理変数として
  ① 20日売買代金中央値（現行フロア3億）  ② 株価（低位株）
の2軸をフロアとして引き上げ、本番と同じ執行（上位2本・50万・100株丸め）で測る。

採用条件（feedback_bt_slots_must_match_live / all_years_positive_is_scope に従う）:
  ・両期間（2016-21 / 2022-26）で改善  ・近傍が高原  ・枠数と株数丸めは本番と同一
  ・勝率だけ上がって総額が落ちるものは「改善」と呼ばない（トレードオフとして併記する）

実行: python -X utf8 _bt_fade_smallcap.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2
YEARS = list(range(2016, 2027))
GAIN_MIN, VR_MAX, DEV_MAX = 6.0, 6.0, 80.0

D0 = pd.read_pickle("_fade_deep.pkl")
D0 = D0[(D0.gain >= GAIN_MIN) & (D0.vr < VR_MAX) & (D0.dev < DEV_MAX)].copy()
D0["ym"] = pd.to_datetime(D0.sig).dt.strftime("%Y-%m")
print(f"[base] 候補 {len(D0):,}件 / {D0.sig.nunique()}日 ({D0.sig.min()}〜{D0.sig.max()})", flush=True)


def build(d: pd.DataFrame, n: int = NPICK) -> pd.DataFrame:
    """本番と同じ並び（乖離+ATRの順位平均）で上位n本。株数丸めも本番と同一。"""
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


def stat(d: pd.DataFrame) -> dict:
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


def row(lab: str, s: dict, base: dict = None) -> None:
    b = base or BASE
    mk = ("両期間改善" if (s["a"] > b["a"] and s["b"] > b["b"])
          else ("勝率のみ" if s["wr"] > b["wr"] else ""))
    print(f"  {lab:<26}{s['n']:>6}{s['days']:>7}{s['wr']:>7.1f}%{s['pf']:>7.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>5}/11{s['worst']:>+10,.0f}円{s['wm']:>+10,.0f}円"
          f"{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>13}")


print("\n" + "=" * 140)
print("① 売買代金フロア（現行3億）＝『薄い＝小型』を切る")
print("=" * 140)
print(HDR)
row("現行 3億以上", BASE)
for f in (4e8, 5e8, 7e8, 1e9, 1.5e9, 2e9, 3e9, 5e9, 1e10):
    lab = f"{f/1e8:.0f}億以上" if f < 1e9 else f"{f/1e8:.0f}億以上"
    row(lab, stat(build(D0[D0.tov >= f])))

print("\n" + "=" * 140)
print("② 株価フロア（低位株を切る）")
print("=" * 140)
print(HDR)
row("現行 制限なし", BASE)
for f in (300, 500, 700, 1000, 1300, 1600, 2000, 2500):
    row(f"{f:,}円以上", stat(build(D0[D0.px >= f])))

print("\n" + "=" * 140)
print("③ 株価の上限も切る（値がさ側＝丸め損が出る帯を落とす）")
print("=" * 140)
print(HDR)
row("現行 5,000円まで", BASE)
for lo, hi in ((0, 3000), (0, 2000), (700, 3000), (1000, 3000), (1000, 2500), (1200, 2000)):
    row(f"{lo:,}〜{hi:,}円", stat(build(D0[(D0.px >= lo) & (D0.px <= hi)])))

print("\n" + "=" * 140)
print("④ 代金フロア × 本数（絞ったぶん本数を増やして総額を取り戻せるか）")
print("=" * 140)
print(f"  {'代金フロア':<12}" + "".join(f"{f'{n}本':>26}" for n in (1, 2, 3, 4)))
for f in (3e8, 5e8, 1e9, 2e9, 3e9, 5e9):
    cells = ""
    for n in (1, 2, 3, 4):
        s = stat(build(D0[D0.tov >= f], n=n))
        cells += f"{s['avg']:>+13,.0f}円{s['wr']:>10.1f}%   "
    print(f"  {f/1e8:>4.0f}億以上   {cells}")

print("\n" + "=" * 140)
print("⑤ 代金 × 株価の面（年平均円 / 勝率）")
print("=" * 140)
PXF = (0, 500, 1000, 1500, 2000)
print(f"  {'代金＼株価下限':<14}" + "".join(f"{('なし' if p == 0 else f'{p:,}円'):>22}" for p in PXF))
for f in (3e8, 5e8, 1e9, 2e9, 3e9):
    cells = ""
    for p in PXF:
        s = stat(build(D0[(D0.tov >= f) & (D0.px >= p)]))
        cells += f"{s['avg']:>+12,.0f}円{s['wr']:>8.1f}%"
    print(f"  {f/1e8:>4.0f}億以上     {cells}")

print("\n" + "=" * 140)
print("⑥ 参考：勝率を上げる他のレバー（同じ執行・上位2本のまま）")
print("=" * 140)
print(HDR)
row("現行", BASE)
row("金曜を撃たない", stat(build(D0[D0.dow != 4])))
row("上位1本だけ", stat(build(D0, n=1)))
row("ATR5%以上のみ", stat(build(D0[D0.atr >= 5])))
row("乖離+15%以上のみ", stat(build(D0[D0.dev >= 15])))
row("前日レンジ8%以上", stat(build(D0[D0.rng >= 8])))
row("終値位置90%以上", stat(build(D0[D0.pos >= 90])))
row("日経±1%以内", stat(build(D0[D0.nk_chg.abs() <= 1])))

print("\n" + "=" * 140)
print("⑦ 年別（採否判断用・上位2本）")
print("=" * 140)
CANDS = [("現行 3億", D0), ("5億以上", D0[D0.tov >= 5e8]), ("10億以上", D0[D0.tov >= 1e9]),
         ("30億以上", D0[D0.tov >= 3e9]), ("株価1,000円以上", D0[D0.px >= 1000]),
         ("10億以上×1,000円以上", D0[(D0.tov >= 1e9) & (D0.px >= 1000)])]
built = {lab: build(d) for lab, d in CANDS}
print(f"  {'年':>6}" + "".join(f"{lab:>22}" for lab, _ in CANDS))
for y in YEARS:
    line = f"  {y:>6}"
    for lab, _ in CANDS:
        b = built[lab]
        line += f"{b[b.y == y].yen.sum():>+21,.0f}円"
    print(line)
line = f"  {'計':>6}"
for lab, _ in CANDS:
    line += f"{built[lab].yen.sum():>+21,.0f}円"
print(line)
line = f"  {'勝率':>6}"
for lab, _ in CANDS:
    b = built[lab]
    line += f"{(b.pnl > 0).mean() * 100:>21.1f}%"
print(line)

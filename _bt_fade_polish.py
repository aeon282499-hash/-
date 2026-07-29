# -*- coding: utf-8 -*-
"""_bt_fade_polish.py — デイトレフェードを磨く（2026-07-29・本番無変更）。

現行（2c6de79）: 前日+6%以上 × 出来高6倍未満 × 25MA乖離80%未満 × 貸借○ × 代金3億以上
                 → 乖離+ATRの順位平均で並べて**上位2本**を寄付成行で空売り → 引成買戻し
                 1玉50万・10年 年+59.1万・勝率55.4%・PF1.22・勝ち11/11年

本人の要望「勝率を上げつつ、さらに儲ける」。両立が要るので、勝率だけ上がって総額が減る案は不採用。

測る軸（現行の到達点から未検証のものだけ）:
  ① **日中OCO**（最有望・未検証）: 引成まで持たず、踏み上げ損切り/利確を日中に置く。
     _fade_deep.pkl に当日の高値h1・安値l1があるので判定できる。
     ※日足しか無いのでSTOP/TPの到達順は不明→**STOP優先（保守側）**を主とし、楽観側も併記
  ② 単変量フィルタの5分位（pos/gu/rng/body/gain2/nk_chg/tov/px/dow など全部）
  ③ 並び順の組み合わせ総当たり（現行=乖離+ATRの順位平均）
  ④ 本数×フィルタの相互作用（絞ったら本数を増やせるか）

採用条件: **両期間（2016-21 / 2022-26）で総額が改善**・近傍が高原・機構が説明できる。
        勝率だけ上がって総額が下がるものは採らない。
実行: python -X utf8 _bt_fade_polish.py
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
print(f"[base] 候補 {len(D0):,}件 ({D0.sig.min()}〜{D0.sig.max()})", flush=True)


def rank_mix(d: pd.DataFrame, cols, ascs=None):
    """複数列の順位平均でスコアを作る（小さいほど上位）。"""
    ascs = ascs or [False] * len(cols)
    r = None
    for c, a in zip(cols, ascs):
        x = d.groupby("sig")[c].rank(ascending=a, pct=True)
        r = x if r is None else r + x
    return r / len(cols)


def build(d: pd.DataFrame, cols=("dev", "atr"), ascs=None, n=NPICK):
    d = d.copy()
    d["mix"] = rank_mix(d, cols, ascs)
    d = d.sort_values(["sig", "mix"])
    d["rank"] = d.groupby("sig").cumcount() + 1
    d = d[d["rank"] <= n].copy()
    d["sh"] = (SIZE / d.o1 // 100 * 100).astype(int)
    return d[(d.sh > 0) & (d.px * 100 <= SIZE)]


def oco(d: pd.DataFrame, stop=None, tp=None, pessimistic=True):
    """日中OCO。stop=踏み上げ%(None=無し) / tp=利確%(None=無し)。空売りなので
    高値が上がると損、安値が下がると得。日足なので到達順は不明＝既定は保守(STOP優先)。"""
    o, h, l, c = d.o1.to_numpy(), d.h1.to_numpy(), d.l1.to_numpy(), d.c1.to_numpy()
    pnl = (o - c) / o * 100                                   # 既定＝引成
    hit_s = np.zeros(len(d), bool); hit_t = np.zeros(len(d), bool)
    if stop is not None:
        hit_s = h >= o * (1 + stop / 100)
    if tp is not None:
        hit_t = l <= o * (1 - tp / 100)
    s_val = -stop if stop is not None else 0.0
    t_val = tp if tp is not None else 0.0
    if pessimistic:                                            # 両方触ったらSTOP勝ち
        pnl = np.where(hit_t & ~hit_s, t_val, pnl)
        pnl = np.where(hit_s, s_val, pnl)
    else:                                                      # 両方触ったらTP勝ち
        pnl = np.where(hit_s & ~hit_t, s_val, pnl)
        pnl = np.where(hit_t, t_val, pnl)
    out = d.copy()
    out["pnl2"] = pnl
    out["yen2"] = pnl / 100 * out.sh * out.o1
    return out


def stat(d: pd.DataFrame, col="yen", pcol="pnl"):
    yr = d.groupby("y")[col].sum().reindex(YEARS, fill_value=0)
    m = d.groupby("ym")[col].sum()
    p = d[pcol]
    loss = -p[p < 0].sum()
    return dict(n=len(d), wr=(p > 0).mean() * 100, pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d[col].sum(), avg=d[col].sum() / 11, win=int((yr > 0).sum()),
                worst=yr.min(), wm=m.min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


B = build(D0)
BASE = stat(B)
print(f"[base] 現行(上位2本・引成): {BASE['n']}件 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"年{BASE['avg']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f}\n")

HDR = (f"  {'設定':<30}{'件数':>6}{'勝率':>8}{'PF':>7}{'年平均':>12}{'勝ち年':>7}"
       f"{'最悪年':>11}{'最悪月':>11}{'前半':>12}{'後半':>12}{'判定':>11}")


def row(lab, s):
    if s is None:
        print(f"  {lab:<30}  —"); return
    mk = ("両期間改善" if (s["a"] > BASE["a"] and s["b"] > BASE["b"])
          else ("片側" if s["tot"] > BASE["tot"] else ""))
    print(f"  {lab:<30}{s['n']:>6}{s['wr']:>7.1f}%{s['pf']:>7.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>5}/11{s['worst']:>+10,.0f}円{s['wm']:>+10,.0f}円"
          f"{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>13}")


print("=" * 130)
print("① 日中OCO ─ 踏み上げ損切りだけ置く（保守判定＝両方触ったらSTOP勝ち）")
print("=" * 130)
print(HDR)
row("現行（引成まで持つ）", BASE)
for s in (2, 3, 4, 5, 6, 8, 10, 12):
    row(f"損切り +{s}%", stat(oco(B, stop=s), "yen2", "pnl2"))

print("\n" + "=" * 130)
print("② 日中OCO ─ 利確だけ置く")
print("=" * 130)
print(HDR)
row("現行（引成まで持つ）", BASE)
for t in (2, 3, 4, 5, 6, 8, 10):
    row(f"利確 -{t}%", stat(oco(B, tp=t), "yen2", "pnl2"))

print("\n" + "=" * 130)
print("③ 日中OCO ─ 損切り×利確の面（保守判定）")
print("=" * 130)
print(f"  {'損切り＼利確':<14}" + "".join(f"{f'-{t}%':>13}" for t in (3, 4, 5, 6, 8)))
for s in (3, 4, 5, 6, 8, 10):
    cells = ""
    for t in (3, 4, 5, 6, 8):
        st = stat(oco(B, stop=s, tp=t), "yen2", "pnl2")
        cells += f"{st['avg']:>+12,.0f}"
    print(f"  +{s}%{'':10}{cells}")
print(f"\n  ※参考: 現行(引成) 年{BASE['avg']:+,.0f}円 / 勝率{BASE['wr']:.1f}%")
print(f"  {'損切り＼利確':<14}" + "".join(f"{f'-{t}%勝率':>13}" for t in (3, 4, 5, 6, 8)))
for s in (3, 4, 5, 6, 8, 10):
    cells = ""
    for t in (3, 4, 5, 6, 8):
        st = stat(oco(B, stop=s, tp=t), "yen2", "pnl2")
        cells += f"{st['wr']:>12.1f}%"
    print(f"  +{s}%{'':10}{cells}")

print("\n" + "=" * 130)
print("④ 単変量フィルタ（5分位・現行の並び順のまま上位2本）")
print("=" * 130)
COLS = [("pos", "前日終値のレンジ内位置(高値引けほど大)"), ("gu", "寄りギャップ%"),
        ("rng", "前日レンジ%"), ("body", "前日実体%"), ("gain2", "前々日騰落%"),
        ("prev_up", "前日までの連騰"), ("nk_chg", "日経の前日比%"), ("tov", "売買代金"),
        ("px", "株価"), ("gain", "前日騰落%"), ("atr", "ATR%"), ("dev", "25MA乖離%")]
for c, lab in COLS:
    d = D0[np.isfinite(D0[c])]
    if len(d) < 1000:
        continue
    q = pd.qcut(d[c], 5, duplicates="drop")
    print(f"\n  === {lab} ===")
    for iv, g in d.groupby(q, observed=True):
        gb = build(g)
        s = stat(gb)
        print(f"    {str(iv):<28}n={s['n']:>5} 勝率{s['wr']:>5.1f}% PF{s['pf']:>5.2f} "
              f"年{s['avg']:>+10,.0f}円 前半{s['a']:>+10,.0f} 後半{s['b']:>+10,.0f}")

print("\n" + "=" * 130)
print("⑤ 曜日")
print("=" * 130)
for dw, g in D0.groupby("dow"):
    nm = ["月", "火", "水", "木", "金"][int(dw)] if int(dw) < 5 else str(dw)
    s = stat(build(g))
    print(f"  {nm}曜  n={s['n']:>5} 勝率{s['wr']:>5.1f}% PF{s['pf']:>5.2f} 年{s['avg']:>+10,.0f}円 "
          f"前半{s['a']:>+10,.0f} 後半{s['b']:>+10,.0f}")

print("\n" + "=" * 130)
print("⑥ 地合い（日経が25MA以下か）")
print("=" * 130)
for v, g in D0.groupby("nk_below"):
    s = stat(build(g))
    print(f"  日経25MA{'以下' if v else '以上'}  n={s['n']:>5} 勝率{s['wr']:>5.1f}% PF{s['pf']:>5.2f} "
          f"年{s['avg']:>+10,.0f}円 前半{s['a']:>+10,.0f} 後半{s['b']:>+10,.0f}")

print("\n" + "=" * 130)
print("⑦ 並び順の組み合わせ（現行=乖離+ATR）")
print("=" * 130)
print(HDR)
row("現行 乖離+ATR", BASE)
for cols, ascs, lab in [
    (("dev",), None, "乖離だけ"), (("atr",), None, "ATRだけ"),
    (("gain",), None, "前日騰落だけ"), (("rng",), None, "前日レンジだけ"),
    (("dev", "atr", "gain"), None, "乖離+ATR+騰落"),
    (("dev", "atr", "rng"), None, "乖離+ATR+レンジ"),
    (("dev", "gain"), None, "乖離+騰落"), (("atr", "gain"), None, "ATR+騰落"),
    (("dev", "atr", "pos"), None, "乖離+ATR+終値位置"),
    (("dev", "atr", "tov"), [False, False, True], "乖離+ATR+代金(小さい順)"),
    (("dev", "atr", "vr"), [False, False, True], "乖離+ATR+出来高比(小さい順)"),
]:
    row(lab, stat(build(D0, cols=cols, ascs=ascs)))

print("\n" + "=" * 130)
print("⑧ 本数（フィルタ無し・現行の並び順）")
print("=" * 130)
print(HDR)
for n in (1, 2, 3, 4):
    row(f"上位{n}本", stat(build(D0, n=n)))

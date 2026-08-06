# -*- coding: utf-8 -*-
"""_bt_fade_jsf100.py — 「貸借だけ（売り禁なし）に絞ると？」新土台100万×1番で測る（2026-08-06）。

8/5の売り禁定量化（picksの30%・利益の62%・PF1.84）は旧土台50万×2本の測定。
本人の問い「貸借だけに絞るとどうなる？」＝制度信用で普通に建てられる玉だけにした場合を
新土台（100万×1番・GO+7%）で測る。実運用に合わせて2通り:
  シムA: 1番が売り禁の日は見送り（その日は撃たない）
  シムB: 売り禁を飛ばして最上位の非・売り禁に建てる（=8月ルール「1番がダメなら2番に100万」の
         "ハイカラ在庫がゼロ"最悪ケース）
現実は「現行(全部建つ)」と「シムB」の間（売り禁でもHYPER在庫があれば建つ・7月実測は建った）。
実行: python -X utf8 _bt_fade_jsf100.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE = 1_000_000
FLAGS = ["Restricted", "DailyPublication", "Monitoring", "RestrictedByJSF", "PrecautionByJSF"]

P = pd.read_pickle("_fade_pool_v5_100.pkl")
G = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
      & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
G["ym"] = G.ent.str[:7]

A = pd.read_pickle("_margin_alert_bal_10y.pkl")
A = A[A.Code.astype(str).str.len() == 5]
A = A[A.Code.astype(str).str[-1] == "0"]          # 優先株ガード
A["code4"] = A.Code.astype(str).str[:4]
A = A.sort_values(["code4", "PubDate"])

# as-of結合: PubDate<=sig の直近（7暦日以内）＝本番の朝が見られる最新公表と同じ
by_code = {c: g.reset_index(drop=True) for c, g in A.groupby("code4")}
flg = {c: np.zeros(len(G)) for c in FLAGS}
on_list = np.zeros(len(G), dtype=bool)
codes = G.ticker.str.replace(".T", "", regex=False)
for i, (cd, d_sig) in enumerate(zip(codes, G.sig)):
    g = by_code.get(cd)
    if g is None:
        continue
    pos = g.PubDate.searchsorted(d_sig, side="right") - 1
    if pos < 0:
        continue
    r = g.iloc[pos]
    if (pd.Timestamp(d_sig) - pd.Timestamp(r.PubDate)).days > 7:
        continue
    on_list[i] = True
    for c in FLAGS:
        v = r[c]
        flg[c][i] = float(v) if not isinstance(v, str) else 0.0
G["al_on"] = on_list
for c in FLAGS:
    G[f"al_{c}"] = flg[c]
G["jsf"] = G.al_RestrictedByJSF == 1
print(f"[join] GO候補{len(G):,}件 リスト載り{G.al_on.mean()*100:.0f}% 売り禁{G.jsf.mean()*100:.0f}%"
      f" (alert期間 {A.PubDate.min()}〜{A.PubDate.max()})")


def rank(d):
    d = d.copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    return d


def settle(d):
    d = d.copy()
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def summ(d, label):
    if len(d) == 0:
        print(f"  {label:<28} 0件"); return
    yy = d.groupby("y").yen.sum()
    ym = d.groupby("ym").yen.sum()
    print(f"  {label:<28}{len(d):>6}玉 勝率{(d.pnl>0).mean()*100:>5.1f}% PF{pf(d.pnl):>5.2f}"
          f" 10年{d.yen.sum():>+13,.0f}円 年平均{d.yen.sum()/11:>+10,.0f}円"
          f" 前半{d[d.y<=2021].yen.sum():>+11,.0f} 後半{d[d.y>=2022].yen.sum():>+11,.0f}"
          f" 勝ち{int((yy>0).sum()):>2}/{yy.index.nunique()} 最悪月{ym.min():>+9,.0f}円")


R = rank(G)
base = settle(R[R.rk == 1])

print("\n" + "=" * 130)
print("① 現行1番玉の層別（新土台100万×1番・情報）")
print("=" * 130)
summ(base, "現行(売り禁も建てる前提)")
summ(base[~base.al_on], "  ├ リスト外（規制なし）")
summ(base[base.al_on & ~base.jsf], "  ├ 載りだが売り禁でない")
summ(base[base.jsf], "  └ 売り禁(制度✕=要ハイカラ)")
summ(base[base.al_Restricted == 1], "  (参考)増担保")

print("\n" + "=" * 130)
print("② 「貸借だけ＝売り禁を撃たない」実運用シム")
print("=" * 130)
summ(base, "現行(全部建つ)")
summ(base[~base.jsf], "シムA: 売り禁の日は見送り")
nb = rank(G[~G.jsf])
summ(settle(nb[nb.rk == 1]), "シムB: 非売り禁へ繰り上げ")
nb2 = rank(G[~G.jsf & (G.al_Restricted != 1)])
summ(settle(nb2[nb2.rk == 1]), "(参考)増担保も避けて繰り上げ")

print("\n" + "=" * 130)
print("③ 1番が売り禁の日の割合（年別）＝8月に在庫確認が要る日の頻度")
print("=" * 130)
byy = base.groupby("y").agg(days=("sig", "nunique"), jsf=("jsf", "sum"))
for y, r in byy.iterrows():
    print(f"  {y}: {int(r.jsf):>3}/{int(r.days):>3}日 ({r.jsf/r.days*100:.0f}%)")

print("\n④ 繰り上げ玉の素顔（シムBで新たに1番になる玉）")
nb1 = settle(nb[nb.rk == 1])
key = set(zip(base.sig, base.ticker))
sub = nb1[[not ((s, t) in key) for s, t in zip(nb1.sig, nb1.ticker)]]
summ(sub, "繰り上がって入る玉")

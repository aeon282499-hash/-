# -*- coding: utf-8 -*-
"""_bt_fade_friends50.py — 友達版フェード(代金7.5億)を1玉50万にした場合の実測（2026-08-18）。

流儀A: 値がさカット50万連動(株価≤5,000円で選定前カット・sh=50万丸め) ＝「全部50万」の正装
流儀B: ライブそのまま(カットは本人CAPITAL_PER_TRADE=70万連動≤7,000円・sh=max(100株))
比較用: 現行友達版100万(カット1万円)
実行: python -X utf8 _bt_fade_friends50.py
"""
import numpy as np
import pandas as pd

YEARS = list(range(2016, 2027))
P = pd.read_pickle("_fade_pool_v5_100.pkl")
P["ym"] = P.ent.str[:7]

def build(tov_min, px_cap, size, min100=False):
    d = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
          & (P.tov >= tov_min) & (P.rng > 5.0) & (P.vol_avg >= 100_000)
          & (P.px * 100 <= px_cap * 100 if False else P.px <= px_cap)].copy()
    r = None
    for col in ("dev", "atr"):
        x = d.groupby("sig")[col].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    sh = (size / d.px // 100 * 100).astype(int)
    d["sh"] = np.maximum(100, sh) if min100 else sh
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d

def st(x, label):
    yr = x.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    mm = x.groupby("ym").yen.sum()
    p = x.pnl
    loss = -p[p < 0].sum()
    pf = p[p > 0].sum() / loss if loss > 0 else np.inf
    exp_max = (x.sh * x.o1).max() / 1e4
    print(f"\n=== {label} ===")
    print(f"  n={len(x)}玉 勝率{(p>0).mean()*100:.1f}% PF{pf:.2f} 平均{p.mean():+.2f}%/玉 "
          f"最大建玉{exp_max:.0f}万")
    print(f"  10年{x.yen.sum()/1e4:+,.0f}万 年平均{x.yen.sum()/11/1e4:+.1f}万 "
          f"勝ち年{int((yr>0).sum())}/11 最悪年{yr.min()/1e4:+.1f}万 "
          f"最悪月{mm.min()/1e4:+.1f}万 最悪1玉{x.yen.min()/1e4:+.1f}万")
    print("  年別: " + " ".join(f"{y}:{v/1e4:+.0f}" for y, v in yr.items()))

TOV = 7.5e8
a = build(TOV, 5_000, 500_000)
b = build(TOV, 7_000, 500_000, min100=True)
c = build(TOV, 10_000, 1_000_000)
st(c[c.rk == 1], "現行 友達100万(カット1万円) 1番のみ")
st(a[a.rk == 1], "流儀A 友達50万(カット5千円連動) 1番のみ")
st(b[b.rk == 1], "流儀B 友達50万(ライブ=カット7千円・max100株) 1番のみ")

# AとCで選定が変わった日数（カット差で1番が入れ替わる日）
sa = a[a.rk == 1][["sig", "ticker"]].set_index("sig").ticker
sc = c[c.rk == 1][["sig", "ticker"]].set_index("sig").ticker
common = sa.index.intersection(sc.index)
diff = (sa.loc[common] != sc.loc[common]).sum()
print(f"\n[選定差] A vs 現行100万: 共通GO日{len(common)}日中 1番が別銘柄={diff}日 "
      f"/ 片方のみGO日={len(set(sa.index) ^ set(sc.index))}日")

# -*- coding: utf-8 -*-
"""_bt_fade_rank2_stats.py — 順位別（1番/2番/3番）の単独成績を新土台(100万/玉・値がさ1万円)で層別。

_bt_fade_rebase100.py のベースライン選定と完全同一:
  前日+7% × 貸借○ × vr<6 × ATR5%以上 × 乖離12%以上 × 代金3億 × レンジ>5% × 乖離+ATR順
実行: python -X utf8 _bt_fade_rank2_stats.py
"""
import numpy as np
import pandas as pd

SIZE = 1_000_000
YEARS = list(range(2016, 2027))
P = pd.read_pickle("_fade_pool_v5_100.pkl")
P["ym"] = P.ent.str[:7]

def build(tov_min):
    d = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
          & (P.tov >= tov_min) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
    r = None
    for col in ("dev", "atr"):
        x = d.groupby("sig")[col].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d

def st(x, label, n_days):
    yr = x.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    mm = x.groupby("ym").yen.sum()
    p = x.pnl
    loss = -p[p < 0].sum()
    pf = p[p > 0].sum() / loss if loss > 0 else np.inf
    print(f"\n=== {label} ===")
    print(f"  n={len(x)}玉 発生日率={x.sig.nunique()/n_days*100:.0f}% "
          f"勝率{(p>0).mean()*100:.1f}% PF{pf:.2f} 平均{p.mean():+.2f}%/玉")
    print(f"  10年{x.yen.sum()/1e4:+,.0f}万 年平均{x.yen.sum()/11/1e4:+.1f}万 "
          f"勝ち年{int((yr>0).sum())}/11 最悪年{yr.min()/1e4:+.1f}万 "
          f"最悪月{mm.min()/1e4:+.1f}万 最悪1玉{x.yen.min()/1e4:+.1f}万")
    print(f"  前半16-21={yr[yr.index<=2021].sum()/1e4:+,.0f}万 "
          f"後半22-26={yr[yr.index>=2022].sum()/1e4:+,.0f}万")
    print("  年別: " + " ".join(f"{y}:{v/1e4:+.0f}" for y, v in yr.items()))

for name, tov in (("本人版(代金3億)", 3e8), ("友達版(代金7.5億)", 7.5e8)):
    d = build(tov)
    n_days = d.sig.nunique()
    print(f"\n──────── {name} ── GO日数={n_days} 総玉={len(d)} ────────")
    for rk in (1, 2, 3):
        st(d[d.rk == rk], f"{rk}番 単独", n_days)
    go2 = d[d.rk == 2].sig.nunique()
    print(f"\n[参考] 2番が存在する日={go2}/{n_days} ({go2/n_days*100:.0f}%)")

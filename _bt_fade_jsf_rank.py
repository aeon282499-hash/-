# -*- coding: utf-8 -*-
"""_bt_fade_jsf_rank.py — 日証金の当日速報を「絞り」でなく「並び順」に混ぜたら？（2026-09-03・_bt_fade_jsf_daily.py の続き）

_bt_fade_jsf_daily.py の所見: 融資残の当日増（信用買いの積み上がり）は五分位で単調・両期間プラス（Q5 PF2.30）。
だが絞りにすると玉数が半減して合計は落ちる（+500万→+322万）。→ 現行の並び順(乖離×ATRの百分位平均)に
第3項として混ぜ、1番100万＋2番50万（8/31〜の実運用）の合算で比較する。
併せて「制限措置あり優先」「貸株残減優先」も同じ土俵で測る。採用バー: 両期間改善×上位3除去でも残る×機構。
実行: python -X utf8 _bt_fade_jsf_rank.py > _log_fade_jsf_rank.txt
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

SIZE1, SIZE2 = 1_000_000, 500_000
P = pd.read_pickle("_fade_pool_v5_100.pkl")
G = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
      & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
G = G[G.sig >= "2023-04-05"].copy()
G["ym"] = G.ent.str[:7]
J: dict = pickle.load(open("_jsf_daily.pkl", "rb"))

rows = []
for r in G.itertuples():
    df = J.get(r.ticker.replace(".T", ""))
    d = pd.Timestamp(r.sig)
    out = dict(loan_chg_r=np.nan, loan_new_r=np.nan, lend_chg_r=np.nan, restricted=0.0, ratio=np.nan)
    if df is not None and len(df):
        sub = df[df.index <= d]
        if len(sub) and sub.index[-1] == d:
            row = sub.iloc[-1]; prev = sub.iloc[-2] if len(sub) >= 2 else None
            va = r.vol_avg if np.isfinite(r.vol_avg) and r.vol_avg > 0 else np.nan
            if prev is not None:
                out["loan_chg_r"] = (row.loan_bal - prev.loan_bal) / va
                out["lend_chg_r"] = (row.lend_bal - prev.lend_bal) / va
            out["loan_new_r"] = row.loan_new / va
            rest = row.get("restriction")
            out["restricted"] = 1.0 if isinstance(rest, str) and rest not in ("", "nan") else 0.0
            out["ratio"] = row.loan_bal / row.lend_bal if row.lend_bal and row.lend_bal > 0 else np.nan
    rows.append(out)
F = pd.DataFrame(rows, index=G.index)
G = pd.concat([G, F], axis=1)


def pct_desc(d, col):
    return d.groupby("sig")[col].rank(ascending=False, pct=True)


def rank_by(d, key_fn):
    d = d.copy()
    d["key"] = key_fn(d)
    d = d.sort_values(["sig", "key", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    return d


def settle(d, size):
    d = d.copy()
    d["sh"] = (size / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def evaluate(label, key_fn):
    R = rank_by(G, key_fn)
    b1 = settle(R[R.rk == 1], SIZE1); b2 = settle(R[R.rk == 2], SIZE2)
    both = pd.concat([b1, b2])
    yy = both.groupby("y").yen.sum(); ym = both.groupby("ym").yen.sum()
    a = both[both.y <= 2024].yen.sum(); b = both[both.y >= 2025].yen.sum()
    top3 = both.nlargest(3, "yen").yen.sum()
    print(f"  {label:<40} 1番{len(b1):>4}玉 PF{pf(b1.pnl):>5.2f} {b1.yen.sum():>+12,.0f} | 2番{len(b2):>4}玉 PF{pf(b2.pnl):>5.2f} {b2.yen.sum():>+11,.0f}"
          f" | 合算{both.yen.sum():>+12,.0f} 23-24{a:>+11,.0f} 25-26{b:>+11,.0f} 最悪月{ym.min():>+9,.0f} 上位3除去{both.yen.sum()-top3:>+12,.0f}"
          f" 年別{ {int(k): round(v/1e4) for k, v in yy.items()} }万")
    return both


cur = lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr")) / 2
print(f"[data] 候補{len(G)}件 {G.sig.min()}〜{G.sig.max()}  融資前日比あり {np.isfinite(G.loan_chg_r).mean()*100:.0f}%")
print("=" * 170)
print("並び順の比較（1番×100万＋2番×50万・2023-04〜2026-08）")
print("=" * 170)
evaluate("現行: 乖離×ATR", cur)
evaluate("現行 + 融資残前日比(3項平均)", lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr") + pct_desc(d, "loan_chg_r").fillna(0.5)) / 3)
evaluate("現行 + 融資新規(3項平均)", lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr") + pct_desc(d, "loan_new_r").fillna(0.5)) / 3)
evaluate("現行 + 融資残前日比(重み2)", lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr") + 2 * pct_desc(d, "loan_chg_r").fillna(0.5)) / 4)
evaluate("融資残前日比のみ", lambda d: pct_desc(d, "loan_chg_r").fillna(0.5))
evaluate("現行 + 貸株残前日比の逆(貸株減を優先)", lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr") + (1 - pct_desc(d, "lend_chg_r").fillna(0.5))) / 3)
evaluate("現行 + 制限措置あり優先(タイブレーク)", lambda d: cur(d) - 0.5 * d.restricted)
evaluate("制限措置あり優先 → 現行", lambda d: (1 - d.restricted) * 10 + cur(d))
evaluate("現行 + 融資前日比 + 制限措置優先", lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr") + pct_desc(d, "loan_chg_r").fillna(0.5)) / 3 - 0.5 * d.restricted)
print("\n[参考] 融資残前日比の閾値で1番を見送り→2番繰り上げ（絞り）")
for th in (-0.02, -0.01, 0.0):
    m = (G.loan_chg_r < th).fillna(False)
    Rn = rank_by(G[~m], cur); b1 = settle(Rn[Rn.rk == 1], SIZE1); b2 = settle(Rn[Rn.rk == 2], SIZE2); both = pd.concat([b1, b2])
    print(f"  融資残前日比÷出来高 < {th:+.2f} を除外: 合算{both.yen.sum():>+12,.0f} (1番{len(b1)}玉 PF{pf(b1.pnl):.2f}) 23-24{both[both.y<=2024].yen.sum():>+11,.0f} 25-26{both[both.y>=2025].yen.sum():>+11,.0f}")

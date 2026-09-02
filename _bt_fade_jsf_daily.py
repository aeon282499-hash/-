# -*- coding: utf-8 -*-
"""_bt_fade_jsf_daily.py — 売りフェードに「日証金の日次貸借残（当日速報）」軸を初計測（2026-09-03）。

背景: 立花API開通で日証金の日次データ（融資/貸株の新規・返済・残高、貸借倍率、逆日歩、制限措置）が
取れるようになった。今までは週次の信用残とJ-Quantsの規制フラグしか無かった。
過去分は taisyaku.jp の銘柄別CSV（2023-04-03〜）＝ _jsf_daily.pkl（_fetch_jsf_history.py）。
フェードの判断時点=シグナル日Dの18:50。日証金の速報は18:30過ぎに出るので **Dの当日値が使える**。

仮説（売り側）:
  H1 その日に貸株（＝空売り）が急増した玉は「踏み上げ燃料」＝翌日のフェードが効きにくい（毒）。
  H2 逆に融資（＝信用買い）が急増した玉は「高値掴みの投げ」が出やすい＝フェード順風。
  H3 貸借倍率が低い（売り長）玉は踏み上げ側＝毒。
  H4 品貸料率（逆日歩）が付いている玉＝株不足＝踏み上げリスク。日計りなのでコストはゼロだが方向性として。
  H5 制限措置（注意喚起/申込制限/停止）あり＝売り禁玉。既知の「売り禁玉PF1.84」の日次版。

測定: 現行1番玉(100万)・2番玉(50万)・候補全体の五分位バケット × 期間分割(2023-24 / 2025-26) ×
      除外/繰り上げシム × 上位3玉除去。採用バーは極みと同じ（両期間改善×高原×上位除去でも残る×機構）。
      ただし **期間は3年4ヶ月＝1相場** なので「採用」ではなく「貯めながら追う仮説」の格付けまで。
実行: python -X utf8 _bt_fade_jsf_daily.py > _log_fade_jsf_daily.txt
"""
from __future__ import annotations

import pickle
import sys

import numpy as np
import pandas as pd

SIZE1, SIZE2 = 1_000_000, 500_000
P = pd.read_pickle("_fade_pool_v5_100.pkl")
G = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
      & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
G = G[G.sig >= "2023-04-05"].copy()
G["ym"] = G.ent.str[:7]
J: dict = pickle.load(open("_jsf_daily.pkl", "rb"))


def rank(d):
    d = d.copy(); r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    return d


def settle(d, size):
    d = d.copy()
    d["sh"] = (size / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


# ── 日証金 as-of 結合（シグナル日Dの値と前日D-1） ────────────────────────────────
feat = {k: [] for k in ("has_jsf", "loan_bal", "lend_bal", "net_bal", "ratio", "loan_new", "lend_new", "loan_chg", "lend_chg",
                        "premium", "restricted", "bid_rank", "lend_new_r", "loan_new_r", "lend_chg_r", "loan_chg_r", "lend_bal_r")}
for r in G.itertuples():
    code = r.ticker.replace(".T", "")
    df = J.get(code)
    d = pd.Timestamp(r.sig)
    row = prev = None
    if df is not None and len(df):
        sub = df[df.index <= d]
        if len(sub) and sub.index[-1] == d:
            row = sub.iloc[-1]
            prev = sub.iloc[-2] if len(sub) >= 2 else None
    if row is None:
        for k in feat:
            feat[k].append(False if k == "has_jsf" else np.nan)
        continue
    lb, lo = row.get("lend_bal"), row.get("loan_bal")
    va = r.vol_avg if np.isfinite(r.vol_avg) and r.vol_avg > 0 else np.nan
    lend_chg = (lb - prev.get("lend_bal")) if prev is not None and np.isfinite(prev.get("lend_bal", np.nan)) else np.nan
    loan_chg = (lo - prev.get("loan_bal")) if prev is not None and np.isfinite(prev.get("loan_bal", np.nan)) else np.nan
    rest = str(row.get("restriction", "")) if isinstance(row.get("restriction"), str) else ""
    feat["has_jsf"].append(True)
    feat["loan_bal"].append(lo); feat["lend_bal"].append(lb); feat["net_bal"].append(row.get("net_bal"))
    feat["ratio"].append(lo / lb if lb and lb > 0 else np.nan)
    feat["loan_new"].append(row.get("loan_new")); feat["lend_new"].append(row.get("lend_new"))
    feat["loan_chg"].append(loan_chg); feat["lend_chg"].append(lend_chg)
    feat["premium"].append(row.get("premium"))
    feat["restricted"].append(1.0 if rest and rest not in ("nan", "") else 0.0)
    feat["bid_rank"].append(row.get("bid_rank") if isinstance(row.get("bid_rank"), str) else "")
    feat["lend_new_r"].append(row.get("lend_new") / va if va else np.nan)
    feat["loan_new_r"].append(row.get("loan_new") / va if va else np.nan)
    feat["lend_chg_r"].append(lend_chg / va if va else np.nan)
    feat["loan_chg_r"].append(loan_chg / va if va else np.nan)
    feat["lend_bal_r"].append(lb / va if va else np.nan)
for k, v in feat.items():
    G[k] = v

R = rank(G)
b1 = settle(R[R.rk == 1], SIZE1)
b2 = settle(R[R.rk == 2], SIZE2)
print(f"[join] 候補{len(G):,}件 / 日証金あり {G.has_jsf.mean()*100:.1f}% / 期間 {G.sig.min()}〜{G.sig.max()}")
print(f"       1番玉 {len(b1)} / 2番玉 {len(b2)}  逆日歩付き(1番) {(b1.premium>0).mean()*100:.0f}%  制限措置あり(1番) {b1.restricted.mean()*100:.0f}%")


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def era(d):
    return np.where(d.y <= 2024, "23-24", "25-26")


def summ(d, label, size_note=""):
    if len(d) == 0:
        print(f"  {label:<34} 0件"); return
    e = era(d)
    a, b = d[e == "23-24"], d[e == "25-26"]
    ym = d.groupby("ym").yen.sum()
    top3 = d.nlargest(3, "yen").yen.sum()
    print(f"  {label:<34}{len(d):>5}玉 勝率{(d.pnl>0).mean()*100:>5.1f}% PF{pf(d.pnl):>5.2f} 平均{d.pnl.mean():>+6.2f}%"
          f" 合計{d.yen.sum():>+12,.0f}円 23-24{a.yen.sum():>+11,.0f} 25-26{b.yen.sum():>+11,.0f}"
          f" 最悪月{ym.min():>+9,.0f} 上位3除去{d.yen.sum()-top3:>+12,.0f}{size_note}")


def buckets(d, col, label, nq=5):
    x = d[np.isfinite(d[col])]
    if len(x) < 25:
        print(f"  {label}: n不足({len(x)})"); return
    try:
        x = x.assign(q=pd.qcut(x[col].rank(method="first"), nq, labels=[f"Q{i+1}" for i in range(nq)]))
    except ValueError:
        return
    print(f"\n  ── {label}（{col}・Q1=小→Q5=大）")
    for q, g in x.groupby("q", observed=True):
        e = era(g); a, b = g[e == "23-24"], g[e == "25-26"]
        print(f"    {q} [{g[col].min():>10.3g},{g[col].max():>10.3g}] n={len(g):>4} 勝率{(g.pnl>0).mean()*100:>5.1f}% PF{pf(g.pnl):>5.2f}"
              f" 平均{g.pnl.mean():>+6.2f}% | 23-24 平均{a.pnl.mean() if len(a) else np.nan:>+6.2f}%(n{len(a)}) 25-26 平均{b.pnl.mean() if len(b) else np.nan:>+6.2f}%(n{len(b)})")


print("\n" + "=" * 140)
print("① ベースライン（新土台）  ※期間=2023-04〜・1相場分・採用判断には短い")
print("=" * 140)
summ(b1, "1番玉×100万（現行）")
summ(b2, "2番玉×50万（現行・8/31〜）")
summ(b1[b1.has_jsf], "  1番のうち日証金データあり")

print("\n" + "=" * 140)
print("② 層別（1番玉）: 日証金の当日速報でこの玉はどう見えていたか")
print("=" * 140)
summ(b1[b1.restricted == 1], "制限措置あり（注意喚起/制限/停止）")
summ(b1[b1.restricted == 0], "制限措置なし")
summ(b1[b1.premium > 0], "逆日歩あり（品貸料率>0）")
summ(b1[~(b1.premium > 0)], "逆日歩なし")
summ(b1[b1.lend_chg > 0], "当日 貸株残 増（空売り積み上がり）")
summ(b1[b1.lend_chg <= 0], "当日 貸株残 減/不変")
summ(b1[b1.loan_chg > 0], "当日 融資残 増（信用買い積み上がり）")
summ(b1[b1.loan_chg <= 0], "当日 融資残 減/不変")
summ(b1[b1.ratio < 1], "貸借倍率<1（売り長）")
summ(b1[b1.ratio >= 1], "貸借倍率≥1（買い長）")
for col, lab in (("ratio", "貸借倍率"), ("lend_new_r", "貸株新規÷平均出来高"), ("loan_new_r", "融資新規÷平均出来高"),
                 ("lend_chg_r", "貸株残前日比÷平均出来高"), ("loan_chg_r", "融資残前日比÷平均出来高"), ("lend_bal_r", "貸株残÷平均出来高"),
                 ("premium", "逆日歩(円)")):
    buckets(b1, col, f"1番玉 {lab}")

print("\n" + "=" * 140)
print("③ 候補全体（1番/2番に限らない）での層別＝nを稼いで方向を見る")
print("=" * 140)
ALL = settle(R, SIZE1)
summ(ALL, "候補全体×100万換算")
for col, lab in (("ratio", "貸借倍率"), ("lend_new_r", "貸株新規÷平均出来高"), ("loan_new_r", "融資新規÷平均出来高"),
                 ("lend_chg_r", "貸株残前日比÷平均出来高"), ("loan_chg_r", "融資残前日比÷平均出来高")):
    buckets(ALL, col, f"候補全体 {lab}")
summ(ALL[ALL.restricted == 1], "候補全体 制限措置あり")
summ(ALL[ALL.restricted == 0], "候補全体 制限措置なし")
summ(ALL[ALL.premium > 0], "候補全体 逆日歩あり")

print("\n" + "=" * 140)
print("④ 実運用シム: 1番が条件Xなら「見送り」/「2番へ繰り上げ」")
print("=" * 140)
rules = {
    "貸株残が当日増(空売り積み上がり)": lambda d: d.lend_chg > 0,
    "貸借倍率<0.5(強い売り長)": lambda d: d.ratio < 0.5,
    "貸借倍率<1(売り長)": lambda d: d.ratio < 1,
    "逆日歩あり": lambda d: d.premium > 0,
    "貸株新規÷出来高 上位20%": lambda d: d.lend_new_r >= d.lend_new_r.quantile(0.8),
    "融資残が当日減(投げ済み)": lambda d: d.loan_chg < 0,
}
summ(b1, "現行（全部撃つ）")
for name, fn in rules.items():
    m = fn(R).fillna(False)
    summ(b1[~fn(b1).fillna(False)], f"見送り: {name}")
    nb = rank(R[~m]); summ(settle(nb[nb.rk == 1], SIZE1), f"繰り上げ: {name}")
    print()

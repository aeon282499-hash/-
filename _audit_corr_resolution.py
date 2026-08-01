# -*- coding: utf-8 -*-
"""_audit_corr_resolution.py — 「3システムの相関はほぼゼロ」を自己監査する（2026-08-01）。

_bt_portfolio_10y.py で「買い×決算 -0.072 / 買い×売り -0.030 / 決算×売り +0.090 ＝ほぼ無相関」
「分散効果41%」「合算のリターン/DD比 8.32」と結論した。だがこれは **月次に丸めた** 相関で、
自分でも一番怪しいと思っている箇所。次の3つを潰す。

  ① 解像度: 日次・週次・月次で相関がどう変わるか
     月次に丸めると、日々の同時性がならされて相関が薄まる可能性がある。
  ② ゼロ埋めの影響: 決算持ち越しは「建玉ゼロの日」が65%ある。
     取引が無い日を0円として合算すると、相関は構造的に0へ引っ張られる。
     → 「両方が取引した日」だけで測り直す。
  ③ テールの相関: 平常時が無相関でも、悪い月だけ同時に沈むなら分散は効かない。
     → 下位10%の月・下位10%の日だけで相関を測る。
  ④ 分散効果41%の確からしさ: DDは1本の経路の極値統計。
     ブロック・ブートストラップで信頼区間を出す。

実行: python -X utf8 _audit_corr_resolution.py
"""
from __future__ import annotations

import json
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

import _bt_portfolio_10y as P   # SW/EA（月次）と各simを再利用
import _bt_sell_improve as S

TD = json.load(open("_trading_days_10y.json", encoding="utf-8"))
TDI = {d: i for i, d in enumerate(TD)}
SIZE = 1_000_000


def shares(p, s=SIZE):
    return max(100, int(s / p / 100) * 100)


# ── 日次の損益系列を作り直す（月次に丸める前の生データ）─────────────
def swing_daily(slots: int = 5) -> pd.Series:
    D = pd.read_csv("_bt10y_picks_10000.csv").sort_values("entry")
    busy, rows = [], []
    for d, g in D.groupby("entry", sort=True):
        i = TDI.get(d)
        if i is None:
            continue
        busy = [x for x in busy if x > i]
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            busy.append(i + 3)
            rows.append({"d": d, "yen": r.pnl / 100 * SIZE})
    return pd.DataFrame(rows).groupby("d").yen.sum()


def earnings_daily(slots: int = 8) -> pd.Series:
    R = pd.read_csv("_earnings_rsi_prod.csv")
    E = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
    E["vol"] = E.groupby("ticker")["gap"].transform(
        lambda s: s.abs().shift(1).expanding(min_periods=3).median())
    E = E.merge(R, on=["ticker", "d0"], how="inner")
    F = pd.read_pickle("_fins_history_nodiv.pkl")
    F["tk"] = [(str(c)[:4] if len(str(c)) == 5 and str(c).endswith("0") else str(c)) + ".T"
               for c in F.Code]
    Q = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)].copy()
    Q["d"] = Q.DiscDate.astype(str)

    def mins(s):
        try:
            return int(str(s)[:2]) * 60 + int(str(s)[3:5])
        except Exception:
            return np.nan

    Q["tmin"] = [mins(x) for x in Q.DiscTime]
    Q["cl"] = np.where(Q.d >= "2024-11-05", 930, 900)
    Q["after"] = Q.tmin >= Q.cl
    Q = Q.dropna(subset=["tmin"]).sort_values(["tk", "d"])
    Q["pa"] = Q.groupby("tk")["after"].shift(1)
    kn = set(zip(Q.tk, Q.d))
    pv = {(t, d): a for t, d, a in zip(Q.tk, Q.d, Q.pa)}
    E["isq"] = [(t, d) in kn for t, d in zip(E.ticker, E.d0)]
    E["pa"] = [pv.get((t, d)) for t, d in zip(E.ticker, E.d0)]
    m = ((E.rsi_prod <= 55) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8) & (E.price <= 10000)
         & E.isq & (E.vol.isna() | (E.vol >= 2.0)) & (E.pa.isna() | (E.pa == True)))  # noqa
    A = E[m].sort_values(["d0", "rsi_prod"])
    cd = sorted(A.d0.unique())
    ci = {d: i for i, d in enumerate(cd)}
    busy, held, rows = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = ci[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            pnl, span = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            busy.append(i + span)
            held[r.ticker] = i + span
            rows.append({"d": r.d0, "yen": pnl / 100 * shares(r.price) * r.price})
    return pd.DataFrame(rows).groupby("d").yen.sum()


def sell_daily() -> pd.Series:
    A = S.D[S.base_mask(S.D)].sort_values(["sig", "score"], ascending=[True, False])
    days = sorted(A.sig.unique())
    di = {d: i for i, d in enumerate(days)}
    live, held, rows = [], {}, []
    for d, g in A.groupby("sig", sort=True):
        i = di[d]
        live = [x for x in live if x >= i]
        held = {t: x for t, x in held.items() if x >= i}
        used, nn = {}, 0
        for r in g.to_dict("records"):
            if nn >= S.MAX_PER_DAY or len(live) >= S.SLOTS:
                break
            if r["ticker"] in held:
                continue
            if r["sector"] and used.get(r["sector"], 0) >= S.SECTOR_CAP:
                continue
            p, k = S.replay(r, 3.0, 5.0, 3)
            sh = int(SIZE / r["entry"] / 100) * 100
            if sh <= 0:
                continue
            if r["sector"]:
                used[r["sector"]] = used.get(r["sector"], 0) + 1
            live.append(i + k)
            held[r["ticker"]] = i + k
            nn += 1
            rows.append({"d": r["sig"], "yen": p / 100 * sh * r["entry"]})
    return pd.DataFrame(rows).groupby("d").yen.sum()


SWd, EAd, SEd = swing_daily(), earnings_daily(), sell_daily()
allday = [d for d in TD if d >= "2016-08-01"]
SWd, EAd, SEd = [s.reindex(allday, fill_value=0.0) for s in (SWd, EAd, SEd)]
print(f"[data] 営業日{len(allday):,}日 / 取引のあった日: "
      f"買い{int((SWd!=0).sum())} 決算{int((EAd!=0).sum())} 売り{int((SEd!=0).sum())}")


def agg(s: pd.Series, rule: str) -> pd.Series:
    idx = pd.to_datetime(s.index)
    return s.set_axis(idx).resample(rule).sum()


print("\n" + "=" * 100)
print("① 解像度を変えると相関は変わるか（月次に丸めたことで薄まっていないか）")
print("=" * 100)
print(f"  {'解像度':<10}{'n':>7}{'買い×決算':>12}{'買い×売り':>12}{'決算×売り':>12}")
for rule, tag in (("D", "日次"), ("W", "週次"), ("ME", "月次"), ("QE", "四半期")):
    a, b, c = agg(SWd, rule), agg(EAd, rule), agg(SEd, rule)
    print(f"  {tag:<10}{len(a):>7}{a.corr(b):>+12.3f}{a.corr(c):>+12.3f}{b.corr(c):>+12.3f}")

print("\n" + "=" * 100)
print("② ゼロ埋めの影響（両方が取引した日だけで測り直す）")
print("=" * 100)
print(f"  {'ペア':<14}{'全日':>10}{'両方取引':>10}{'n(両方)':>9}   判定")
for (x, y, tag) in ((SWd, EAd, "買い×決算"), (SWd, SEd, "買い×売り"), (EAd, SEd, "決算×売り")):
    both = (x != 0) & (y != 0)
    r_all, r_both = x.corr(y), x[both].corr(y[both])
    v = "変わらない" if abs(r_both - r_all) < 0.10 else "★ズレる"
    print(f"  {tag:<14}{r_all:>+10.3f}{r_both:>+10.3f}{int(both.sum()):>9}   {v}")

print("\n" + "=" * 100)
print("③ テールの相関（悪い時だけ同時に沈んでいないか）")
print("=" * 100)
for rule, tag in (("D", "日次"), ("ME", "月次")):
    a, b, c = agg(SWd, rule), agg(EAd, rule), agg(SEd, rule)
    tot = a + b + c
    q = tot.quantile(0.10)
    bad = tot <= q
    print(f"\n  [{tag}] 合算の下位10%（n={int(bad.sum())}）")
    print(f"    {'ペア':<14}{'全期間':>10}{'下位10%':>10}   判定")
    for (x, y, t2) in ((a, b, "買い×決算"), (a, c, "買い×売り"), (b, c, "決算×売り")):
        r1, r2 = x.corr(y), x[bad].corr(y[bad])
        v = "★悪化" if (r2 - r1) > 0.15 else "問題なし"
        print(f"    {t2:<14}{r1:>+10.3f}{r2:>+10.3f}   {v}")

print("\n" + "=" * 100)
print("④ 分散効果41%はどのくらい確からしいか（ブロック・ブートストラップ）")
print("=" * 100)


def maxdd(s: pd.Series) -> float:
    c = s.cumsum()
    return float((c - c.cummax()).min())


M = pd.concat([agg(SWd, "ME"), agg(EAd, "ME"), agg(SEd, "ME")], axis=1)
M.columns = ["sw", "ea", "se"]
obs_dd = {k: maxdd(M[k]) for k in M.columns}
obs_all = maxdd(M.sum(axis=1))
obs_ben = 1 - abs(obs_all) / abs(sum(obs_dd.values()))
print(f"  実測: 単体DD合計 {sum(obs_dd.values())/1e4:,.0f}万 / 合算DD {obs_all/1e4:,.0f}万"
      f" → 分散効果 {obs_ben*100:.0f}%")

rng = np.random.default_rng(42)
BLOCK = 6          # 6ヶ月ブロック＝レジームの持続をある程度残す
bens, dds = [], []
n = len(M)
for _ in range(2000):
    idx = []
    while len(idx) < n:
        s0 = rng.integers(0, n - BLOCK)
        idx += list(range(s0, s0 + BLOCK))
    idx = idx[:n]
    B = M.iloc[idx].reset_index(drop=True)
    d_each = sum(maxdd(B[k]) for k in B.columns)
    d_all = maxdd(B.sum(axis=1))
    if d_each == 0:
        continue
    bens.append(1 - abs(d_all) / abs(d_each))
    dds.append(d_all)
bens = np.array(bens)
print(f"  ブートストラップ2,000回（6ヶ月ブロック）: 分散効果の中央値 {np.median(bens)*100:.0f}%"
      f" / 90%区間 {np.percentile(bens,5)*100:.0f}〜{np.percentile(bens,95)*100:.0f}%")
print(f"  合算DDの90%区間 {np.percentile(dds,5)/1e4:,.0f}万 〜 {np.percentile(dds,95)/1e4:,.0f}万"
      f"（実測 {obs_all/1e4:,.0f}万）")
print(f"  分散効果が20%を下回る確率 {np.mean(bens < 0.20)*100:.0f}%"
      f" / ゼロ以下になる確率 {np.mean(bens <= 0)*100:.0f}%")

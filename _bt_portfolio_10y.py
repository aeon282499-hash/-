# -*- coding: utf-8 -*-
"""_bt_portfolio_10y.py — スイング × 決算持ち越し の合算リスク（2026-08-01・本番無変更）。

本人「まだまだ改善の余地ありだね／もう少しリスク管理しながら資産増やせない」。

単体システムのツマミは出尽くした（決算は12軸＋ファンダ＋ヘッジ＋枠＋順位モデルが全滅、
残ったのは決算ボラゲート1本）。まだ一度も測っていないのは **システム間の関係** ＝
相関・合算DD・資金配分。ここが «リスク管理しながら増やす» の本体になる。

  ・2つのシステムは同じ «売られすぎ» DNAなので、同時に沈む可能性がある
    （記憶にも「スイングと2018-19は同時に沈む＝2系統の分散は限定的」とある）
  ・でも定量化していない。相関が低ければ、同じ口座で両方回すほうが
    DDあたりのリターンが上がる＝サイズを上げられる。

データ:
  スイング大 … _bt10y_picks_10000.csv（本番同一の日次選定済み・価格帯1万以下）
  決算持ち越し … _earnings_events_rich2.csv + 本番の全ゲート（実像ベース）

実行: python -X utf8 _bt_portfolio_10y.py
"""
from __future__ import annotations

import json
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

TD = json.load(open("_trading_days_10y.json", encoding="utf-8"))
TDI = {d: i for i, d in enumerate(TD)}
SIZE = 1_000_000


def shares(p: float, size: int = SIZE) -> int:
    return max(100, int(size / p / 100) * 100)


# ── スイング大（5枠・MAX_HOLD=3営業日・1玉100万）─────────────────
def swing_monthly(slots: int = 5) -> pd.Series:
    P = pd.read_csv("_bt10y_picks_10000.csv").sort_values("entry")
    busy, rows = [], []
    for d, g in P.groupby("entry", sort=True):
        i = TDI.get(d)
        if i is None:
            continue
        busy = [x for x in busy if x > i]
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            busy.append(i + 3)                 # MAX_HOLD=3営業日ぶん枠を占有
            rows.append({"d": d, "yen": r.pnl / 100 * SIZE})
    S = pd.DataFrame(rows)
    print(f"[swing] {len(S):,}件 / {S.d.min()}〜{S.d.max()} / 合計{S.yen.sum()/1e4:+,.0f}万")
    return S.groupby(S.d.str[:7]).yen.sum()


# ── 決算持ち越し（本番の全ゲート込み＝実像ベース）───────────────────
def earnings_monthly(slots: int = 8) -> pd.Series:
    R = pd.read_csv("_earnings_rsi_prod.csv")
    E = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
    E["vol"] = E.groupby("ticker")["gap"].transform(
        lambda s: s.abs().shift(1).expanding(min_periods=3).median())
    E = E.merge(R, on=["ticker", "d0"], how="inner")

    F = pd.read_pickle("_fins_history.pkl")
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
    Q["close"] = np.where(Q.d >= "2024-11-05", 15 * 60 + 30, 15 * 60)
    Q["after"] = Q.tmin >= Q["close"]
    Q = Q.dropna(subset=["tmin"]).sort_values(["tk", "d"])
    Q["pafter"] = Q.groupby("tk")["after"].shift(1)
    known = set(zip(Q.tk, Q.d))
    prev = {(t, d): a for t, d, a in zip(Q.tk, Q.d, Q.pafter)}

    E["isq"] = [(t, d) in known for t, d in zip(E.ticker, E.d0)]
    E["pafter"] = [prev.get((t, d)) for t, d in zip(E.ticker, E.d0)]

    m = ((E.rsi_prod <= 55) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8) & (E.price <= 10000)
         & E.isq & (E.vol.isna() | (E.vol >= 2.0))
         & (E.pafter.isna() | (E.pafter == True)))          # noqa: E712
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
    S = pd.DataFrame(rows)
    print(f"[earnings] {len(S):,}件 / {S.d.min()}〜{S.d.max()} / 合計{S.yen.sum()/1e4:+,.0f}万")
    return S.groupby(S.d.str[:7]).yen.sum()


SW = swing_monthly()
EA = earnings_monthly()
idx = sorted(set(SW.index) | set(EA.index))
SW, EA = SW.reindex(idx, fill_value=0), EA.reindex(idx, fill_value=0)


def stats(s: pd.Series, cap: float, tag: str):
    c = s.cumsum()
    dd = float((c - c.cummax()).min())
    run = mx = 0
    for v in s:
        run = run + 1 if v < 0 else 0
        mx = max(mx, run)
    yrs = len(s) / 12
    print(f"  {tag:<30}{s.sum()/1e4:>+8,.0f}万 年{s.sum()/1e4/yrs:>+6.0f}万 "
          f"DD{dd/1e4:>+7,.0f}万 プラス月{int((s>0).sum())}/{len(s)} "
          f"最悪月{s.min()/1e4:>+5.0f}万 連敗{mx}ヶ月 リターン/DD {s.sum()/abs(dd):>5.2f}")
    return dd


print("\n" + "=" * 112)
print("① 単体 vs 合算（1玉100万・スイング5枠・決算8枠）")
print("=" * 112)
d1 = stats(SW, 5e6, "スイング大のみ")
d2 = stats(EA, 8e6, "決算持ち越しのみ")
d3 = stats(SW + EA, 13e6, "合算（両方フルサイズ）")
print(f"\n  単純合計のDD {(d1+d2)/1e4:+,.0f}万 に対し 実際の合算DD {d3/1e4:+,.0f}万"
      f" → 分散効果 {(1-abs(d3)/abs(d1+d2))*100:.0f}%")

print("\n② 相関（月次）")
r = SW.corr(EA)
both_neg = ((SW < 0) & (EA < 0)).mean() * 100
print(f"  月次リターンの相関 {r:+.3f}")
print(f"  両方マイナスの月 {both_neg:.0f}% （無相関なら約{((SW<0).mean()*(EA<0).mean())*100:.0f}%）")
for y0, y1, tag in ((2017, 2021, "2017-21"), (2022, 2026, "2022-26")):
    m = [i for i in idx if y0 <= int(i[:4]) <= y1]
    print(f"    {tag}: 相関{SW[m].corr(EA[m]):+.3f} / "
          f"スイング{SW[m].sum()/1e4:+,.0f}万 決算{EA[m].sum()/1e4:+,.0f}万")

print("\n③ 年別（万円）")
ys = sorted({i[:4] for i in idx})
print(f"  {'':<10}" + "".join(f"{y:>8}" for y in ys))
for s, tag in ((SW, "スイング"), (EA, "決算"), (SW + EA, "合算")):
    print(f"  {tag:<10}" + "".join(f"{s[[i for i in idx if i[:4]==y]].sum()/1e4:>+8.0f}" for y in ys))

print("\n" + "=" * 112)
print("④ 口座800万でDDを-30%(-240万)以内に収める配分の総当たり")
print("=" * 112)
print(f"  {'スイング':>8}{'決算':>8}{'10年計':>12}{'年利':>9}{'最大DD':>11}{'口座比':>9}{'判定':>8}")
best = None
for ws in np.arange(0, 1.01, 0.1):
    for we in np.arange(0, 1.01, 0.1):
        if ws + we == 0:
            continue
        s = SW * ws + EA * we
        c = s.cumsum()
        dd = float((c - c.cummax()).min())
        ok = abs(dd) <= 2_400_000
        row = (ws, we, s.sum(), dd, ok)
        if ok and (best is None or s.sum() > best[2]):
            best = row
for ws, we in [(1.0, 1.0), (1.0, 0.5), (0.5, 1.0), (1.0, 0.0), (0.0, 1.0)] + \
              ([(best[0], best[1])] if best else []):
    s = SW * ws + EA * we
    c = s.cumsum()
    dd = float((c - c.cummax()).min())
    tag = "★最良" if best and (ws, we) == (best[0], best[1]) else ("OK" if abs(dd) <= 2_400_000 else "線超え")
    print(f"  {ws*100:>7.0f}%{we*100:>7.0f}%{s.sum()/1e4:>11,.0f}万"
          f"{s.sum()/1e4/(len(s)/12):>8,.0f}万{dd/1e4:>10,.0f}万{dd/8e6*100:>8.1f}%{tag:>8}")
print("\n  ※%は「1玉100万に対する倍率」。50%なら1玉50万で回すという意味。")

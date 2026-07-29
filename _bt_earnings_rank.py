# -*- coding: utf-8 -*-
"""_bt_earnings_rank.py — 決算持ち越しの「並び順」と「絞り込み」を隅々まで探す（2026-07-29）。

本人の問い: 今日みたいに5銘柄来ると困る。もっと儲かる順に並べられないか。
            信用買残・セクター・直近の同業種の決算の流れ…で勝率70%は作れないか。

前提（2026-07-26の9軸フル検証で既に棄却済み。同じことは繰り返さない）:
  ✗ 業種cap / 買残フィルタ / 入口ボラ正規化 / PEAD閾値ボラ正規化 / PEAD日数
  ✗ 下側PEAD / 選定順序 / 枠数 / 市場決算反応ゲート
2026-07-29に追加で判明: RSIの絶対水準<28は平均マイナス。ただしRSI下限は後半が悪化＝片側で不採用。

ここで新しく測るもの:
  ① 単変量の5分位 vs 翌寄りギャップ（days_cover / ratio / atr_pct / price / tov20 / runup5 / rsi）
  ② 33業種ごとの期待値（サンプル数つき）
  ③ **同業種の直近決算モメンタム**（新規）: 同じ33業種で直近N営業日に決算を出した銘柄の
     平均ギャップ。これは9軸に含まれていない。前日までに確定した情報だけを使う（d0'<=d0-1）
  ④ 市場全体の直近決算モメンタム（業種版と対で見る）
  ⑤ 発表時刻（earnings_times.json・引け後/場中）
  ⑥ 枠数を絞る（1〜8）を最新データで再測定
  ⑦ 上で有望だったものを合成スコアにして並べ替え → 勝率と円

採用条件: 両期間（2016-21 / 2022-26）で改善・近傍が高原・機構の説明がつく。
実行: python -X utf8 _bt_earnings_rank.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

YEARS = list(range(2016, 2027))
SIZE, SLOTS = 1_000_000, 8
E = pd.read_csv("_earnings_events_rich2.csv")
E["d0"] = E["d0"].astype(str)

# 本番条件（ルールA）
LIVE = ((E.rsi <= 45) & (E.runup5 < -3) & (E.tov20 >= 7.5e8)
        & (E.price <= 10_000) & np.isfinite(E.gap))
A = E[LIVE].copy()
print(f"[base] 全決算イベント {len(E):,} / 本番条件通過 {len(A):,} "
      f"({A.d0.min()}〜{A.d0.max()})", flush=True)


def pf(x):
    l = -x[x < 0].sum()
    return x[x > 0].sum() / l if l > 0 else np.inf


def sim(df: pd.DataFrame, order_col="rsi", asc=True, slots=SLOTS, size=SIZE):
    """本番と同じ枠シム（1日slots枠・PEAD延長つき・実株数）。"""
    d = df[np.isfinite(df.gap)].sort_values(["d0", order_col], ascending=[True, asc])
    days = sorted(d.d0.unique()); di = {x: i for i, x in enumerate(days)}
    busy, held, out = [], {}, []
    for day, g in d.groupby("d0", sort=True):
        i = di[day]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if r.ticker in held:
                continue
            sh = int(size / r.price / 100) * 100
            if sh <= 0:
                continue
            p, sp = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            if not np.isfinite(p):
                continue
            busy.append(i + sp); held[r.ticker] = i + sp
            out.append({"y": r.year, "pnl": p, "yen": p / 100 * sh * r.price})
    if not out:
        return None
    B = pd.DataFrame(out)
    yr = B.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    return dict(n=len(B), tot=B.yen.sum(), win=int((yr > 0).sum()), worst=yr.min(),
                pf=pf(B.pnl), wr=(B.pnl > 0).mean() * 100, mean=B.pnl.mean(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()), yr=yr)


BASE = sim(A)
print(f"[base] 現行: {BASE['n']}件 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"10年{BASE['tot']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f}\n",
      flush=True)


def quintiles(col, label, df=None):
    d = (A if df is None else df)
    d = d[np.isfinite(d[col])]
    if len(d) < 500:
        print(f"  {label}: n不足({len(d)})")
        return
    q = pd.qcut(d[col], 5, duplicates="drop")
    print(f"\n  === {label} ===")
    print(f"    {'帯':<26}{'n':>6}{'平均gap':>10}{'勝率':>8}{'>+8%':>7}{'<-5%':>7}"
          f"{'前半平均':>10}{'後半平均':>10}")
    for iv, g in d.groupby(q, observed=True):
        ga = g[g.year <= 2021].gap; gb = g[g.year >= 2022].gap
        print(f"    {str(iv):<26}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
              f"{(g.gap>8).mean()*100:>6.1f}%{(g.gap<-5).mean()*100:>6.1f}%"
              f"{ga.mean():>+9.2f}%{gb.mean():>+9.2f}%")


print("=" * 104)
print("① 単変量の5分位 vs 翌寄りギャップ")
print("=" * 104)
for col, lab in (("days_cover", "信用買残の回転日数（買残÷20日代金）"),
                 ("ratio", "信用倍率（買残÷売残）"),
                 ("atr_pct", "ATR%（ボラ）"),
                 ("price", "株価"),
                 ("tov20", "売買代金20日中央値"),
                 ("runup5", "5日騰落（どれだけ売られたか）"),
                 ("rsi", "RSI")):
    quintiles(col, lab)

print("\n" + "=" * 104)
print("② 33業種ごとの期待値（n≥30のみ）")
print("=" * 104)
sec = A[A.sector.notna()].groupby("sector").agg(
    n=("gap", "size"), mean=("gap", "mean"), wr=("gap", lambda x: (x > 0).mean() * 100))
sec = sec[sec.n >= 30].sort_values("mean", ascending=False)
print(f"  {'業種':<20}{'n':>6}{'平均gap':>10}{'勝率':>8}")
for s, r in sec.iterrows():
    print(f"  {s:<20}{int(r.n):>6}{r['mean']:>+9.2f}%{r.wr:>7.1f}%")

print("\n" + "=" * 104)
print("③ 同業種の直近決算モメンタム（新規軸・前日までに確定した情報のみ）")
print("=" * 104)
# 全決算イベント（本番条件に限らない）から、業種ごとの直近ギャップ平均を作る
ALL = E[np.isfinite(E.gap) & E.sector.notna()][["d0", "sector", "gap"]].copy()
ALL = ALL.sort_values("d0")
days_all = sorted(ALL.d0.unique()); dmap = {d: i for i, d in enumerate(days_all)}
ALL["di"] = ALL.d0.map(dmap)
A2 = A[A.sector.notna()].copy()
A2["di"] = A2.d0.map(dmap)
A2 = A2[A2.di.notna()]

for win in (5, 10, 20, 40):
    vals, cnts = [], []
    for r in A2.itertuples():
        m = ALL[(ALL.sector == r.sector) & (ALL.di < r.di) & (ALL.di >= r.di - win)]
        vals.append(m.gap.mean() if len(m) >= 2 else np.nan)
        cnts.append(len(m))
    A2[f"secmom{win}"] = vals
    ok = A2[np.isfinite(A2[f"secmom{win}"])]
    print(f"\n  --- 直近{win}営業日の同業種決算ギャップ平均（測れた {len(ok)}/{len(A2)}件）---")
    if len(ok) < 500:
        print("    n不足")
        continue
    q = pd.qcut(ok[f"secmom{win}"], 5, duplicates="drop")
    print(f"    {'帯':<24}{'n':>6}{'平均gap':>10}{'勝率':>8}{'前半':>9}{'後半':>9}")
    for iv, g in ok.groupby(q, observed=True):
        print(f"    {str(iv):<24}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
              f"{g[g.year<=2021].gap.mean():>+8.2f}%{g[g.year>=2022].gap.mean():>+8.2f}%")

print("\n" + "=" * 104)
print("④ 市場全体の直近決算モメンタム")
print("=" * 104)
MK = E[np.isfinite(E.gap)][["d0", "gap"]].copy().sort_values("d0")
MK["di"] = MK.d0.map(dmap)
MK = MK[MK.di.notna()]
mk_by_di = MK.groupby("di").gap.agg(["mean", "size"])
for win in (3, 5, 10, 20):
    roll = mk_by_di["mean"].rolling(win, min_periods=2).mean().shift(1)
    A2[f"mkt{win}"] = A2.di.map(roll)
    ok = A2[np.isfinite(A2[f"mkt{win}"])]
    if len(ok) < 500:
        continue
    q = pd.qcut(ok[f"mkt{win}"], 5, duplicates="drop")
    print(f"\n  --- 直近{win}営業日の全決算ギャップ平均（{len(ok)}件）---")
    print(f"    {'帯':<24}{'n':>6}{'平均gap':>10}{'勝率':>8}{'前半':>9}{'後半':>9}")
    for iv, g in ok.groupby(q, observed=True):
        print(f"    {str(iv):<24}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
              f"{g[g.year<=2021].gap.mean():>+8.2f}%{g[g.year>=2022].gap.mean():>+8.2f}%")

print("\n" + "=" * 104)
print("⑤ 発表時刻（引け後 / 場中）")
print("=" * 104)
p = Path("earnings_times.json")
if p.exists():
    try:
        raw = json.load(open(p, encoding="utf-8"))
        recs = raw.get("records", raw) if isinstance(raw, dict) else raw
        tmap = {}
        if isinstance(recs, dict):
            for k, v in recs.items():
                tmap[k] = v if isinstance(v, str) else (v.get("time") if isinstance(v, dict) else None)
        A["t"] = A.apply(lambda r: tmap.get(f"{r.ticker}_{r.d0}") or tmap.get(r.ticker), axis=1)
        got = A[A.t.notna()]
        print(f"  時刻が取れた: {len(got)}/{len(A)}件")
        if len(got) >= 300:
            def bucket(s):
                try:
                    hh = int(str(s)[:2])
                except Exception:
                    return "不明"
                return "引け後(15時以降)" if hh >= 15 else ("場中" if hh >= 9 else "寄り前")
            got = got.assign(b=got.t.map(bucket))
            for b, g in got.groupby("b"):
                print(f"  {b:<16}{len(g):>6}件 平均{g.gap.mean():>+6.2f}% 勝率{(g.gap>0).mean()*100:>5.1f}%")
        else:
            print("  n不足 → 判定不能")
    except Exception as e:
        print(f"  読込失敗: {e}")
else:
    print("  earnings_times.json なし")

print("\n" + "=" * 104)
print("⑥ 枠数（1〜8）を最新データで再測定")
print("=" * 104)
print(f"  {'枠':<6}{'件数':>7}{'勝率':>8}{'PF':>7}{'10年計':>13}{'勝ち年':>8}{'前半':>13}{'後半':>13}")
for s in range(1, 9):
    r = sim(A, slots=s)
    print(f"  {s}枠{'':3}{r['n']:>7}{r['wr']:>7.1f}%{r['pf']:>7.2f}{r['tot']:>+12,.0f}円"
          f"{r['win']:>6}/11{r['a']:>+12,.0f}円{r['b']:>+12,.0f}円")

print("\n" + "=" * 104)
print("⑦ 並び順の総当たり（単変量・現行=RSI昇順）")
print("=" * 104)
print(f"  {'並び順':<26}{'件数':>7}{'勝率':>8}{'PF':>7}{'10年計':>13}{'勝ち年':>8}{'前半':>13}{'後半':>13}{'判定':>10}")
for col, asc, lab in (("rsi", True, "RSI昇順（現行）"), ("rsi", False, "RSI降順"),
                      ("runup5", True, "5日騰落 昇順（深い順）"), ("runup5", False, "5日騰落 降順（浅い順）"),
                      ("days_cover", True, "買残回転 昇順（軽い順）"), ("days_cover", False, "買残回転 降順"),
                      ("atr_pct", True, "ATR%昇順"), ("atr_pct", False, "ATR%降順"),
                      ("tov20", False, "売買代金 降順"), ("tov20", True, "売買代金 昇順"),
                      ("price", True, "株価 昇順"), ("price", False, "株価 降順"),
                      ("ratio", True, "信用倍率 昇順"), ("ratio", False, "信用倍率 降順")):
    r = sim(A, order_col=col, asc=asc)
    if r is None:
        continue
    mk = "両期間改善" if (r["a"] > BASE["a"] and r["b"] > BASE["b"]) else ("片側" if r["tot"] > BASE["tot"] else "")
    print(f"  {lab:<26}{r['n']:>7}{r['wr']:>7.1f}%{r['pf']:>7.2f}{r['tot']:>+12,.0f}円"
          f"{r['win']:>6}/11{r['a']:>+12,.0f}円{r['b']:>+12,.0f}円{mk:>12}")

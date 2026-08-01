# -*- coding: utf-8 -*-
"""_bt_earnings_intraday_v2.py — 場中除外ルールの改修案を実測（2026-08-01）。

背景: 本人「場中決算は一切買ってない。15:30以降の発表銘柄しか買ってない」。
現行実装(1eb2c62)は「前回の発表時刻が場中なら弾く」だが、これは
第一工業製薬(前回14:00→今回16:00・gap+16.65%の爆益玉)を弾いてしまう。

本人の実手順は「買う直前に、もう発表済みかを見る」。これを機械化する:
  新ルール(combo) =
    ①当日14:55までに発表済み(TDnetで観測可能)  → 弾く   ※事実ベース
    ②未発表でも前回が[15:00,15:30)の引け直前型 → 弾く   ※15:00は最大バケット
    ③それ以外(前回が午前〜14時台でも当日未発表) → 通す   ※発表枠を引け後に移した公算

比較: 除外なし / 現行(前回場中は全部弾く) / combo / 理想(当日実時刻<15:30を弾く=実装不可)
データ: earnings_times.json は2022年以降のみ → 評価は2022-2026に限定。
本番同一: RSI≤55×runup5<-3×tov7.5億×価格≤1万×ボラゲート≥2.0×RSI昇順8枠×PEAD+8%→5日。

実行: python -X utf8 _bt_earnings_intraday_v2.py
"""
from __future__ import annotations

import json
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

RSI_MAX, RUNUP_MAX, TOV_MIN, PRICE_CAP = 55.0, -3.0, 7.5e8, 10_000
PEAD_THR, PEAD_DAYS, SLOTS = 8.0, 5, 8
SIZE, VOL_GATE = 1_000_000, 2.0

RAW = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
RAW["egap_vol"] = RAW.groupby("ticker")["gap"].transform(
    lambda s: s.abs().shift(1).expanding(min_periods=3).median())

TIMES = json.load(open("earnings_times.json", encoding="utf-8"))


def to_min(t: str | None) -> float:
    if not t:
        return np.nan
    try:
        return int(t[:2]) * 60 + int(t[3:5])
    except Exception:
        return np.nan


# 当日実時刻と前回時刻を付与
tmap = {(tk, d): to_min(t) for tk, dm in TIMES.items() for d, t in dm.items()}
RAW["t_today"] = [tmap.get((tk, d), np.nan) for tk, d in zip(RAW.ticker, RAW.d0)]
prev_t = []
sorted_times = {tk: sorted(dm.items()) for tk, dm in TIMES.items()}
for tk, d in zip(RAW.ticker, RAW.d0):
    dm = sorted_times.get(tk, [])
    lo, hi, ans = 0, len(dm) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dm[mid][0] < d:
            ans = dm[mid][1]
            lo = mid + 1
        else:
            hi = mid - 1
    prev_t.append(to_min(ans))
RAW["t_prev"] = prev_t

E = RAW[(RAW.rsi <= RSI_MAX) & (RAW.runup5 < RUNUP_MAX)
        & (RAW.tov20 >= TOV_MIN) & (RAW.price <= PRICE_CAP)].copy()
E = E[E.egap_vol.isna() | (E.egap_vol >= VOL_GATE)]
E = E[E.year >= 2022].sort_values(["d0", "rsi"]).reset_index(drop=True)
cov_today = E.t_today.notna().mean() * 100
cov_prev = E.t_prev.notna().mean() * 100
print(f"[候補 2022-26] {len(E):,}件 / 当日時刻カバー{cov_today:.0f}% / 前回時刻カバー{cov_prev:.0f}%")

M1455, M1500, M1530 = 14 * 60 + 55, 15 * 60, 15 * 60 + 30

# 各ルールのマスク（True=買う）。時刻欠測はフェイルオープン=買う。
rules = {
    "除外なし": pd.Series(True, index=E.index),
    "現行=前回場中は弾く": ~(E.t_prev < M1530),
    "combo=当日既発表or引け直前型を弾く": ~((E.t_today <= M1455) | ((E.t_prev >= M1500) & (E.t_prev < M1530))),
    "理想=当日実時刻<15:30を弾く(実装不可)": ~(E.t_today < M1530),
}

# 分類精度: 理想を正解としたとき
ideal_buy = rules["理想=当日実時刻<15:30を弾く(実装不可)"]
known = E.t_today.notna()
for tag in ("現行=前回場中は弾く", "combo=当日既発表or引け直前型を弾く"):
    r = rules[tag]
    slip = (r & ~ideal_buy & known).sum()        # 場中なのに買ってしまう
    lost = (~r & ideal_buy & known).sum()        # 引け後なのに弾いてしまう
    print(f"  [{tag}] 場中すり抜け {slip}件({slip/known.sum()*100:.1f}%) / "
          f"引け後の取りこぼし {lost}件({lost/known.sum()*100:.1f}%)")


def sim(mask):
    A = E[mask]
    days = sorted(A.d0.unique())
    di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= SLOTS:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            pnl, span = (r.r5, PEAD_DAYS) if (r.gap > PEAD_THR and np.isfinite(r.r5)) else (r.gap, 1)
            busy.append(i + span)
            held[r.ticker] = i + span
            out.append(dict(year=r.year, pnl=pnl))
    P = pd.DataFrame(out)
    yen = P.pnl * SIZE / 100
    cum = yen.cumsum()
    dd = float((cum - cum.cummax()).min())
    yr = yen.groupby(P.year).sum().reindex(range(2022, 2027), fill_value=0)
    return dict(n=len(P), tot=float(yen.sum()), dd=dd, wr=(P.pnl > 0).mean() * 100,
                pos=int((yr > 0).sum()), years={y: v / 1e4 for y, v in yr.items()})


print(f"\n{'ルール':<36}{'件数':>6}{'4.5年計':>9}{'最大DD':>8}{'勝率':>7}{'勝年':>5}  年別(万)")
for tag, mask in rules.items():
    r = sim(mask)
    ys = " ".join(f"{y}:{v:+,.0f}" for y, v in r["years"].items())
    print(f"{tag:<36}{r['n']:>6}{r['tot']/1e4:>8,.0f}万{r['dd']/1e4:>7,.0f}万"
          f"{r['wr']:>6.1f}%{r['pos']:>4}/5  {ys}")

# 7月の実例でスモーク: 第一工業製薬型(前回14:00・当日16:00)は通るか
for tag, tp, tt, want in [("第一工業製薬型(前回14:00→当日16:00)", 14 * 60, 16 * 60, True),
                          ("ツガミ型(前回13:00→当日13:00既発表)", 13 * 60, 13 * 60, False),
                          ("滋賀銀行型(前回15:00→当日15:00)", 15 * 60, 15 * 60, False),
                          ("沖電気型(前回13:00→当日15:00)", 13 * 60, 15 * 60, True)]:
    buy = not ((tt <= M1455) or (M1500 <= tp < M1530))
    print(f"  combo判定 {tag}: {'買う' if buy else '弾く'} (期待={'買う' if want else '弾く'})"
          f" {'OK' if buy == want else 'NG'}")

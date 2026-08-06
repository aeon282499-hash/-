# -*- coding: utf-8 -*-
"""_bt_fx_scalp_m1.py — スキャルピングの算数を実データで見せる（2026-08-06）。

本人「スキャルピングはだめなん？過去の値動き見てイン/アウトルールで売買したら？」への回答。
Dukascopy無料1分足（USDJPY・直近約1年）で分足スケールのイン/アウトルールを総当たりし、
グロスのエッジ(pips/回)とXMスタンダードのスプレッド(1.6pips)を並べる。
スキャルの生死は「予測が当たるか」ではなく「エッジ(pips) > スプレッド(pips)」の算数。
実行: python -X utf8 _bt_fx_scalp_m1.py
"""
from __future__ import annotations

import datetime as dt
import lzma
import os
import struct
import time

import numpy as np
import pandas as pd

CACHE = "_fx_usdjpy_m1.pkl"
SPREAD_PIPS = 1.6          # XMスタンダード USDJPY 平均（要実測・KIWAMI口座なら0.7+手数料）
PIP = 0.01                 # JPYペアの1pip


def fetch():
    import requests
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
         "Referer": "https://freeserv.dukascopy.com/"}
    end = dt.date(2026, 7, 31)
    start = dt.date(2025, 8, 1)
    frames = []
    d = start
    n_req = 0
    while d <= end:
        if d.weekday() < 5:                       # 土日スキップ（FXは月-金）
            url = (f"https://datafeed.dukascopy.com/datafeed/USDJPY/{d.year}/"
                   f"{d.month-1:02d}/{d.day:02d}/BID_candles_min_1.bi5")
            for attempt in range(3):
                try:
                    r = requests.get(url, headers=h, timeout=30)
                    if r.status_code == 429:
                        time.sleep(5); continue
                    break
                except Exception:
                    time.sleep(3)
            n_req += 1
            if r.status_code == 200 and len(r.content) > 0:
                try:
                    raw = lzma.decompress(r.content)
                    n = len(raw) // 24
                    rec = struct.iter_unpack(">5if", raw)
                    day0 = dt.datetime(d.year, d.month, d.day)
                    rows = [(day0 + dt.timedelta(seconds=t), o / 1000, c / 1000)
                            for t, o, c, l_, h_, v in rec]
                    frames.append(pd.DataFrame(rows, columns=["ts", "open", "close"]))
                except Exception as e:
                    print(f"  {d} decode失敗 {e}")
            if n_req % 30 == 0:
                print(f"  ...{d} 取得済み{n_req}日", flush=True)
            time.sleep(0.4)
        d += dt.timedelta(days=1)
    df = pd.concat(frames).set_index("ts").sort_index()
    df.to_pickle(CACHE)
    print(f"[save] {CACHE} {len(df):,}本 ({df.index[0]}〜{df.index[-1]})")
    return df


D = pd.read_pickle(CACHE) if os.path.exists(CACHE) else fetch()
c = D.close
print(f"[data] USDJPY 1分足 {len(c):,}本 {c.index[0].date()}〜{c.index[-1].date()}"
      f" (Dukascopy BID・UTC)")

mid = c.index[len(c) // 2]


def run(pos, label, hold):
    """pos: その分足の確定時点で決めたポジション。次のhold分のリターンを取る。"""
    fwd = (c.shift(-hold) / c - 1) * 100
    m = pos != 0
    n = int(m.sum())
    if n < 200:
        return
    gross_pct = (pos[m] * fwd[m]).dropna()
    gross_pips = gross_pct.mean() / 100 * c[m].mean() / PIP
    net_pips = gross_pips - SPREAD_PIPS
    day_trades = n / c.index.normalize().nunique()
    h1 = gross_pct[gross_pct.index <= mid].mean()
    h2 = gross_pct[gross_pct.index > mid].mean()
    ann_net = net_pips * PIP / c[m].mean() * 100 * day_trades * 245
    print(f"  {label:<26} n={n:>7,} ({day_trades:>5.1f}回/日) "
          f"グロス{gross_pips:>+7.3f}pips/回 → ネット{net_pips:>+7.2f}pips/回 "
          f"年率{ann_net:>+9.0f}% 前半{h1:>+7.4f}%/後半{h2:>+7.4f}%")


print(f"\n  XMスタンダードのスプレッド = {SPREAD_PIPS}pips/往復。これを超えるグロスが必要。\n")

r1 = c.pct_change() * 100
for k in (1, 5, 15):
    for hold in (1, 5, 15):
        rk = (c / c.shift(k) - 1) * 100
        run(np.sign(rk), f"モメンタム{k}分→{hold}分持ち", hold)
        run(-np.sign(rk), f"逆張り{k}分→{hold}分持ち", hold)

# レンジブレイク（30分高値/安値・終値ベース＝足ラベル罠回避）
hi = c.rolling(30).max().shift(1)
lo = c.rolling(30).min().shift(1)
pos = pd.Series(0.0, index=c.index)
pos[c > hi] = 1.0
pos[c < lo] = -1.0
for hold in (5, 15, 30):
    run(pos, f"30分レンジブレイク→{hold}分", hold)

# 大きめの1分足に反応（0.1%≒15pips級の急変動）
for thr in (0.05, 0.10):
    p2 = pd.Series(0.0, index=c.index)
    p2[r1 > thr] = -1.0
    p2[r1 < -thr] = 1.0
    for hold in (5, 15):
        run(p2, f"急変動{thr}%フェード→{hold}分", hold)

print(f"""
  ── 読み方 ──
  ・グロスが+1.6pipsを超える行が「XMでスキャルが成立する」行。
  ・比較: 株フェードのエッジは1回+0.93%≒コスト(0.1-0.2%)の5〜9倍。
    スキャルは同じ倍率を出すには1回+8〜14pipsのグロスが要る。""")

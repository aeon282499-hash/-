# -*- coding: utf-8 -*-
"""_bt_fx_daily_grid.py — FX日足の古典戦略グリッド×XMコスト（2026-08-06）。

Alpha Vantage FX_DAILY(full・無料・約19年)で主要5ペアを取得し、
古典戦略（MAクロス/モメンタム/RSI逆張り/ブレイクアウト/大変動フェード）を
XMスタンダード口座の実コスト（スプレッド+スワップ概算）で総当たり。
予告どおり「単純テクニカルはコスト後で全滅」の検証。生き残りが出たら両期間・近傍で検査。
実行: python -X utf8 _bt_fx_daily_grid.py
"""
from __future__ import annotations

import os
import time

import numpy as np
import pandas as pd

CACHE = "_fx_daily_av.pkl"
# (from,to, スプレッド往復%概算, 表示名)
PAIRS = [("USD", "JPY", 0.011, "USDJPY"), ("EUR", "USD", 0.016, "EURUSD"),
         ("EUR", "JPY", 0.014, "EURJPY"), ("GBP", "JPY", 0.019, "GBPJPY"),
         ("AUD", "JPY", 0.031, "AUDJPY")]
SWAP = 0.008     # 保有1晩あたり%（買い売り平均の概算・XMで要実測）


def fetch():
    import requests
    from dotenv import load_dotenv
    load_dotenv()
    key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    out = {}
    for i, (a, b, _, nm) in enumerate(PAIRS):
        if i:
            time.sleep(12)
        r = requests.get("https://www.alphavantage.co/query", params={
            "function": "FX_DAILY", "from_symbol": a, "to_symbol": b,
            "outputsize": "full", "apikey": key}, timeout=60)
        ts = r.json().get("Time Series FX (Daily)")
        if not ts:
            print(f"[fetch] {nm} 失敗: {str(r.json())[:100]}")
            continue
        df = pd.DataFrame({"Date": list(ts),
                           "Close": [float(v["4. close"]) for v in ts.values()]})
        df["Date"] = pd.to_datetime(df.Date)
        out[nm] = df.sort_values("Date").set_index("Date").Close
        print(f"[fetch] {nm} {len(df)}日 ({df.Date.min().date()}〜{df.Date.max().date()})")
    pd.to_pickle(out, CACHE)
    return out


FX = pd.read_pickle(CACHE) if os.path.exists(CACHE) else fetch()


def bt(c, pos, spread):
    """pos: 前日終値までの情報で決めた当日のポジション(+1/0/-1)。翌日リターンに適用。"""
    ret = c.pct_change().shift(-1) * 100          # 当日→翌日
    pos = pos.fillna(0)
    gross = pos * ret
    trades = pos.diff().abs().fillna(0)           # ポジ変化でスプレッド
    cost = trades * spread / 2 * 100 / 100        # 変化1単位あたり片道
    cost = trades * (spread / 2)
    swap = pos.abs() * SWAP
    net = gross - cost - swap
    net = net.dropna()
    if len(net) < 500 or pos.abs().sum() < 50:
        return None
    y = net.index.year
    yr = net.groupby(y).sum()
    mid = net.index[len(net) // 2]
    return dict(ann=net.mean() * 245, h1=net[net.index <= mid].mean() * 245,
                h2=net[net.index > mid].mean() * 245,
                win=int((yr > 0).sum()), ny=yr.index.nunique(),
                gross_ann=gross.dropna().mean() * 245,
                exp=pos.abs().mean() * 100)


def show(nm, lab, r):
    if r is None:
        return
    mark = " ★" if (r["ann"] > 2 and r["h1"] > 0 and r["h2"] > 0
                    and r["win"] >= r["ny"] * 0.6) else ""
    print(f"  {nm:<8}{lab:<22} 年率net{r['ann']:>+6.1f}% (gross{r['gross_ann']:>+6.1f}%)"
          f" 前半{r['h1']:>+6.1f}%/後半{r['h2']:>+6.1f}% 勝ち{r['win']:>2}/{r['ny']}"
          f" 稼働{r['exp']:>3.0f}%{mark}")


print("\n★の条件: net年率>+2% かつ 前半後半ともプラス かつ 勝ち年6割以上\n")

for nm, c in FX.items():
    sp = next(p[2] for p in PAIRS if p[3] == nm)
    print(f"── {nm} ({c.index[0].date()}〜{c.index[-1].date()}) スプレッド{sp}% スワップ{SWAP}%/晩")
    # MAクロス（常時イン・ロング/ショート）
    for f, s in ((5, 25), (10, 50), (20, 100), (50, 200)):
        pos = (c.rolling(f).mean() > c.rolling(s).mean()).astype(int) * 2 - 1
        show(nm, f"MAクロス{f}/{s}", bt(c, pos.astype(float), sp))
    # k日モメンタム（符号に順張り・1日持ち）
    for k in (1, 5, 20, 60):
        pos = np.sign(c.pct_change(k))
        show(nm, f"モメンタム{k}日順張り", bt(c, pos, sp))
    # k日リバーサル（逆張り）
    for k in (1, 5):
        pos = -np.sign(c.pct_change(k))
        show(nm, f"リバーサル{k}日逆張り", bt(c, pos, sp))
    # RSI逆張り
    delta = c.diff()
    up = delta.clip(lower=0).rolling(14).mean()
    dn = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + up / dn)
    pos = pd.Series(0.0, index=c.index)
    pos[rsi < 30] = 1.0
    pos[rsi > 70] = -1.0
    pos = pos.replace(0, np.nan).ffill().where((rsi < 45) | (rsi > 55) | pos.notna(), 0).fillna(0)
    show(nm, "RSI14逆張り(30/70)", bt(c, pos, sp))
    # ドンチャン20日ブレイク（10日逆で手仕舞い）
    hi20 = c.rolling(20).max().shift(1)
    lo20 = c.rolling(20).min().shift(1)
    hi10 = c.rolling(10).max().shift(1)
    lo10 = c.rolling(10).min().shift(1)
    pos = pd.Series(np.nan, index=c.index)
    pos[c >= hi20] = 1.0
    pos[c <= lo20] = -1.0
    pos[(c <= lo10)] = pos[(c <= lo10)].where(pos[(c <= lo10)] != 1.0, 0.0)
    pos = pos.ffill().fillna(0)
    show(nm, "ドンチャン20日ブレイク", bt(c, pos, sp))
    # 大変動フェード（前日|ret|>閾値→翌日逆張り1日）
    r1 = c.pct_change() * 100
    for thr in (0.8, 1.2, 1.8):
        pos = pd.Series(0.0, index=c.index)
        pos[r1 > thr] = -1.0
        pos[r1 < -thr] = 1.0
        show(nm, f"大変動{thr}%フェード", bt(c, pos, sp))
    print()

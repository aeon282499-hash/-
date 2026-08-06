# -*- coding: utf-8 -*-
"""_bt_fx_ma_intraday.py — 5分/15分足の移動平均デイトレ検証（2026-08-07）。

本人「スキャルは諦める。5分とか15分の5MA・20MA・25MAは？」への回答。
王道のMA手法をUSDJPY 1年分のM1から作った5分/15分足で総当たり:
  ①MAクロス(5/20, 5/25, 20/75, 25/100) 常時イン・ドテン
  ②パーフェクトオーダー(5>20>75で買い・逆で売り・崩れたらノーポジ)
  ③25MA押し目(25MAが上向き＆価格>75MAの上昇トレンド中、価格が25MAまで落ちたら買い)
判定はスキャルと同じ算数: グロスpips/回 vs スプレッド(スタンダード1.6/KIWAMI0.7)。
実行: python -X utf8 _bt_fx_ma_intraday.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PIP = 0.01
D = pd.read_pickle("_fx_usdjpy_m1.pkl")
mid_ts = D.index[len(D) // 2]


def bt(c, pos, label, tf):
    pos = pos.fillna(0.0)
    ret = c.pct_change().shift(-1) * 100        # 当バー確定→次バーのリターン
    gross = (pos * ret).dropna()
    chg = pos.diff().abs().fillna(pos.abs())
    n_trades = float(chg.sum()) / 2             # 往復数
    if n_trades < 30:
        return
    px = c.mean()
    gross_pips_total = gross.sum() / 100 * px / PIP
    g_per = gross_pips_total / n_trades
    days = c.index.normalize().nunique()
    hold_h = (pos != 0).sum() / n_trades * (5 if tf == "5min" else 15) / 60
    h1 = gross[gross.index <= mid_ts].sum()
    h2 = gross[gross.index > mid_ts].sum()
    out = []
    for sp, nm in ((1.6, "std"), (0.7, "極")):
        net_total_pips = gross_pips_total - n_trades * sp
        ann = net_total_pips * PIP / px * 100 / days * 245
        out.append(f"{nm}:{net_total_pips/ (days/245):>+8.0f}pips/年({ann:+.1f}%)")
    print(f"  {tf:>5} {label:<24} {n_trades/days:>5.2f}回/日 平均{hold_h:>5.1f}h持ち "
          f"グロス{g_per:>+6.2f}pips/回 → {out[0]} {out[1]} "
          f"前半{h1:>+6.1f}%/後半{h2:>+6.1f}%")


for tf in ("5min", "15min"):
    c = D.close.resample(tf).last().dropna()
    print(f"\n── {tf}足 ({len(c):,}本・USDJPY 2025-08〜2026-07) "
          f"スプレッド: スタンダード1.6pips/KIWAMI0.7pips ──")
    ma = {n: c.rolling(n).mean() for n in (5, 20, 25, 75, 100)}
    # ①MAクロス（ドテン）
    for f, s in ((5, 20), (5, 25), (20, 75), (25, 100)):
        bt(c, np.sign(ma[f] - ma[s]), f"MAクロス{f}/{s}ドテン", tf)
    # ②パーフェクトオーダー
    po = pd.Series(0.0, index=c.index)
    po[(ma[5] > ma[20]) & (ma[20] > ma[75])] = 1.0
    po[(ma[5] < ma[20]) & (ma[20] < ma[75])] = -1.0
    bt(c, po, "パーフェクトオーダー5/20/75", tf)
    # ③25MA押し目買い（上昇トレンド中のタッチ→12本持ち）＋鏡像の戻り売り
    up = (ma[25].diff(5) > 0) & (c > ma[75])
    dn = (ma[25].diff(5) < 0) & (c < ma[75])
    touch_dn = (c <= ma[25]) & (c.shift(1) > ma[25].shift(1))
    touch_up = (c >= ma[25]) & (c.shift(1) < ma[25].shift(1))
    for hold in (6, 12, 24):
        sig = pd.Series(np.nan, index=c.index)
        sig[up & touch_dn] = 1.0
        sig[dn & touch_up] = -1.0
        pos = sig.ffill(limit=hold).fillna(0.0)
        bt(c, pos, f"25MA押し目/戻り {hold}本持ち", tf)

print("""
  ── 読み方 ──
  グロスpips/回がスプレッド(1.6/0.7)を超えていなければ、その行は構造的に負け。
  ※1年サンプル＝レジーム1個。生き残り候補が出た場合のみ年数を増やして再検証する。""")

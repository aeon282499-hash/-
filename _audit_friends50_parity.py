# -*- coding: utf-8 -*-
"""_audit_friends50_parity.py — 友達用フェード(50万×#1・7.5億)がBTと同じ選定になるかの突合（2026-08-18）。

検証は3層:
  ①定数ガード: FRIENDS_SIZE/PICKS/TOV_MIN と GO閾値がBT採用値のまま（変えたらBT取り直しの合図）
  ②規約の同値性: BT流儀（株価≤FRIENDS_SIZE/100 の事前カット・50万丸め）と
    ライブ流儀（last_c*100>capital 除外・max(100,…)株）が**玉単位で完全一致**すること
  ③公式値の再現: 10年プール(_fade_pool_v5_100.pkl)で n=889 / 勝率59.2% / PF1.52 /
    +421万 / 勝ち年10/11（_bt_fade_friends50.py 流儀A・2026-08-18確定）

実行: python -X utf8 _audit_friends50_parity.py   （全部OKなら exit 0）
"""
import sys

import numpy as np
import pandas as pd

import daytrade_paper as DP

FAIL = []

def check(name, ok, detail=""):
    print(f"  [{'OK' if ok else 'NG'}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        FAIL.append(name)

print("== ①定数ガード（変えたらBT取り直し） ==")
check("FRIENDS_SIZE=500_000", DP.FRIENDS_SIZE == 500_000, f"実際={DP.FRIENDS_SIZE}")
check("FRIENDS_PICKS=1（#1のみ・代打なし）", DP.FRIENDS_PICKS == 1, f"実際={DP.FRIENDS_PICKS}")
check("FRIENDS_TOV_MIN=7.5億", DP.FRIENDS_TOV_MIN == 7.5e8, f"実際={DP.FRIENDS_TOV_MIN:.3g}")
check("GO閾値: gain+7%/ATR5%/乖離12%/vr<6/レンジ>5%",
      (DP.DAILY_PICK_GAIN_MIN, DP.FADE_ATR_MIN, DP.FADE_DEV25_MIN,
       DP.FADE_VOL_RATIO_MAX, DP.STICKY_RANGE_MIN) == (7.0, 5.0, 12.0, 6.0, 0.05))

print("== ②規約の同値性（BT流儀 vs ライブ流儀） ==")
P = pd.read_pickle("_fade_pool_v5_100.pkl")
base = P[(P.gain >= DP.DAILY_PICK_GAIN_MIN) & (P.vr < DP.FADE_VOL_RATIO_MAX)
         & (P.atr >= DP.FADE_ATR_MIN) & (P.dev >= DP.FADE_DEV25_MIN)
         & (P.tov >= DP.FRIENDS_TOV_MIN) & (P.rng > DP.STICKY_RANGE_MIN * 100)
         & (P.vol_avg >= 100_000)]

def pick1(d, sh_fn):
    d = d.copy()
    r = None
    for col in ("dev", "atr"):
        x = d.groupby("sig")[col].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d = d[d.groupby("sig").cumcount() == 0].copy()      # rk==1
    d["sh"] = sh_fn(d.px)
    d = d[d.sh > 0]
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d

# BT流儀: 事前カット px≤SIZE/100・丸め int(SIZE/px//100*100)
bt = pick1(base[base.px <= DP.FRIENDS_SIZE / 100],
           lambda px: (DP.FRIENDS_SIZE / px // 100 * 100).astype(int))
# ライブ流儀: last_c*100>capital 除外・max(100, int(SIZE/prev_close/100)*100)株
lv = pick1(base[~(base.px * 100 > DP.FRIENDS_SIZE)],
           lambda px: np.maximum(100, (DP.FRIENDS_SIZE / px / 100).astype(int) * 100))

same = (len(bt) == len(lv)
        and (bt[["sig", "ticker", "sh"]].reset_index(drop=True)
             .equals(lv[["sig", "ticker", "sh"]].reset_index(drop=True))))
check("BT流儀とライブ流儀で 選定玉・株数が完全一致", same,
      f"BT {len(bt)}玉 / ライブ {len(lv)}玉")

print("== ③公式値の再現（10年） ==")
p = bt.pnl
pf = p[p > 0].sum() / -p[p < 0].sum()
yr = bt.groupby("y").yen.sum()
tot = bt.yen.sum()
print(f"  n={len(bt)} 勝率{(p > 0).mean() * 100:.1f}% PF{pf:.2f} "
      f"10年{tot / 1e4:+,.0f}万 勝ち年{int((yr > 0).sum())}/{len(yr)}")
check("n=889", len(bt) == 889, f"実際={len(bt)}")
check("勝率=59.2%", round((p > 0).mean() * 100, 1) == 59.2)
check("PF=1.52", round(pf, 2) == 1.52)
check("10年合計=+421万", round(tot / 1e4) == 421, f"実際={tot / 1e4:+.1f}万")
check("勝ち年=10/11", int((yr > 0).sum()) == 10)

print()
if FAIL:
    print(f"NG {len(FAIL)}件: {FAIL}")
    print("→ 定数かロジックがBTとズレている。直すか、_bt_fade_friends50.py で再測定して"
          "このスクリプトの期待値を更新すること（無言で握りつぶさない）。")
    sys.exit(1)
print("友達50万パリティ: 全部OK（ライブ選定＝BT選定）")

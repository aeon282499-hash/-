# -*- coding: utf-8 -*-
"""_sell_gate_count.py — スイングSELLが「一回も出ない」のは仕様か故障かの実測(2026-07-16)。
bt_sell_grid_fast.py の precompute をそのまま使い、本番条件
(前日比+3%/RSI60/乖離+4%/ATR2.5/20億/④値幅or出来高) の成立件数を
日経25MAゲートあり/なしで年別に数える。
"""
import pickle

import numpy as np
import pandas as pd

from bt_sell_grid_fast import precompute_indicators, RANGE_MULT, VOL_MULT, \
    TURNOVER_MIN, ATR_VOL_CAP

RSI_MIN, DEV_MIN, DCH_MIN = 60.0, 4.0, 3.0  # screener.py judge_sell_signal_pre と一致

c = pickle.load(open("jquants_cache.pkl", "rb"))
data, names = c["all_data"], c["name_map"]
print(f"[data] pkl 〜{c['end']} / {len(data)}銘柄")

nk = data["1321.T"].sort_index().copy()
nk["MA25"] = nk["Close"].rolling(25).mean()
gate = (nk["Close"] < nk["MA25"])  # 日経25MA割れ=SELL配信可

rows = []
for tk, df in data.items():
    if names.get(tk) is None or len(df) < 50:
        continue
    d = precompute_indicators(df.sort_index())
    m = (
        (d["RSI"] >= RSI_MIN) & (d["DEV"] >= DEV_MIN) & (d["DAY_CHANGE"] >= DCH_MIN)
        & (d["ATR_PCT"] <= ATR_VOL_CAP) & (d["TURNOVER_PREV"] >= TURNOVER_MIN)
        & ((d["RANGE_RATIO_PREV"] >= RANGE_MULT) | (d["VOL_RATIO_PREV"] >= VOL_MULT))
    )
    for ts in d.index[m.to_numpy()]:
        rows.append({"date": ts, "ticker": tk, "name": names.get(tk),
                     "gated": bool(gate.get(ts, False))})

D = pd.DataFrame(rows)
D["year"] = D["date"].dt.year
print(f"\n本番SELL条件の成立（シグナル日ベース・翌朝配信）全 {len(D)}件")
print(f"{'年':<6}{'ゲートなし':>10}{'日経25MA割れ日のみ(本番)':>26}")
for y, g in D.groupby("year"):
    print(f"{y:<6}{len(g):>10}{int(g['gated'].sum()):>20}")
print(f"{'合計':<6}{len(D):>10}{int(D['gated'].sum()):>20}")

live = D[(D["date"] >= "2026-05-06") & D["gated"]]
print(f"\n現行条件が本番稼働した2026-05-06以降（ゲート込み）: {len(live)}件")
ung = D[D["date"] >= "2026-05-06"]
print(f"同期間ゲートを外した場合: {len(ung)}件")
if len(ung):
    for _, r in ung.tail(10).iterrows():
        print(f"  {r['date']:%Y-%m-%d} {r['ticker']} {r['name']} gate={'開' if r['gated'] else '閉'}")

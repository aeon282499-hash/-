# -*- coding: utf-8 -*-
"""VI帯×期間: 高VI玉が前半(17-21)でも良いか（機構がレジーム横断か）。2026-09-03・本番無変更。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date")
NV["nvi"] = pd.to_numeric(NV.bv1, errors="coerce"); NV = NV.dropna(subset=["nvi"]); NV["nvi_lag"] = NV.nvi.shift(1)
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Ct, NV[["Date", "nvi", "nvi_lag"]], left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
R0["nvi"] = J.nvi_lag.to_numpy()[R0.i.to_numpy()]   # 実装可能なラグ+1日版
R0["half"] = np.where(R0.y <= 2021, "17-21", "22-26")
R0["b"] = pd.cut(R0.nvi, [0, 15, 20, 25, 99], labels=["<=15", "15-20", "20-25", ">25"])
g = R0.groupby(["half", "b"], observed=True).agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda x: (x > 0).mean() * 100), yen=("yen", "sum"))
print("■ 建てた玉: 期間×VI帯（ラグ+1日）"); print(g.round(3).to_string())
# 候補レベル（選定の運を除く）: 全候補の翌日〜3日の出口リプレイ pnl0 を VI帯で層別
C = pd.DataFrame({"y": YEAR, "pnl": pnl0, "nvi": J.nvi_lag.to_numpy()}); C = C[BASE & np.isfinite(pnl0)]
C["half"] = np.where(C.y <= 2021, "17-21", "22-26"); C["b"] = pd.cut(C.nvi, [0, 15, 20, 25, 99], labels=["<=15", "15-20", "20-25", ">25"])
print("\n■ 候補レベル: 期間×VI帯"); print(C.groupby(["half", "b"], observed=True).agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda x: (x > 0).mean() * 100)).round(3).to_string())
print("\n■ 年×VI帯 候補平均%"); print(C.groupby(["y", "b"], observed=True).pnl.mean().unstack().round(2).to_string())
print("\n[done]")

import os, pickle, pandas as pd, numpy as np, json
os.chdir(r"C:\Users\星野\Documents\my-first-project")
B = pickle.load(open("_bars_daily_5y_1024.pkl", "rb"))
for code, d0 in (("67580", "2024-10-01"), ("94320", "2023-07-01"), ("22820", "2026-09-29")):
    x = B[(B.Code == code) & (B.Date >= pd.Timestamp(d0) - pd.Timedelta(days=6)) & (B.Date <= pd.Timestamp(d0) + pd.Timedelta(days=4))][["Date", "C", "Vo", "AdjFactor"]]
    print(code); print(x.to_string(index=False))
print("AdjFactor != 1 rows:", (B.AdjFactor != 1).sum(), " codes", B.Code.nunique(), " 5桁コード例", B.Code.head(3).tolist())
M = pickle.load(open("_master_1024.pkl", "rb"))
print("master", M.shape, list(M.columns)); print(M.head(3).to_string())
print(M.MktNm.value_counts().to_dict() if "MktNm" in M.columns else "")
# 上場廃止候補: bulk のコードで最終日が 2026-10-30 より前
last = B.groupby("Code").Date.max()
print("bulk codes", len(last), " 最終日が10/30より前(廃止等)", (last < "2026-10-30").sum(), " 2022年以降に消えた", ((last < "2026-10-30") & (last >= "2022-01-01")).sum())
T = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
tcodes = {k[:4] for k in T}
gone = last[(last < "2026-10-30")]
print("  消えたコードのうち立花に無い", sum(1 for c in gone.index if c[:4] not in tcodes), "/", len(gone))
# 消えた銘柄の代金規模
Va = B[B.Code.isin(gone.index)].groupby("Code").Va.median()
print("  消えた銘柄の代金中央値≥20億", (Va >= 2e9).sum(), " ≥5億", (Va >= 5e8).sum())
print(json.load(open("jpx_earnings_schedule.json", encoding="utf-8")).__class__, list(json.load(open("jpx_earnings_schedule.json", encoding="utf-8")).items())[:2] if isinstance(json.load(open("jpx_earnings_schedule.json", encoding="utf-8")), dict) else "")
ec = json.load(open("earnings_calendar.json", encoding="utf-8")); print(type(ec), list(ec.items())[:2] if isinstance(ec, dict) else ec[:2])

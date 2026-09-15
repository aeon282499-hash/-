# -*- coding: utf-8 -*-
"""_snap_valuation_10y.py — J-Quants V2 /equities/valuation（2026-09-14提供開始）を全銘柄×10年スナップショット。
履歴は10年ローリング窓（2016-09-15〜）で毎日古い側が消えるので、取れるうちに pkl 化する（[[reference_jquants_indices_plan]]）。
列: Date/Code/EPS/FwdEPS/BPS/ROE/FwdROE/PER/FwdPER/PBR/MktCap。出力: _valuation_10y.pkl（DataFrame・Date/Code index）。
実行: python -X utf8 _snap_valuation_10y.py   （銘柄ごとに1要求・約4,450要求・途中保存あり・再開可）
"""
import json, os, pickle, sys, time, io
import pandas as pd, requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for ln in open(".env", encoding="utf-8-sig"):
    if "=" in ln and not ln.startswith("#"):
        k, v = ln.strip().split("=", 1); env[k] = v.strip().strip('"')
H = {"x-api-key": env["JQUANTS_API_KEY"]}; B = "https://api.jquants.com/v2"
OUT = "_valuation_10y.pkl"; PART = "_valuation_10y.part.pkl"

# 銘柄一覧＝当日の全銘柄バリュエーション（4,446件）
r = requests.get(B + "/equities/valuation", headers=H, params={"date": "2026-09-14"}, timeout=120).json()
codes = sorted({x["Code"] for x in r[[k for k in r if k != "pagination_key"][0]]})
print("codes", len(codes))
done = {}
if os.path.exists(PART):
    done = pickle.load(open(PART, "rb"))
frames = []
t0 = time.time()
for i, c in enumerate(codes, 1):
    if c in done:
        continue
    rows, key = [], None
    for _ in range(20):
        q = {"code": c, "from": "2016-09-15", "to": "2026-09-14"}
        if key: q["pagination_key"] = key
        for attempt in range(4):
            try:
                rr = requests.get(B + "/equities/valuation", headers=H, params=q, timeout=120)
                if rr.status_code == 429:
                    time.sleep(10); continue
                j = rr.json(); break
            except Exception as e:  # noqa: BLE001
                time.sleep(5); j = {"message": str(e)}
        if "message" in j:
            print("ERR", c, j["message"][:80]); break
        k = [x for x in j if x != "pagination_key"][0]; rows += j[k]; key = j.get("pagination_key")
        if not key: break
    done[c] = rows
    if i % 100 == 0:
        pickle.dump(done, open(PART, "wb"))
        print(f"{i}/{len(codes)} {c} rows={len(rows)} 経過{(time.time()-t0)/60:.0f}分", flush=True)
    time.sleep(0.15)
pickle.dump(done, open(PART, "wb"))
df = pd.DataFrame([x for v in done.values() for x in v])
for col in ("EPS", "FwdEPS", "BPS", "ROE", "FwdROE", "PER", "FwdPER", "PBR", "MktCap"):
    df[col] = pd.to_numeric(df[col], errors="coerce")
df["Date"] = pd.to_datetime(df["Date"]); df["Code"] = df["Code"].astype(str)
df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
df.to_pickle(OUT)
print("saved", OUT, df.shape, df.Date.min(), df.Date.max())

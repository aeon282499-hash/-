# -*- coding: utf-8 -*-
"""_fetch_nk225_iv.py — 日経VI代替: J-Quants 日経225オプション(IV/BaseVol)から日次ボラ指標を作る（2026-09-03）。
各営業日: 期近/期先の BaseVol、ATM(原資産に最も近い行使価格)のIV(コール/プット平均)、VI風30日補間。
出力: _nk225_iv_daily.pkl（50日ごとに逐次保存・再実行は続きから）。"""
import os, sys, time, requests, pandas as pd, numpy as np, jpholiday
MAX = int(sys.argv[1]) if len(sys.argv) > 1 else 10**9
from datetime import date, timedelta
key=[l.split("=",1)[1].strip() for l in open(".env",encoding="utf-8") if l.startswith("JQUANTS_API_KEY=")][0]
H={"x-api-key":key}; B="https://api.jquants.com/v2"; OUT="_nk225_iv_daily.pkl"
done = pd.read_pickle(OUT) if os.path.exists(OUT) else pd.DataFrame()
have = set(done["Date"]) if len(done) else set()
rows = list(done.to_dict("records")) if len(done) else []
d = date(2016, 9, 5); end = date(2026, 9, 3); n = 0
while d <= end:
    ds = d.strftime("%Y-%m-%d")
    if d.weekday() < 5 and not jpholiday.is_holiday(d) and ds not in have and not (d.month == 12 and d.day == 31) and not (d.month == 1 and d.day <= 3):
        data = []; params = {"date": ds}
        for _ in range(5):
            try:
                r = requests.get(B + "/derivatives/bars/daily/options/225", headers=H, params=params, timeout=(10, 60))
            except Exception as e:
                time.sleep(3); continue
            if r.status_code == 429: time.sleep(5); continue
            if r.status_code != 200: break
            j = r.json(); data += j.get("data", [])
            pk = j.get("pagination_key")
            if not pk: break
            params["pagination_key"] = pk
        rec = {"Date": ds, "n": len(data)}
        try:
            if data:
                df = pd.DataFrame(data)
                df = df[(df["LTD"] > ds)]
                if len(df):
                    df["dte"] = (pd.to_datetime(df["LTD"]) - pd.Timestamp(ds)).dt.days
                    cms = sorted(df["CM"].unique(), key=lambda c: df[df.CM == c].dte.iloc[0])
                    for k, cm in enumerate(cms[:2]):
                        g = df[df.CM == cm]
                        bv = pd.to_numeric(g["BaseVol"], errors="coerce"); bv = bv[bv > 0]
                        rec[f"bv{k+1}"] = float(bv.median()) if len(bv) else np.nan
                        rec[f"dte{k+1}"] = int(g.dte.iloc[0])
                        up = pd.to_numeric(g["UnderPx"], errors="coerce").dropna()
                        if len(up):
                            u = float(up.iloc[0]); rec["under"] = u
                            g2 = g[(pd.to_numeric(g["IV"], errors="coerce") > 0)].copy()
                            g2["dist"] = (g2["Strike"] - u).abs()
                            atm = g2.nsmallest(4, "dist")
                            rec[f"iv{k+1}"] = float(pd.to_numeric(atm["IV"], errors="coerce").mean()) if len(atm) else np.nan
                    if "bv1" in rec and "bv2" in rec and np.isfinite(rec["bv1"]) and np.isfinite(rec["bv2"]):
                        t1, t2 = rec["dte1"], rec["dte2"]
                        if t1 < 30 <= t2 and t2 > t1:
                            w = (t2 - 30) / (t2 - t1)
                            rec["vi30"] = float(np.sqrt(max(w * rec["bv1"]**2 * t1 + (1 - w) * rec["bv2"]**2 * t2, 0) / 30))
                        else:
                            rec["vi30"] = rec["bv1"]
        except Exception as _e:
            print(ds, "ERR", _e, flush=True)
        rows.append(rec); n += 1
        print(ds, rec.get("n"), rec.get("vi30"), flush=True)
        if n % 25 == 0:
            pd.DataFrame(rows).to_pickle(OUT)
        if n >= MAX:
            break
        time.sleep(0.25)
    d += timedelta(days=1)
pd.DataFrame(rows).to_pickle(OUT); print("done", len(rows))

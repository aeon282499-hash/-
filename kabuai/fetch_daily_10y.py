# -*- coding: utf-8 -*-
"""fetch_daily_10y.py — CI 用: J-Quants V2 から全銘柄の日足を N 営業日ぶん取り、PC と同じ形の jquants_cache.pkl を作る（2026-11-15）。

PC の jquants_cache*.pkl（gitignore）が無い CI で 10年BT（_bt_vcp_10y_1115.py など）を回すため。
- 普通株のみ（5桁目=0）。ETF/REIT 等を外すため、銘柄一覧（/equities/master）を過去の複数時点で取って和集合にする
  ＝上場廃止銘柄も残す（生存バイアスを避ける・PC キャッシュと同じ扱い）。
- 1日ずつ取得して列を絞った小さな DataFrame で持つ（数百万行でも数百MB に収める）。
出力: ../jquants_cache.pkl = {"all_data": {"7203.T": DataFrame[Open,High,Low,Close,Volume]}, "name_map": {...}, "end": "YYYY-MM-DD"}
      ../jquants_cache_2016_2021.pkl = {"all_data": {}}（空・読み込み側の互換用）
実行: cd kabuai && python fetch_daily_10y.py 2440
"""
from __future__ import annotations

import pickle
import sys
import time
from datetime import timedelta

import pandas as pd

from build_data import _is_common_stock, _jquants_get, _jquants_key, _recent_trading_days

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2440
COLS = {"AdjO": "Open", "AdjH": "High", "AdjL": "Low", "AdjC": "Close", "AdjVo": "Volume"}
STOCK_MKT = ("プライム", "スタンダード", "グロース", "市場第一部", "市場第二部", "マザーズ", "JASDAQ", "ＪＡＳＤＡＱ", "東証")


def get(path: str, token: str, params: dict) -> dict:
    while True:
        try:
            return _jquants_get(path, token, params)
        except Exception as e:
            if "429" in str(e):
                print("  レート制限 → 60秒待機", flush=True); time.sleep(60); continue
            raise


def main() -> None:
    token = _jquants_key()
    days = _recent_trading_days(N)
    # 銘柄一覧を約半年ごとに取って和集合（上場廃止も残す）
    names: dict = {}
    for ds in days[::120] + [days[-1]]:
        try:
            m = get("/equities/master", token, {"date": ds.replace("-", "")})
        except Exception as e:
            print(f"  master {ds} 失敗: {e}"); continue
        for it in m.get("data", []):
            code = str(it.get("Code", ""))
            mk = str(it.get("MktNm", ""))
            if _is_common_stock(code) and any(k in mk for k in STOCK_MKT) and "ETF" not in mk and "REIT" not in mk:
                names.setdefault(code[:4], it.get("CoName", code[:4]))
        time.sleep(1.2)
    if not names:   # date 指定が使えない場合は現在の一覧だけ（上場廃止が抜ける＝やや楽観側）
        print("  ⚠ 過去時点の銘柄一覧が取れない → 現在の一覧のみ（上場廃止銘柄が抜ける）", flush=True)
        for it in get("/equities/master", token, {}).get("data", []):
            code = str(it.get("Code", ""))
            if _is_common_stock(code) and any(k in str(it.get("MktNm", "")) for k in STOCK_MKT):
                names.setdefault(code[:4], it.get("CoName", code[:4]))
    print(f"[fetch] 普通株 {len(names):,} 銘柄（{len(days[::120]) + 1} 時点の和集合）/ {days[0]}〜{days[-1]}（{len(days)}営業日）", flush=True)

    parts = []
    for i, ds in enumerate(days):
        pk = None
        while True:
            params = {"date": ds}
            if pk:
                params["pagination_key"] = pk
            try:
                d = get("/equities/bars/daily", token, params)
            except Exception as e:
                print(f"  {ds} 失敗: {e}"); break
            rows = d.get("data", [])
            if rows:
                df = pd.DataFrame(rows)
                if {"Code", "Date", *COLS}.issubset(df.columns):
                    df = df[["Code", "Date", *COLS]]
                    df = df[df.Code.astype(str).str[4:].isin(["", "0"])]
                    df["Code"] = df.Code.astype(str).str[:4]
                    df = df[df.Code.isin(names)]
                    for k in COLS:
                        df[k] = pd.to_numeric(df[k], errors="coerce").astype("float32" if k != "AdjVo" else "float64")
                    parts.append(df)
            pk = d.get("pagination_key")
            time.sleep(1.2)
            if not pk:
                break
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(days)} 日", flush=True)

    A = pd.concat(parts, ignore_index=True).rename(columns=COLS)
    A["Date"] = pd.to_datetime(A.Date)
    data = {}
    for code, g in A.groupby("Code", sort=False):
        sub = g.drop(columns="Code").set_index("Date").sort_index()
        sub = sub[~sub.index.duplicated(keep="last")].dropna(subset=["Close"])
        if len(sub):
            data[f"{code}.T"] = sub.astype(float)
    end = A.Date.max().strftime("%Y-%m-%d")
    pickle.dump({"all_data": data, "name_map": names, "end": end}, open("../jquants_cache.pkl", "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    pickle.dump({"all_data": {}}, open("../jquants_cache_2016_2021.pkl", "wb"))
    print(f"[fetch] {len(data):,} 銘柄・{len(A):,} 行・最新日 {end} → ../jquants_cache.pkl", flush=True)


if __name__ == "__main__":
    main()

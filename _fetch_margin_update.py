# -*- coding: utf-8 -*-
"""_fetch_margin_update.py — _margin_10y.pkl の差分更新（2026-07-29）。

_fetch_margin_10y.py は10年分を一括で取り直す設計（2時間級）で、しかも完了形式(dict)の
pkl があると再実行を拒否する。日々のBT更新で必要なのは「前回終端より後の金曜」だけなので、
不足週だけ取って既存 dict に追記する。

実行: python -X utf8 _fetch_margin_update.py
"""
from __future__ import annotations

import pickle
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
from screener import _jquants_get, _jquants_id_token

OUT = Path("_margin_10y.pkl")
store: dict = pickle.load(open(OUT, "rb"))
if not isinstance(store, dict):
    raise SystemExit("[margin] _margin_10y.pkl が dict 形式でない → _fetch_margin_10y.py を使うこと")

need = set(store)
last = max(v.index.max() for v in store.values() if v is not None and len(v)).date()
print(f"[margin] 既存 {len(store)}銘柄 / 終端 {last}", flush=True)

# 不足している金曜（終端の翌週以降・今日まで）
fridays: list[date] = []
cur = last + timedelta(days=7)
today = date.today()
while cur <= today:
    fridays.append(cur)
    cur += timedelta(days=7)
if not fridays:
    raise SystemExit("[margin] 追加すべき週なし（最新）")
print(f"[margin] 追加取得 {len(fridays)}週: {[f.isoformat() for f in fridays]}", flush=True)

token = _jquants_id_token()
records: list[dict] = []
for fri in fridays:
    for back in range(0, 3):                     # 金→木→水（祝日フォールバック）
        ds = (fri - timedelta(days=back)).isoformat()
        try:
            d = _jquants_get("/markets/margin-interest", token, {"date": ds})
        except Exception as e:
            if "429" in str(e):
                print("  rate limit → 60s", flush=True)
                time.sleep(60)
                continue
            print(f"  {ds} 失敗: {str(e)[:70]}", flush=True)
            break
        rows = d.get("data", [])
        if rows:
            hit = [r for r in rows if str(r.get("Code", ""))[:4] in need]
            records.extend(hit)
            print(f"  {ds}: {len(rows):,}行中 対象{len(hit):,}行", flush=True)
            break
        time.sleep(0.4)
    time.sleep(1.1)

if not records:
    raise SystemExit("[margin] 取得ゼロ（まだ公表前の可能性）→ 既存pklは無変更")

df = pd.DataFrame(records)
df["code4"] = df["Code"].astype(str).str[:4]
df["Date"] = pd.to_datetime(df["Date"])
added = 0
for c4, g in df.groupby("code4"):
    g = g.sort_values("Date").set_index("Date")
    old = store.get(c4)
    m = pd.concat([old, g]) if old is not None and len(old) else g
    m = m.sort_index()
    store[c4] = m[~m.index.duplicated(keep="last")]
    added += len(g)

pickle.dump(store, open(OUT, "wb"))
new_last = max(v.index.max() for v in store.values() if v is not None and len(v)).date()
print(f"[margin] 完了: {len(store)}銘柄 / +{added:,}行 / 終端 {last} → {new_last}", flush=True)

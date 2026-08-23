# -*- coding: utf-8 -*-
"""_bt_edinet_lvh.py — 大量保有報告書(5%ルール)イベントスタディ（2026-08-25）。

イベント: 大量保有報告書の提出（2022-01〜2026-07・5,414件・_edinet_lvh.jsonl）
エントリー: 提出日の翌営業日寄り成行（提出時刻に関わらず翌日＝look-ahead構造的ゼロ）
出口: 1/5/10/20営業日目の終値（コスト0.2%/往復）
層別: 保有目的 / 保有割合帯 / 流動性 / 前後半(2022-23 vs 2024-26)
家訓: 上位3銘柄除去・低位株フロア・n明示。同一(銘柄,エントリー日)は1イベントに集約。
実行: python -X utf8 _bt_edinet_lvh.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd

COST = 0.2
HOLDS = (1, 5, 10, 20)

recs = []
with open("_edinet_lvh.jsonl", encoding="utf-8") as f:
    for ln in f:
        try:
            r = json.loads(ln)
        except Exception:
            continue
        if r.get("err") or not r.get("icode") or r.get("ratio") is None:
            continue
        recs.append(r)
print(f"[load] 有効 {len(recs):,}件")

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}


def series(tk):
    if tk in SER:
        return SER[tk]
    dfs = [d for src in (old["all_data"], new["all_data"])
           if (d := src.get(tk)) is not None and len(d)]
    if not dfs:
        SER[tk] = None
        return None
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    SER[tk] = (df["Open"].astype(float).to_numpy(), df["Close"].astype(float).to_numpy(),
               (df["Close"] * df["Volume"]).rolling(20).mean().astype(float).to_numpy(),
               df.index.strftime("%Y-%m-%d").to_numpy())
    return SER[tk]


def purpose_bucket(p: str) -> str:
    p = p or ""
    if any(k in p for k in ("重要提案", "経営", "支配", "子会社", "提携", "統合", "買収")):
        return "支配・提案系"
    if "政策" in p:
        return "政策投資"
    if "純投資" in p:
        return "純投資"
    return "その他"


rows = []
for r in recs:
    code = str(r["icode"]).strip()
    if not code or len(code) < 4:
        continue
    tk = code[:4] + ".T"
    a = series(tk)
    if a is None:
        continue
    o, c, tov, dates = a
    sub_d = (r.get("submit") or "")[:10]
    if not sub_d:
        continue
    pos = np.searchsorted(dates, sub_d, side="right")   # 提出日の翌営業日
    if pos >= len(dates) or pos < 1:
        continue
    e = o[pos]
    if not (e > 0) or not np.isfinite(e):
        continue
    row = {"tk": tk, "iname": r.get("iname"), "ent": dates[pos], "y": int(dates[pos][:4]),
           "e": e, "px": c[pos - 1], "tov": tov[pos - 1] if np.isfinite(tov[pos - 1]) else 0.0,
           "ratio": r["ratio"] * 100, "bucket": purpose_bucket(r.get("purpose")),
           "hour": int((r.get("submit") or "00:00")[-5:-3] or 0)}
    for hkey in HOLDS:
        j = pos + hkey - 1
        row[f"h{hkey}"] = (c[j] / e - 1) * 100 - COST if j < len(c) else np.nan
    rows.append(row)

D = pd.DataFrame(rows)
D = D.sort_values(["ent", "ratio"], ascending=[True, False]).drop_duplicates(["tk", "ent"], keep="first")
print(f"[events] 価格ジョイン後 {len(D):,}イベント（同銘柄同日を集約）")


def show(d: pd.DataFrame, lab: str):
    if len(d) < 30:
        print(f"  {lab:<26} n={len(d):>5} — 判定不能")
        return
    parts = [f"  {lab:<26} n={len(d):>5}"]
    for hkey in HOLDS:
        x = d[f"h{hkey}"].dropna()
        if not len(x):
            continue
        neg = -x[x <= 0].sum()
        pf = x[x > 0].sum() / neg if neg else np.inf
        parts.append(f"| {hkey:>2}日 {x.mean():+.2f}% PF{pf:.2f}")
    h1 = d[d.y <= 2023]["h10"].dropna()
    h2 = d[d.y >= 2024]["h10"].dropna()
    parts.append(f"| 10日前半{h1.mean():+.2f}/後半{h2.mean():+.2f}")
    print(" ".join(parts))


print("\n=== 全体・保有目的別（平均リターン%・コスト0.2%込み） ===")
show(D, "全部")
for b in ("支配・提案系", "純投資", "政策投資", "その他"):
    show(D[D.bucket == b], b)

print("\n=== 保有割合帯（全体） ===")
for lo, hi, lab in ((5, 6, "5-6%"), (6, 8, "6-8%"), (8, 100, "8%以上")):
    show(D[(D.ratio >= lo) & (D.ratio < hi)], lab)

print("\n=== 流動性・株価フロア（全体・h10で最良の切り口を後で交差） ===")
for f, lab in ((1e8, "代金1億+"), (3e8, "代金3億+"), (1e9, "代金10億+")):
    show(D[D.tov >= f], lab)
show(D[D.px >= 300], "株価300円+")
show(D[D.px < 300], "株価300円未満")

print("\n=== 提出時刻（引け後提出=翌寄りが最初の反応 vs 場中提出） ===")
show(D[D.hour >= 15], "15時以降提出")
show(D[D.hour < 15], "15時前提出")

# 上位依存（全体h10）
x = D.dropna(subset=["h10"]).copy()
x["contrib"] = x["h10"]
top3 = x.groupby("tk")["h10"].sum().nlargest(3)
print(f"\n上位3銘柄({', '.join(top3.index)})のh10合計寄与: {top3.sum():+.0f}%ポイント / 全体合計 {x['h10'].sum():+.0f}%ポイント")

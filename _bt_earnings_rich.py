# -*- coding: utf-8 -*-
"""_bt_earnings_rich.py — 決算持ち越しの「未接続の軸」を10年イベント表に配線する（2026-07-26）。

背景: 決算持ち越しは既存ツマミ（枠数/PEAD閾値/流動性/価格上限/prev_gap/場中/ショート）を
      検証し尽くして「フロンティア到達」としたが、スイングで両期間頑健と確定した3つの構造
      （①業種分散cap ②信用買残回転フィルタ ③ボラ正規化）は一度も横展開していない。
      本スクリプトはその3軸を判定できるようにイベント表を拡張するだけ（本番コード無変更）。

出力: _earnings_events_rich.csv
  base : ticker d0 year price gap ret5 rsi runup5 tov20(中央値)
  new  : atr_pct(ATR14/終値%) adv20(20日平均代金) sector(S33) days_cover(買残回転日数) ratio(信用倍率)

実行: python -X utf8 _bt_earnings_rich.py
"""
from __future__ import annotations

import json
import pickle
from datetime import timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
from screener import is_etf_ticker, calc_rsi

OLD_FROM, OLD_TO = "2016-10-01", "2021-12-31"
# 「翌営業日」とみなせる暦日の上限。年末年始(12/30〜1/3)やGWを跨いでも実測で最大6日なので
# 10日あれば正常な連休は全部通り、キャッシュの穴（数ヶ月〜数年）だけを弾ける。
GAP_MAX_DAYS = 10

print("[load] キャッシュ読込中...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
name_map = dict(old["name_map"]); name_map.update(new["name_map"])
SEC: dict = json.load(open("sector33_map.json", encoding="utf-8"))
MAR: dict = pickle.load(open("_margin_10y_full.pkl", "rb"))
print(f"[load] pkl旧{len(old['all_data'])} 新{len(new['all_data'])} / 業種{len(SEC)} / 信用{len(MAR)}", flush=True)


def merged(tk: str) -> pd.DataFrame | None:
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    df = pd.concat(dfs).sort_index()
    return df[~df.index.duplicated(keep="last")]


# ── ① 旧期間(2016H2-2021)のイベントを構築（_bt_earnings_10y と同一定義 + ret5） ──
cal_old = json.load(open("earnings_calendar_2016_2021.json", encoding="utf-8"))
cal_old.pop("_done_days", None)
rows = []
for tk, dates in cal_old.items():
    df = merged(tk)
    if df is None:
        continue
    name = name_map.get(tk)
    if name is None or is_etf_ticker(tk, name):
        continue
    close = df["Close"].astype(float); openp = df["Open"].astype(float)
    tov = close * df["Volume"].astype(float)
    idx_str = df.index.strftime("%Y-%m-%d")
    pos_map = {s: i for i, s in enumerate(idx_str)}
    n = len(df)
    for disc in dates:
        if not (OLD_FROM <= disc <= OLD_TO):
            continue
        p = pos_map.get(disc)
        if p is None:
            loc = df.index.searchsorted(pd.Timestamp(disc), side="right") - 1
            if loc < 0:
                continue
            p = int(loc)
        if p < 25 or p + 1 >= n:
            continue
        c0 = float(close.iloc[p]); o1 = float(openp.iloc[p + 1])
        if not (c0 > 0 and o1 > 0):
            continue
        # 【2026-07-29 重大バグ修正】旧/新キャッシュを concat しているため、銘柄によっては
        # 途中に数年の穴がある（例 7550.T 旧2021-10-01で終わり新2025-10-02から＝4年欠測）。
        # iloc[p+1] は「次の行」であって「翌営業日」ではないので、穴をまたぐと4年分の
        # 値上がりを一晩のギャップとして計上してしまう（7550.T gap+103.7% / 9434.T gap+5156%）。
        # 実害: 汚染7玉で10年+1,069万→+670万・前半+209万→-109万・勝ち9→8年と、
        #       3割超が水増しされていた。暦日で連続性を確認して弾く。
        if (df.index[p + 1] - df.index[p]).days > GAP_MAX_DAYS:
            continue
        seq = close.iloc[max(0, p - 90):p + 1].dropna()
        rsi = calc_rsi(seq)
        if rsi is None or len(seq) < 6:
            continue
        rows.append({
            "ticker": tk, "d0": idx_str[p], "year": int(idx_str[p][:4]), "price": c0,
            "gap": (o1 / c0 - 1) * 100,
            # ret5 も同じ理由で暦日の連続性を確認する（5営業日＝通常7暦日以内、
            # 年末年始/GWを跨いでも14日を超えることはない）。
            "ret5": ((float(close.iloc[p + 5]) / c0 - 1) * 100
                     if (p + 5 < n and (df.index[p + 5] - df.index[p]).days <= GAP_MAX_DAYS * 3)
                     else np.nan),
            "rsi": rsi,
            "runup5": (c0 / float(seq.iloc[-6]) - 1) * 100 if float(seq.iloc[-6]) > 0 else np.nan,
            "tov20": float(tov.iloc[max(0, p - 19):p + 1].median()),
        })
E_old = pd.DataFrame(rows).drop_duplicates(subset=["ticker", "d0"])
print(f"[events] 旧期間 {len(E_old):,}件 ({E_old['d0'].min()}〜{E_old['d0'].max()})", flush=True)

# ── ② 新期間は既存BTと同一イベント（earnings_hold_events.csv）をそのまま使う ──
E_new = pd.read_csv("earnings_hold_events.csv")[
    ["ticker", "d0", "year", "price", "gap", "ret5", "rsi", "runup5", "tov20"]]
print(f"[events] 新期間 {len(E_new):,}件 ({E_new['d0'].min()}〜{E_new['d0'].max()})", flush=True)

E = pd.concat([E_old, E_new], ignore_index=True)
# 旧期間側は上で dedupe 済みだが、新期間側と結合後にも重複が残っていた（2026-07-29に59件確認。
# 同一(ticker,d0)が最大10行あり、その分だけ枠を食って選定を歪めていた）。ここで一度だけ潰す。
_before = len(E)
E = E.drop_duplicates(subset=["ticker", "d0"], keep="first").reset_index(drop=True)
if len(E) != _before:
    print(f"[dedupe] 重複 {_before - len(E):,}件を除去 → {len(E):,}件", flush=True)

# ── 一晩のギャップとしてあり得ない値を落とす（2026-07-29）──
# 暦日ガードを入れてもなお2種類が残る:
#   ①キャッシュの継ぎ目(2021-10-01→10-04)そのもの。旧/新で株式分割の調整基準が違い、
#     連続した営業日なのに価格が2倍に飛ぶ（4394.T 544→1,104）。暦日では検出できない。
#   ②ストップ高連続で値幅制限が拡大した実際の値動き（3041.T 674→1,074→2,274）。
#     こちらは本物だが、+100%のギャップを当てにするBTは実運用の再現にならない。
# どちらも「大引けで買って翌寄りで売る」戦略の期待値を歪めるので、両方まとめて落とす。
# 日本株の通常の値幅制限は±20〜30%程度なので、30%を超える一晩の変化は稀な例外として扱う。
GAP_SANE = 30.0
_b2 = len(E)
E = E[E["gap"].abs() <= GAP_SANE].reset_index(drop=True)
if len(E) != _b2:
    print(f"[sanity] |gap|>{GAP_SANE:.0f}% の {_b2 - len(E):,}件を除去 → {len(E):,}件", flush=True)
E["dt"] = pd.to_datetime(E["d0"])

# ── ③ ATR%・20日平均代金を銘柄ごとに一括計算して join ──
print("[calc] ATR%/ADV20 を計算中...", flush=True)
atr_map: dict[str, pd.Series] = {}
adv_map: dict[str, pd.Series] = {}
for tk in E["ticker"].unique():
    df = merged(tk)
    if df is None or len(df) < 30:
        continue
    high = df["High"].astype(float); low = df["Low"].astype(float)
    close = df["Close"].astype(float)
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    atr_map[tk] = (tr.rolling(14).mean() / close * 100)
    adv_map[tk] = (close * df["Volume"].astype(float)).rolling(20).mean()

atr_vals, adv_vals = [], []
for tk, dt in zip(E["ticker"].to_numpy(), E["dt"].to_numpy()):
    s = atr_map.get(tk); a = adv_map.get(tk)
    atr_vals.append(float(s.get(dt, np.nan)) if s is not None else np.nan)
    adv_vals.append(float(a.get(dt, np.nan)) if a is not None else np.nan)
E["atr_pct"] = atr_vals
E["adv20"] = adv_vals

# ── ④ 業種（S33） ──
E["sector"] = E["ticker"].map(SEC)

# ── ⑤ 信用買残（週末残高・公表ラグ4日で安全側） ──
print("[calc] 信用買残を join 中...", flush=True)
dc_vals, ratio_vals = [], []
for tk, dt, px, adv in zip(E["ticker"], E["dt"], E["price"], E["adv20"]):
    mdf = MAR.get(tk[:4])
    if mdf is None or len(mdf) == 0:
        dc_vals.append(np.nan); ratio_vals.append(np.nan); continue
    m = mdf[mdf.index <= (dt - timedelta(days=4))]
    if m.empty:
        dc_vals.append(np.nan); ratio_vals.append(np.nan); continue
    last = m.iloc[-1]
    lv = float(last.get("LongVol") or np.nan); sv = float(last.get("ShrtVol") or np.nan)
    ratio_vals.append(lv / sv if (np.isfinite(lv) and np.isfinite(sv) and sv > 0) else np.nan)
    dc_vals.append(lv * px / adv if (np.isfinite(lv) and np.isfinite(adv) and adv > 0) else np.nan)
E["days_cover"] = dc_vals
E["ratio"] = ratio_vals

E.drop(columns=["dt"]).to_csv("_earnings_events_rich.csv", index=False)
print(f"\n[save] _earnings_events_rich.csv {len(E):,}件")
for c in ("atr_pct", "adv20", "sector", "days_cover", "ratio"):
    print(f"  {c:<12} 欠損 {E[c].isna().mean() * 100:5.1f}%")

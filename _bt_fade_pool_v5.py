# -*- coding: utf-8 -*-
"""_bt_fade_pool_v5.py — 張り付き除外の閾値を振るためのプール（2026-07-31）。

v2は rng>5% でプールを作っていたので「今より緩める方向」が測れなかった。
ここでは張り付き除外を一切掛けずに集め、閾値は検証側で振る。
併せて「翌日の寄りが値幅制限の上限に張り付いていたか」も記録する（＝現実に売れない玉）。

なぜ作り直すか: これまでの検証は `_fade_deep.pkl` の上で行っていたが、監査
（_audit_fade_parity.py）で**BT側に3つの誤り**が見つかった。
  ①ETF判定に screener.is_etf_ticker を使い、実在株10銘柄（ブルボン/ミネベアミツミ等）を落としていた
  ②出来高比 vr の分母がシグナル日を含む20日（本番はシグナル日を除く20日）
  ③株数を翌日始値で計算（本番は前日終値。発注時に始値は分からない）
さらに本番側も株価300円下限を撤廃したので、**土台が変わった＝過去の棄却判断は全部やり直し**。

このプールの定義（本番 daily_top_fades と完全に一致させる）:
  貸借○ / 前日+5%以上（閾値検証のため本番のGO=6%より緩く取る）/ 20日代金中央値3億以上 /
  張り付き除外(レンジ>5%) / 株価は上限のみ(100株が予算内) / 20日平均出来高10万株以上 /
  ETFは「銘柄マスタに名前が無い＝真のETF」だけを除外
出力: _fade_pool_v5.pkl

実行: python -X utf8 _bt_fade_pool_v2.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd

CAP, TOV_MIN, STICKY_MIN, GAIN_FLOOR = 500_000, 3e8, 0.0, 5.0

print("[load] 読込中...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
YRS = sorted(ISS)
nm = dict(old["name_map"]); nm.update(new["name_map"])
try:
    SEC = json.load(open("sector33_map.json", encoding="utf-8"))
except Exception:
    SEC = {}
# 決算日（決算跨ぎの検証用）。d0=決算発表日。
try:
    _E = pd.read_csv("_earnings_events_rich2.csv", usecols=["ticker", "d0"])
    EARN = set(zip(_E.ticker, _E.d0))
    EARN_BY_TK: dict[str, set] = {}
    for t, d in EARN:
        EARN_BY_TK.setdefault(t, set()).add(d)
except Exception:
    EARN_BY_TK = {}


_LIM = [(100,30),(200,50),(500,80),(700,100),(1000,150),(1500,300),(2000,400),(3000,500),
        (5000,700),(7000,1000),(10000,1500),(15000,3000),(20000,4000),(30000,5000),(50000,7000),(70000,10000)]


def _lim_up(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return base + w
    return base + 10000.0


def iss_for(y):
    return ISS[min(YRS, key=lambda a: (abs(a - y), a))]


def merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


# 日経（地合い）
nk = merge("1321.T")
nkc = nk["Close"].astype(float)
nk_below = {d.strftime("%Y-%m-%d"): bool(v) for d, v in (nkc < nkc.rolling(25).mean()).items()}
nk_chg = {d.strftime("%Y-%m-%d"): float(v) for d, v in (nkc.pct_change() * 100).items()}

rows = []
for tk in set(old["all_data"]) | set(new["all_data"]):
    name = nm.get(tk)
    if name is None:            # 名前が引けない＝真のETF/ETN（本番は銘柄マスタで同じ判定）
        continue
    df = merge(tk)
    if df is None or len(df) < 40:
        continue
    o = df["Open"].astype(float).to_numpy(); c = df["Close"].astype(float).to_numpy()
    h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy()
    v = df["Volume"].astype(float).to_numpy()
    cs, vs, hs, ls = map(pd.Series, (c, v, h, l))
    tov = (cs * vs).rolling(20).median().to_numpy()
    vma = vs.shift(1).rolling(20).mean().to_numpy()       # 本番＝シグナル日を除く20日
    pc = cs.shift(1)
    tr = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1)
    atr = (tr.rolling(14).mean() / cs * 100).to_numpy()
    ma25 = cs.rolling(25).mean().to_numpy()
    ma5 = cs.rolling(5).mean().to_numpy()
    idx = df.index
    ed = EARN_BY_TK.get(tk, set())
    sec = SEC.get(tk, "")
    for t in range(26, len(c) - 1):
        if not (c[t - 1] > 0 and c[t] > 0):
            continue
        gain = (c[t] / c[t - 1] - 1) * 100
        if gain < GAIN_FLOOR or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
            continue
        rng = (h[t] - l[t]) / c[t]
        pass  # 張り付き除外はプールでは掛けない（閾値を後から振るため）
        y = idx[t + 1].year
        if iss_for(y).get(str(tk)[:4], "?") != "2":
            continue
        if not (o[t + 1] > 0 and c[t + 1] > 0) or c[t] * 100 > CAP:
            continue
        if not (np.isfinite(vma[t]) and vma[t] >= 100_000):
            continue
        d0 = idx[t].strftime("%Y-%m-%d")
        # 直近5営業日以内に決算があったか（決算跨ぎ・ドリフト中かの判定用）
        recent = [idx[k].strftime("%Y-%m-%d") for k in range(max(0, t - 4), t + 1)]
        rows.append({
            "sig": d0, "ent": idx[t + 1].strftime("%Y-%m-%d"), "y": y, "ticker": tk, "sector": sec,
            "gain": gain, "px": c[t], "o1": o[t + 1], "c1": c[t + 1],
            "h1": h[t + 1], "l1": l[t + 1],
            "atr": atr[t], "dev": (c[t] / ma25[t] - 1) * 100 if ma25[t] > 0 else 0.0,
            "dev5": (c[t] / ma5[t] - 1) * 100 if ma5[t] > 0 else 0.0,
            "vr": v[t] / vma[t], "tov": tov[t], "vol_avg": vma[t],
            "rng": rng * 100,
            "pos": (c[t] - l[t]) / (h[t] - l[t]) * 100 if h[t] > l[t] else 50.0,
            "body": (c[t] - o[t]) / (h[t] - l[t]) * 100 if h[t] > l[t] else 0.0,
            "gu": (o[t + 1] / c[t] - 1) * 100,
            "gain2": (c[t] / c[t - 2] - 1) * 100 if c[t - 2] > 0 else 0.0,
            "prev_up": (c[t - 1] / c[t - 2] - 1) * 100 if c[t - 2] > 0 else 0.0,
            "up_days": int(sum(1 for k in range(t - 4, t + 1) if k > 0 and c[k] > c[k - 1])),
            "nk_below": nk_below.get(d0, False), "nk_chg": nk_chg.get(d0, 0.0),
            "dow": idx[t + 1].dayofweek,
            "earn5": bool(ed & set(recent)),          # 直近5営業日に決算があった
            # 翌日の寄りが値幅制限の上限に張り付いていたか＝現実には空売りできない玉。
            # BTが「売れない玉」を約定扱いしていないかの監査用（[[feedback_bt_fillability_limit]]）。
            "o1_is_limit": bool(o[t + 1] >= _lim_up(c[t]) - 1e-9),
            "o1_locked": bool(o[t + 1] >= _lim_up(c[t]) - 1e-9 and h[t + 1] == l[t + 1]),
        })
D = pd.DataFrame(rows).sort_values(["sig", "ticker"])
D["pnl"] = (D.o1 - D.c1) / D.o1 * 100
D.to_pickle("_fade_pool_v5.pkl")
print(f"[save] _fade_pool_v5.pkl {len(D):,}件 / {D.sig.nunique():,}日 ({D.sig.min()}〜{D.sig.max()})")
print(f"  gain>=6の件数 {int((D.gain>=6).sum()):,} / 業種が付いた割合 {(D.sector!='').mean()*100:.0f}%"
      f" / 決算5日内 {D.earn5.mean()*100:.1f}%")

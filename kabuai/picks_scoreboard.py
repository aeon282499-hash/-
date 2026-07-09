# -*- coding: utf-8 -*-
"""picks_scoreboard.py — ④「買い候補の最近の調子」メーターの共通計算ロジック。

build_data.py（CIで毎日 fresh 計算）と make_picks_scoreboard.py（手元pklで長期版・
フォールバック）の両方から `compute()` を呼ぶ。v3.1の✅買い候補（反発/買い集め・
売買代金10億以上・¥1000以上・業種cap3・EV順）を過去日に忠実再現し、翌朝寄り→8営業日・
損切り-12%・利確なしで実成績を集計、直近RECENT_WEEKS週の地合い(regime)を返す。
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

RECENT_WEEKS = 8
SL, HOLD = 12.0, 8
PICK_MIN_OKU, PRICE_MIN, SECTOR_CAP = 10.0, 1000, 3
# 期待値（シグナル種別のexit平均・cap3のタイブレーク＆EV順に使う。web/index.htmlのEV順と整合）
EV_MAP = {"strong_reversal": 2.68, "strong_accum": 1.71, "accum": 1.19, "reversal": 0.88}
LABELS = {"strong_reversal": ("⚡", "強反転"), "reversal": ("🔄", "反転"),
          "strong_accum": ("🔥", "強買い集め"), "accum": ("📈", "買い集め")}


def _name(name_map: dict, tk: str):
    for k in (tk, tk.replace(".T", ""), tk[:4]):
        if k in name_map:
            return str(name_map[k])
    return None


def _sim8(ei, e, ln, cn, n):
    last = ei + HOLD - 1
    if last > n - 1:
        return None
    stop = e * (1 - SL / 100)
    for k in range(ei, last + 1):
        if ln[k] <= stop:
            return -SL
    return (cn[last] - e) / e * 100


def _stat(x: pd.DataFrame) -> dict:
    n = len(x)
    if n == 0:
        return {"n": 0, "win": 0, "avg": 0}
    p = x["pnl"].to_numpy()
    return {"n": int(n), "win": round((p > 0).mean() * 100, 1), "avg": round(float(p.mean()), 2)}


def compute(data: dict, name_map: dict, sec_map: dict, as_of: str, skip=None) -> dict | None:
    """
    data: {ticker: DataFrame(OHLCV)}。呼び出し側で株式ユニバース限定済みなら skip=None でよい
          （build_data のCIは load_jquants_api が master でETF除外済み）。手元pklから呼ぶ時は
          skip=is_etf_ticker を渡してETFを除く。
    戻り値: スコアボード dict（発動が無ければ None）。
    """
    since = f"{(as_of or '2026')[:4]}-01-01"
    rows = []
    for tk, df in data.items():
        name = _name(name_map, tk)
        if name is None:
            continue
        if skip is not None and skip(tk, name):
            continue
        df = df.sort_index()
        if len(df) < 80:
            continue
        cl = df["Close"].astype(float); hi = df["High"].astype(float)
        lo = df["Low"].astype(float); vo = df["Volume"].astype(float); op = df["Open"].astype(float)
        delta = cl.diff()
        ag = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        al = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).fillna(50.0)
        vr = vo.rolling(5).mean() / (vo.rolling(25).mean() + 1e-9)
        rng = (hi - lo) / cl
        rz = rng.rolling(5).mean() / (rng.rolling(20).median() + 1e-9)
        r5f = cl / cl.shift(5) - 1.0
        r5 = r5f * 100; r1 = (cl / cl.shift(1) - 1.0) * 100; r20 = (cl / cl.shift(20) - 1.0) * 100
        power = (np.where(r5f >= 0, 1.0, -1.0) * ((vr - 1.0) * 3.0 + (rz - 1.0) * 2.0)).clip(-3.0, 8.0)
        turn = cl * vo.rolling(25).mean()
        sa = ((vr >= 2.0) & (power >= 4.0) & (r5 >= 5.0) & (rsi < 85)).to_numpy()
        ac = ((vr >= 1.5) & (power >= 2.0) & (r5 >= 2.0) & (rsi < 80)).to_numpy()
        sr = ((r20 < -10) & (r5 >= 4.0) & (r1 > 0) & (rsi < 55)).to_numpy()
        rv = ((r20 < 0) & (r5 > 0) & (r1 > 0) & (rsi < 55)).to_numpy()
        buy = sa | ac | sr | rv
        on = op.to_numpy(); ln = lo.to_numpy(); cn = cl.to_numpy(); tn = turn.to_numpy()
        n = len(cn); dates = df.index.strftime("%Y-%m-%d")
        sec = sec_map.get(tk.replace(".T", "")) or ("_" + tk)
        for t in np.where(buy)[0]:
            ei = t + 1
            if ei >= n or dates[ei] < since:
                continue
            if not (tn[t] >= PICK_MIN_OKU * 1e8) or not (cn[t] >= PRICE_MIN):
                continue
            e = on[ei]
            if not (e > 0) or np.isnan(e):
                continue
            pnl = _sim8(ei, e, ln, cn, n)
            if pnl is None:
                continue
            sigs = [k for k, m in (("strong_reversal", sr), ("strong_accum", sa),
                                   ("accum", ac), ("reversal", rv)) if m[t]]
            rows.append({"entry": dates[ei], "sector": sec,
                         "ev": max(EV_MAP.get(k, 0) for k in sigs),
                         "main": max(sigs, key=lambda k: EV_MAP.get(k, 0)), "pnl": pnl})

    D = pd.DataFrame(rows)
    if D.empty:
        return None
    # 日次でフロントの選定を再現（EV降順→業種cap3）
    picks = []
    for _d, g in D.groupby("entry"):
        g = g.sort_values("ev", ascending=False)
        cnt = defaultdict(int)
        for _, r in g.iterrows():
            if cnt[r["sector"]] >= SECTOR_CAP:
                continue
            cnt[r["sector"]] += 1
            picks.append(r)
    P = pd.DataFrame(picks)
    P["date"] = pd.to_datetime(P["entry"])
    P["week"] = P["date"].dt.strftime("%G-W%V")
    P["month"] = P["date"].dt.strftime("%Y-%m")

    weeks = sorted(P["week"].unique())
    recent_weeks = weeks[-RECENT_WEEKS:]
    R = P[P["week"].isin(recent_weeks)]
    rec = _stat(R)
    avg = rec["avg"]
    regime = "cold" if avg < -0.3 else ("warm" if avg > 0.5 else "flat")

    months = [dict(m=m, **_stat(P[P["month"] == m])) for m in sorted(P["month"].unique())]
    by_signal = []
    for k in ["strong_reversal", "strong_accum", "accum", "reversal"]:
        s = _stat(R[R["main"] == k])
        em, lb = LABELS[k]
        by_signal.append({"k": k, "emoji": em, "label": lb, **s})

    from datetime import datetime
    return {
        "generated": datetime.now().strftime("%Y-%m-%d"),
        "as_of": str(as_of or ""),
        "recent_weeks": RECENT_WEEKS,
        "recent": {"n": rec["n"], "win": rec["win"], "avg": rec["avg"],
                   "since_week": recent_weeks[0] if recent_weeks else None},
        "regime": regime,
        "months": months,
        "by_signal": by_signal,
        "note": ("✅買い候補（反発/買い集め・板の厚い10億以上・業種分散）を過去日に再現し、"
                 "翌朝寄り→8営業日・損切り-12%で集計した実績。上位数銘柄でなく候補全体の平均。"
                 "理論値（手数料・スリッページ未考慮）で将来を保証しません。"),
    }

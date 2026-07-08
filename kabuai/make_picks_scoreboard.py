# -*- coding: utf-8 -*-
"""make_picks_scoreboard.py — ④「買い候補の直近の調子メーター」用データを事前計算しコミット。

v3.1の✅買い候補(反発/買い集め・10億・¥1000・業種cap3・EV順)を過去日に忠実再現し、
8日/-12%の実成績（勝率/平均/月別/主シグナル別）＋直近の地合い判定(regime)を
`picks_scoreboard.json` に書き出す。build_data.py が存在すれば latest.json に注入。
track_longterm.json / rebound_history.json と同じく **月1手動で再生成→コミット**。

実行: python make_picks_scoreboard.py
"""
import json
import os
import pickle
import sys
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # 親dir(screener)
load_dotenv()
from screener import is_etf_ticker  # noqa: E402

SINCE = "2026-01-01"
RECENT_WEEKS = 8
SL, HOLD = 12.0, 8
PICK_MIN_OKU, PRICE_MIN, SECTOR_CAP = 10.0, 1000, 3
EV_MAP = {"strong_reversal": 2.68, "strong_accum": 1.71, "accum": 1.19, "reversal": 0.88}
LABELS = {"strong_reversal": ("⚡", "強反転"), "reversal": ("🔄", "反転"),
          "strong_accum": ("🔥", "強買い集め"), "accum": ("📈", "買い集め")}


def sim8(ei, e, ln, cn, n):
    last = ei + HOLD - 1
    if last > n - 1:
        return None
    stop = e * (1 - SL / 100)
    for k in range(ei, last + 1):
        if ln[k] <= stop:
            return -SL
    return (cn[last] - e) / e * 100


def _stat(x):
    n = len(x)
    if n == 0:
        return {"n": 0, "win": 0, "avg": 0}
    p = x["pnl"].to_numpy()
    return {"n": int(n), "win": round((p > 0).mean() * 100, 1), "avg": round(float(p.mean()), 2)}


def main():
    pkl = os.path.join(os.path.dirname(HERE), "jquants_cache.pkl")
    c = pickle.load(open(pkl, "rb"))
    data, name_map = c["all_data"], c["name_map"]
    try:
        secraw = json.load(open(os.path.join(os.path.dirname(HERE), "sector33_map.json"), encoding="utf-8"))
        SEC = {str(k)[:4]: v for k, v in secraw.items()}
    except Exception:
        SEC = {}

    rows = []
    for tk, df in data.items():
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):
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
        sec = SEC.get(tk.replace(".T", "")) or ("_" + tk)
        for t in np.where(buy)[0]:
            ei = t + 1
            if ei >= n or dates[ei] < SINCE:
                continue
            if not (tn[t] >= PICK_MIN_OKU * 1e8) or not (cn[t] >= PRICE_MIN):
                continue
            e = on[ei]
            if not (e > 0) or np.isnan(e):
                continue
            pnl = sim8(ei, e, ln, cn, n)
            if pnl is None:
                continue
            sigs = [k for k, m in (("strong_reversal", sr), ("strong_accum", sa),
                                   ("accum", ac), ("reversal", rv)) if m[t]]
            rows.append({"entry": dates[ei], "sector": sec,
                         "ev": max(EV_MAP.get(k, 0) for k in sigs),
                         "main": max(sigs, key=lambda k: EV_MAP.get(k, 0)), "pnl": pnl})

    D = pd.DataFrame(rows)
    picks = []
    for d, g in D.groupby("entry"):
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
    # 地合い判定（1件あたり平均で）: 明確なプラス=追い風 / 明確なマイナス=不調 / 中間=ふつう
    avg = rec["avg"]
    regime = "cold" if avg < -0.3 else ("warm" if avg > 0.5 else "flat")

    months = [dict(m=m, **_stat(P[P["month"] == m])) for m in sorted(P["month"].unique())]
    by_signal = []
    for k in ["strong_reversal", "strong_accum", "accum", "reversal"]:
        s = _stat(R[R["main"] == k])
        em, lb = LABELS[k]
        by_signal.append({"k": k, "emoji": em, "label": lb, **s})

    out = {
        "generated": datetime.now().strftime("%Y-%m-%d"),
        "as_of": str(c.get("end", "")),
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
    path = os.path.join(HERE, "picks_scoreboard.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[scoreboard] {path}")
    print(f"  as_of {out['as_of']} / 直近{RECENT_WEEKS}週: {rec['n']}件 勝率{rec['win']}% 平均{rec['avg']:+}% → regime={regime}")
    for m in months:
        print(f"    {m['m']}: {m['n']}件 勝率{m['win']}% 平均{m['avg']:+}%")
    for b in by_signal:
        print(f"    {b['label']}: {b['n']}件 勝率{b['win']}% 平均{b['avg']:+}%")


if __name__ == "__main__":
    main()

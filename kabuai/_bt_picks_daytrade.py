# -*- coding: utf-8 -*-
"""_bt_picks_daytrade.py — ✅買い候補(v3.1再現)を「寄り成行→当日引け」でデイトレしたら？
ユーザー質問(2026-07-16)「買い推奨銘柄を売り(or買い)で当日成で入って引けで手仕舞いできない？」の実測。

bt_recent_picks.py の候補再現ロジックをそのまま流用し、出口だけ差し替え:
  A) デイトレ買い:   T+1寄り買い → T+1引け売り
  B) デイトレ空売り: T+1寄り空売り → T+1引け買戻し（=Aの符号反転・コスト前）
  C) 参考=現行運用:  T+1寄り買い → 8営業日/-12%損切り（同一サンプル）
年別に 件数/勝率/平均/PF/累積 を出す。空売りの踏み上げテール(p99/最悪)も出す。
実行: python _bt_picks_daytrade.py
"""
import json
import os
import pickle
import sys
from collections import defaultdict

import numpy as np
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()
from screener import is_etf_ticker  # noqa: E402

SINCE = "2022-01-01"
SL, HOLD = 12.0, 8
PICK_MIN_OKU, PRICE_MIN, SECTOR_CAP = 10.0, 1000, 3
EV_MAP = {"strong_reversal": 2.68, "strong_accum": 1.71, "accum": 1.19, "reversal": 0.88}


def sim8(ei, e, ln, cn, n):
    last = ei + HOLD - 1
    if last > n - 1:
        return None
    stop = e * (1 - SL / 100)
    for k in range(ei, last + 1):
        if ln[k] <= stop:
            return -SL
    return (cn[last] - e) / e * 100


def main():
    c = pickle.load(open("../jquants_cache.pkl", "rb"))
    data, name_map = c["all_data"], c["name_map"]
    try:
        secraw = json.load(open("../sector33_map.json", encoding="utf-8"))
        SEC = {str(k)[:4]: v for k, v in secraw.items()}
    except Exception:
        SEC = {}
    print(f"[data] pkl 〜{c['end']} / sector33 {len(SEC)}銘柄 / 起点{SINCE}")

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
        on = op.to_numpy(); ln = lo.to_numpy(); cn = cl.to_numpy()
        hn = hi.to_numpy(); tn = turn.to_numpy()
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
            day = (cn[ei] - e) / e * 100          # 寄り→引け（買い方向）
            gap = (e - cn[t]) / cn[t] * 100       # 前日終値→寄りギャップ
            hi_run = (hn[ei] - e) / e * 100       # 寄り→当日高値（ショートの最大逆行）
            flat = (hn[ei] == ln[ei])             # 寄りから値幅なし＝張り付き級（約定疑義）
            pnl8 = sim8(ei, e, ln, cn, n)
            sigs = [k for k, m in (("strong_reversal", sr), ("strong_accum", sa),
                                   ("accum", ac), ("reversal", rv)) if m[t]]
            ev = max(EV_MAP.get(k, 0) for k in sigs)
            main_sig = max(sigs, key=lambda k: EV_MAP.get(k, 0))
            rows.append({"entry": dates[ei], "ticker": tk, "sector": sec, "ev": ev,
                         "main": main_sig, "day": day, "gap": gap, "hi_run": hi_run,
                         "flat": flat, "pnl8": pnl8})

    D = pd.DataFrame(rows)
    picks = []
    for d, g in D.groupby("entry"):
        g = g.sort_values("ev", ascending=False)
        cnt = defaultdict(int); sel = []
        for _, r in g.iterrows():
            if cnt[r["sector"]] >= SECTOR_CAP:
                continue
            cnt[r["sector"]] += 1
            sel.append(r)
        picks.extend(sel)
    P = pd.DataFrame(picks)
    P["year"] = pd.to_datetime(P["entry"]).dt.year

    def stat(p):
        p = p[~np.isnan(p)]
        n = len(p)
        if n == 0:
            return dict(n=0, win=0, avg=0, cum=0, pf=0)
        gain = p[p > 0].sum(); loss = -p[p < 0].sum()
        return dict(n=n, win=(p > 0).mean()*100, avg=p.mean(), cum=p.sum(),
                    pf=(gain/loss if loss > 0 else float("inf")))

    print(f"\n[picks] v3.1再現の買い候補: {len(P):,}件（{SINCE}〜）")
    print(f"  寄り張り付き級(高値=安値・約定疑義): {int(P['flat'].sum())}件 → 除外")
    P = P[~P["flat"]]

    fmt = "  {:<6}{:>6} | 勝率{:>5.1f}% 平均{:>+6.2f}% PF{:>5.2f} 累積{:>+8.0f}%"
    for label, col, sign in (("A) デイトレ買い(寄り→引け)", "day", 1),
                             ("B) デイトレ空売り(寄り空売り→引け買戻し・コスト前)", "day", -1),
                             ("C) 参考=現行8日/-12%保有", "pnl8", 1)):
        print(f"\n■ {label}")
        allp = sign * P[col].to_numpy(dtype=float)
        s = stat(allp)
        print(fmt.format("全期間", s["n"], s["win"], s["avg"], s["pf"], s["cum"]))
        for y, g in P.groupby("year"):
            s = stat(sign * g[col].to_numpy(dtype=float))
            print(fmt.format(str(y), s["n"], s["win"], s["avg"], s["pf"], s["cum"]))

    # ショートの踏み上げテール（寄り空売り→当日高値の最大逆行）
    hr = P["hi_run"].to_numpy(dtype=float)
    print("\n■ 空売りの日中最大逆行（寄り→高値・踏まれ幅）")
    print(f"  平均+{np.nanmean(hr):.2f}% / 中央値+{np.nanmedian(hr):.2f}% / "
          f"p95 +{np.nanpercentile(hr, 95):.2f}% / p99 +{np.nanpercentile(hr, 99):.2f}% / "
          f"最悪 +{np.nanmax(hr):.2f}%")
    over8 = (hr >= 8).mean() * 100
    print(f"  日中+8%以上踏まれる率: {over8:.1f}%（S高級の踏み上げ遭遇率）")

    # ギャップ情報（寄りでどれだけ高く始まるか）
    gp = P["gap"].to_numpy(dtype=float)
    print("\n■ 前日終値→翌寄りギャップ（参考）")
    print(f"  平均{np.nanmean(gp):+.2f}% / 中央値{np.nanmedian(gp):+.2f}%")

    # 型別（デイトレ買い方向・全期間）
    print("\n■ 型別のデイトレ買い(寄り→引け)・全期間")
    for k in ["strong_reversal", "strong_accum", "accum", "reversal"]:
        s = stat(P.loc[P["main"] == k, "day"].to_numpy(dtype=float))
        print(f"    {k:<16}: {s['n']:>5}件 勝率{s['win']:>5.1f}% 平均{s['avg']:>+6.2f}% PF{s['pf']:.2f}")


if __name__ == "__main__":
    main()

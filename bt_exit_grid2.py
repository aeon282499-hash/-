# -*- coding: utf-8 -*-
"""bt_exit_grid2.py — 未検証で残っていた出口/入口パラメータのフルグリッド（2026-06-11）。

検証済み: TP(3-5で5最強・STOP3固定下)・MAXHOLD(3が最適)・選定ランキング(現行最適)。
未検証: ①損切り幅STOP(-3%固定のまま一度もスイープされていない)
        ②RSI回復閾値(50固定) ③ギャップアップ日の見送り(check_gap_entryは未使用)
グリッド: STOP{2,2.5,3,3.5,4,5} × TP{4,5,6,7,なし} × RSI閾値{50,55,60,なし} × gap上限{なし,2%,1%}
評価: 本番同等の選定(勝ちやすさスコアtop5/日・保有中除外=現行出口で固定)に対し、
実OCO約定(2日目以降の寄りがstop/tp貫通なら寄り値)・翌朝寄りentry・MAXHOLD3日終値。
採用基準: 現行(STOP3/TP5/RSI50/gapなし)比で明確な改善＋前半(2022-23)後半(2024-26)同方向
＋年次頑健＋近傍が滑らか。合格したら backtest_range.py 公式クラウドBTで最終裏取り→本番。
実行: python bt_exit_grid2.py
"""
from __future__ import annotations

import json
import pickle
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from screener import is_etf_ticker

SINCE = "2022-01-01"
TURNOVER_MIN = 2e9
PRICE_MAX = 10_000.0
MAXH = 3
EARN_WIN = 3
CUR = (3.0, 5.0, 50.0, None)   # 現行: STOP3 / TP5 / RSI50 / gapフィルタなし


def load_earnings():
    cal = json.load(open("earnings_calendar.json", encoding="utf-8"))
    out = {}
    for tk, ds in cal.items():
        s = set()
        for x in ds:
            try:
                d = datetime.strptime(x, "%Y-%m-%d").date()
            except Exception:
                continue
            for off in range(-EARN_WIN, EARN_WIN + 1):
                s.add((d + timedelta(days=off)).strftime("%Y-%m-%d"))
        out[tk] = s
    return out


def collect() -> pd.DataFrame:
    c = pickle.load(open("jquants_cache.pkl", "rb"))
    data, name_map = c["all_data"], c["name_map"]
    earn = load_earnings()
    since_ts = pd.Timestamp(SINCE)
    rows = []
    nn = 0
    for tk, df in data.items():
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name) or df is None or len(df) < 40:
            continue
        nn += 1
        df = df.sort_index()
        o = df["Open"].astype(float); h = df["High"].astype(float)
        l = df["Low"].astype(float); cl = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
        ma25 = cl.rolling(25).mean()
        dev = ((cl - ma25) / ma25 * 100).round(2)
        pc = cl.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        rrx = ((h - l).shift(1) / atr.shift(1)).round(2)
        vrx = (v.shift(1) / v.shift(2).rolling(20).mean()).round(2)
        tov = cl.shift(1) * v.shift(1)
        atr_pct = atr / cl * 100
        cand = ((rsi <= 45) & (dev <= -1.5) & ((rrx >= 1.5) | (vrx >= 2.0))
                & (tov >= TURNOVER_MIN) & (atr_pct <= 3.0) & (cl <= PRICE_MAX))
        cand &= cl.index >= since_ts
        if not cand.fillna(False).any():
            continue
        on, hn, ln, cn, rn = o.to_numpy(), h.to_numpy(), l.to_numpy(), cl.to_numpy(), rsi.to_numpy()
        idx = cl.index; n = len(cn)
        ewin = earn.get(tk, set())
        for t in np.where(cand.fillna(False).to_numpy())[0]:
            p = t + 1
            if p + MAXH - 1 >= n:
                continue
            entry_day = idx[p].strftime("%Y-%m-%d")
            if entry_day in ewin:
                continue
            e = on[p]
            if not (e > 0) or np.isnan(e):
                continue
            rsi_t = rn[t]; dev_t = float(dev.iloc[t]); tov_t = float(tov.iloc[t])
            score = (1 / (1 + ((rsi_t - 38) / 8) ** 2) * 0.30
                     + 1 / (1 + ((dev_t + 3) / 2) ** 2) * 0.30
                     + np.log10(max(tov_t, 1) / 1e9 + 1) / 3 * 0.40)
            # 現行出口での保有日数(選定の保有中除外用・STOP3/TP5/RSI50)
            stop = e * 0.97; tpp = e * 1.05
            ex_off = MAXH - 1
            for k in range(MAXH):
                q = p + k
                if (k > 0 and on[q] > 0 and not np.isnan(on[q]) and (on[q] <= stop or on[q] >= tpp)) \
                        or ln[q] <= stop or hn[q] >= tpp or (not np.isnan(rn[q]) and rn[q] >= 50):
                    ex_off = k
                    break
            rows.append({
                "entry": entry_day, "year": idx[t].year, "ticker": tk,
                "score": score, "c0": cn[t], "e": e, "ex_off": ex_off,
                "o2": on[p + 1], "o3": on[p + 2],
                "h1": hn[p], "h2": hn[p + 1], "h3": hn[p + 2],
                "l1": ln[p], "l2": ln[p + 1], "l3": ln[p + 2],
                "c1": cn[p], "c2": cn[p + 1], "c3": cn[p + 2],
                "r1": rn[p], "r2": rn[p + 1], "r3": rn[p + 2],
            })
    D = pd.DataFrame(rows)
    print(f"[collect] {nn}銘柄走査 / 全候補 {len(D):,} 件")
    return D


def select_top5(D: pd.DataFrame) -> pd.DataFrame:
    days_all = sorted(D["entry"].unique())
    day_index = {d: i for i, d in enumerate(days_all)}
    Ds = D.sort_values(["entry", "score"], ascending=[True, False])
    open_until: dict = {}
    keep_idx = []
    for d, pool in Ds.groupby("entry", sort=True):
        open_until = {tk: u for tk, u in open_until.items() if u >= d}
        picked = 0
        for i, r in pool.iterrows():
            if picked >= 5:
                break
            if r["ticker"] in open_until:
                continue
            picked += 1
            exit_i = min(day_index[d] + int(r["ex_off"]), len(days_all) - 1)
            open_until[r["ticker"]] = days_all[exit_i]
            keep_idx.append(i)
    S = D.loc[keep_idx].reset_index(drop=True)
    print(f"[select] 本番同等top5選定後 {len(S):,} 件 / {S['entry'].nunique()}営業日")
    return S


def sim_combo(S, stop_pct, tp_pct, rsi_th, gap_max):
    e = S["e"].to_numpy()
    c0 = S["c0"].to_numpy()
    ret = np.full(len(S), np.nan)
    alive = np.ones(len(S), bool)
    if gap_max is not None:   # 寄りが前日終値比 +gap_max% 超なら見送り(エントリーしない)
        skip = (e / c0 - 1) * 100 > gap_max
        alive &= ~skip
    stop = e * (1 - stop_pct / 100)
    tpp = e * (1 + tp_pct / 100) if tp_pct is not None else np.full(len(S), np.inf)
    tpv = tp_pct if tp_pct is not None else np.nan
    for k, (okey, hkey, lkey, ckey, rkey) in enumerate(
            ((None, "h1", "l1", "c1", "r1"), ("o2", "h2", "l2", "c2", "r2"), ("o3", "h3", "l3", "c3", "r3"))):
        h = S[hkey].to_numpy(); l = S[lkey].to_numpy()
        c = S[ckey].to_numpy(); r = S[rkey].to_numpy()
        if okey is not None:   # 2日目以降: 寄りギャップ貫通は寄り値約定
            op = S[okey].to_numpy()
            gap_hit = alive & (op > 0) & ~np.isnan(op) & ((op <= stop) | (op >= tpp))
            ret[gap_hit] = (op[gap_hit] - e[gap_hit]) / e[gap_hit] * 100
            alive &= ~gap_hit
        sh = alive & (l <= stop)
        ret[sh] = -stop_pct
        alive &= ~sh
        th_ = alive & (h >= tpp)
        ret[th_] = tpv
        alive &= ~th_
        if rsi_th is not None:
            rx = alive & ~np.isnan(r) & (r >= rsi_th)
        else:
            rx = np.zeros(len(S), bool)
        if k == MAXH - 1:
            rx = alive   # 最終日は全決済
        ret[rx] = (c[rx] - e[rx]) / e[rx] * 100
        alive &= ~rx
    m = ~np.isnan(ret)
    return ret[m], S["year"].to_numpy()[m]


def metrics(ret, years):
    if ret.size < 100:
        return None
    g = ret[ret > 0].sum(); ls = ret[ret < 0].sum()
    pf = g / abs(ls) if ls < 0 else float("inf")
    e_m = ret[years <= 2023].mean(); l_m = ret[years >= 2024].mean()
    pos = tot = 0
    for y in np.unique(years):
        yy = ret[years == y]
        if yy.size >= 30:
            tot += 1
            pos += 1 if yy.mean() > 0 else 0
    return dict(n=ret.size, win=(ret > 0).mean() * 100, avg=ret.mean(), pf=pf,
                tot_ret=ret.sum(), early=e_m, late=l_m, pos=pos, ytot=tot)


def main():
    import os
    CACHE = "_exit_grid_cache.pkl"
    if os.path.exists(CACHE):
        D = pd.read_pickle(CACHE)
        print(f"[cache] {len(D):,} 件ロード")
    else:
        D = collect()
        D.to_pickle(CACHE)
    S = select_top5(D)

    stops = (2.0, 2.5, 3.0, 3.5, 4.0, 5.0)
    tps = (4.0, 5.0, 6.0, 7.0, None)
    rsis = (50.0, 55.0, 60.0, None)
    gaps = (None, 2.0, 1.0)

    res = []
    for sp in stops:
        for tp in tps:
            for rt in rsis:
                for gp in gaps:
                    ret, yr = sim_combo(S, sp, tp, rt, gp)
                    m = metrics(ret, yr)
                    if m:
                        m.update(stop=sp, tp=tp, rsi=rt, gap=gp)
                        res.append(m)
    R = pd.DataFrame(res)

    def fmt(r):
        tp = f"{r['tp']:.0f}" if pd.notna(r["tp"]) else "なし"
        rs = f"{r['rsi']:.0f}" if pd.notna(r["rsi"]) else "なし"
        gp = f"{r['gap']:.0f}%" if pd.notna(r["gap"]) else "なし"
        return (f"STOP{r['stop']:.1f}/TP{tp}/RSI{rs}/gap{gp}: n={r['n']:,} 勝率{r['win']:.1f}% "
                f"平均{r['avg']:+.3f}% PF{r['pf']:.2f} 累積{r['tot_ret']:+.0f}% "
                f"前半{r['early']:+.3f}/後半{r['late']:+.3f} 陽性年{r['pos']}/{r['ytot']}")

    cur = R[(R["stop"] == CUR[0]) & (R["tp"] == CUR[1]) & (R["rsi"] == CUR[2]) & (R["gap"].isna())]
    print("\n===== 現行 =====")
    print("  " + fmt(cur.iloc[0]))

    print("\n===== 平均リターン上位20（頑健性つき） =====")
    for _, r in R.sort_values("avg", ascending=False).head(20).iterrows():
        print("  " + fmt(r))

    print("\n===== PF上位10 =====")
    for _, r in R.sort_values("pf", ascending=False).head(10).iterrows():
        print("  " + fmt(r))

    # 1次元スライス（現行近傍の滑らかさ）
    print("\n===== 1次元感度（他は現行固定） =====")
    for sp in stops:
        r = R[(R["stop"] == sp) & (R["tp"] == 5.0) & (R["rsi"] == 50.0) & (R["gap"].isna())]
        if len(r):
            print("  " + fmt(r.iloc[0]))
    for tp in tps:
        r = R[(R["stop"] == 3.0) & ((R["tp"] == tp) if tp is not None else R["tp"].isna())
              & (R["rsi"] == 50.0) & (R["gap"].isna())]
        if len(r):
            print("  " + fmt(r.iloc[0]))
    for rt in rsis:
        r = R[(R["stop"] == 3.0) & (R["tp"] == 5.0)
              & ((R["rsi"] == rt) if rt is not None else R["rsi"].isna()) & (R["gap"].isna())]
        if len(r):
            print("  " + fmt(r.iloc[0]))
    for gp in gaps:
        r = R[(R["stop"] == 3.0) & (R["tp"] == 5.0) & (R["rsi"] == 50.0)
              & ((R["gap"] == gp) if gp is not None else R["gap"].isna())]
        if len(r):
            print("  " + fmt(r.iloc[0]))

    R.to_csv("exit_grid2_results.csv", index=False)
    print("\n出力: exit_grid2_results.csv")


if __name__ == "__main__":
    main()

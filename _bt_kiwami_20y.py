# -*- coding: utf-8 -*-
"""_bt_kiwami_20y.py — 極み買いの現行ルールを立花20年日足（2001〜）に当てる（2026-09-03）。

目的: 「10年BTは2021年以降のレジーム依存」の疑いを、J-Quantsの10年窓の外（2001〜2016）で初めて検証する。
ルール改善ではなく頑健性の確認。入口/出口/選定は本番と同一:
  入口 RSI≤45 / 25MA乖離≤-1.5% / (rr≥1.5 | vr≥2.0) / 前日代金≥20億 / ATR%≤3.0
  寄指×1.01 NOFILL・出口 実OCO(TP+5/STOP-3)+RSI50回復+MAXHOLD3・1日5本×業種cap3・3枠×100万・値がさ≤1万円
  決算±3日除外と買残回転(dc>1.2除外)は **データがある期間だけ**（決算=earnings_calendar.json、信用残=_margin_10y.pkl）
  → 2001〜2016はこの2フィルタ無し。公平比較のため 2017〜2026 も「フィルタ無し版」を併記する。

データ源の突合: 2017〜2026 は J-Quants（公式+3,108,516円）と立花で同じパイプラインを回して差を見る。
注意: 立花のユニバースは「今上場している銘柄」＝上場廃止銘柄が過去期間に居ない（生存者バイアス・強気側に歪む）。

実行: python -X utf8 _bt_kiwami_20y.py [--src tachibana|jquants|both]
出力: _log_kiwami_20y.txt / _bt_kiwami_20y_picks_<src>.csv
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

from bt_10y_robustness import load_earnings, sim
from screener import is_etf_ticker, yose_limit_price

RSI_T, DEV_T, RR_T, VR_T, TOV_T, ATR_T = 45.0, -1.5, 1.5, 2.0, 2e9, 3.0
MAX_SIG, SECTOR_CAP, SLOTS, SIZE, PX_CAP, DC_MAX = 5, 3, 3, 1_000_000, 10_000, 1.2
SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))


def load_source(src: str) -> tuple[dict, dict]:
    if src == "tachibana":
        d = pickle.load(open("tachibana_history.pkl", "rb"))
        name_map = dict(pickle.load(open("jquants_cache.pkl", "rb"))["name_map"])
        return d["all_data"], name_map
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    name_map = dict(old["name_map"]); name_map.update(new["name_map"])
    data: dict = {}
    for s in (old["all_data"], new["all_data"]):
        for tk, df in s.items():
            data.setdefault(tk, []).append(df)
    merged = {}
    for tk, dfs in data.items():
        df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index()
        merged[tk] = df[~df.index.duplicated(keep="last")]
    return merged, name_map


def build_pool(all_data: dict, name_map: dict, earn: dict, M: dict, since: str) -> pd.DataFrame:
    rows = []
    nn = nofill = 0
    since_ts = pd.Timestamp(since)
    for tk, df in all_data.items():
        if df is None or len(df) < 140:
            continue
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):   # 公式(bt_10y_robustness)と同じ: 名前不明は除外
            continue
        df = df.dropna(subset=["Close"])
        o = df["Open"].astype(float); h = df["High"].astype(float)
        l = df["Low"].astype(float); cl = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        nn += 1
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
        ma25 = cl.rolling(25).mean()
        dev = ((cl - ma25) / ma25 * 100).round(2)
        pc = cl.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        rr = ((h - l).shift(1) / atr.shift(1)).round(2)
        vr = (v.shift(1) / v.shift(2).rolling(20).mean()).round(2)
        tov = cl.shift(1) * v.shift(1)
        atr_pct = atr / cl * 100
        adv20 = (cl * v).rolling(20).mean()
        cand = ((rsi <= RSI_T) & (dev <= DEV_T) & ((rr >= RR_T) | (vr >= VR_T))
                & (tov >= TOV_T) & (atr_pct <= ATR_T))
        cand &= cl.index >= since_ts
        cand = cand.fillna(False)
        if not cand.any():
            continue
        on = o.to_numpy(); hn = h.to_numpy(); ln = l.to_numpy()
        cn = cl.to_numpy(); rn = rsi.to_numpy()
        idx = cl.index; n = len(cn)
        ewin = earn.get(tk) or earn.get(tk.replace(".T", "")) or set()
        mdf = M.get(tk[:4])
        for t in np.where(cand.to_numpy())[0]:
            if t + 2 >= n:
                continue
            entry_ts = idx[t + 1]
            entry_day = entry_ts.strftime("%Y-%m-%d")
            in_earn = entry_day in ewin
            lp = yose_limit_price(cn[t])
            if lp and on[t + 1] > lp:
                nofill += 1
                continue
            res, exoff = sim(t + 1, on, hn, ln, cn, rn, n)
            if res is None:
                continue
            rsi_t, dev_t, tov_t = rn[t], dev.iloc[t], tov.iloc[t]
            score = (1 / (1 + ((rsi_t - 38) / 8) ** 2) * 0.30
                     + 1 / (1 + ((dev_t + 3) / 2) ** 2) * 0.30
                     + np.log10(max(tov_t, 1) / 1e9 + 1) / 3 * 0.40)
            days_cover = np.nan
            if mdf is not None and len(mdf):
                m = mdf[mdf.index <= entry_ts - timedelta(days=4)]
                if len(m):
                    long_v = float(m.iloc[-1].get("LongVol") or np.nan)
                    a_prev = adv20.iloc[t]
                    if np.isfinite(long_v) and np.isfinite(a_prev) and a_prev > 0:
                        days_cover = long_v * cn[t] / a_prev
            rows.append({"entry": entry_day, "year": entry_ts.year, "ticker": tk, "price": cn[t],
                         "score": score, "pnl": res, "exoff": int(exoff), "in_earn": in_earn,
                         "days_cover": days_cover, "tov": float(tov_t)})
    D = pd.DataFrame(rows)
    print(f"  [pool] {len(D):,}件 / {nn}銘柄走査 / NOFILL {nofill:,}", flush=True)
    return D


def select(D: pd.DataFrame, use_earn: bool, use_dc: bool) -> pd.DataFrame:
    """極みの選定シム（_bt_kiwami_exit_axes.run と同じ2段構造）。"""
    C = D.sort_values(["entry", "score"], ascending=[True, False]).reset_index(drop=True)
    ok = (C["price"] <= PX_CAP).to_numpy().copy()
    if use_earn:
        ok &= ~C["in_earn"].to_numpy()
    if use_dc:
        ok &= ~(C["days_cover"] > DC_MAX).to_numpy()
    days = sorted(C["entry"].unique()); gdi = {d: i for i, d in enumerate(days)}
    by_day: dict = {}
    for i, d in enumerate(C["entry"]):
        by_day.setdefault(gdi[d], []).append(i)
    TICK = C["ticker"].to_numpy(); YEAR = C["year"].to_numpy(); PNL = C["pnl"].to_numpy()
    E = C["price"].to_numpy(); EXO = C["exoff"].to_numpy()
    SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
    ou: dict = {}; os_: dict = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]:
            del ou[tk]; del os_[tk]
        sc: dict = {}
        for s in os_.values():
            sc[s] = sc.get(s, 0) + 1
        cnt = 0
        for i in by_day.get(d, []):
            if cnt >= MAX_SIG:
                break
            if not ok[i] or TICK[i] in ou:
                continue
            s = SEC[i]
            if sc.get(s, 0) >= SECTOR_CAP:
                continue
            cnt += 1
            ex = min(d + int(EXO[i]), len(days) - 1)
            ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1
            picks.append((d, ex, int(YEAR[i]), float(PNL[i]), float(E[i]), TICK[i], days[d]))
    live: list = []; rows = []
    for d, ex, y, p, e, tk, day in picks:
        live = [x for x in live if x >= d]
        if len(live) >= SLOTS:
            continue
        sh = int(SIZE / e / 100) * 100
        if sh <= 0:
            continue
        live.append(ex)
        rows.append({"entry": day, "y": y, "ticker": tk, "pnl": p, "yen": p / 100 * sh * e})
    return pd.DataFrame(rows)


def yearly(R: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    g = R.groupby("y")
    out = pd.DataFrame({"n": g.size(), "win%": (g.pnl.apply(lambda s: (s > 0).mean() * 100)),
                        "avg%": g.pnl.mean(), "yen": g.yen.sum()}).reindex(years)
    out["n"] = out["n"].fillna(0).astype(int); out["yen"] = out["yen"].fillna(0)
    return out


def pf(R: pd.DataFrame) -> float:
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    return gp / gl if gl else float("inf")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="both")
    ap.add_argument("--reuse-pool", action="store_true", help="既存の _bt_kiwami_20y_pool_<src>.csv を使う")
    a = ap.parse_args()
    earn = load_earnings()
    M = pickle.load(open("_margin_10y.pkl", "rb"))
    srcs = ["jquants", "tachibana"] if a.src == "both" else [a.src]
    results = {}
    for src in srcs:
        print(f"\n=== {src} ===", flush=True)
        all_data, name_map = load_source(src)
        first = min(df.index.min() for df in all_data.values() if len(df))
        print(f"  銘柄 {len(all_data)} / 最古 {first.date()}", flush=True)
        pool_csv = f"_bt_kiwami_20y_pool_{src}.csv"
        if a.reuse_pool and __import__("os").path.exists(pool_csv):
            D = pd.read_csv(pool_csv)
            D = D[D["ticker"].map(name_map).notna()].reset_index(drop=True)
            print(f"  [pool] 再利用 {pool_csv} {len(D):,}件（名前不明を除外後）", flush=True)
        else:
            D = build_pool(all_data, name_map, earn, M, "2001-01-01" if src == "tachibana" else "2017-01-01")
            D.to_csv(pool_csv, index=False)
        results[src] = {
            "full": select(D, True, True),
            "nofilter": select(D, False, False),
        }
        results[src]["full"].to_csv(f"_bt_kiwami_20y_picks_{src}.csv", index=False)

    lines = []
    def p(s=""):
        print(s); lines.append(s)
    p("\n" + "=" * 100)
    p("極み買い 現行ルール × 20年（3枠×100万・値がさ≤1万）  yen=円損益 / win%=勝率 / avg%=平均損益%")
    p("=" * 100)
    for src, R in results.items():
        years = list(range(2001 if src == "tachibana" else 2017, 2027))
        for label, key in (("決算±3除外＋dc1.2（本番同一・2016以前はデータ無し＝素通し）", "full"),
                           ("フィルタ無し（2001〜2016と同じ土俵）", "nofilter")):
            r = R[key]
            p(f"\n[{src}] {label}")
            p(yearly(r, years).round(2).to_string())
            for lo, hi, nm in ((2001, 2008, "2001-08"), (2009, 2016, "2009-16"), (2017, 2021, "2017-21"), (2022, 2026, "2022-26")):
                s = r[(r.y >= lo) & (r.y <= hi)]
                if len(s):
                    yy = s.groupby("y").yen.sum()
                    p(f"  {nm}: n={len(s):>5} PF={pf(s):.2f} 合計={s.yen.sum():>+14,.0f} 勝ち年={int((yy>0).sum())}/{len(yy)} 最悪年={yy.min():>+12,.0f} 平均%={s.pnl.mean():+.3f}")
            yy = r.groupby("y").yen.sum().reindex(years, fill_value=0)
            p(f"  全期間: n={len(r)} PF={pf(r):.2f} 合計={r.yen.sum():+,.0f} 勝ち年={int((yy>0).sum())}/{len(years)}")
    if "jquants" in results and "tachibana" in results:
        j = results["jquants"]["full"]; t = results["tachibana"]["full"]; t = t[t.y >= 2017]
        p(f"\n[データ源突合 2017-26 本番同一フィルタ] J-Quants {j.yen.sum():+,.0f} (n={len(j)})  vs  立花 {t.yen.sum():+,.0f} (n={len(t)})  公式基準 +3,108,516")
    open("_log_kiwami_20y.txt", "w", encoding="utf-8").write("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# -*- coding: utf-8 -*-
"""_bt_buy_20y_grid.py — 買い（押し目）ルールを26年で再設計できるか：入口×出口フルグリッド（2026-09-03）。

問い: 現行の極み買いは2022年以降しか勝てない（_bt_kiwami_20y.py）。同じ「短期の押し目買い」の枠内で、
      2001〜2016でも勝てるパラメータ面は存在するか。存在するならそれは2017〜2026でも勝つか（真のOOS）。
設計: フェーズ1=緩い閾値で広域プール（特徴量＋翌5日のOHLC/RSIを保存）→ フェーズ2=入口48×出口54=2,592セルを
      極み実構成（1日5本×業種cap3・3枠×100万・≤1万円・寄指×1.01）で選定シム。
判定の作法（[[feedback_bt_workflow]] / [[feedback_all_years_positive_is_scope]]）:
  ①フルグリッドを終える ②「2001-16で最良」のセルを先に確定 ③そのセルの2017-26を見る（設計期間外＝OOS）
  ④4期間すべてPF>1のセルがいくつあるか（面か針か） ⑤上位20玉除去でも残るか。
注意: 現存銘柄のみ（生存者バイアス・買い側は強気に歪む）／決算除外・買残回転フィルタ無し。
実行: python -X utf8 _bt_buy_20y_grid.py [--phase 1|2|both]   出力 _bt_buy_20y_wide.pkl / _bt_buy_20y_grid.csv / _log_buy_20y_grid.txt
"""
from __future__ import annotations

import argparse
import itertools
import json
import pickle
import sys
import time

import numpy as np
import pandas as pd

from screener import is_etf_ticker, yose_limit_price

H = 5
MAX_SIG, SECTOR_CAP, SLOTS, SIZE, PX_CAP = 5, 3, 3, 1_000_000, 10_000
WIDE = dict(rsi=55.0, dev=0.0, rr=1.0, vr=1.3, tov=5e8, atr=6.0)
POOL = "_bt_buy_20y_wide.pkl"


def phase1():
    ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
    nm = dict(pickle.load(open("jquants_cache.pkl", "rb"))["name_map"])
    recs = []
    fw = {k: [] for k in ("OP", "HI", "LO", "CL", "RS")}
    t0 = time.time(); nn = 0
    for tk, df in ALL.items():
        name = nm.get(tk)
        if name is None or is_etf_ticker(tk, name) or df is None or len(df) < 140:
            continue
        df = df.dropna(subset=["Close"])
        o = df["Open"].astype(float); h = df["High"].astype(float); l = df["Low"].astype(float)
        cl = df["Close"].astype(float); v = df["Volume"].astype(float)
        nn += 1
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
        ma25 = cl.rolling(25).mean(); dev = ((cl - ma25) / ma25 * 100).round(2)
        ma75 = cl.rolling(75).mean(); dev75 = ((cl - ma75) / ma75 * 100).round(2)
        pc = cl.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        rr = ((h - l).shift(1) / atr.shift(1)).round(2)
        vr = (v.shift(1) / v.shift(2).rolling(20).mean()).round(2)
        tov = cl.shift(1) * v.shift(1)
        atr_pct = atr / cl * 100
        ret3 = (cl / cl.shift(3) - 1) * 100
        cand = ((rsi <= WIDE["rsi"]) & (dev <= WIDE["dev"]) & ((rr >= WIDE["rr"]) | (vr >= WIDE["vr"]))
                & (tov >= WIDE["tov"]) & (atr_pct <= WIDE["atr"])).fillna(False).to_numpy()
        on = o.to_numpy(); hn = h.to_numpy(); ln = l.to_numpy(); cn = cl.to_numpy(); rn = rsi.to_numpy()
        idx = df.index; n = len(cn)
        for t in np.where(cand)[0]:
            if t + H >= n:
                continue
            e = on[t + 1]
            if not (e > 0):
                continue
            lp = yose_limit_price(cn[t])
            nofill = bool(lp and e > lp)
            recs.append((idx[t + 1].strftime("%Y-%m-%d"), idx[t + 1].year, tk, cn[t], e, nofill,
                         rn[t], dev.iloc[t], dev75.iloc[t], rr.iloc[t], vr.iloc[t], tov.iloc[t], atr_pct.iloc[t], ret3.iloc[t]))
            fw["OP"].append(on[t + 1:t + 1 + H]); fw["HI"].append(hn[t + 1:t + 1 + H])
            fw["LO"].append(ln[t + 1:t + 1 + H]); fw["CL"].append(cn[t + 1:t + 1 + H]); fw["RS"].append(rn[t + 1:t + 1 + H])
        if nn % 500 == 0:
            print(f"  {nn}銘柄 候補{len(recs):,} {time.time()-t0:.0f}s", flush=True)
    C = pd.DataFrame(recs, columns=["entry", "year", "ticker", "price", "E", "nofill", "rsi", "dev", "dev75", "rr", "vr", "tov", "atr", "ret3"])
    arrs = {k: np.array(v, dtype=float) for k, v in fw.items()}
    pickle.dump({"C": C, **arrs}, open(POOL, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[phase1] 候補{len(C):,}件 / {nn}銘柄 / {time.time()-t0:.0f}s → {POOL}", flush=True)


def phase2():
    P = pickle.load(open(POOL, "rb")); C = P["C"]
    C["score"] = (1 / (1 + ((C.rsi - 38) / 8) ** 2) * 0.30 + 1 / (1 + ((C.dev + 3) / 2) ** 2) * 0.30
                  + np.log10(np.maximum(C.tov, 1) / 1e9 + 1) / 3 * 0.40)
    order = np.lexsort((-C.score.to_numpy(), C.entry.to_numpy()))
    C = C.iloc[order].reset_index(drop=True)
    OP, HI, LO, CL, RS = (P[k][order] for k in ("OP", "HI", "LO", "CL", "RS"))
    n = len(C); E = C.E.to_numpy()
    days = sorted(C.entry.unique()); gdi = {d: i for i, d in enumerate(days)}
    DAY = C.entry.map(gdi).to_numpy()
    by_day = [[] for _ in days]
    for i, d in enumerate(DAY):
        by_day[d].append(i)
    SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))
    TICK = C.ticker.to_numpy(); YEAR = C.year.to_numpy()
    SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
    PRICE = C.price.to_numpy(); NOFILL = C.nofill.to_numpy()
    RSI, DEV, RR, VR, TOV, ATR = (C[c].to_numpy() for c in ("rsi", "dev", "rr", "vr", "tov", "atr"))
    print(f"[phase2] 候補{n:,} 営業日{len(days)}", flush=True)

    def replay(tp, stop, hold, rsith):
        valid = np.isfinite(E) & np.isfinite(CL[:, hold - 1])
        pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
        sl = E * (1 - stop / 100); tl = E * (1 + tp / 100) if tp is not None else np.full(n, np.inf)
        for k in range(hold):
            live = ~done
            if not live.any():
                break
            if k > 0:
                op = OP[:, k]
                g = live & np.isfinite(op) & (op > 0) & ((op <= sl) | (op >= tl))
                pnl[g] = (op[g] - E[g]) / E[g] * 100; exo[g] = k; done |= g; live = ~done
            s = live & (LO[:, k] <= sl); pnl[s] = -stop; exo[s] = k; done |= s; live = ~done
            if tp is not None:
                t = live & (HI[:, k] >= tl); pnl[t] = tp; exo[t] = k; done |= t; live = ~done
            rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]) if rsith is not None else np.zeros(n, bool)
            r = live & (rc | (k == hold - 1)); pnl[r] = (CL[r, k] - E[r]) / E[r] * 100; exo[r] = k; done |= r
        return pnl, exo

    def run(ok, pnl, exo):
        ok = ok & np.isfinite(pnl)
        ou: dict = {}; os_: dict = {}; picks = []
        for d in range(len(days)):
            for tk in [t for t, u in ou.items() if u < d]:
                del ou[tk]; del os_[tk]
            sc: dict = {}
            for s in os_.values():
                sc[s] = sc.get(s, 0) + 1
            cnt = 0
            for i in by_day[d]:
                if cnt >= MAX_SIG:
                    break
                if not ok[i] or TICK[i] in ou:
                    continue
                s = SEC[i]
                if sc.get(s, 0) >= SECTOR_CAP:
                    continue
                cnt += 1
                ex = min(d + int(exo[i]), len(days) - 1)
                ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1
                picks.append((d, ex, YEAR[i], pnl[i], E[i]))
        live: list = []; ys = []; yens = []
        for d, ex, y, p, e in picks:
            live = [x for x in live if x >= d]
            if len(live) >= SLOTS:
                continue
            sh = int(SIZE / e / 100) * 100
            if sh <= 0:
                continue
            live.append(ex); ys.append(y); yens.append(p / 100 * sh * e)
        return np.array(ys), np.array(yens)

    def stats(ys, yens):
        out = {"n": len(ys)}
        if len(ys) == 0:
            return out
        yy = pd.Series(yens).groupby(ys).sum()
        for lo, hi, nm_ in ((2001, 2008, "e1"), (2009, 2016, "e2"), (2017, 2021, "e3"), (2022, 2026, "e4")):
            m = (ys >= lo) & (ys <= hi); s = yens[m]
            gp = s[s > 0].sum(); gl = -s[s <= 0].sum()
            out[nm_] = s.sum(); out[nm_ + "_pf"] = gp / gl if gl else np.inf; out[nm_ + "_n"] = int(m.sum())
        out["total"] = yens.sum(); out["design"] = out["e1"] + out["e2"]; out["oos"] = out["e3"] + out["e4"]
        out["win_years"] = int((yy > 0).sum()); out["worst_year"] = yy.min()
        out["ex20"] = yens.sum() - np.sort(yens)[-20:].sum()
        return out

    ENTRY = list(itertools.product((35.0, 40.0, 45.0, 50.0), (-1.5, -3.0, -5.0), (2e9, 8e8), (3.0, 4.5)))
    EXIT = list(itertools.product((3.0, 5.0, None), (2.0, 3.0, 4.0), (2, 3, 5), (50.0, None)))
    rows = []; t0 = time.time(); k = 0
    exit_cache = {}
    for (tp, stop, hold, rsith) in EXIT:
        exit_cache[(tp, stop, hold, rsith)] = replay(tp, stop, hold, rsith)
    base_ok = (PRICE <= PX_CAP) & ~NOFILL
    for (rsi_t, dev_t, tov_t, atr_t) in ENTRY:
        ent_ok = base_ok & (RSI <= rsi_t) & (DEV <= dev_t) & ((RR >= 1.5) | (VR >= 2.0)) & (TOV >= tov_t) & (ATR <= atr_t)
        for (tp, stop, hold, rsith), (pnl, exo) in exit_cache.items():
            ys, yens = run(ent_ok, pnl, exo)
            st = stats(ys, yens)
            st.update(rsi=rsi_t, dev=dev_t, tov=tov_t, atr=atr_t, tp=tp if tp is not None else 0, stop=stop, hold=hold, rsi_exit=rsith if rsith is not None else 0)
            rows.append(st); k += 1
            if k % 100 == 0:
                print(f"  {k}/{len(ENTRY)*len(EXIT)} {time.time()-t0:.0f}s", flush=True)
                pd.DataFrame(rows).to_csv("_bt_buy_20y_grid.csv", index=False)
    Gd = pd.DataFrame(rows); Gd.to_csv("_bt_buy_20y_grid.csv", index=False)
    lines = []
    def p(s=""):
        print(s); lines.append(s)
    cols = ["rsi", "dev", "tov", "atr", "tp", "stop", "hold", "rsi_exit", "n", "e1", "e2", "e3", "e4", "e1_pf", "e2_pf", "e3_pf", "e4_pf", "design", "oos", "total", "win_years", "worst_year", "ex20"]
    fmt = {c: "{:,.0f}".format for c in ("e1", "e2", "e3", "e4", "design", "oos", "total", "worst_year", "ex20", "tov")}
    fmt.update({c: "{:.2f}".format for c in ("e1_pf", "e2_pf", "e3_pf", "e4_pf")})
    p("=" * 160); p(f"買いグリッド {len(Gd)}セル（3枠×100万・≤1万円・寄指×1.01）  e1=2001-08 e2=2009-16 e3=2017-21 e4=2022-26  design=e1+e2  oos=e3+e4"); p("=" * 160)
    cur = Gd[(Gd.rsi == 45) & (Gd.dev == -1.5) & (Gd.tov == 2e9) & (Gd.atr == 3.0) & (Gd.tp == 5) & (Gd.stop == 3) & (Gd.hold == 3) & (Gd.rsi_exit == 50)]
    p("\n[現行ルールのセル]"); p(cur[cols].to_string(index=False, formatters=fmt))
    p("\n[① 設計期間(2001-16)で最良の10セル → その2017-26(OOS)]")
    p(Gd.sort_values("design", ascending=False).head(10)[cols].to_string(index=False, formatters=fmt))
    rob = Gd[(Gd.e1_pf > 1) & (Gd.e2_pf > 1) & (Gd.e3_pf > 1) & (Gd.e4_pf > 1)]
    p(f"\n[② 4期間すべてPF>1のセル: {len(rob)}/{len(Gd)}]")
    if len(rob):
        p(rob.sort_values("ex20", ascending=False).head(15)[cols].to_string(index=False, formatters=fmt))
    p("\n[③ 26年合計で最良の10セル]")
    p(Gd.sort_values("total", ascending=False).head(10)[cols].to_string(index=False, formatters=fmt))
    p("\n[④ 2022-26(e4)で最良の10セル（現行が居る面）]")
    p(Gd.sort_values("e4", ascending=False).head(10)[cols].to_string(index=False, formatters=fmt))
    p("\n[⑤ 期間別に「PF>1のセル数」＝その時代に押し目買いの面がそもそも存在したか]")
    for e in ("e1", "e2", "e3", "e4"):
        p(f"  {e}: PF>1 {int((Gd[e+'_pf']>1).sum())}/{len(Gd)}  合計プラス {int((Gd[e]>0).sum())}/{len(Gd)}  中央値合計 {Gd[e].median():,.0f}")
    open("_log_buy_20y_grid.txt", "w", encoding="utf-8").write("\n".join(lines))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--phase", default="both"); a = ap.parse_args()
    if a.phase in ("1", "both"):
        phase1()
    if a.phase in ("2", "both"):
        phase2()

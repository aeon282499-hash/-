# -*- coding: utf-8 -*-
"""bb_engine.py — ボリンジャーバンドσ乖離の分足総当たりエンジン(2026-10-31・検証のみ・発注なし)
規約: BBは足iまでの終値で計算 → シグナルは足iの確定後 → 建玉は足i+1の始値(+不利1tick) → 出口も次足始値(−不利1tick) → 往復0.10%。
     1銘柄1日1トレード(時間帯内の最初のシグナル)。日足の土俵条件は前日までの情報＋当日寄値(GUのみ)。
"""
from __future__ import annotations
import sys, os, time, pickle
import numpy as np, pandas as pd

COST_RT = 0.10  # 往復% (デイトレ信用の滑り相当)
STD_UB = np.array([3000, 5000, 30000, 50000, 300000, 500000, 3e6, 5e6, 3e7, 1e12])
STD_TK = np.array([1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 10000], float)
T5_UB = np.array([1000, 3000, 10000, 30000, 100000, 300000, 1e6, 3e6, 1e7, 1e12])
T5_TK = np.array([0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000, 1000], float)
EXCL = r"C:\Users\星野\Downloads\trade_bt\uwick_bt\fade5y\_excl_2026.pkl"

PERIODS = (20, 25, 40)
SIGMAS = (1.5, 2.0, 2.5, 3.0)
DEFS = ("D1_外", "D2_外0.5%", "D3_2本連続外")
BANDS = ("lower", "upper")
SIDES = (1, -1)
SLS = (None, 0.5, 1.0, 2.0)
BASE_EXITS = ("MA回帰", "反対バンド", "N6本", "N12本", "引け")
EXITS = [(b, s) for b in BASE_EXITS for s in (None, 1.0)] + [("引け", 0.5), ("引け", 2.0)]
UNIS = ("U0_全", "U1_代金5億+", "U2_代金20億+", "U3_GU+3%", "U4_前日+8%", "U5_株価<1000", "U6_株価1000-5000", "U7_株価5000+")
EARNS = ("決算込", "決算除外")


def tick_of(px, is500):
    tk = np.where(is500, T5_TK[np.searchsorted(T5_UB, px - 1e-9)], STD_TK[np.searchsorted(STD_UB, px - 1e-9)])
    return tk


class Data:
    def __init__(self, kind):
        self.kind = kind
        t0 = time.time()
        if kind == "5m":
            src = pd.read_pickle(r"C:\Users\星野\Downloads\trade_bt\data_1022\yf_5m.pkl")
            full_bars = 66; drop_tod = {"09:00"}; self.half_day = 30
        else:
            src = pd.read_pickle(r"C:\Users\星野\Documents\my-first-project\_intraday_cache_wide.pkl")
            full_bars = 23; drop_tod = {"15:30"}; self.half_day = 40
        self.full_bars = full_bars
        parts = []
        for tk in sorted(src):
            d = src[tk]
            if d is None or len(d) < full_bars * 10: continue
            d = d[["dt", "o", "h", "l", "c", "v"]].copy()
            d = d.dropna()
            d = d[(d.c > 0) & (d.o > 0) & (d.h > 0) & (d.l > 0)]
            d["tod"] = d.dt.dt.strftime("%H:%M")
            d = d[~d.tod.isin(drop_tod)]
            if len(d) < full_bars * 10: continue
            d["tk"] = tk; parts.append(d)
        del src
        df = pd.concat(parts, ignore_index=True); del parts
        df["date"] = df.dt.dt.strftime("%Y-%m-%d")
        df = df.sort_values(["tk", "dt"]).reset_index(drop=True)
        self.tickers = np.array(sorted(df.tk.unique())); self.dates = np.array(sorted(df.date.unique()))
        self.sid = np.searchsorted(self.tickers, df.tk.to_numpy())
        self.did = np.searchsorted(self.dates, df.date.to_numpy())
        self.tod = df.tod.to_numpy()
        hh = df.dt.dt.hour.to_numpy(); mm = df.dt.dt.minute.to_numpy(); self.tmin = hh * 60 + mm
        self.o = df.o.to_numpy(float); self.h = df.h.to_numpy(float); self.l = df.l.to_numpy(float); self.c = df.c.to_numpy(float); self.v = df.v.to_numpy(float)
        n = len(df); self.n = n
        key = self.sid.astype(np.int64) * 100000 + self.did
        st = np.flatnonzero(np.r_[True, key[1:] != key[:-1]]); en = np.r_[st[1:], n]
        self.sd_start, self.sd_end = st, en
        grp = np.repeat(np.arange(len(st)), en - st)  # stock-day group id per row
        self.grp = grp
        self.row_dstart = st[grp]; self.row_dend = en[grp]
        sst = np.flatnonzero(np.r_[True, self.sid[1:] != self.sid[:-1]]); sgrp = np.repeat(np.arange(len(sst)), np.r_[sst[1:], n] - sst)
        self.row_sstart = sst[sgrp]
        # tick table per stock
        is500 = np.zeros(len(self.tickers), bool)
        for s in range(len(self.tickers)):
            px = self.c[sst[s]:(sst[s + 1] if s + 1 < len(sst) else n)][::7]
            q = px / STD_TK[np.searchsorted(STD_UB, px - 1e-9)]
            is500[s] = (np.abs(q - np.round(q)) > 1e-6).mean() > 0.01
        self.is500_row = is500[self.sid]
        # ---- stock-day table (前日までの情報 + 当日寄値) ----
        G = len(st)
        g_sid = self.sid[st]; g_did = self.did[st]
        g_nbar = en - st
        tv = np.add.reduceat(self.c * self.v, st); g_close = self.c[en - 1]; g_open = self.o[st]
        prev_same = np.r_[False, (g_sid[1:] == g_sid[:-1]) & (g_did[1:] == g_did[:-1] + 1)]  # 直前の営業日が揃っている
        g_pc = np.where(prev_same, np.r_[np.nan, g_close[:-1]], np.nan)
        g_ppc = np.where(prev_same & np.r_[False, prev_same[:-1]], np.r_[np.nan, np.nan, g_close[:-2]], np.nan)
        g_ptv = np.where(prev_same, np.r_[np.nan, tv[:-1]], np.nan)
        g_pnbar = np.where(prev_same, np.r_[0, g_nbar[:-1]], 0)
        self.G = G; self.g_sid = g_sid; self.g_did = g_did; self.g_pc = g_pc; self.g_ptv = g_ptv
        self.g_gu = g_open / g_pc - 1; self.g_pchg = g_pc / g_ppc - 1
        base = np.isfinite(g_pc) & (g_pc >= 100) & (g_pc <= 100000) & (g_ptv >= 1e8) & (g_pnbar >= 0.9 * full_bars)
        self.uni = {
            "U0_全": base,
            "U1_代金5億+": base & (g_ptv >= 5e8),
            "U2_代金20億+": base & (g_ptv >= 2e9),
            "U3_GU+3%": base & (self.g_gu >= 0.03),
            "U4_前日+8%": base & (self.g_pchg >= 0.08),
            "U5_株価<1000": base & (g_pc < 1000),
            "U6_株価1000-5000": base & (g_pc >= 1000) & (g_pc < 5000),
            "U7_株価5000+": base & (g_pc >= 5000),
        }
        # ---- 決算除外: 開示日当日 + 翌営業日 ----
        fins = pd.read_pickle(EXCL)["fins"]
        code5 = {t: t.split(".")[0] for t in self.tickers}
        tk_by_code = {}
        for t, c5 in code5.items(): tk_by_code.setdefault(c5 + "0" if len(c5) == 4 else c5, t)
        sidx = {t: i for i, t in enumerate(self.tickers)}; didx = {d: i for i, d in enumerate(self.dates)}
        ex = np.zeros(G, bool); gkey = g_sid.astype(np.int64) * 100000 + g_did
        hit = set()
        dl = list(self.dates)
        for d, lst in fins.items():
            if d < dl[0] or d > dl[-1]:
                # 前営業日の夜の開示 → 期間初日に効くケースだけ拾う
                pass
            for c, tm in lst:
                t = tk_by_code.get(c)
                if t is None: continue
                s = sidx[t]
                # 当日
                if d in didx: hit.add(s * 100000 + didx[d])
                # 翌営業日
                j = np.searchsorted(self.dates, d, side="right")
                if j < len(self.dates): hit.add(s * 100000 + j)
        ex = np.isin(gkey, np.fromiter(hit, np.int64, len(hit)))
        self.g_earn = ex
        self.half = np.where(g_did < self.half_day, 1, 2)
        self.summary = f"rows={n:,} stocks={len(self.tickers)} days={len(self.dates)} ({self.dates[0]}..{self.dates[-1]}) stock-days={G:,} base(U0)={base.sum():,} earn-excl(stock-days)={ex.sum():,} T500-tick stocks={is500.sum()}; U sizes: " + ", ".join(f"{k}={int(v.sum()):,}" for k, v in self.uni.items())
        print(f"[data {kind}] {self.summary} {time.time()-t0:.0f}s", flush=True)

    # ---- 時間帯 (シグナル足の開始時刻・分) ----
    def windows(self):
        if self.kind == "5m":
            return {"W0_全": (9 * 60 + 5, 14 * 60 + 25), "W1_9:30〜": (9 * 60 + 30, 14 * 60 + 25), "W2_10:00〜": (10 * 60, 14 * 60 + 25),
                    "W3_前場": (9 * 60 + 5, 11 * 60 + 20), "W4_後場": (12 * 60 + 30, 14 * 60 + 25)}
        return {"W0_全": (9 * 60, 14 * 60 + 15), "W1_9:30〜": (9 * 60 + 30, 14 * 60 + 15), "W2_10:00〜": (10 * 60, 14 * 60 + 15),
                "W3_前場": (9 * 60, 11 * 60), "W4_後場": (12 * 60 + 30, 14 * 60 + 15)}

    def bb(self, p, sig):
        s = pd.Series(self.c)
        ma = s.rolling(p).mean().to_numpy(copy=True); sd = s.rolling(p).std(ddof=0).to_numpy(copy=True)
        bad = (np.arange(self.n) - self.row_sstart) < p - 1
        ma[bad] = np.nan; sd[bad] = np.nan
        return ma, ma + sig * sd, ma - sig * sd

    def signals(self, up, lo, band, dfn):
        c = self.c
        if band == "lower":
            out = c < lo; dev = (lo - c) / c
        else:
            out = c > up; dev = (c - up) / c
        out &= np.isfinite(up)
        if dfn.startswith("D1"): return out
        if dfn.startswith("D2"): return out & (dev >= 0.005)
        prev = np.r_[False, out[:-1]] & (np.r_[-1, self.row_dstart[:-1]] == self.row_dstart)  # 前足も同日で外
        return out & prev

    def first_in_window(self, sig, w):
        lo, hi = w
        ok = sig & (self.tmin >= lo) & (self.tmin <= hi) & (np.arange(self.n) + 1 < self.row_dend)
        idx = np.flatnonzero(ok)
        if not len(idx): return idx
        g = self.grp[idx]
        first = np.r_[True, g[1:] != g[:-1]]
        return idx[first]

    def simulate(self, e, ma, up, lo, band):
        """e: シグナル足index配列。戻り: dict exit_name -> 出口価格配列(side別はSLのみ)。建値 = o[e+1]。
        MA回帰: シグナル時の(c-ma)符号が反転した足 → 次足始値。反対バンド: バンド逆側到達 → 次足始値。N本: e+N の次足始値。引け: 日の最終足終値。
        SL(side別): 終値ベースで建値から -sl% → 次足始値。"""
        n_e = len(e); L = self.full_bars
        dend = self.row_dend[e]
        J = e[:, None] + 1 + np.arange(L)[None, :]
        valid = J < dend[:, None]
        Jc = np.minimum(J, self.n - 1)
        cJ = self.c[Jc]; oJ = self.o[Jc]; maJ = ma[Jc]
        ep = self.o[e + 1]
        last_c = self.c[dend - 1]
        def exit_px_at(k):
            """k: 出口判定足の列index(0始まり=e+1)。次足始値、無ければ引け"""
            j = e + 1 + k; nxt = j + 1
            has = nxt < dend
            return np.where(has, self.o[np.minimum(nxt, self.n - 1)], last_c), np.where(has, nxt, dend - 1)
        def first_true(cond):
            cond = cond & valid
            anyc = cond.any(1); k = cond.argmax(1)
            return anyc, k
        res = {}
        state = np.sign(self.c[e] - ma[e])  # lower→-1, upper→+1
        # MA回帰
        anyc, k = first_true((cJ - maJ) * state[:, None] <= 0)
        px, j = exit_px_at(k); res["MA回帰"] = (np.where(anyc, px, last_c), np.where(anyc, j, dend - 1))
        # 反対バンド
        oppJ = up[Jc] if band == "lower" else lo[Jc]
        anyc, k = first_true((cJ - oppJ) * state[:, None] <= 0)
        px, j = exit_px_at(k); res["反対バンド"] = (np.where(anyc, px, last_c), np.where(anyc, j, dend - 1))
        for N in (6, 12):
            k = np.full(n_e, N - 1); has = (e + N) < dend
            px, j = exit_px_at(k); res[f"N{N}本"] = (np.where(has, px, last_c), np.where(has, j, dend - 1))
        res["引け"] = (last_c, dend - 1)
        # SL side別
        r = (cJ - ep[:, None]) / ep[:, None] * 100
        for side in SIDES:
            for sl in SLS[1:]:
                anyc, k = first_true(side * r <= -sl)
                px, j = exit_px_at(k)
                res[("SL", side, sl)] = (anyc, px, j)
        return ep, res

    def returns(self, e, ep, res, side, with_cost=False):
        """各出口(EXITS)の%リターン行列 (n_e × len(EXITS))。コスト: 往復0.10% + 不利1tick×2。with_cost=True なら (ret, cost) を返す"""
        is5 = self.is500_row[e]
        tk_e = tick_of(ep, is5)
        ep_eff = ep + side * tk_e
        out = np.empty((len(e), len(EXITS))); cost = np.empty_like(out)
        for ci, (bx, sl) in enumerate(EXITS):
            px, j = res[bx]
            if sl is not None:
                anyc, spx, sj = res[("SL", side, sl)]
                use = anyc & (sj < j)
                px = np.where(use, spx, px); j = np.where(use, sj, j)
            tk_x = tick_of(px, is5)
            px_eff = px - side * tk_x
            out[:, ci] = side * (px_eff - ep_eff) / ep_eff * 100 - COST_RT
            cost[:, ci] = (tk_e / ep + tk_x / px) * 100 + COST_RT
        return (out, cost) if with_cost else out

    def random_entries(self, groups, w, rng):
        """groups: 対象の銘柄日id配列。各銘柄日で時間帯 w 内の足を1本ランダムに選ぶ(同数・同銘柄・同時間帯)。戻り: 足index配列(groups順)"""
        lo, hi = w
        ok = (self.tmin >= lo) & (self.tmin <= hi) & (np.arange(self.n) + 1 < self.row_dend) & np.isin(self.grp, groups)
        idx = np.flatnonzero(ok)
        r = rng.random(len(idx)); g = self.grp[idx]
        order = np.lexsort((r, g)); idx = idx[order]; g = g[order]
        first = np.r_[True, g[1:] != g[:-1]]
        pick = idx[first]; pg = g[first]
        # groups順に並べ直し(見つからない銘柄日は落とす)
        pos = np.searchsorted(pg, groups)
        pos = np.minimum(pos, len(pg) - 1)
        okg = pg[pos] == groups
        return pick[pos[okg]]

    def trades(self, p, sg, dfn, band, wn, side, un, earn_excl):
        """1セルの玉明細(全出口列) DataFrame と、ランダム比較用のコンテキスト"""
        ma, up, lo = self.bb(p, sg); sig = self.signals(up, lo, band, dfn); w = self.windows()[wn]
        e = self.first_in_window(sig, w); g = self.grp[e]
        m = self.uni[un][g] & (~self.g_earn[g] if earn_excl else True)
        e = e[m]; g = g[m]
        ep, res = self.simulate(e, ma, up, lo, band)
        R, C = self.returns(e, ep, res, side, with_cost=True)
        df = pd.DataFrame({"tk": self.tickers[self.sid[e]], "date": self.dates[self.did[e]], "tod": self.tod[e], "grp": g, "half": self.half[g], "ep": ep})
        for ci, (bx, sl) in enumerate(EXITS):
            df[f"{bx}|{sl:g}" if sl is not None else f"{bx}|0"] = R[:, ci]
        df["cost_eod"] = C[:, EXITS.index(("引け", None))]
        return df, (ma, up, lo, w, band, side)

    def random_returns(self, groups, ctx, rng):
        ma, up, lo, w, band, side = ctx
        e = self.random_entries(groups, w, rng)
        if len(e) == 0: return None
        ep, res = self.simulate(e, ma, up, lo, band)
        return self.returns(e, ep, res, side), self.half[self.grp[e]]


def agg_block(R, mask):
    sub = R[mask]
    n = len(sub)
    if n == 0: return np.zeros(5 * R.shape[1])
    return np.concatenate([np.full(R.shape[1], n), sub.sum(0), (sub > 0).sum(0), sub.clip(min=0).sum(0), -sub.clip(max=0).sum(0)])


def run_grid(kind, out_pkl, periods=PERIODS, sigmas=SIGMAS):
    D = Data(kind); W = D.windows()
    rows = []; t0 = time.time(); ncell = 0
    for p in periods:
        for sg in sigmas:
            ma, up, lo = D.bb(p, sg)
            for band in BANDS:
                for dfn in DEFS:
                    sig = D.signals(up, lo, band, dfn)
                    for wn, w in W.items():
                        e = D.first_in_window(sig, w)
                        if len(e) < 30: continue
                        ep, res = D.simulate(e, ma, up, lo, band)
                        g = D.grp[e]; half = D.half[g]
                        for side in SIDES:
                            R, C = D.returns(e, ep, res, side, with_cost=True)
                            for un in UNIS:
                                um = D.uni[un][g]
                                for ei, en in enumerate(EARNS):
                                    m = um & (~D.g_earn[g] if ei else True)
                                    if m.sum() < 1: continue
                                    a_all = agg_block(R, m); a_1 = agg_block(R, m & (half == 1)); a_2 = agg_block(R, m & (half == 2))
                                    csum = C[m].sum(0)
                                    K = len(EXITS)
                                    for ci, (bx, sl) in enumerate(EXITS):
                                        rows.append((kind, p, sg, dfn, band, wn, side, bx, sl if sl is not None else 0.0, un, en,
                                                     a_all[ci], a_all[K + ci], a_all[2 * K + ci], a_all[3 * K + ci], a_all[4 * K + ci],
                                                     a_1[ci], a_1[K + ci], a_1[2 * K + ci], a_1[3 * K + ci], a_1[4 * K + ci],
                                                     a_2[ci], a_2[K + ci], a_2[2 * K + ci], a_2[3 * K + ci], a_2[4 * K + ci], csum[ci]))
                                    ncell += K
            print(f"  [{kind}] p={p} σ={sg} done cells={ncell:,} {time.time()-t0:.0f}s", flush=True)
    cols = ["kind", "period", "sigma", "def", "band", "window", "side", "exit", "sl", "uni", "earn",
            "n", "sum", "nwin", "gp", "gl", "n1", "sum1", "nwin1", "gp1", "gl1", "n2", "sum2", "nwin2", "gp2", "gl2", "cost"]
    df = pd.DataFrame(rows, columns=cols)
    df.to_pickle(out_pkl)
    print(f"[grid {kind}] rows={len(df):,} saved {out_pkl} {time.time()-t0:.0f}s", flush=True)
    return df


if __name__ == "__main__":
    kind = sys.argv[1]
    here = os.path.dirname(os.path.abspath(__file__))
    run_grid(kind, os.path.join(here, f"grid_{kind}.pkl"))

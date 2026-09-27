# -*- coding: utf-8 -*-
"""_audit_wariyasu_parity.py — 割安B: 本番コード(wariyasu_signal.py)を10年に当てた玉が、BTと1玉単位で一致するかを確かめる（2026-09-27）。
ルールは wariyasu_signal.py 冒頭の定数（WY_RULE と RULES）を読む。差し替えたらこれをやり直す。
  python -X utf8 _audit_wariyasu_parity.py        → 今の WY_RULE で
  python -X utf8 _audit_wariyasu_parity.py C1     → C1 に切り替えて（R_opt も同様）

■ R_opt（押し目の指値型・audit_ropt）: 基準=_bt_souzai_opt_0927.py そのもの（「定義」の前までを exec＝slim の読み込み・to_cands・run・metrics）を
  _bt_souzai_opt5_0927.py の最終構成と同じ呼び方で回す。本番側はパネルの四本値・出来高・バリュエーションと _fins_history.pkl から
  本番の関数（calc_liq/calc_t4/配当予想の状態更新/calc_dy/calc_ep/calc_compz/calc_b_threshold/entry_mask/cands_of_day）で信号を作り直し、
  本番のエンジン Engine.step を毎日回す。①エンジンをBTと同じ動き（待ちの指値を全部持つ）にして玉が全部一致するか ②信号も本番の関数で
  ③値がさ見送りを両方に ④本番の置き方（空き枠の数だけスコア上位に指値）とBTの理想化の差。
■ C1（翌寄り成行型・audit_c1）: 基準=_bt_souzai_sig2_0927.pkl の B/T1/LIQ/DY/NEXT_E/RAWR → _bt_souzai_v2sim_0927.py の run()（写経 bt_run）。
  本番側は同じく本番の関数で作り直して比べる。別途 market_calendar.csv とパネルの営業日・val_prev を _valuation_10y.pkl の生データに当てる検査。
同じにしたもの（本番のデータでは再現できない部分）: PBR/予想PER/BPS/EPS はBTのパネル（前営業日・最大3日遡り）。決算日は BT の NEXT_E（実際の開示日）。
継ぎ目(2021-10)で株価が飛ぶ銘柄の除外(bad)はBTだけの事情なので同じ除外を当てる。値がさの判定の生の終値はBTと同じく調整後の終値×RAWR。
出力: 玉の違い（理由つき）・成績・直近の候補と持ち玉（_audit_wariyasu_bt_recent.json）。メモリ 約2GB・1〜2分。
"""
import gc
import json
import math
import os
import pickle
import sys
import time

import numpy as np
import pandas as pd

import wariyasu_signal as W


# ═════ BTの関数（_bt_souzai_v2sim_0927.py から写経。大域変数を引数にしただけ）═════
def bt_entry_day(M, lookback=20):
    prior = pd.DataFrame(M.astype(np.float32)).shift(1).rolling(lookback, min_periods=1).max().fillna(0).to_numpy() > 0
    return M & ~prior


def bt_to_cands(mask, score=None):
    s_, j_ = np.nonzero(mask)
    sc = score[s_, j_] if score is not None else np.zeros(len(s_))
    out = {}
    for s, j, v in zip(s_, j_, sc):
        out.setdefault(int(s), []).append((int(j), float(v) if np.isfinite(v) else -1e9))
    return out


BT_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
          (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000),
          (200000, 40000), (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000), (1e18, 300000)]
BT_LB = np.array([b for b, _ in BT_LIM]); BT_LW = np.array([w for _, w in BT_LIM], float)


def bt_make_stuck(O, H, C, RAWR):
    def stuck_up(t, j):
        pc = C[t - 1, j]
        if not (np.isfinite(pc) and np.isfinite(O[t, j]) and np.isfinite(H[t, j])): return False
        raw = pc * RAWR[t - 1, j]
        if not (raw > 0): raw = pc
        w = BT_LW[np.searchsorted(BT_LB, raw, side="right")] / raw
        return bool((O[t, j] / pc - 1 >= w - 0.003) and (H[t, j] <= O[t, j] * 1.0005))
    return stuck_up


def bt_run(cands, X, stop=0.07, trail=0.05, act=0.05, maxhold=20, entry="open", earn_gap=10, tp=None, pbr=None,
           t_start=1, t_end=None, t4_limit=0.98, t4_days=3, miss_days=5):
    """BTの run()（乱数・keep・STATは外した。0.98/+3/5 は引数にしただけ）"""
    O, H, L, C, NEXT_E, cal, PBRd = X["O"], X["H"], X["L"], X["C"], X["NEXT_E"], X["cal"], X["PBRd"]
    bad, si, stuck_up = X["bad"], X["si"], X["stuck_up"]
    SLOTS, SIZE, SIDE, RATE = X["SLOTS"], X["SIZE"], X["SIDE"], X["RATE"]
    T = C.shape[0]
    t_end = T if t_end is None else t_end
    held = {}; pend = []; tr = []
    eq = np.zeros(T); realized = 0.0
    for t in range(max(1, t_start), t_end):
        free = SLOTS - len(held)
        if entry == "open":
            cl = cands.get(t - 1)
            if cl:
                cl = list(cl)
                cl.sort(key=lambda x: -x[1])
                for j, _ in cl:
                    if free <= 0: break
                    if j in held: continue
                    o = O[t, j]
                    if not (np.isfinite(o) and o > 0): continue
                    if NEXT_E[t - 1, j] - (t - 1) <= earn_gap: continue
                    if j in bad and t - 1 <= si + 1 <= t + maxhold: continue
                    if stuck_up(t, j): continue
                    held[j] = dict(e=t, px=o, peak=o, ne=NEXT_E[t - 1, j], last=o); free -= 1
        else:
            kp = []
            pend.sort(key=lambda x: -x[3])
            for (j, lim, exp, sc, ne) in pend:
                if t > exp or j in held: continue
                if free > 0 and np.isfinite(L[t, j]) and L[t, j] <= lim and np.isfinite(O[t, j]):
                    px = min(O[t, j], lim); held[j] = dict(e=t, px=px, peak=px, ne=ne, last=px); free -= 1
                else:
                    kp.append((j, lim, exp, sc, ne))
            pend = kp
        for j in list(held):
            p = held[j]
            o, h, l, c = O[t, j], H[t, j], L[t, j], C[t, j]
            if not np.isfinite(c):
                p["miss"] = p.get("miss", 0) + 1
                if p["miss"] > miss_days:
                    r = p["last"] / p["px"] - 1 - 2 * SIDE - RATE * (cal[t] - cal[p["e"]]) / 365
                    tr.append((p["e"], t, j, r, "廃止等", p["px"], p["last"])); realized += SIZE * r; del held[j]
                continue
            p["miss"] = 0
            x = why = None
            if p.get("pbrx") and np.isfinite(o): x, why = o, "PBR>1"
            ls = p["px"] * (1 - stop) if stop else -np.inf
            lt = p["peak"] * (1 - trail) if (trail and p["peak"] >= p["px"] * (1 + act)) else -np.inf
            lvl, lw = (lt, "トレーリング") if lt > ls else (ls, "損切り")
            tpl = p["px"] * (1 + tp) if tp else np.inf
            if x is None and p["e"] < t and np.isfinite(o):
                if o <= lvl: x, why = o, lw
                elif o >= tpl: x, why = o, "利確"
            if x is None:
                if np.isfinite(l) and l <= lvl: x, why = lvl, lw
                elif np.isfinite(h) and h >= tpl: x, why = tpl, "利確"
            if x is None:
                if (t - p["e"] + 1) >= maxhold: x, why = c, "期限"
                elif t >= p["ne"] - 1: x, why = c, "決算前"
            if x is not None:
                r = x / p["px"] - 1 - 2 * SIDE - RATE * (cal[t] - cal[p["e"]]) / 365
                tr.append((p["e"], t, j, r, why, p["px"], x)); realized += SIZE * r; del held[j]
            else:
                if np.isfinite(h): p["peak"] = max(p["peak"], h)
                p["last"] = c
                if pbr is not None and np.isfinite(PBRd[t, j]) and PBRd[t, j] > pbr: p["pbrx"] = True
        if entry != "open":
            for j, sc in cands.get(t, []):
                if np.isfinite(C[t, j]) and NEXT_E[t, j] - t > earn_gap and not (j in bad and t <= si + 1 <= t + maxhold):
                    pend.append((j, C[t, j] * t4_limit, t + t4_days, sc, NEXT_E[t, j]))
        eq[t] = realized + sum(SIZE * (p["last"] / p["px"] - 1) for p in held.values())
    openp = [(p["e"], j, p["last"] / p["px"] - 1) for j, p in held.items()]
    return tr, eq, openp


# ═════ 本番のエンジンを同じパネルで回す ═════
def prod_run(cands, X, eng, codes, t_start=1, t_end=None):
    """cands: {s: [(code, score), ...]}（本番の cands_of_day の出力）。W.Engine.step を毎日回す"""
    O, H, L, C, NEXT_E, cal, PBRd, RAWR = X["O"], X["H"], X["L"], X["C"], X["NEXT_E"], X["cal"], X["PBRd"], X["RAWR"]
    bad, si, SIZE = X["bad"], X["si"], X["SIZE"]
    T = C.shape[0]
    t_end = T if t_end is None else t_end
    cix = {c: j for j, c in enumerate(codes)}
    openmode = eng.entry in ("T1", "first")

    def bar(t, code):
        j = cix[code]
        return O[t, j], H[t, j], L[t, j], C[t, j]

    def ne_of(s, code):
        return int(NEXT_E[s, cix[code]])

    def cal_days(t):
        return int(cal[t])

    def is_stuck(t, code):
        j = cix[code]
        pc = C[t - 1, j]
        if not (np.isfinite(pc) and np.isfinite(O[t, j]) and np.isfinite(H[t, j])):
            return False
        raw = pc * RAWR[t - 1, j]
        if not (raw > 0):
            raw = pc
        return W.stuck_up(raw, O[t, j], H[t, j], ratio_open=O[t, j] / pc)

    def skip(s, code):       # BTだけの継ぎ目の除外（本番には無い）
        j = cix[code]
        if j not in bad:
            return False
        return (s <= si + 1 <= s + 1 + eng.maxhold) if openmode else (s <= si + 1 <= s + eng.maxhold)

    def pbr_of(t, code):
        return PBRd[t, cix[code]]

    held, pend, tr, stuck = {}, [], [], []
    eq = np.zeros(T); realized = 0.0
    for t in range(max(1, t_start), t_end):
        ev = eng.step(held, pend, t, cands.get(t - 1), cands.get(t), bar, ne_of, cal_days, is_stuck=is_stuck,
                      pbr_of=pbr_of if eng.pbr_tp is not None else None, skip=skip)
        for e in ev:
            if e["kind"] == "exit":
                tr.append((e["e"], t, cix[e["code"]], e["r"], e["why"], e["px"], e["x"]))
                realized += SIZE * np.float64(e["r"])          # BTと同じ型（評価額の足し算の丸めまで揃える）
            elif e["kind"] == "stuck":
                stuck.append((t, cix[e["code"]]))
        eq[t] = realized + sum(SIZE * (p["last"] / p["px"] - 1) for p in held.values())
    openp = [(p["e"], cix[c], p["last"] / p["px"] - 1) for c, p in held.items()]
    return tr, eq, openp, stuck


def summ(res, dates, SIZE, t_start=1, t_end=None):
    tr, eq = res[0], res[1]
    T = len(dates)
    t_end = T if t_end is None else t_end
    R = pd.DataFrame([x[:5] for x in tr], columns=["e", "x", "j", "r", "why"])
    if R.empty:
        return dict(n=0)
    r = R.r.values.astype(float)
    yrs = (dates[min(t_end, T) - 1] - dates[max(t_start, 1)]).days / 365.25
    e_ = eq[max(t_start, 1):t_end]
    dd = (e_ - np.maximum.accumulate(e_)).min() if len(e_) else 0
    hold = (R.x - R.e + 1).values
    return dict(n=len(r), per_yr=len(r) / yrs, win=(r > 0).mean() * 100, pf=r[r > 0].sum() / max(1e-9, -r[r <= 0].sum()),
                total=r.sum() * SIZE / 1e4, dd=dd / 1e4, hold_avg=hold.mean(), hold_med=float(np.median(hold)),
                why=" ".join(f"{k}{v:.0f}%" for k, v in (R.why.value_counts(normalize=True) * 100).items()))


def fmt(s):
    if not s.get("n"):
        return "玉なし"
    return (f"{s['n']}玉(年{s['per_yr']:.1f}) {s['total']:+.1f}万 PF{s['pf']:.2f} 勝率{s['win']:.1f}% DD{s['dd']:+.1f}万 "
            f"平均保有{s['hold_avg']:.1f}日(中央{s['hold_med']:.0f}) 出口[{s['why']}]")


def audit_c1():
    """C1（翌寄り成行型）: 基準=_bt_souzai_v2sim_0927.py の run()（写経 bt_run）と sig2"""
    t0 = time.time()
    print(f"ルール[{W.WY_RULE}]: B={W.WY_B_MODE} PBR≤{W.WY_PBR_MAX} PER≤{W.WY_PER_MAX} 利回り≥{W.WY_DY_MIN} 並べ方={W.WY_RANK_KEY} 入り方={W.WY_ENTRY} "
          f"T1≤{W.WY_T1_RATIO} 代金≥{W.WY_TOV_MIN:g} 枠{W.WY_SLOTS}×{W.WY_SIZE:,} 損切{W.WY_STOP} トレール{W.WY_TRAIL}/{W.WY_TRAIL_ACT} "
          f"最長{W.WY_MAXHOLD} 決算{W.WY_EARN_GAP} 利確{W.WY_TP} PBR利確{W.WY_PBR_TP}", flush=True)
    DEFAULT = (W.WY_B_MODE == "threshold" and (W.WY_PBR_MAX, W.WY_PER_MAX, W.WY_DY_MIN) == (1.0, 10.0, 3.0))

    # ---- BTの出力（sig2）: 要る配列だけ残す ----
    S = pickle.load(open("_bt_souzai_sig2_0927.pkl", "rb"))
    keep = ["B", "T1", "T4", "LIQ", "DY", "PBR1", "PER1", "NEXT_E", "RAWR", "bad", "si"]
    S = {k: S[k] for k in keep}
    gc.collect()
    print(f"sig2 読み込み {time.time() - t0:.0f}s", flush=True)
    # ---- 価格パネル ----
    P = pickle.load(open("_bt_souzai_panel_0927.pkl", "rb"))
    dates, codes = P["dates"], list(P["codes"])
    O, H, L, C, V = (P["P"][k] for k in ("O", "H", "L", "C", "V"))
    VAL = {k: P["VAL"][k] for k in ("PBR", "FwdPER", "BPS")}
    del P
    gc.collect()
    T, N = C.shape
    ds = [str(d.date()) for d in dates]
    cal = dates.values.astype("datetime64[D]").astype(np.int64)
    print(f"パネル {T}日×{N}銘柄 {ds[0]}〜{ds[-1]}  {time.time() - t0:.0f}s", flush=True)

    # ① 営業日: market_calendar.csv とパネル
    mc = W.Cal()
    lo, hi = max(ds[0], mc.days[0]), ds[-1]
    a = {d for d in mc.days if lo <= d <= hi}; b = {d for d in ds if lo <= d <= hi}
    print(f"\n① 営業日 {lo}〜{hi}: market_calendar.csv {len(a)}日 / パネル {len(b)}日 / 片方だけ: カレンダーだけ{sorted(a - b)[:10]} パネルだけ{sorted(b - a)[:10]}")

    X = dict(O=O, H=H, L=L, C=C, NEXT_E=S["NEXT_E"], cal=cal, PBRd=VAL["PBR"], RAWR=S["RAWR"], bad=S["bad"], si=S["si"],
             stuck_up=bt_make_stuck(O, H, C, S["RAWR"]), SLOTS=W.WY_SLOTS, SIZE=W.WY_SIZE, SIDE=W.WY_SIDE_COST, RATE=W.WY_RATE)

    # ---- 基準(BT) ----
    if W.WY_B_MODE != "threshold" or W.WY_RANK_KEY == "compz":
        print("⚠ 合成スコアを使う設定は audit_ropt（_bt_souzai_opt_0927 が基準）で監査する → ここでは中止")
        return 1
    with np.errstate(invalid="ignore"):
        if DEFAULT:
            B_ref = S["B"]
        elif W.WY_B_MODE == "threshold":
            B_ref = (S["PBR1"] <= W.WY_PBR_MAX) & (S["PBR1"] > 0) & (S["PER1"] > 0) & (S["PER1"] <= W.WY_PER_MAX) & (S["DY"] >= W.WY_DY_MIN)
        B_chk = (S["PBR1"] <= 1.0) & (S["PBR1"] > 0) & (S["PER1"] > 0) & (S["PER1"] <= 10) & (S["DY"] >= 3.0)
    print(f"  (確認) sig2 の B と BTの式(PBR1/PER1/DYから)の差 {int((B_chk != S['B']).sum())}マス")
    if W.WY_T1_RATIO == 0.6:
        T1_ref = S["T1"]
    else:
        prevC = np.vstack([np.full((1, N), np.nan, np.float32), C[:-1]])
        with np.errstate(invalid="ignore"):
            TR = np.fmax(H - L, np.fmax(np.abs(H - prevC), np.abs(L - prevC)))
        a5 = pd.DataFrame(TR).rolling(5).mean().to_numpy(np.float32); a20 = pd.DataFrame(TR).rolling(20).mean().to_numpy(np.float32)
        with np.errstate(invalid="ignore", divide="ignore"):
            T1_ref = (a5 / a20) <= W.WY_T1_RATIO
        del prevC, TR, a5, a20
    if W.WY_TOV_MIN == 1e8:
        LIQ_ref = S["LIQ"]
    else:
        with np.errstate(invalid="ignore"):
            LIQ_ref = pd.DataFrame(C * V).rolling(20, min_periods=15).mean().to_numpy(np.float32) >= W.WY_TOV_MIN
    SC_ref = {"dy": S["DY"], "pbr": -S["PBR1"], "per": -S["PER1"]}.get(W.WY_RANK_KEY)
    if W.WY_ENTRY == "T1":
        M_ref = B_ref & T1_ref & LIQ_ref
    elif W.WY_ENTRY == "first":
        M_ref = bt_entry_day(B_ref, W.WY_FIRST_LOOKBACK) & LIQ_ref
    else:
        M_ref = B_ref & S["T4"] & LIQ_ref
    cands_ref = bt_to_cands(M_ref, SC_ref)
    cands_ref0 = {s_: list(v_) for s_, v_ in cands_ref.items()}
    # 値がさの見送り（1枠で100株に届かない＝シグナル日の生の終値×100 > 1枠）: BTに無い本番のルール → 基準にも同じ見送りを当てる。
    # 生の終値 = 調整後の終値 × RAWR（BTのS高判定と同じ換算）
    unaff = []
    if W.WY_SKIP_UNAFFORDABLE:
        for s_ in list(cands_ref):
            keep_ = [(j, v_) for j, v_ in cands_ref[s_] if C[s_, j] * S["RAWR"][s_, j] * 100 <= W.WY_SIZE]
            unaff += [(s_, j) for j, v_ in cands_ref[s_] if (j, v_) not in keep_]
            cands_ref[s_] = keep_
    kw = dict(stop=W.WY_STOP, trail=W.WY_TRAIL, act=W.WY_TRAIL_ACT, maxhold=W.WY_MAXHOLD, earn_gap=W.WY_EARN_GAP, tp=W.WY_TP,
              pbr=W.WY_PBR_TP, entry="open" if W.WY_ENTRY in ("T1", "first") else "pullback", t4_limit=W.WY_T4_LIMIT,
              t4_days=W.WY_T4_DAYS, miss_days=W.WY_MISS_DAYS)
    res_ref0 = bt_run(cands_ref0, X, **kw)
    res_ref = bt_run(cands_ref, X, **kw)
    s_ref = summ(res_ref, dates, W.WY_SIZE)
    I2026 = int(np.searchsorted(dates, pd.Timestamp("2026-01-01")))
    print(f"\n基準(BTのコード×BTの信号・1枠{W.WY_SIZE // 10000}万) 値がさ見送りなし: {fmt(summ(res_ref0, dates, W.WY_SIZE))}")
    if W.WY_SKIP_UNAFFORDABLE:
        a0 = {(int(x[0]), int(x[2])) for x in res_ref0[0]}; a1 = {(int(x[0]), int(x[2])) for x in res_ref[0]}
        print(f"   値がさ見送りの候補 {len(unaff)}件: " + "・".join(f"{ds[s_]} {codes[j]}(終値{float(C[s_, j] * S['RAWR'][s_, j]):,.0f}円)" for s_, j in unaff[:10])
              + f" → 玉が変わった {len(a0 ^ a1)}件（見送りなしだけ{len(a0 - a1)}・ありだけ{len(a1 - a0)}）")
    print(f"基準(本番のルール＝値がさ見送り{'あり' if W.WY_SKIP_UNAFFORDABLE else 'なし'}): {fmt(s_ref)}  {time.time() - t0:.0f}s", flush=True)

    # ---- 本番 ----
    T1_p = W.calc_t1(H, L, C)
    LIQ_p = W.calc_liq(C, V)
    T4_p = W.calc_t4(C) if W.WY_ENTRY == "T4" else None
    print(f"② T1 の違い {int((T1_p != T1_ref).sum())}マス / 代金1億(LIQ) の違い {int((LIQ_p != LIQ_ref).sum())}マス"
          + (f" / T4 の違い {int((T4_p != S['T4']).sum())}マス" if T4_p is not None else ""), flush=True)
    # 配当予想: _fins_history.pkl を開示日（有効日）ごとに本番の状態更新へ流す
    F = pd.read_pickle(W.FINS_PKL)[["Code", "DiscDate", "DiscTime", "DocType", "FDivAnn", "NxFDivAnn"]]
    F = F[F.Code.astype(str).str.len() == 5].copy()
    F["c4"] = F.Code.str[:4]
    cix = {c: j for j, c in enumerate(codes)}
    F = F[F.c4.isin(cix)]
    for c in ("FDivAnn", "NxFDivAnn"):
        F[c] = pd.to_numeric(F[c], errors="coerce")
    F["dt"] = pd.to_datetime(F.DiscDate)
    F["sidx"] = np.searchsorted(dates.values, F.dt.values, side="right") - 1
    F = F[(F.sidx >= 0) & (F.sidx < T)]
    F = F.sort_values(["c4", "dt", "DiscTime"]).reset_index(drop=True)      # BTと同じ並び
    pcal = W.Cal(days=ds)
    eff = lambda s: pcal.days[pcal.idx(s)] if pcal.idx(s) >= 0 else None
    bad_eff = int(sum(eff(str(x)[:10]) != ds[s] for x, s in zip(F.DiscDate.values, F.sidx.values)))
    print(f"   開示 {len(F):,}行 / 本番の有効日とBTの sidx の違い {bad_eff}行", flush=True)
    by_s = {}
    recs = F[["Code", "DiscDate", "DiscTime", "DocType", "FDivAnn", "NxFDivAnn"]].to_dict("records")
    for rr, s in zip(recs, F.sidx.values):
        rr["DiscDate"] = str(rr["DiscDate"])[:10]
        by_s.setdefault(int(s), []).append(rr)
    del F, recs
    gc.collect()
    divs, stmt = {}, {}
    DIV_p = np.full((T, N), np.nan, np.float32)
    cur = np.full(N, np.nan, np.float32)
    for t in range(1, T):
        rows = by_s.get(t - 1)
        if rows:
            W.apply_fins_rows(divs, rows, eff, stmt=stmt)
            for c4 in {r["Code"][:4] for r in rows}:
                cur[cix[c4]] = W.div_as_of(divs, c4, ds[t - 1])
        DIV_p[t] = cur
    PBR1 = np.vstack([np.full((1, N), np.nan, np.float32), VAL["PBR"][:-1]])
    PER1 = np.vstack([np.full((1, N), np.nan, np.float32), VAL["FwdPER"][:-1]])
    BPS1 = np.vstack([np.full((1, N), np.nan, np.float32), VAL["BPS"][:-1]])
    DY_p = W.calc_dy(DIV_p, PBR1, BPS1)
    both = np.isfinite(DY_p) & np.isfinite(S["DY"])
    nan_diff = int((np.isfinite(DY_p) != np.isfinite(S["DY"])).sum())
    val_diff = int((DY_p[both] != S["DY"][both]).sum())
    print(f"③ 利回り(DY): 値の違い {val_diff}マス / 値の有無の違い {nan_diff}マス（どちらも0なら配当予想の状態更新はBTと同じ）", flush=True)
    del DIV_p
    B_p = W.calc_b_threshold(PBR1, PER1, DY_p)
    print(f"④ 割安B の違い {int((B_p != B_ref).sum())}マス（B {int(B_ref.sum()):,}マス中）", flush=True)
    score_p = W.rank_score(DY_p, PBR1, PER1)
    M_p = W.entry_mask(B_p, LIQ_p, T1_p, T4_p)
    print(f"⑤ 建てる候補の印 の違い {int((M_p != M_ref).sum())}マス（{int(M_ref.sum()):,}マス中）", flush=True)
    cands_p = {}
    for s in np.nonzero(M_p.any(axis=1))[0]:
        lst = W.cands_of_day(M_p[s], score_p[s], codes)
        cands_p[int(s)] = [(c, v_) for c, v_ in lst if W.affordable(float(C[s, cix[c]] * S["RAWR"][s, cix[c]]))]
    # 参考（帳簿外）に出す「割安に入った初日」も同じか。BTの「B入り初日・損切り10%・翌寄り」(+423万/PF1.53) を両方のエンジンで
    F_ref = bt_entry_day(S["B"], 20) & S["LIQ"]
    F_p = W.entry_mask(W.calc_b_threshold(PBR1, PER1, DY_p, 1.0, 10.0, 3.0), LIQ_p, mode="first")
    cf_ref = bt_to_cands(F_ref, S["DY"])
    cf_p = {int(s): W.cands_of_day(F_p[s], DY_p[s], codes) for s in np.nonzero(F_p.any(axis=1))[0]}
    kwf = dict(kw, stop=0.10, entry="open")
    rf = bt_run(cf_ref, X, **kwf)
    pf = prod_run(cf_p, X, W.Engine(stop=0.10, entry="first"), codes)
    samef = sorted((int(x[0]), int(x[2]), x[4], float(x[3])) for x in rf[0]) == sorted((int(x[0]), int(x[2]), x[4], float(x[3])) for x in pf[0])
    print(f"   参考の「割安に入った初日」の印の違い {int((F_p != F_ref).sum())}マス / BT(損切り10%) {fmt(summ(rf, dates, W.WY_SIZE))}"
          f" → 本番のエンジンで玉の一致 {'✅' if samef else '❌'}", flush=True)
    ndiff = 0
    for s in sorted(set(cands_p) | set(cands_ref)):
        a_ = [codes[j] for j, _ in sorted(cands_ref.get(s, []), key=lambda x: -x[1])]
        b_ = [c for c, _ in cands_p.get(s, [])]
        if a_ != b_:
            ndiff += 1
            if ndiff <= 5:
                print(f"   候補の並びが違う日 {ds[s]}: BT {a_[:6]} / 本番 {b_[:6]}")
    print(f"   候補の並びが違う日 {ndiff}日（候補のある日 {len(cands_ref)}日中）", flush=True)
    del PBR1, PER1, BPS1, DY_p, B_p, M_p, T1_p, LIQ_p
    gc.collect()

    eng = W.Engine()
    res_p = prod_run(cands_p, X, eng, codes)
    s_p = summ(res_p, dates, W.WY_SIZE)
    print(f"\n本番(本番の関数×本番のエンジン): {fmt(s_p)}  {time.time() - t0:.0f}s", flush=True)

    # ---- 玉の突き合わせ ----
    def key(x):
        return (int(x[0]), int(x[2]))
    A_ = {key(x): x for x in res_ref[0]}
    B_ = {key(x): x for x in res_p[0]}
    only_ref = sorted(set(A_) - set(B_)); only_p = sorted(set(B_) - set(A_))
    diff = []
    for k in sorted(set(A_) & set(B_)):
        a, b = A_[k], B_[k]
        if int(a[1]) != int(b[1]) or a[4] != b[4] or abs(float(a[3]) - float(b[3])) > 1e-6 or abs(float(a[6]) - float(b[6])) > 1e-3:
            diff.append((k, a, b))
    print(f"\n⑥ 玉の突き合わせ: BT {len(A_)}玉 / 本番 {len(B_)}玉 / 両方にある玉 {len(set(A_) & set(B_))} / "
          f"BTだけ {len(only_ref)} / 本番だけ {len(only_p)} / 出口(日・理由・値・損益)が違う {len(diff)}")
    rmax = max([abs(float(A_[k][3]) - float(B_[k][3])) for k in set(A_) & set(B_)] or [0])
    print(f"   損益率の差の最大 {rmax:.2e}（float32 の丸め以下なら同じ）")

    def why_mismatch(k, side):
        e, j = k
        s = e - 1
        inR = any(jj == j for jj, _ in cands_ref.get(s, [])); inP = any(c == codes[j] for c, _ in cands_p.get(s, []))
        if inR != inP:
            return f"シグナル日{ds[s]}の候補が違う(BT{'あり' if inR else 'なし'}/本番{'あり' if inP else 'なし'})"
        return "候補は同じ→枠の埋まり方の違い（直前の玉の出口の違いの連鎖）"
    for k in only_ref[:20]:
        x = A_[k]; print(f"   BTだけ: {ds[k[0]]} {codes[k[1]]} → {ds[int(x[1])]} {x[4]} {float(x[3]) * 100:+.2f}%  理由: {why_mismatch(k, 'ref')}")
    for k in only_p[:20]:
        x = B_[k]; print(f"   本番だけ: {ds[k[0]]} {codes[k[1]]} → {ds[int(x[1])]} {x[4]} {float(x[3]) * 100:+.2f}%  理由: {why_mismatch(k, 'p')}")
    for k, a, b in diff[:20]:
        print(f"   出口違い: {ds[k[0]]} {codes[k[1]]} BT {ds[int(a[1])]} {a[4]} {float(a[6]):.2f} / 本番 {ds[int(b[1])]} {b[4]} {float(b[6]):.2f}")
    r26 = bt_run(cands_ref, X, t_start=I2026, **kw)                      # BTの2026年の数字と同じ測り方（1月に空の口座から）
    p26 = prod_run(cands_p, X, eng, codes, t_start=I2026)
    s26r = summ(r26, dates, W.WY_SIZE, t_start=I2026); s26p = summ(p26, dates, W.WY_SIZE, t_start=I2026)
    same26 = sorted((int(x[0]), int(x[1]), int(x[2]), x[4], float(x[3])) for x in r26[0]) == \
        sorted((int(x[0]), int(x[1]), int(x[2]), x[4], float(x[3])) for x in p26[0])
    print(f"\n2026年(1/5〜{ds[-1]}・1月に空の口座から): BT {fmt(s26r)} 持ち越し{len(r26[2])}玉 含み{sum(o[2] for o in r26[2]) * W.WY_SIZE / 1e4:+.1f}万\n"
          f"                                    本番 {fmt(s26p)} 持ち越し{len(p26[2])}玉 含み{sum(o[2] for o in p26[2]) * W.WY_SIZE / 1e4:+.1f}万"
          f"  → 玉の一致 {'✅' if same26 else '❌'}")
    # 継ぎ目の除外（BTだけの事情）が何玉に効いたか
    nbad = sum(1 for s, lst in cands_ref.items() for j, _ in lst if j in S["bad"] and s <= S["si"] + 1 <= s + 1 + W.WY_MAXHOLD)
    print(f"   継ぎ目(2021-10)の除外に当たった候補 {nbad}件（BTだけの事情。本番は同じ除外を当てて比べた）")
    print(f"   寄りS高張り付きで見送った候補（本番のエンジン） {len(res_p[3])}件")

    # ---- ⑦ val_prev をバリュエーションの生データに当てる ----
    try:
        v = pd.read_pickle("_valuation_10y.pkl")
        base = pd.Timestamp(v["base"]); cols = v["cols"]
        kP, kE, kB = cols.index("PBR"), cols.index("FwdPER"), cols.index("BPS")
        vlast = base + pd.Timedelta(days=max(int(np.nanmax(a_[:, 0])) for a_ in v["data"].values() if len(a_)))
        iv = int(np.searchsorted(dates, vlast, side="right")) - 1
        rng = np.random.default_rng(7)
        smp = sorted(set(rng.choice(np.arange(40, iv + 1), size=100, replace=False).tolist()))
        dayoff = {}
        for c, a_ in v["data"].items():
            o_ = a_[:, 0].astype(np.int64)
            if len(o_) and not np.all(o_[1:] >= o_[:-1]):
                srt = np.argsort(o_, kind="stable"); v["data"][c] = a_ = a_[srt]; o_ = o_[srt]
            dayoff[c] = o_
        nd = 0; ncmp = 0
        for t in smp:
            vd = []
            for k in range(1, 5):
                off = int((dates[t - k] - base).days)
                dct = {}
                for c, a_ in v["data"].items():
                    jj = int(np.searchsorted(dayoff[c], off, side="right")) - 1       # 同じ日が複数あれば最後の行（BTの keep="last"）
                    if jj >= 0 and dayoff[c][jj] == off:
                        rr = a_[jj]
                        dct[c] = (rr[kP], rr[kE], rr[kB])
                vd.append(dct)
            pb, pe, bp = W.val_prev(vd, codes)
            for arr, k in ((pb, "PBR"), (pe, "FwdPER"), (bp, "BPS")):
                ref = VAL[k][t - 1]
                ok = np.isfinite(arr) | np.isfinite(ref)
                ncmp += int(ok.sum())
                nd += int((~((arr == ref) | (np.isnan(arr) & np.isnan(ref))) & ok).sum())
        print(f"\n⑦ 前営業日のバリュエーション(val_prev)を生データに当てた: 抽出{len(smp)}日 {ncmp:,}値のうち違い {nd}値"
              f"（〜{vlast.date()}・0ならBTの reindex→ffill(limit=3) と同じ）", flush=True)
        del v, dayoff
    except FileNotFoundError:
        print("⑦ _valuation_10y.pkl が無い → 省略")

    # ---- 直近の候補とBTの持ち玉（ドライランの突き合わせ用）----
    recent = {}
    for s in range(max(0, T - 30), T):
        recent[ds[s]] = [codes[j] for j, _ in sorted(cands_ref.get(s, []), key=lambda x: -x[1])]
    openp = [{"code": codes[j], "entry": ds[e], "pnl_pct": round(float(r) * 100, 2)} for e, j, r in res_ref[2]]
    last_tr = [{"code": codes[int(x[2])], "entry": ds[int(x[0])], "exit": ds[int(x[1])], "why": x[4], "r_pct": round(float(x[3]) * 100, 2)}
               for x in sorted(res_ref[0], key=lambda x: x[1])[-12:]]
    json.dump({"last_date": ds[-1], "cands": recent, "open": openp, "last_trades": last_tr,
               "summary_ref": {k: (round(v_, 3) if isinstance(v_, float) else v_) for k, v_ in s_ref.items()},
               "summary_prod": {k: (round(v_, 3) if isinstance(v_, float) else v_) for k, v_ in s_p.items()}},
              open(f"_audit_wariyasu_bt_recent_{W.WY_RULE}.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\nBTの持ち玉({ds[-1]}時点): " + ("・".join(f"{o['code']}({o['entry']}〜 {o['pnl_pct']:+.1f}%)" for o in openp) or "なし"))
    verdict = (not only_ref and not only_p and not diff and same26)
    print(f"\n判定: {'✅ 10年の全玉が一致' if verdict else '❌ 不一致あり（上の一覧）'}  {time.time() - t0:.0f}s")
    return 0 if verdict else 1


def audit_ropt():
    """R_opt（押し目の指値型）: 基準=_bt_souzai_opt_0927.py そのもの（「定義」の前まで＝slimの読み込み・to_cands・run・metrics を exec）。
    _bt_souzai_opt5_0927.py の最終構成と同じ呼び方で10年を回し、本番の関数（パネルと開示から作り直した信号）×本番のエンジンと玉単位で比べる"""
    t0 = time.time()
    print(f"ルール[{W.WY_RULE}]: B={W.WY_B_MODE} PBR≤{W.WY_PBR_MAX} PER≤{W.WY_PER_MAX} 利回り≥{W.WY_DY_MIN} 並べ方={W.WY_RANK_KEY} "
          f"入り方={W.WY_ENTRY}(終値×{W.WY_T4_LIMIT}・{W.WY_T4_DAYS}日・置き方={W.WY_T4_ORDERS}) 代金≥{W.WY_TOV_MIN:g} 枠{W.WY_SLOTS}×{W.WY_SIZE:,} "
          f"損切{W.WY_STOP} トレール{W.WY_TRAIL}/{W.WY_TRAIL_ACT} 最長{W.WY_MAXHOLD} 決算{W.WY_EARN_GAP} 値がさ見送り={W.WY_SKIP_UNAFFORDABLE}", flush=True)
    if W.WY_TP or W.WY_T4_LIMIT != 0.98 or W.WY_T4_DAYS != 3 or W.WY_FIRST_LOOKBACK != 20:
        print("⚠ BT(opt)の run() に無い設定（固定利確・指値の率/日数・初日の窓）→ 基準は既定値のまま。その部分は照合にならない")
    G = {}
    src = open("_bt_souzai_opt_0927.py", encoding="utf-8").read()
    exec(src.split("# ================= 定義 =================")[0], G)
    dates, codes = G["dates"], list(G["codes"])
    O, H, L, C = G["O"], G["H"], G["L"], G["C"]
    PBR1, PER1, DY, EP1, COMPZ = G["PBR1"], G["PER1"], G["DY"], G["EP1"], G["COMPZ"]
    LIQ, T1, T4, NEXT_E, RAWR, bad, si = G["LIQ"], G["T1"], G["T4"], G["NEXT_E"], G["RAWR"], G["bad"], G["si"]
    T, N = C.shape
    ds = [str(d.date()) for d in dates]
    I2026 = int(np.searchsorted(dates, pd.Timestamp("2026-01-01")))
    metrics = G["metrics"]

    def fm(m):
        return (f"{m['n']}玉 {m['total']:+.1f}万(含み込み) PF{m['pf']:.2f} 勝率{m['win']:.0f}% DD{m['dd']:+.1f}万 月平均{m['mavg']:+.1f}万 "
                f"月の勝率{m['mwin']:.0f}% 最悪月{m['mworst']:+.1f}万 月{m['epm']:.1f}件 撃たない月{m['zero']:.0f}% 月次シャープ{m['sharpe']:.2f}")

    # ---- 基準（BTのコードとBTの信号）----
    with np.errstate(invalid="ignore"):
        VALID = LIQ & (C > 0)
        if W.WY_B_MODE == "threshold":
            B_ref = (PBR1 > 0) & (PBR1 <= W.WY_PBR_MAX) & (PER1 > 0) & (PER1 <= W.WY_PER_MAX) & (DY >= W.WY_DY_MIN)
        else:
            COMPP = np.full((T, N), np.nan, np.float32)            # BTの定義をそのまま
            for t in range(T):
                m = np.isfinite(COMPZ[t]); k = int(m.sum())
                if k < 100:
                    continue
                idx = np.nonzero(m)[0]; order = np.argsort(-COMPZ[t, idx], kind="stable")
                pr = np.empty(k, np.float32); pr[order] = (np.arange(k) + 0.5) / k; COMPP[t, idx] = pr
            B_ref = COMPP <= W.WY_COMPOSITE_TOP_PCT / 100
    M_ref = {"T4": lambda: B_ref & T4 & VALID, "T1": lambda: B_ref & T1 & VALID, "first": lambda: G["entry_day"](B_ref) & VALID}[W.WY_ENTRY]()
    SC_ref = {"compz": COMPZ, "dy": DY, "pbr": -PBR1, "per": -PER1}[W.WY_RANK_KEY]
    cands_ref = G["to_cands"](M_ref, SC_ref)
    runkw = dict(slots=W.WY_SLOTS, stop=W.WY_STOP, trail=W.WY_TRAIL, act=W.WY_TRAIL_ACT, maxhold=W.WY_MAXHOLD,
                 entry="pullback" if W.WY_ENTRY == "T4" else "open", earn_gap=W.WY_EARN_GAP, pbr=W.WY_PBR_TP)
    tr100, eq100 = G["run"](cands_ref, size=1_000_000, **runkw)
    print(f"\n基準の再現（_bt_souzai_opt5_0927 と同じ呼び方・{W.WY_SLOTS}枠×100万）: 10年 {fm(metrics((tr100, eq100), 1_000_000))}\n"
          f"   （_bt_souzai_opt5_0927.log の最終構成: 10年 +660万 玉471 勝率62% PF1.80 DD-113 月平均+5.4万 月3.8件 撃たない月9% 月次シャープ1.21）", flush=True)
    trB, eqB = G["run"](cands_ref, size=W.WY_SIZE, **runkw)

    # ---- 本番の関数で信号を作り直す（パネルの四本値・出来高・バリュエーションと _fins_history.pkl から）----
    P = pickle.load(open("_bt_souzai_panel_0927.pkl", "rb"))
    assert list(P["codes"]) == codes and (P["dates"] == dates).all()
    V = P["P"]["V"]
    VAL = {k: P["VAL"][k] for k in ("PBR", "FwdPER", "BPS", "EPS", "FwdEPS")}
    del P
    gc.collect()
    sh = lambda A: np.vstack([np.full((1, N), np.nan, np.float32), A[:-1]])
    PBR1p, PER1p, BPS1p, EPS1p, FEPS1p = (sh(VAL[k]) for k in ("PBR", "FwdPER", "BPS", "EPS", "FwdEPS"))
    del VAL
    LIQ_p = W.calc_liq(C, V)
    del V
    T4_p = W.calc_t4(C) if W.WY_ENTRY == "T4" else None
    T1_p = W.calc_t1(H, L, C) if W.WY_ENTRY == "T1" else None
    cix = {c: j for j, c in enumerate(codes)}
    F = pd.read_pickle(W.FINS_PKL)[["Code", "DiscDate", "DiscTime", "DocType", "FDivAnn", "NxFDivAnn"]]
    F = F[F.Code.astype(str).str.len() == 5].copy()
    F["c4"] = F.Code.str[:4]
    F = F[F.c4.isin(cix)]
    for c in ("FDivAnn", "NxFDivAnn"):
        F[c] = pd.to_numeric(F[c], errors="coerce")
    F["dt"] = pd.to_datetime(F.DiscDate)
    F["sidx"] = np.searchsorted(dates.values, F.dt.values, side="right") - 1
    F = F[(F.sidx >= 0) & (F.sidx < T)].sort_values(["c4", "dt", "DiscTime"]).reset_index(drop=True)
    pcal = W.Cal(days=ds)
    eff = lambda s_: pcal.days[pcal.idx(s_)] if pcal.idx(s_) >= 0 else None
    by_s = {}
    for rr, s_ in zip(F[["Code", "DiscDate", "DiscTime", "DocType", "FDivAnn", "NxFDivAnn"]].to_dict("records"), F.sidx.values):
        rr["DiscDate"] = str(rr["DiscDate"])[:10]
        by_s.setdefault(int(s_), []).append(rr)
    del F
    gc.collect()
    divs = {}
    DIV_p = np.full((T, N), np.nan, np.float32)
    cur = np.full(N, np.nan, np.float32)
    for t in range(1, T):
        rows = by_s.get(t - 1)
        if rows:
            W.apply_fins_rows(divs, rows, eff)
            for c4 in {r["Code"][:4] for r in rows}:
                cur[cix[c4]] = W.div_as_of(divs, c4, ds[t - 1])
        DIV_p[t] = cur
    DY_p = W.calc_dy(DIV_p, PBR1p, BPS1p)
    del DIV_p
    EP1_p = W.calc_ep(PBR1p, BPS1p, EPS1p, FEPS1p)
    del EPS1p, FEPS1p
    COMPZ_p = W.calc_compz(PBR1p, EP1_p, DY_p, LIQ_p) if W.needs_compz() else None

    def same(a, b):
        fa, fb = np.isfinite(a), np.isfinite(b)
        return f"有無{int((fa != fb).sum())}・値{int((a[fa & fb] != b[fa & fb]).sum())}"
    print(f"\n本番の関数で作り直した信号とBTの信号の違い（マス数）: 代金1億 {int((LIQ_p != LIQ).sum())} / "
          + (f"押し目T4 {int((T4_p != T4).sum())} / " if T4_p is not None else "")
          + (f"収縮T1 {int((T1_p != T1).sum())} / " if T1_p is not None else "")
          + f"PBR[{same(PBR1p, PBR1)}] / PER[{same(PER1p, PER1)}] / 利回り[{same(DY_p, DY)}] / 益回り[{same(EP1_p, EP1)}]"
          + (f" / 合成スコア[{same(COMPZ_p, COMPZ)}]" if COMPZ_p is not None else ""), flush=True)
    B_p = W.calc_b_threshold(PBR1p, PER1p, DY_p) if W.WY_B_MODE == "threshold" else W.calc_b_composite(COMPZ_p)
    M_p = W.entry_mask(B_p, LIQ_p, T1_p, T4_p)
    score_p = W.rank_score(DY_p, PBR1p, PER1p, COMPZ_p)
    print(f"   割安 {int((B_p != B_ref).sum())} / 候補の印 {int((M_p != M_ref).sum())}（{int(M_ref.sum()):,}マス中）", flush=True)
    cands_p = {int(s_): W.cands_of_day(M_p[s_], score_p[s_], codes) for s_ in np.nonzero(M_p.any(axis=1))[0]}
    nd = 0
    for s_ in sorted(set(cands_p) | set(cands_ref)):
        a_ = [codes[j] for j, _ in sorted(cands_ref.get(s_, []), key=lambda x: -x[1])]
        b_ = [c for c, _ in cands_p.get(s_, [])]
        if a_ != b_:
            nd += 1
            if nd <= 5:
                print(f"   候補の並びが違う日 {ds[s_]}: BT {a_[:6]} / 本番 {b_[:6]}")
    print(f"   候補の並び（{W.WY_RANK_KEY}の順位）が違う日 {nd}日（候補のある日 {len(cands_ref)}日中）", flush=True)
    del PBR1p, PER1p, BPS1p, DY_p, EP1_p, COMPZ_p, B_p, M_p, score_p, LIQ_p
    gc.collect()

    X = dict(O=O, H=H, L=L, C=C, NEXT_E=NEXT_E, cal=G["cal"], PBRd=G["PBRd"], RAWR=RAWR, bad=bad, si=si, SIZE=W.WY_SIZE)

    def cmp(ra, rb, lab):
        A_ = {(int(x[0]), int(x[2])): x for x in ra}; B_ = {(int(x[0]), int(x[2])): x for x in rb}
        oa, ob = sorted(set(A_) - set(B_)), sorted(set(B_) - set(A_))
        dx = [k for k in set(A_) & set(B_) if int(A_[k][1]) != int(B_[k][1]) or A_[k][4] != B_[k][4] or float(A_[k][3]) != float(B_[k][3])]
        ok = not oa and not ob and not dx
        print(f"   {lab}: BT {len(A_)}玉 / 本番 {len(B_)}玉 / BTだけ {len(oa)} / 本番だけ {len(ob)} / 出口(日・理由・損益)違い {len(dx)}"
              f" → {'✅ 一致' if ok else '差あり'}", flush=True)
        for k in oa[:6]:
            x = A_[k]; print(f"      BTだけ: {ds[k[0]]}約定 {codes[k[1]]} → {ds[int(x[1])]} {x[4]} {float(x[3]) * 100:+.2f}%")
        for k in ob[:6]:
            x = B_[k]; print(f"      本番だけ: {ds[k[0]]}約定 {codes[k[1]]} → {ds[int(x[1])]} {x[4]} {float(x[3]) * 100:+.2f}%")
        return ok

    print("\n玉の突き合わせ（10年・1玉単位＝約定日・銘柄・手仕舞い日・理由・損益率がビット単位で同じか）")
    eng_all = W.Engine(t4_orders="all")
    r1 = prod_run({s_: [(codes[j], v) for j, v in sorted(l_, key=lambda x: -x[1])] for s_, l_ in cands_ref.items()}, X, eng_all, codes)
    ok1 = cmp(trB, r1[0], "① 本番のエンジン(待ちの指値を全部持つ=BTと同じ動き)×BTの候補")
    r2 = prod_run(cands_p, X, eng_all, codes)
    ok2 = cmp(trB, r2[0], "② ①の候補を本番の関数で作り直したもの")
    r26b = G["run"](cands_ref, size=W.WY_SIZE, t_start=I2026, **runkw)
    r26p = prod_run(cands_p, X, eng_all, codes, t_start=I2026)
    ok26 = cmp(r26b[0], r26p[0], "   2026年(1/5〜・空の口座から)")
    raw = lambda s_, j: float(C[s_, j] * RAWR[s_, j])          # シグナル日の生の終値（BTのS高判定と同じ換算）
    cands_ref_a = {s_: [(j, v) for j, v in l_ if W.affordable(raw(s_, j))] for s_, l_ in cands_ref.items()}
    n_un = sum(len(l_) for l_ in cands_ref.values()) - sum(len(l_) for l_ in cands_ref_a.values())
    trBa, eqBa = G["run"](cands_ref_a, size=W.WY_SIZE, **runkw)
    cands_p_a = {s_: [(c, v) for c, v in l_ if W.affordable(raw(s_, cix[c]))] for s_, l_ in cands_p.items()}
    r3 = prod_run(cands_p_a, X, eng_all, codes)
    ok3 = cmp(trBa, r3[0], f"③ ②＋値がさ見送り（1枠{W.WY_SIZE // 10000}万で100株未満＝候補から{n_un}件抜ける）をBTにも同じく")
    eng_p = W.Engine()
    r4 = prod_run(cands_p_a if W.WY_SKIP_UNAFFORDABLE else cands_p, X, eng_p, codes)
    cmp(trBa if W.WY_SKIP_UNAFFORDABLE else trB, r4[0], f"④ 本番の置き方（{W.WY_T4_ORDERS}）とBT(全部持つ)の差")
    r4_26 = prod_run(cands_p_a if W.WY_SKIP_UNAFFORDABLE else cands_p, X, eng_p, codes, t_start=I2026)

    print(f"\n成績（1枠{W.WY_SIZE // 10000}万×{W.WY_SLOTS}枠・含み込みの月次＝_bt_souzai_opt の metrics）")
    for lab, res, res26 in (("BT（全部の指値・値がさ見送りなし）", (trB, eqB), r26b),
                            ("BT＋値がさ見送り", (trBa, eqBa), None),
                            (f"本番（{W.WY_T4_ORDERS}・値がさ見送り{'あり' if W.WY_SKIP_UNAFFORDABLE else 'なし'}）", (r4[0], r4[1]), r4_26)):
        tr5 = [x[:5] for x in res[0]]
        print(f"   {lab}\n      10年   {fm(metrics((tr5, res[1]), W.WY_SIZE))}")
        if res26 is not None:
            m26 = metrics(([x[:5] for x in res26[0]], res26[1]), W.WY_SIZE, t_start=I2026)
            print(f"      2026年(1/5〜{ds[-1]}・空の口座から) {fm(m26)}")
    yr_ = {}
    for x in r4[0]:
        yr_[ds[int(x[0])][:4]] = yr_.get(ds[int(x[0])][:4], 0.0) + float(x[3]) * W.WY_SIZE / 1e4
    print("   本番の年別(万・決済ベース・約定年): " + " ".join(f"{k}:{v:+.0f}" for k, v in sorted(yr_.items())))
    recent = {ds[s_]: [c for c, _ in (cands_p_a if W.WY_SKIP_UNAFFORDABLE else cands_p).get(s_, [])] for s_ in range(max(0, T - 30), T)}
    json.dump({"rule": W.WY_RULE, "last_date": ds[-1], "cands": recent,
               "open": [{"code": codes[j], "entry": ds[e], "pnl_pct": round(float(r) * 100, 2)} for e, j, r in r4[2]],
               "last_trades": [{"code": codes[int(x[2])], "entry": ds[int(x[0])], "exit": ds[int(x[1])], "why": x[4],
                                "r_pct": round(float(x[3]) * 100, 2)} for x in sorted(r4[0], key=lambda x: x[1])[-12:]]},
              open(f"_audit_wariyasu_bt_recent_{W.WY_RULE}.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"   本番の持ち玉({ds[-1]}時点): " + ("・".join(f"{codes[j]}({ds[e]}〜 {float(r) * 100:+.1f}%)" for e, j, r in r4[2]) or "なし"))
    verdict = ok1 and ok2 and ok26 and ok3
    print(f"\n判定: {'✅ BTと同じ動きのエンジン・本番の関数の信号で10年の全玉が一致（①②③と2026年）' if verdict else '❌ 不一致あり（上の一覧）'}"
          f"。④は本番の置き方とBTの理想化との差を測ったもの  {time.time() - t0:.0f}s")
    return 0 if verdict else 1


def main():
    if len(sys.argv) > 1 and sys.argv[1] in W.RULES:
        W.use_rule(sys.argv[1])
    return audit_ropt() if W.WY_ENTRY == "T4" else audit_c1()


if __name__ == "__main__":
    sys.exit(main())

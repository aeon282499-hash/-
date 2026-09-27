# -*- coding: utf-8 -*-
"""_test_wariyasu.py — 割安B（wariyasu_signal.py）のテスト（2026-09-27）
データのダウンロードなし（合成データと market_calendar.csv だけ）・数十秒。
実行: python -X utf8 _test_wariyasu.py
"""
import copy
import io
import json
import math
import os
import shutil
import sys
import tempfile
import traceback
from contextlib import redirect_stdout
from datetime import date, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)
import wariyasu_signal as W                                                    # noqa: E402
from _audit_wariyasu_parity import bt_run, bt_to_cands, bt_make_stuck, bt_entry_day, prod_run   # noqa: E402

F32 = np.float32
TESTS = []


def test(f):
    TESTS.append(f)
    return f


def weekdays(start, n, skip=()):
    out, d = [], date.fromisoformat(start)
    while len(out) < n:
        if d.weekday() < 5 and d.isoformat() not in skip:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ───────────────────────── 割安B の条件 ─────────────────────────
@test
def t_b_condition():
    PBR = np.array([1.0, 1.0001, 0.0, -0.5, np.nan, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], F32)
    PER = np.array([5, 5, 5, 5, 5, 10.0, 10.01, 0, -3, 5, np.nan], F32)
    DY = np.array([3.0, 3, 3, 3, 3, 3, 3, 3, 3, 2.999, 3], F32)
    exp = [True, False, False, False, False, True, False, False, False, False, False]
    got = W.calc_b_threshold(PBR, PER, DY, pbr_max=1.0, per_max=10.0, dy_min=3.0).tolist()      # C1 のしきい値
    assert got == exp, got
    got = W.calc_b_threshold(PBR, PER, DY, pbr_max=1.2, per_max=15.0, dy_min=3.0).tolist()      # R_opt のしきい値
    assert got == [True, True, False, False, False, True, True, False, False, False, False], got
    # 利回り=配当予想÷(PBR×BPS)。配当35円・PBR0.8・BPS1250 → 株価1000円 → 3.5%
    dy = W.calc_dy(np.array([35.0, 25.0, np.nan], F32), np.array([0.8, 0.8, 0.8], F32), np.array([1250, 1250, 1250], F32))
    assert abs(dy[0] - 3.5) < 1e-5 and abs(dy[1] - 2.5) < 1e-5 and np.isnan(dy[2])
    assert W.calc_b_threshold(np.full(3, 0.8, F32), np.full(3, 8.0, F32), dy, 1.0, 10.0, 3.0).tolist() == [True, False, False]
    # 差し替えたしきい値が効く
    assert W.calc_b_threshold(np.array([1.2], F32), np.array([8.0], F32), np.array([3.5], F32), pbr_max=1.5)[0]


# ───────────────────────── T1 と 代金 ─────────────────────────
@test
def t_t1_liq_definition():
    T = 30
    H = np.full((T, 2), 110, F32); L = np.full((T, 2), 100, F32); C = np.full((T, 2), 105, F32)
    H[-5:, 0], L[-5:, 0] = 107.5, 102.5          # 最後の5日の値幅5（それまで10）→ ATR5/ATR20 = 5/8.75 = 0.571
    H[-5:, 1], L[-5:, 1] = 108, 102              # 値幅6 → 6/9.0 = 0.667
    t1 = W.calc_t1(H, L, C)
    assert t1[-1].tolist() == [True, False], t1[-1]
    assert not t1[:20].any()                     # 20日そろうまでは出ない・そろっても比1.0は外れ
    C = np.full((25, 3), 1000, F32); V = np.full((25, 3), 1e5, F32)     # 代金 1億/日
    V[:11, 0] = np.nan                           # 値が14日だけ → 15日未満で外れ
    V[:10, 1] = np.nan                           # 15日 → 平均1億で入る
    V[:, 2] = 0.99e5                             # 0.99億 → 外れ
    assert W.calc_liq(C, V)[-1].tolist() == [False, True, False]


@test
def t_window_equals_full_history():
    """本番は直近25営業日の窓で計算する → 10年の履歴で計算した最終行と同じか"""
    rng = np.random.default_rng(1)
    T, N = 300, 60
    C = (1000 * np.exp(np.cumsum(rng.normal(0, 0.02, (T, N)), axis=0))).astype(F32)
    vol = np.repeat(rng.choice([1.0, 0.25], (T // 10 + 1, N)), 10, axis=0)[:T]      # 値幅が急に縮む時期を混ぜる（T1 が立つ）
    H = (C * (1 + vol * rng.uniform(0, 0.03, (T, N)))).astype(F32)
    L = (C * (1 - vol * rng.uniform(0, 0.03, (T, N)))).astype(F32)
    V = rng.uniform(3e4, 1.6e5, (T, N)).astype(F32)
    miss = rng.random((T, N)) < 0.03
    for A in (C, H, L, V):
        A[miss] = np.nan
    t1_full, liq_full = W.calc_t1(H, L, C), W.calc_liq(C, V)
    n_t1 = 0
    for t in range(40, T):
        s = slice(t - 24, t + 1)
        assert (W.calc_t1(H[s], L[s], C[s])[-1] == t1_full[t]).all(), t
        assert (W.calc_liq(C[s], V[s])[-1] == liq_full[t]).all(), t
        n_t1 += int(t1_full[t].sum())
    assert n_t1 > 50 and liq_full[40:].any() and not liq_full[40:].all(), (n_t1, liq_full[40:].mean())


# ───────────────────────── 配当予想の状態 ─────────────────────────
@test
def t_dividend_state_rules():
    cal = W.Cal(days=weekdays("2026-05-11", 7))
    eff = lambda s: cal.days[cal.idx(s)] if cal.idx(s) >= 0 else None
    divs, stmt = {}, {}
    rows = [
        dict(Code="11110", DiscDate="2026-05-12", DiscTime="15:00:00", DocType="FYFinancialStatements_Consolidated_JP", FDivAnn=None, NxFDivAnn=40),
        dict(Code="22220", DiscDate="2026-05-12", DiscTime="15:00:00", DocType="1QFinancialStatements_Consolidated_JP", FDivAnn="", NxFDivAnn=50),
        dict(Code="33330", DiscDate="2026-05-13", DiscTime="16:00:00", DocType="DividendForecastRevision", FDivAnn=35, NxFDivAnn=None),
        dict(Code="33330", DiscDate="2026-05-13", DiscTime="15:00:00", DocType="2QFinancialStatements_Consolidated_JP", FDivAnn=30, NxFDivAnn=None),
        dict(Code="44440", DiscDate="2026-05-14", DiscTime=None, DocType="DividendForecastRevision", FDivAnn=12, NxFDivAnn=None),
        dict(Code="44440", DiscDate="2026-05-14", DiscTime="15:30:00", DocType="3QFinancialStatements_Consolidated_JP", FDivAnn=10, NxFDivAnn=None),
        dict(Code="55550", DiscDate="2026-05-16", DiscTime="12:00:00", DocType="FYFinancialStatements_Consolidated_JP", FDivAnn=20, NxFDivAnn=25),
        dict(Code="66660", DiscDate="2026-05-12", DiscTime="15:00:00", DocType="EarnForecastRevision", FDivAnn=None, NxFDivAnn=None),
    ]
    W.apply_fins_rows(divs, rows, eff, stmt=stmt)
    assert W.div_as_of(divs, "1111", "2026-05-12") == 40            # 年次決算で今期が空 → 来期の予想
    assert "2222" not in divs                                        # 四半期で今期が空 → 使わない
    assert W.div_as_of(divs, "3333", "2026-05-13") == 35             # 同じ日は後の時刻が勝つ（並びは時刻で直す）
    assert W.div_as_of(divs, "4444", "2026-05-14") == 12             # 時刻の無い行はその日の最後（BTの並べ方）
    assert math.isnan(W.div_as_of(divs, "5555", "2026-05-14"))       # 土曜の開示は金曜の有効日 →
    assert W.div_as_of(divs, "5555", "2026-05-15") == 20             #   月曜の判定（前営業日=金曜まで）から使う
    assert math.isnan(W.div_as_of(divs, "1111", "2026-05-11"))       # 開示日の判定（前営業日まで）ではまだ使わない
    assert stmt == {"1111": ["2026-05-12"], "2222": ["2026-05-12"], "3333": ["2026-05-13"], "4444": ["2026-05-14"],
                    "5555": ["2026-05-15"]}, stmt                    # 決算短信だけ（修正は入れない）


def bt_ffill_rows(idx, vals, T):
    col = np.full(T, np.nan)
    ok = np.isfinite(vals)
    s = pd.Series(vals[ok], index=idx[ok]); s = s[~s.index.duplicated(keep="last")]
    col[s.index.values] = s.values
    return pd.Series(col).ffill().to_numpy(np.float32)


@test
def t_dividend_state_equals_bt():
    """ランダムな開示の束で、本番の状態更新(apply_fins_rows/div_as_of)とBTの配列計算(ffill_rows→1日ずらし)が同じ"""
    rng = np.random.default_rng(3)
    days = weekdays("2025-01-06", 120, skip={"2025-02-11", "2025-03-20"})
    dates = pd.DatetimeIndex(days)
    T = len(days)
    codes = [f"{1000 + i}" for i in range(15)]
    docs = ["FYFinancialStatements_Consolidated_JP", "1QFinancialStatements_Consolidated_JP", "DividendForecastRevision",
            "EarnForecastRevision", "2QFinancialStatements_NonConsolidated_JP"]
    rows = []
    for _ in range(500):
        d0 = date(2024, 12, 20) + timedelta(days=int(rng.integers(0, 190)))
        rows.append(dict(Code=codes[rng.integers(0, 15)] + "0", DiscDate=d0.isoformat(),
                         DiscTime=[None, "15:00:00", "15:30:00", "16:00:00"][rng.integers(0, 4)], DocType=docs[rng.integers(0, 5)],
                         FDivAnn=(float(rng.integers(0, 80)) if rng.random() < 0.5 else np.nan),
                         NxFDivAnn=(float(rng.integers(0, 80)) if rng.random() < 0.6 else np.nan)))
    Fd = pd.DataFrame(rows)
    Fd["c4"] = Fd.Code.str[:4]; Fd["dt"] = pd.to_datetime(Fd.DiscDate)
    Fd["sidx"] = np.searchsorted(dates.values, Fd.dt.values, side="right") - 1
    Fd = Fd[(Fd.sidx >= 0) & (Fd.sidx < T)].sort_values(["c4", "dt", "DiscTime"]).reset_index(drop=True)
    DIV = np.full((T, len(codes)), np.nan, np.float32)
    for c4, g in Fd.groupby("c4", sort=False):
        dv = np.where(np.isfinite(g.FDivAnn.values), g.FDivAnn.values,
                      np.where(g.DocType.str.startswith("FY").values, g.NxFDivAnn.values, np.nan))
        DIV[:, codes.index(c4)] = bt_ffill_rows(g.sidx.values, dv, T)
    DIV = np.vstack([np.full((1, len(codes)), np.nan, np.float32), DIV[:-1]])
    cal = W.Cal(days=days)
    eff = lambda s: cal.days[cal.idx(s)] if cal.idx(s) >= 0 else None
    divs = {}
    by_s = {}
    for r in Fd.to_dict("records"):
        by_s.setdefault(int(r["sidx"]), []).append(r)
    n_val = 0
    for t in range(1, T):
        W.apply_fins_rows(divs, by_s.get(t - 1, []), eff)
        for j, c in enumerate(codes):
            got = W.div_as_of(divs, c, days[t - 1])
            ref = DIV[t, j]
            assert (np.isnan(ref) and math.isnan(got)) or F32(got) == ref, (days[t], c, got, ref)
            n_val += int(np.isfinite(ref))
    assert n_val > 500


# ───────────────────────── 前営業日のバリュエーション ─────────────────────────
@test
def t_val_prev_equals_ffill_limit3():
    rng = np.random.default_rng(5)
    dates = pd.bdate_range("2025-01-06", periods=80)
    codes = ["1000", "1001", "1002", "1003"]
    raw = {}
    for c in codes:
        idx = dates[rng.random(80) > 0.3]
        df = pd.DataFrame(rng.uniform(0.5, 2, (len(idx), 3)).astype(F32), index=idx, columns=["PBR", "FwdPER", "BPS"])
        raw[c] = df.mask(rng.random(df.shape) < 0.25)
    full = {c: raw[c].reindex(dates).ffill(limit=3) for c in codes}
    for t in range(5, 80):
        vd = [{c: tuple(raw[c].loc[dates[t - k]].values) for c in codes if dates[t - k] in raw[c].index} for k in range(1, 5)]
        outs = W.val_prev(vd, codes)
        for j, c in enumerate(codes):
            ref = full[c].iloc[t - 1]
            for got, k in zip(outs, ("PBR", "FwdPER", "BPS")):
                a, b = got[j], F32(ref[k])
                assert a == b or (np.isnan(a) and np.isnan(b)), (t, c, k, a, b)


# ───────────────────────── 帳簿のエンジン = BTの run() ─────────────────────────
def synth_market(seed, T=260, N=25):
    rng = np.random.default_rng(seed)
    O, H, L, C = (np.empty((T, N)) for _ in range(4))
    px = rng.integers(300, 3000, N).astype(float)
    for t in range(T):
        o = np.maximum(1, np.round(px * (1 + rng.normal(0, 0.015, N))))
        r100 = rng.random(N) < 0.2
        o[r100] = np.maximum(100, np.round(o[r100] / 100) * 100)              # 100円単位の寄り＝損切り水準が刻みに乗る
        c = np.maximum(1, np.round(o * (1 + rng.normal(0, 0.02, N))))
        h = np.maximum(o, c) + np.round(rng.uniform(0, 0.03, N) * o)
        lo = np.maximum(1, np.minimum(o, c) - np.round(rng.uniform(0, 0.03, N) * o))
        O[t], H[t], L[t], C[t] = o, h, lo, c
        px = c
    for _ in range(60):                                                        # 安値が建値×0.93ちょうどの日を混ぜる
        t, j = int(rng.integers(2, T - 1)), int(rng.integers(0, N))
        for tb, ta in ((t - 1, t), (t, t)):
            lv = O[tb, j] * 93 / 100
            if O[tb, j] % 100 == 0 and lv < min(O[ta, j], C[ta, j]):
                L[ta, j] = lv
    miss = rng.random((T, N)) < 0.01
    for A in (O, H, L, C):
        A[miss] = np.nan
    for A in (O, H, L, C):
        A[180:, 0] = np.nan                                                    # 上場廃止
    return [A.astype(F32) for A in (O, H, L, C)]


def synth_ctx(seed, T=260, N=25):
    O, H, L, C = synth_market(seed, T, N)
    rng = np.random.default_rng(1000 + seed)
    EARN = np.zeros((T, N), bool)
    for j in range(N):
        t = int(rng.integers(5, 60))
        while t < T:
            EARN[t, j] = True
            t += int(rng.integers(40, 70))
    NEXT_E = np.full((T, N), T + 99, np.int32); nxt = np.full(N, T + 99, np.int32)
    for t_ in range(T - 1, -1, -1):
        NEXT_E[t_] = nxt; nxt = np.where(EARN[t_], t_, nxt)
    days = weekdays("2020-01-06", T)
    cal = np.array([date.fromisoformat(d).toordinal() for d in days], np.int64)
    RAWR = rng.uniform(0.5, 2.0, (T, N)).astype(F32)
    PBRd = rng.uniform(0.6, 1.4, (T, N)).astype(F32)
    X = dict(O=O, H=H, L=L, C=C, NEXT_E=NEXT_E, cal=cal, PBRd=PBRd, RAWR=RAWR, bad={3, 7}, si=120,
             stuck_up=bt_make_stuck(O, H, C, RAWR), SLOTS=3, SIZE=1_000_000, SIDE=0.001, RATE=0.028)
    M = rng.random((T, N)) < 0.08
    score = np.round(rng.uniform(0, 8, (T, N))).astype(F32)                   # 同じ値を多くして並び順の同点処理も試す
    score[rng.random((T, N)) < 0.1] = np.nan
    M[150:180, 0] = True; score[150:180, 0] = 99                               # 廃止になる銘柄を持たせる（廃止等の出口）
    return X, M, score, days


def eng_for(kw):
    return W.Engine(slots=3, stop=kw.get("stop", 0.07) or 0, trail=kw.get("trail", 0.05) or 0, act=kw.get("act", 0.05),
                    maxhold=kw.get("maxhold", 20), earn_gap=kw.get("earn_gap", 10), tp=kw.get("tp"), pbr_tp=kw.get("pbr"),
                    entry="T4" if kw.get("entry") == "pullback" else "T1", side=0.001, rate=0.028, miss_days=5,
                    t4_limit=0.98, t4_days=3, t4_orders="all")


@test
def t_engine_equals_bt_random():
    variants = [dict(), dict(stop=0.10), dict(trail=None), dict(tp=0.08), dict(maxhold=5), dict(earn_gap=3), dict(pbr=1.2),
                dict(entry="pullback"), dict(stop=None, trail=0.03, act=0.02), dict(entry="pullback", tp=0.05, maxhold=8)]
    n_tr = n_tie = n_why = 0
    whys = set()
    for seed in range(5):
        X, M, score, days = synth_ctx(seed)
        codes = [f"{1300 + j}" for j in range(M.shape[1])]
        cands_ref = bt_to_cands(M, score)
        cands_p = {int(s): W.cands_of_day(M[s], score[s], codes) for s in range(M.shape[0]) if M[s].any()}
        for kw in variants:
            ref = bt_run(cands_ref, X, **kw)
            got = prod_run(cands_p, X, eng_for(kw), codes)
            a = sorted((int(x[0]), int(x[1]), int(x[2]), x[4]) for x in ref[0])
            b = sorted((int(x[0]), int(x[1]), int(x[2]), x[4]) for x in got[0])
            assert a == b, (seed, kw, [x for x in a if x not in b][:3], [x for x in b if x not in a][:3])
            ra = {(int(x[0]), int(x[2])): float(x[3]) for x in ref[0]}
            rb = {(int(x[0]), int(x[2])): float(x[3]) for x in got[0]}
            assert all(ra[k] == rb[k] for k in ra), (seed, kw)                 # 損益率もビット単位で同じ
            assert np.array_equal(ref[1], got[1]), (seed, kw)                  # 毎日の評価額も同じ
            assert sorted((int(e), int(j)) for e, j, _ in ref[2]) == sorted((int(e), int(j)) for e, j, _ in got[2])
            n_tr += len(a)
            for x in ref[0]:
                whys.add(x[4])
                if x[4] == "損切り" and float(x[6]) == float(F32(x[5]) * F32(0.93)) and float(x[5]) % 100 == 0:
                    n_tie += 1
    assert whys >= {"損切り", "トレーリング", "期限", "決算前", "利確", "PBR>1", "廃止等"}, whys
    print(f"    玉 {n_tr} 件がBTと一致（出口の種類 {sorted(whys)}・100円単位の建値の損切り {n_tie} 件）")


# ───────────────────────── 出口の順序（手で作った場面）─────────────────────────
def one_position(bars, ne=10 ** 6, eng=None, cands_day=0):
    """bars[t]=(O,H,L,C)。t=cands_day の候補で t+1 の寄りに建て、手仕舞いまで回す"""
    eng = eng or W.Engine(slots=3, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=10, tp=None, pbr_tp=None, entry="T1")
    held, evs = {}, []
    for t in range(cands_day + 1, len(bars)):
        ev = eng.step(held, [], t, [("1111", 5.0)] if t == cands_day + 1 else [], [],
                      lambda tt, c: tuple(F32(x) for x in bars[tt]), lambda s, c: ne, lambda tt: tt)
        evs += ev
        if any(e["kind"] == "exit" for e in ev):
            break
    return evs


@test
def t_exit_order():
    base = [(1000, 1000, 1000, 1000), (1000, 1010, 995, 1000)]               # t=1 に寄り1000円で建つ
    ex = lambda evs: [e for e in evs if e["kind"] == "exit"][0]
    # a) 翌日以降に寄りが損切り水準(930)を下回ったら寄りで
    e = ex(one_position(base + [(920, 925, 900, 910)]))
    assert (e["why"], e["x"], e["t"]) == ("損切り", 920.0, 2), e
    # b) 安値が930円ちょうど → 930円で（float32 の判定。float64 だと 929.999… で切れない）
    assert 1000.0 * (1 - 0.07) < 930.0
    e = ex(one_position(base + [(950, 960, 930, 940)]))
    assert (e["why"], e["x"]) == ("損切り", 930.0), e
    # c) +5%乗ったらトレーリング（高値の更新はその日の判定の後）: t=2 の高値1060・安値1000では切れない → t=3 の安値1007で
    evs = one_position(base + [(1000, 1060, 1000, 1050), (1040, 1045, 1007, 1010)])
    e = ex(evs)
    assert (e["why"], e["x"], e["t"]) == ("トレーリング", 1007.0, 3), e
    # d) 買った日も安値が水準に触れたら水準で（寄りの窓開けは買った翌日から）
    e = ex(one_position([(1000, 1000, 1000, 1000), (1000, 1000, 920, 925)]))
    assert (e["why"], e["x"], e["t"]) == ("損切り", 930.0, 1), e
    # e) 期限の日に損切りにも触れたら損切りが先
    eng = W.Engine(slots=3, stop=0.07, trail=0.05, act=0.05, maxhold=3, earn_gap=10, tp=None, pbr_tp=None, entry="T1")
    e = ex(one_position(base + [(1000, 1005, 995, 1000), (990, 995, 925, 990)], eng=eng))
    assert (e["why"], e["x"], e["t"]) == ("損切り", 930.0, 3), e
    e = ex(one_position(base + [(1000, 1005, 995, 1000), (990, 995, 985, 990)], eng=eng))
    assert (e["why"], e["x"], e["t"]) == ("期限", 990.0, 3), e             # 買った日を1日目として3日目の大引け
    # f) 決算の前営業日の大引け（ne=5 → t=4 の引け）
    eng = W.Engine(slots=3, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=2, tp=None, pbr_tp=None, entry="T1")
    e = ex(one_position(base + [(1000, 1005, 995, 1001)] * 5, ne=5, eng=eng))
    assert (e["why"], e["x"], e["t"]) == ("決算前", 1001.0, 4), e
    # g) トレーリングの水準は max(損切り, 高値×0.95)。高値1050ちょうどで発動（1000×1.05 = 1050）
    lv = W.Engine().levels(1000, 1050)
    assert lv["trail_on"] and lv["level"] == float(F32(1050) * F32(0.95)) and lv["stop"] == 930.0
    lv = W.Engine().levels(1000, 1049)
    assert not lv["trail_on"] and lv["level"] == 930.0


@test
def t_expiry_across_holidays():
    cal = W.Cal()
    # 9/21〜23 の連休と 10/12(スポーツの日) を飛ばして、9/10 を1日目とした20営業日目
    assert cal.add("2026-09-10", 19) == "2026-10-13", cal.add("2026-09-10", 19)
    e0 = cal.idx("2026-09-09")
    bars = {t: (F32(1000), F32(1005), F32(995), F32(1000)) for t in range(e0, e0 + 30)}
    eng = W.Engine(slots=3, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=10, tp=None, pbr_tp=None, entry="T1")
    held, got = {}, None
    for t in range(e0 + 1, e0 + 30):
        ev = eng.step(held, [], t, [("1111", 5.0)] if t == e0 + 1 else [], [], lambda tt, c: bars[tt], lambda s, c: W.NO_EARN,
                      lambda tt: date.fromisoformat(cal.days[tt]).toordinal())
        for e in ev:
            if e["kind"] == "exit":
                got = (cal.days[t], e["why"])
        if got:
            break
    assert got == ("2026-10-13", "期限"), got
    # 配信の文面も同じ日を出す（前営業日 10/9 の夜 → 「10/13(火)の大引けで売り（期限）」）
    book = [dict(code="1111", name="テスト", signal_date="2026-09-09", entry_date="2026-09-10", px=1000.0, peak=1005.0, last=1000.0,
                 ne_date=None, shares=1000, status="open")]
    day = dict(date="2026-10-09", B=np.zeros(1, bool), first_cands=[])
    emb = W.build_embed("2026-10-09", cal, day, [], book, [], {}, W.Engine())
    assert "10/13(火)の大引けで売り（期限）" in emb["description"], emb["description"]


# ───────────────────────── 寄りストップ高・決算の見送り ─────────────────────────
@test
def t_stuck_up():
    assert W.limit_width(999) == 150 and W.limit_width(1000) == 300                # 東証の値幅（1000円以上1500円未満は300円）
    assert W.stuck_up(1000, 1300, 1300) and W.stuck_up(1000, 1298, 1298)      # S高1300円・上限-0.3%まで張り付き扱い
    assert not W.stuck_up(1000, 1296, 1296)                                    # 上限-0.4% は寄れた扱い
    assert not W.stuck_up(1000, 1150, 1150) and W.stuck_up(999, 1149, 1149)    # 999円なら値幅150円＝1149円がS高
    assert not W.stuck_up(1000, 1300, 1301)                                    # 高値が寄りより上＝張り付いていない
    assert not W.stuck_up(float("nan"), 1300, 1300) and not W.stuck_up(1000, float("nan"), 1300)
    # BTの関数と同じ答え（境目の近くを多めに）
    rng = np.random.default_rng(11)
    T, N = 400, 30
    C = np.round(rng.uniform(50, 60000, (T, N))).astype(F32)
    RAWR = rng.choice([1.0, 1.0, 0.5, 2.0, 0.3333], (T, N)).astype(F32)
    O = np.empty((T, N), F32); H = np.empty((T, N), F32)
    for t in range(1, T):
        raw = C[t - 1] * RAWR[t - 1]
        w = np.array([W.limit_width(float(x)) for x in raw]) / raw
        O[t] = (C[t - 1] * (1 + w - rng.choice([0, 0.001, 0.0029, 0.003, 0.0031, 0.01], N))).astype(F32)
        H[t] = (O[t] * rng.choice([1.0, 1.0004, 1.0006], N)).astype(F32)
    bt_st = bt_make_stuck(O, H, C, RAWR)
    n_true = 0
    for t in range(1, T):
        for j in range(N):
            raw = C[t - 1, j] * RAWR[t - 1, j]
            got = W.stuck_up(raw, O[t, j], H[t, j], ratio_open=O[t, j] / C[t - 1, j])
            assert got == bt_st(t, j), (t, j, raw, O[t, j], C[t - 1, j], H[t, j])
            n_true += got
    assert 1000 < n_true < (T - 1) * N - 1000
    # エンジン: 1番手が張り付きなら飛ばして次の候補を買う
    bars = {"AAAA": [(1000, 1000, 1000, 1000), (1300, 1300, 1300, 1300)], "BBBB": [(500, 500, 500, 500), (510, 515, 505, 512)]}
    eng = W.Engine(slots=1, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=10, tp=None, pbr_tp=None, entry="T1")
    held = {}
    ev = eng.step(held, [], 1, [("AAAA", 9.0), ("BBBB", 5.0)], [], lambda t, c: tuple(F32(x) for x in bars[c][t]),
                  lambda s, c: W.NO_EARN, lambda t: t, is_stuck=lambda t, c: W.stuck_up(bars[c][t - 1][3], bars[c][t][0], bars[c][t][1]))
    assert [(e["kind"], e["code"]) for e in ev] == [("stuck", "AAAA"), ("entry", "BBBB")] and list(held) == ["BBBB"], ev


@test
def t_earnings_gap_and_calendar():
    for gap, expect in ((10, False), (11, True)):                               # 決算まで10営業日以内は建てない
        eng = W.Engine(slots=3, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=10, tp=None, pbr_tp=None, entry="T1")
        held = {}
        eng.step(held, [], 5, [("AAAA", 1.0)], [], lambda t, c: (F32(1000),) * 4, lambda s, c: s + gap, lambda t: t)
        assert ("AAAA" in held) == expect, gap
    cal = W.Cal(days=weekdays("2026-08-03", 90))
    ec = W.EarnCal(official={"1111": ["2026-10-30"]}, est={"1111": ["2026-10-20"], "2222": ["2026-09-01", "2026-11-05"],
                                                          "3333": ["2026-10-31"]})
    assert ec.next_date("1111", "2026-10-01", "2026-10-01", cal) == "2026-10-30"              # 公式があれば推定は使わない
    assert ec.next_date("2222", "2026-10-01", "2026-10-01", cal) == "2026-11-05"              # 推定は今日より先だけ
    assert ec.next_date("2222", "2026-08-20", "2026-10-01", cal, stmt={"2222": ["2026-09-01"]}) == "2026-09-01"   # 実際の決算短信
    assert ec.next_date("2222", "2026-08-20", "2026-10-01", cal) == "2026-11-05"              # 過去の推定日（=何かの開示）は使わない
    assert W.ne_idx(cal, "2026-10-31") == cal.idx("2026-10-30")                              # 土曜の予定は前の営業日（BTの sidx）
    assert ec.next_date("4444", "2026-10-01", "2026-10-01", cal) is None and W.ne_idx(cal, None) == W.NO_EARN
    # 予定も推定も無い銘柄は決算短信の実績から（1年前の同じ四半期＋364日・最後の短信＋91日の早い方）
    hist = {"5555": ["2025-11-07", "2026-02-06", "2026-05-12", "2026-08-07"], "6666": ["2026-08-07"]}
    assert ec.next_date("5555", "2026-09-28", "2026-09-28", cal, stmt=hist) == "2026-11-06"
    assert ec.next_date("6666", "2026-09-28", "2026-09-28", cal, stmt=hist) == "2026-11-06"


# ───────────────────────── 通知先 ─────────────────────────
class _Env:
    def __init__(self, **kv):
        self.kv = kv
    def __enter__(self):
        self.saved = {k: os.environ.get(k) for k in (W.WEBHOOK_ENV, W.WEBHOOK_FALLBACK_ENV)}
        for k in self.saved:
            os.environ.pop(k, None)
        for k, v in self.kv.items():
            os.environ[k] = v
    def __exit__(self, *a):
        for k, v in self.saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v


@test
def t_webhook_fallback_to_gokujo():
    import requests
    calls = []

    class R:
        status_code = 204
    orig = requests.post
    requests.post = lambda url, **kw: (calls.append((url, kw)), R())[1]
    try:
        with _Env(**{W.WEBHOOK_FALLBACK_ENV: "https://discord.example/gokujo"}):
            assert W._webhook_url() == "https://discord.example/gokujo"          # 専用URLが無い → 極上chへ
            assert W.post_discord({"title": "💴【割安B】テスト"}, dry=False)
        with _Env(**{W.WEBHOOK_FALLBACK_ENV: "https://discord.example/gokujo", W.WEBHOOK_ENV: "https://discord.example/wariyasu"}):
            assert W.post_discord({"title": "💴【割安B】テスト2"}, dry=False)     # 専用URLがあれば専用へ
    finally:
        requests.post = orig
    assert [c[0] for c in calls] == ["https://discord.example/gokujo", "https://discord.example/wariyasu"], calls
    assert calls[0][1]["json"]["embeds"][0]["title"].startswith("💴【割安B】")


@test
def t_no_webhook_no_crash():
    with _Env():
        buf = io.StringIO()
        with redirect_stdout(buf):
            assert W.post_discord({"title": "x"}, dry=False) is False
        assert "未設定" in buf.getvalue()


# ───────────────────────── 本番の run() を合成の相場で通す ─────────────────────────
class FakeMarket:
    """k日目に 1001/1002/1003/1005 が割安×ボラ収縮になる相場。1005 は翌寄りS高張り付き、1002 は k+2 に1:2分割。
    1001 は k+3 に安値930円ちょうど（損切り）、1003 は k+2 に高値1060 → k+3 に安値1007（トレーリング）"""

    def __init__(self, days, k):
        self.days, self.k = days, k
        self.codes = [f"{1001 + i}" for i in range(40)]
        self.dy = {"1001": 50, "1002": 40, "1003": 35, "1004": 20, "1005": 45, "1006": 360}     # 1006 は株価6000円(利回り6%)の値がさ
        self.calls = {"bars": 0, "val": 0, "fins": 0}

    def raw(self, c, t):
        if c == "1006":
            return tuple(6 * x for x in self._raw("1001", t if t != self.k + 3 else self.k + 2))
        return self._raw(c, t)

    def _raw(self, c, t):
        k = self.k
        if c in ("1001", "1002", "1003", "1004", "1005") and t >= k - 3:
            o, h, l, cl = 1000, 1004, 996, 1000
        else:
            o, h, l, cl = 1000, 1010, 990, 1000
        if c == "1005" and t >= k + 1:                                         # 前日1000円 → 値幅300円 → S高1300円に張り付き
            o, h, l, cl = (1300, 1300, 1300, 1300) if t == k + 1 else (1300, 1304, 1296, 1300)
        if c == "1001" and t == k + 3:
            o, h, l, cl = 990, 995, 930, 960
        if c == "1003" and t == k + 2:
            o, h, l, cl = 1000, 1060, 1000, 1050
        if c == "1003" and t == k + 3:
            o, h, l, cl = 1040, 1045, 1007, 1010
        if c == "1002" and t >= k + 2:
            o, h, l, cl = 500, 502, 498, 500
        return o, h, l, cl

    def fetch_bars(self, token, d):
        self.calls["bars"] += 1
        t = self.days.index(d)
        out = {}
        for c in self.codes:
            o, h, l, cl = self.raw(c, t)
            adj = 0.5 if (c == "1002" and t < self.k + 2) else 1.0                 # 調整後=分割後の尺度
            fac = 0.5 if (c == "1002" and t == self.k + 2) else 1.0
            out[c] = (o * adj, h * adj, l * adj, cl * adj, 2e5 / adj, float(o), float(h), float(l), float(cl), fac)
        return out

    def fetch_val(self, token, d):
        self.calls["val"] += 1
        return {c: ((0.8, 8.0, 7500.0 if c == "1006" else 1250.0, 100.0, 100.0) if c in self.dy else (2.0, 15.0, 500.0, 50.0, 50.0))
                for c in self.codes}

    def fetch_fins(self, token, d):
        self.calls["fins"] += 1
        if d != self.days[2]:
            return []
        return [dict(Code=c + "0", DiscDate=d, DiscTime="15:00:00", DocType="FYFinancialStatements_Consolidated_JP", FDivAnn=v,
                     NxFDivAnn=None) for c, v in self.dy.items()]

    @staticmethod
    def names(token, codes):
        return {c: f"テスト{c}" for c in codes}


@test
def t_run_end_to_end():
    """C1（翌寄り成行）で run() を通す"""
    rule0 = W.WY_RULE
    W.use_rule("C1")
    try:
        _run_end_to_end_c1()
    finally:
        W.use_rule(rule0)


def _run_end_to_end_c1():
    days = weekdays("2026-03-02", 75, skip={"2026-03-20"})
    k = 30
    mk = FakeMarket(days, k)
    cal = W.Cal(days=days)
    earn = W.EarnCal(official={}, est={})
    tmp = tempfile.mkdtemp(prefix="wy_test_")
    cwd = os.getcwd()
    posted = []
    try:
        os.chdir(tmp)
        W._save(W.STATE_FILE, {"version": 1, "fins_last": (date.fromisoformat(days[0]) - timedelta(days=1)).isoformat(),
                               "divs": {}, "stmt": {}, "b_hist": {}, "cands_hist": {}, "sent": [], "pend": [], "settled": days[k - 1]})
        W._save(W.BOOK_FILE, [])

        def go(i, force=False, post=True):
            fn = (lambda e, dry: posted.append(e) or True) if post else None
            with redirect_stdout(io.StringIO()):
                return W.run(days[i], force=force, token="x", cal=cal, earn=earn, fetch_bars=mk.fetch_bars, fetch_val=mk.fetch_val,
                             fetch_fins=mk.fetch_fins, fetch_names_fn=mk.names, post_fn=fn)

        r = go(k)
        assert [x["code"] for x in r["cands"]] == ["1006", "1001", "1005", "1002", "1003"], r["cands"]    # 利回りの高い順・1004(2%)は割安でない
        emb = posted[-1]
        assert emb["title"].startswith("💴【割安B】") and "買い3件" in emb["title"], emb["title"]
        d = emb["description"]
        nxt = W._md(days[k + 1])
        assert f"{nxt} の寄りで成行買い" in d and "🟢 1. **テスト1001（1001）**" in d and "⚪補欠 4. **テスト1003（1003）**" in d, d
        assert "50万=500株 / 100万=1,000株" in d and "利回り5.0%" in d and "PBR0.80" in d, d
        assert "今日の候補の見送り: テスト1006（1006） 値がさ（100株60万＞1枠50万）" in d, d        # 1枠50万で100株に届かない
        st = W._load(W.STATE_FILE, {})
        assert days[k] in st["sent"] and st["settled"] == days[k] and [x[0] for x in st["cands_hist"][days[k]]] == ["1001", "1005", "1002", "1003"]
        assert W.div_as_of(st["divs"], "1001", days[k]) == 50

        r = go(k + 1)                                                            # 1005 は寄りS高張り付き → 1001/1002/1003 を建てる
        kinds = [(e["kind"], e["code"]) for e in r["events"]]
        assert kinds == [("entry", "1001"), ("stuck", "1005"), ("entry", "1002"), ("entry", "1003")], kinds
        book = W._load(W.BOOK_FILE, [])
        assert [(p["code"], p["entry_date"], p["px"], p["shares"], p["status"]) for p in book] == [
            ("1001", days[k + 1], 1000.0, 500, "open"), ("1002", days[k + 1], 1000.0, 500, "open"), ("1003", days[k + 1], 1000.0, 500, "open")]
        d = posted[-1]["description"]
        assert "保有 3/3" in d and "逆指値の売り **930円**" in d and "寄りがストップ高張り付き" in d and "の買い候補なし" in d, d
        assert "テスト1001（1001） 保有中" in d, d

        go(k + 2)                                                                # 1002 分割 → 建値500円・2000株
        book2 = W._load(W.BOOK_FILE, [])
        p2 = [p for p in book2 if p["code"] == "1002"][0]
        assert (p2["px"], p2["shares"]) == (500.0, 1000), p2
        assert [p for p in book2 if p["code"] == "1003"][0]["peak"] == 1060.0
        st_before = W._load(W.STATE_FILE, {})
        go(k + 2, force=True)                                                    # 同じ日のやり直し → 二重計上しない
        assert W._load(W.BOOK_FILE, []) == book2
        assert go(k + 2)["skipped"] == "sent"
        assert W._load(W.STATE_FILE, {})["settled"] == st_before["settled"]

        r = go(k + 4)                                                            # k+3 の配信が欠けた → k+3 を先に追いつかせる
        assert r["late_days"] == [days[k + 3]], r["late_days"]
        ex = {e["code"]: e for e in r["events"] if e["kind"] == "exit"}
        assert (ex["1001"]["why"], ex["1001"]["x"], ex["1001"]["date"]) == ("損切り", 930.0, days[k + 3]), ex
        assert (ex["1003"]["why"], ex["1003"]["x"], ex["1003"]["date"]) == ("トレーリング", 1007.0, days[k + 3]), ex
        assert "配信が欠けた分も追いつかせた" in posted[-1]["description"]
        for i in range(k + 5, k + 21):
            go(i)
        book = W._load(W.BOOK_FILE, [])
        p2 = [p for p in book if p["code"] == "1002"][0]
        assert (p2["status"], p2["why"], p2["exit_date"], p2["hold_days"]) == ("closed", "期限", days[k + 20], 20), p2
        r1001 = [p for p in book if p["code"] == "1001"][0]["r"]
        cd = date.fromisoformat(days[k + 3]).toordinal() - date.fromisoformat(days[k + 1]).toordinal()
        assert abs(r1001 - (0.93 - 1 - 0.002 - 0.028 * cd / 365)) < 1e-6, r1001
        assert all(p["status"] == "closed" for p in book[:3])
        sig = W._load(W.SIG_FILE, {})
        assert sig["date"] == days[k + 20] and "cands" in sig
        # 通知先が無くても落ちない（run() を本物の post_discord で）
        with _Env():
            r = go(k + 21, post=False)
        assert "embed" in r and days[k + 21] in W._load(W.STATE_FILE, {})["sent"]
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp, ignore_errors=True)


@test
def t_message_text():
    """C1（翌寄り成行）の文面"""
    rule0 = W.WY_RULE
    W.use_rule("C1")
    try:
        _message_text_c1()
    finally:
        W.use_rule(rule0)


def _message_text_c1():
    cal = W.Cal(days=weekdays("2026-09-01", 40))
    today = cal.days[10]
    day = dict(date=today, B=np.ones(5, bool), first_cands=[("7777", 4.2), ("8888", 3.9)])
    rows = [dict(code="1111", name="割安一号", score=5.2, PBR=0.71, PER=7.3, DY=5.2, tov20_oku=3.4, close=1234.0, ne_date="2026-11-10",
                 shares={"1000000": 800, "500000": 400}, ok=True, skip=""),
            dict(code="2222", name="", score=4.0, PBR=0.9, PER=9.0, DY=4.0, tov20_oku=1.2, close=15000.0, ne_date=None,
                 shares={"1000000": 0, "500000": 0}, ok=False, skip="値がさ（100株150万＞1枠50万）"),
            dict(code="3333", name="決算近い", score=3.5, PBR=0.9, PER=9.0, DY=3.5, tov20_oku=1.2, close=500.0, ne_date=cal.days[15],
                 shares={"1000000": 2000, "500000": 1000}, ok=False, skip="決算まで5営業日")]
    book = [dict(code="4444", name="保有株", signal_date=cal.days[4], entry_date=cal.days[5], px=1000.0, peak=1080.0, last=1050.0,
                 ne_date=cal.days[40 - 1], shares=1000, status="open"),
            dict(code="5555", name="済み", entry_date=cal.days[1], exit_date=cal.days[9], px=800.0, status="closed", pnl_yen=21000, r=0.042)]
    ev = [dict(kind="exit", code="5555", date=today, px=800.0, x=833.6, r=0.042, why="トレーリング")]
    emb = W.build_embed(today, cal, day, rows, book, ev, {"7777": "初日株"}, W.Engine())
    t, d, f = emb["title"], emb["description"], emb["footer"]["text"]
    assert t == f"💴【割安B】{W._md(today)}引け → {W._md(cal.days[11])} 買い1件（保有1/3）", t
    assert "🟢 1. **割安一号（1111）** 終値1,234円 → 50万=400株 / 100万=800株" in d, d
    assert "今日の候補の見送り: 2222 値がさ（100株150万＞1枠50万）・決算近い（3333） 決算まで5営業日" in d, d
    assert "・**保有株（4444）** 1,000株 建値1,000円" in d and "トレーリング中（高値1,080円の-5%）" in d, d
    assert f"逆指値の売り **{float(F32(1080) * F32(0.95)):,.0f}円**" in d, d
    assert "✅" in d and "トレーリング 済み（5555） 800→834円 +4.20%（+2.1万・コスト込み）" in d, d
    assert "参考（帳簿外・損切り10%で使う型）今日はじめて割安に入った: 初日株（7777） 利回り4.2%" in d, d
    assert "ルール[C1]: PBR≤1・予想PER≤10・利回り≥3%" in f and "損切り-7%" in f and "20営業日目の大引け" in f and "1枠50万" in f, f
    assert "紙の帳簿: 1件 +2.1万（勝ち1）" in f, f
    assert len(d) <= 4096 and len(t) <= 256 and len(f) <= 2048


@test
def t_constants_are_swappable():
    """定数を差し替えると関数・エンジン・文面に効く（最適化の結果を入れる時の確認）"""
    saved = {k: getattr(W, k) for k in ("WY_STOP", "WY_SLOTS", "WY_PBR_MAX", "WY_RANK_KEY", "WY_ENTRY")}
    try:
        W.WY_STOP, W.WY_SLOTS, W.WY_PBR_MAX, W.WY_RANK_KEY, W.WY_ENTRY = 0.05, 2, 0.8, "pbr", "first"
        eng = W.Engine()
        assert eng.stop == 0.05 and eng.slots == 2 and eng.entry == "first"
        assert not W.calc_b_threshold(np.array([0.9], F32), np.array([8], F32), np.array([4], F32))[0]
        PBR = np.array([0.5, 0.7], F32)
        assert (W.rank_score(np.array([3, 4], F32), PBR, np.array([8, 8], F32)) == -PBR).all()
        B = np.array([[1, 0], [1, 1], [0, 1]], bool); LIQ = np.ones((3, 2), bool)
        assert W.entry_mask(B, LIQ).tolist() == [[True, False], [False, True], [False, False]]
        assert (W.entry_day(B) == bt_entry_day(B, 20)).all()
    finally:
        for k, v in saved.items():
            setattr(W, k, v)
    rule0 = W.WY_RULE
    try:
        W.use_rule("C1")
        assert (W.WY_RULE, W.WY_ENTRY, W.WY_PBR_MAX, W.WY_PER_MAX, W.WY_RANK_KEY) == ("C1", "T1", 1.0, 10.0, "dy")
        assert W.Engine().entry == "T1"
        W.use_rule("R_opt")
        assert (W.WY_RULE, W.WY_ENTRY, W.WY_PBR_MAX, W.WY_PER_MAX, W.WY_RANK_KEY, W.WY_T4_ORDERS) == ("R_opt", "T4", 1.2, 15.0, "compz", "top_free")
        assert W.Engine().t4_orders == "top_free" and W.WY_SLOTS == 3 and W.WY_SIZE == 500_000
    finally:
        W.use_rule(rule0)


# ───────────────────────── 配当落調整金・利回りの要確認（2026-09-27）─────────────────────────
@test
def t_ex_div_dates():
    """権利落ち日 = 権利確定日(期末以前の最後の営業日)の1営業日前（2019-07-18より前の期末は2営業日前）・割合は権利付き最終日までの開示"""
    cal = W.Cal()
    fy = {}
    st = "3QFinancialStatements_Consolidated_JP"
    W.fy_note_row(fy, "1111", "2026-02-10", {"CurFYEn": "2026-03-31", "DocType": st, "FDivFY": 30, "FDivAnn": 40})
    assert fy["1111"] == {"end": "03-31", "fr": [["2026-02-10", 0.75]]}, fy
    assert W.ex_div_share(fy, "1111", "2026-03-30", cal) == (0.75, "期末")          # 3/31(火)確定 → 3/27(金)権利付き → 3/30(月)落ち
    assert W.ex_div_share(fy, "1111", "2026-03-27", cal) == (0.0, "")
    assert W.ex_div_share(fy, "1111", "2026-03-31", cal) == (0.0, "")
    assert W.ex_div_share(fy, "1111", "2026-09-29", cal) == (0.25, "中間")          # 9/30(水)確定 → 9/29(火)落ち
    W.fy_note_row(fy, "1111", "2026-09-29", {"CurFYEn": "2027-03-31", "DocType": "DividendForecastRevision", "FDivFY": 40, "FDivAnn": 40})
    assert W.ex_div_share(fy, "1111", "2026-09-29", cal) == (0.25, "中間")          # 権利落ち日当日の開示は使わない（権利付き最終日まで）
    assert W.ex_div_share(fy, "1111", "2027-03-30", cal) == (1.0, "期末")           # 3/31(水)確定 → 3/30(火)落ち・新しい割合1.0
    W.fy_note_row(fy, "2222", "2025-11-10", {"CurFYEn": "2025-12-31", "DocType": st, "FDivFY": 20, "FDivAnn": 40})
    assert W.ex_div_share(fy, "2222", "2025-12-29", cal) == (0.5, "期末")           # 12/31は休み → 12/30確定 → 12/26(金)権利付き → 12/29落ち
    assert W.ex_div_share(fy, "2222", "2026-06-29", cal) == (0.5, "中間")           # 6/30(火)確定 → 6/29(月)落ち
    W.fy_note_row(fy, "5204", "2019-01-24", {"CurFYEn": "2019-03-20", "DocType": st, "FDivFY": 45, "FDivAnn": 45})
    W.fy_note_row(fy, "5204", "2019-02-04", {"CurFYEn": "2019-03-31", "DocType": "DividendForecastRevision", "FDivFY": 65, "FDivAnn": 65})
    assert fy["5204"]["end"] == "03-20", fy["5204"]                                  # 修正開示の食い違う期末は採らない（短信が正）
    assert W.ex_div_share(fy, "5204", "2019-03-18", cal) == (1.0, "期末")           # 旧ルール: 3/20(水)確定 → 3/15(金)権利付き → 3/18(月)落ち
    W.fy_note_row(fy, "5204", "2025-11-01", {"CurFYEn": "2026-03-20", "DocType": st, "FDivFY": 45, "FDivAnn": 45})
    assert W.ex_div_share(fy, "5204", "2026-03-18", cal) == (1.0, "期末")           # 3/20(金)は春分の日 → 3/19確定 → 3/18落ち
    W.fy_note_row(fy, "3333", "2019-02-01", {"CurFYEn": "2019-03-31", "DocType": st})
    assert W.ex_div_share(fy, "3333", "2019-03-27", cal) == (0.5, "期末")           # 旧ルール: 3/29(金)確定 → 3/26権利付き → 3/27落ち・割合なし=0.5
    W.fy_note_row(fy, "4444", "2025-12-01", {"CurFYEn": "2026-02-28", "DocType": st, "FDivFY": 10, "FDivAnn": 10})
    assert W.ex_div_share(fy, "4444", "2026-02-26", cal) == (1.0, "期末")           # 2月末決算: 2/27(金)確定 → 2/26落ち
    assert W.ex_div_share(fy, "4444", "2026-08-28", cal) == (0.0, "")               # 期末だけの配当 → 中間(8月)は割合0
    assert W.ex_div_share({}, "9999", "2026-03-30", cal) == (0.0, "")
    assert abs(W.div_credit_r(4.0, 0.5, 1000.0, 1000.0) - 0.04 * 0.5 * 0.85) < 1e-12
    assert math.isnan(W.div_credit_r(39.7, 1.0, 1000.0, 1000.0))                    # 利回りが疑わしい → 数えない
    assert math.isnan(W.div_credit_r(float("nan"), 1.0, 1000.0, 1000.0))


class FakeMarketDiv(FakeMarket):
    """FakeMarket に 1002 の決算期末（k+6日目＝k+5日目が権利落ち）と期末の割合1.0を足したもの"""

    def fetch_fins(self, token, d):
        rows = super().fetch_fins(token, d)
        for r in rows:
            if r["Code"] == "10020":
                r.update(CurFYEn=self.days[self.k + 6], FDivFY=40.0)
                r["FDivAnn"] = 40.0
        return rows


@test
def t_div_credit_end_to_end():
    """保有中に権利落ちを迎えた玉に配当落調整金が別枠で入り、前の晩に予告・当日の帳簿・決済・フッターに出る。損益(r)は変わらない"""
    rule0 = W.WY_RULE
    W.use_rule("C1")
    days = weekdays("2026-03-02", 75, skip={"2026-03-20"})
    k = 30
    mk = FakeMarketDiv(days, k)
    cal = W.Cal(days=days)
    earn = W.EarnCal(official={}, est={})
    tmp = tempfile.mkdtemp(prefix="wy_test_div_")
    cwd = os.getcwd()
    posted = []
    try:
        os.chdir(tmp)
        W._save(W.STATE_FILE, {"version": 1, "fins_last": (date.fromisoformat(days[0]) - timedelta(days=1)).isoformat(),
                               "divs": {}, "stmt": {}, "b_hist": {}, "cands_hist": {}, "sent": [], "pend": [], "settled": days[k - 1]})
        W._save(W.BOOK_FILE, [])

        def go(i):
            with redirect_stdout(io.StringIO()):
                return W.run(days[i], token="x", cal=cal, earn=earn, fetch_bars=mk.fetch_bars, fetch_val=mk.fetch_val,
                             fetch_fins=mk.fetch_fins, fetch_names_fn=mk.names, post_fn=lambda e, dry: posted.append(e) or True)
        for i in range(k, k + 4):
            go(i)
        st = W._load(W.STATE_FILE, {})
        assert st["fy"]["1002"]["end"] == days[k + 6][5:], st["fy"].get("1002")
        assert W.ex_div_share(st["fy"], "1002", days[k + 5], cal) == (1.0, "期末")
        r = go(k + 4)                                                            # 明日(k+5)が権利落ち → 予告
        d = posted[-1]["description"]
        assert f"📌 {W._md(days[k + 5])}は権利落ち日（期末）" in d and "配当落調整金 約17,000円の見込み" in d, d
        r = go(k + 5)                                                            # 権利落ち日: 利回り4%×割合1.0×前日終値500÷建値500×0.85
        dv = [e for e in r["events"] if e["kind"] == "div"]
        assert [(e["code"], e["yen"], e["which"]) for e in dv] == [("1002", 17000, "期末")], r["events"]
        d = posted[-1]["description"]
        assert "💴" in d and "権利落ち（期末）テスト1002（1002） 配当落調整金 +17,000円の見込み" in d, d
        book = W._load(W.BOOK_FILE, [])
        p2 = [p for p in book if p["code"] == "1002"][0]
        assert (p2["div_yen"], p2["div_r"]) == (17000, 0.034), p2
        for i in range(k + 6, k + 21):
            go(i)
        book = W._load(W.BOOK_FILE, [])
        p2 = [p for p in book if p["code"] == "1002"][0]
        assert (p2["status"], p2["why"], p2["div_yen"]) == ("closed", "期限", 17000), p2
        assert p2["pnl_yen"] == round(W.WY_SIZE * p2["r"]) and p2["r"] < 0.001, p2              # 帳簿の損益(r)は株価だけ（配当は別枠）
        d = posted[-1]["description"]
        assert "＋配当落調整金+1.7万" in d, d
        assert "・配当落調整金 +1.7万（別枠）" in posted[-1]["footer"]["text"], posted[-1]["footer"]["text"]
        assert "配当落調整金を足すと約+240万" in posted[-1]["footer"]["text"]
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp, ignore_errors=True)
        W.use_rule(rule0)


@test
def t_dy_warn_display():
    """予想利回り15%超は候補に「要確認」を付ける（候補から外さない）"""
    rule0 = W.WY_RULE
    W.use_rule("C1")
    try:
        cal = W.Cal(days=weekdays("2026-09-01", 40))
        today = cal.days[10]
        day = dict(date=today, B=np.ones(5, bool), first_cands=[])
        rows = [dict(code="9434", name="分割ずれ", score=39.7, PBR=0.9, PER=9.0, DY=39.7, tov20_oku=50.0, close=180.0, ne_date="2026-11-10",
                     shares={"1000000": 5500, "500000": 2700}, ok=True, skip=""),
                dict(code="1111", name="ふつう", score=5.2, PBR=0.71, PER=7.3, DY=5.2, tov20_oku=3.4, close=1234.0, ne_date="2026-11-10",
                     shares={"1000000": 800, "500000": 400}, ok=True, skip="")]
        d = W.build_embed(today, cal, day, rows, [], [], {}, W.Engine())["description"]
        line1 = [x for x in d.split("\n") if "利回り39.7%" in x][0]
        line2 = [x for x in d.split("\n") if "利回り5.2%" in x][0]
        assert "⚠️要確認" in line1 and "⚠️要確認" not in line2, d
        assert "🟢 1. **分割ずれ（9434）**" in d, d
    finally:
        W.use_rule(rule0)


# ───────────────────────── R_opt（押し目の指値・合成スコア）─────────────────────────
def bt_compz(PBR1, EP1, DY, LIQ):
    """_bt_souzai_opt_prep_0927.py の COMPZ をそのまま"""
    T, N = PBR1.shape
    COMPZ = np.full((T, N), np.nan, np.float32)

    def z_w(x):
        lo, hi = np.nanpercentile(x, [1, 99]); x = np.clip(x, lo, hi)
        return (x - np.nanmean(x)) / (np.nanstd(x) + 1e-12)
    with np.errstate(invalid="ignore", divide="ignore"):
        BP = np.where(PBR1 > 0, 1.0 / PBR1, np.nan)
        DYz = np.where(np.isfinite(DY), DY, 0.0)
    for t in range(T):
        m = LIQ[t] & np.isfinite(BP[t]) & np.isfinite(EP1[t])
        if m.sum() < 100:
            continue
        z = (z_w(BP[t, m]) + z_w(EP1[t, m]) + z_w(DYz[t, m])) / 3.0
        COMPZ[t, m] = z
    return COMPZ


@test
def t_compz_ep_t4_equal_bt():
    rng = np.random.default_rng(21)
    T, N = 6, 400
    PBR = rng.uniform(0.3, 3.0, (T, N)).astype(F32); PBR[rng.random((T, N)) < 0.05] = -0.5; PBR[rng.random((T, N)) < 0.05] = np.nan
    BPS = rng.uniform(100, 3000, (T, N)).astype(F32)
    EPS = rng.uniform(-50, 300, (T, N)).astype(F32); EPS[rng.random((T, N)) < 0.1] = np.nan
    FEPS = rng.uniform(-50, 300, (T, N)).astype(F32); FEPS[rng.random((T, N)) < 0.3] = np.nan
    DY = rng.uniform(0, 7, (T, N)).astype(F32); DY[rng.random((T, N)) < 0.2] = np.nan
    LIQ = rng.random((T, N)) < 0.8
    LIQ[5, 90:] = False                                                         # 最後の日は100銘柄未満 → NaN
    with np.errstate(invalid="ignore", divide="ignore"):
        EP_bt = (np.where(np.isfinite(FEPS), FEPS, EPS) / (PBR * BPS)).astype(F32)
    EP = W.calc_ep(PBR, BPS, EPS, FEPS)
    assert np.array_equal(EP, EP_bt, equal_nan=True)
    Z_bt = bt_compz(PBR, EP, DY, LIQ)
    Z = W.calc_compz(PBR, EP, DY, LIQ)
    assert np.array_equal(Z, Z_bt, equal_nan=True) and np.isfinite(Z[:5]).sum() > 1000 and not np.isfinite(Z[5]).any()
    for t in range(T):                                                         # 本番は1日分の行で計算する → 同じ値
        assert np.array_equal(W.calc_compz(PBR[t][None], EP[t][None], DY[t][None], LIQ[t][None])[0], Z[t], equal_nan=True)
    # 押し目T4 の定義（21日の最高終値・20〜59営業日前の最安・0.382〜0.618 戻し）を1マスずつ
    T2, N2 = 140, 12
    C = (1000 * np.exp(np.cumsum(rng.normal(0, 0.03, (T2, N2)), axis=0))).astype(F32)
    C[rng.random((T2, N2)) < 0.02] = np.nan
    t4 = W.calc_t4(C)
    n_true = 0
    for t in range(T2):
        for j in range(N2):
            exp = False
            if t >= 59:
                w21, w40 = C[t - 20:t + 1, j], C[t - 59:t - 19, j]
                if np.isfinite(w21).all() and np.isfinite(w40).all():
                    mx, mn = F32(w21.max()), F32(w40.min())
                    with np.errstate(invalid="ignore", divide="ignore"):
                        rt = (mx - C[t, j]) / (mx - mn)
                    exp = bool(mx >= F32(1.10) * mn and F32(0.382) <= rt <= F32(0.618))
            assert bool(t4[t, j]) == exp, (t, j)
            n_true += exp
    assert n_true > 20


@test
def t_t4_orders_top_free_vs_all():
    """待ちの指値: "all"=全部持って安値が触れた中からスコア順（BT） / "top_free"=前の晩に空き枠の数だけスコア上位に置いた分だけ（本番）"""
    bars = {"A": (102, 103, 101, 102), "B": (102, 103, 99, 101), "C": (102, 103, 98, 101), "D": (102, 103, 90, 95)}
    mk_pend = lambda: [("D", F32(100), 5, 1.0, W.NO_EARN), ("A", F32(100), 5, 4.0, W.NO_EARN), ("C", F32(100), 5, 2.0, W.NO_EARN),
                       ("B", F32(100), 5, 3.0, W.NO_EARN), ("B", F32(95), 5, 0.5, W.NO_EARN), ("E", F32(100), 0, 9.0, W.NO_EARN)]
    res = {}
    for mode in ("all", "top_free"):
        eng = W.Engine(slots=2, stop=0.07, trail=0.05, act=0.05, maxhold=20, earn_gap=10, tp=None, pbr_tp=None, entry="T4", t4_orders=mode)
        held, pend = {}, mk_pend()
        ev = eng.step(held, pend, 1, [], [], lambda t, c: tuple(F32(x) for x in bars[c]), lambda s, c: W.NO_EARN, lambda t: t)
        res[mode] = ([(e["code"], e["px"]) for e in ev if e["kind"] == "entry"], sorted(x[0] for x in pend))
    assert res["all"][0] == [("B", 100.0), ("C", 100.0)], res           # スコア順に B(3)・C(2)。A は触れず、D は枠が無い
    assert res["top_free"][0] == [("B", 100.0)], res                    # 置いたのは A(4)・B(3) だけ → B だけ約定
    assert res["all"][1] == ["A", "D"] and res["top_free"][1] == ["A", "C", "D"], res   # 期限切れ(E)と、持った銘柄(B)の別の指値は消える
    eng = W.Engine(slots=3, entry="T4", t4_orders="top_free")
    pend = mk_pend()
    live = eng.live_orders(pend, {}, 3, 1)
    assert [pend[i][0] for i in live] == ["A", "B", "C"], live            # 同じ銘柄は1本（B はスコアの高い方の指値）・期限切れは除く


class FakeMarketT4:
    """k日目に 2001〜2005 と値がさの2006 が割安×押し目(T4)。合成スコアの順は 2001(=2006)>2002>2003>2004>2005。
    k+1: 2001 安値1070 → 指値1078で約定 / 2002 安値1085 → 約定せず / 2003 寄り1076 → 寄りで約定 / 2004 安値1050（置いていない＝約定しない）
    k+2: 2002 安値1078ちょうど → 約定。k+3: 2001 安値1000 → 損切り(1078×0.93)"""

    def __init__(self, days, k):
        self.days, self.k = days, k
        self.codes = [f"{2001 + i}" for i in range(120)]
        rng = np.random.default_rng(9)
        self.v = {}
        for i, c in enumerate(self.codes):
            if i < 6:
                self.v[c] = ([0.6, 0.8, 0.9, 1.0, 1.1, 0.5][i], [0.12, 0.10, 0.09, 0.08, 0.07, 0.13][i], [4.5, 4.0, 3.5, 3.2, 3.1, 5.0][i])
            else:
                self.v[c] = (float(rng.uniform(1.3, 3.0)), float(rng.uniform(0.01, 0.06)), float(rng.uniform(0.0, 2.5)))

    def ohlc(self, c, t):
        k = self.k
        if c not in ("2001", "2002", "2003", "2004", "2005", "2006"):
            return (1000.0, 1010.0, 990.0, 1000.0)
        sc = 6.0 if c == "2006" else 1.0
        if t > k:
            tbl = {"2001": {k + 1: (1090, 1095, 1070, 1080), k + 2: (1080, 1090, 1075, 1085), k + 3: (1070, 1075, 1000, 1010)},
                   "2002": {k + 1: (1100, 1135, 1085, 1130), k + 2: (1090, 1095, 1078, 1085)},
                   "2003": {k + 1: (1076, 1090, 1075, 1080)},
                   "2004": {k + 1: (1080, 1085, 1050, 1060)}}
            dflt = (1100, 1105, 1095, 1100) if c in ("2005", "2006") else (1080, 1090, 1075, 1085)
            return tuple(float(x) * sc for x in tbl.get(c, {}).get(t, dflt))
        cl = self._close(t) * sc
        o = self._close(t - 1) * sc if t > 0 else cl
        return (o, max(o, cl) + 5 * sc, min(o, cl) - 5 * sc, cl)

    def _close(self, t):
        k = self.k
        if t <= k - 30:
            return 1000.0
        if t <= k - 5:
            return 1000.0 + 200.0 * (t - (k - 30)) / 25
        return {k - 4: 1200.0, k - 3: 1195.0, k - 2: 1190.0, k - 1: 1180.0, k: 1100.0}[t]

    def fetch_bars(self, token, d):
        t = self.days.index(d)
        return {c: (*self.ohlc(c, t), 2e5, *self.ohlc(c, t), 1.0) for c in self.codes}

    def fetch_val(self, token, d):
        t = self.days.index(d)
        out = {}
        for c in self.codes:
            pbr, ep, _ = self.v[c]
            cl = self.ohlc(c, t)[3]
            out[c] = (pbr, 1.0 / ep, cl / pbr, ep * cl, ep * cl)
        return out

    def fetch_fins(self, token, d):
        if d != self.days[2]:
            return []
        rows = []
        for c in self.codes:
            dy = self.v[c][2]
            ref = self.ohlc(c, self.k - 1)[3]
            rows.append(dict(Code=c + "0", DiscDate=d, DiscTime="15:00:00", DocType="FYFinancialStatements_Consolidated_JP",
                             FDivAnn=dy / 100 * ref, NxFDivAnn=None))
        return rows

    @staticmethod
    def names(token, codes):
        return {c: f"テスト{c}" for c in codes}


@test
def t_run_end_to_end_ropt():
    """R_opt（押し目の指値・合成スコア・空き枠の数だけ）で run() を通す"""
    rule0 = W.WY_RULE
    W.use_rule("R_opt")
    days = weekdays("2026-01-05", 110)
    k = 70
    mk = FakeMarketT4(days, k)
    cal = W.Cal(days=days)
    earn = W.EarnCal(official={}, est={})
    tmp = tempfile.mkdtemp(prefix="wy_test_")
    cwd = os.getcwd()
    posted = []
    try:
        os.chdir(tmp)
        W._save(W.STATE_FILE, {"version": 1, "fins_last": (date.fromisoformat(days[0]) - timedelta(days=1)).isoformat(),
                               "divs": {}, "stmt": {}, "b_hist": {}, "cands_hist": {}, "sent": [], "pend": [], "settled": days[k - 1]})
        W._save(W.BOOK_FILE, [])

        def go(i):
            with redirect_stdout(io.StringIO()):
                return W.run(days[i], token="x", cal=cal, earn=earn, fetch_bars=mk.fetch_bars, fetch_val=mk.fetch_val,
                             fetch_fins=mk.fetch_fins, fetch_names_fn=mk.names, post_fn=lambda e, dry: posted.append(e) or True)

        r = go(k)
        assert [x["code"] for x in r["cands"]] == ["2001", "2006", "2002", "2003", "2004", "2005"], [(x["code"], x["score"]) for x in r["cands"]]
        assert [(o["code"], o["limit"], o["live"], o["exp"]) for o in r["orders"]] == [
            ("2001", 1078.0, True, days[k + 3]), ("2002", 1078.0, True, days[k + 3]), ("2003", 1078.0, True, days[k + 3]),
            ("2004", 1078.0, False, days[k + 3]), ("2005", 1078.0, False, days[k + 3])], r["orders"]
        emb = posted[-1]
        t_, d_ = emb["title"], emb["description"]
        assert t_ == f"💴【割安B】{W._md(days[k])}引け → {W._md(days[k + 1])} 指値3件（保有0/3）", t_
        assert f"🟢 1. **テスト2001（2001）** 指値**1,078円**（{W._md(days[k + 3])}まで） → 50万=400株 / 100万=900株" in d_, d_
        assert "⚪補欠 4. **テスト2004（2004）**" in d_ and "今日の候補の見送り: テスト2006（2006） 値がさ（100株66万＞1枠50万）" in d_, d_
        assert "PBR0.60・予想PER8.3・利回り4.5%・スコア+" in d_ and "ルール[R_opt]: PBR≤1.2・予想PER≤15・利回り≥3%" in emb["footer"]["text"], d_
        st = W._load(W.STATE_FILE, {})
        assert [x[0] for x in st["pend"]] == ["2001", "2002", "2003", "2004", "2005"] and st["pend"][0][5]["sig"] == days[k]

        r = go(k + 1)                                  # 置いた3本のうち 2001(安値1070)・2003(寄り1076) が約定。2004 は置いていない
        assert [(e["kind"], e["code"], e["px"]) for e in r["events"]] == [("entry", "2001", 1078.0), ("entry", "2003", 1076.0)], r["events"]
        book = W._load(W.BOOK_FILE, [])
        assert [(p["code"], p["px"], p["limit"], p["shares"], p["signal_date"]) for p in book] == [
            ("2001", 1078.0, 1078.0, 400, days[k]), ("2003", 1076.0, 1078.0, 400, days[k])], book
        d_ = posted[-1]["description"]
        assert f"🛒 {W._md(days[k + 1])} 約定 テスト2001（2001） 指値1,078円 → 1,078円" in d_ and "指値1,078円 → 1,076円" in d_, d_
        assert [o["code"] for o in r["orders"] if o["live"]] == ["2002"], r["orders"]     # 空き枠1 → スコア上位の 2002 だけ置く

        r = go(k + 2)                                  # 2002 は安値1078ちょうど → 約定（float32 の判定）
        assert [(e["kind"], e["code"], e["px"]) for e in r["events"]] == [("entry", "2002", 1078.0)], r["events"]
        assert "枠が埋まっている（3/3）" in posted[-1]["description"]

        r = go(k + 3)                                  # 2001 は安値1000 → 損切り（1078×0.93）
        ex = [e for e in r["events"] if e["kind"] == "exit"]
        assert [(e["code"], e["why"]) for e in ex] == [("2001", "損切り")] and ex[0]["x"] == float(F32(1078) * F32(0.93)), ex
        book = W._load(W.BOOK_FILE, [])
        assert [(p["code"], p["status"]) for p in book] == [("2001", "closed"), ("2003", "open"), ("2002", "open")], book
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp, ignore_errors=True)
        W.use_rule(rule0)


def main():
    ok = 0
    for f in TESTS:
        try:
            f()
            print(f"  ✅ {f.__name__}")
            ok += 1
        except Exception:
            print(f"  ❌ {f.__name__}")
            traceback.print_exc()
    print(f"\n{ok}/{len(TESTS)} 通過")
    return 0 if ok == len(TESTS) else 1


if __name__ == "__main__":
    sys.exit(main())

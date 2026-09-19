# -*- coding: utf-8 -*-
"""_bt_fibo_scalein_gap_short_0919.py — 鏡像: 寄り付きギャップ銘柄の「フィボナッチ戻り売り」（買い下がりBTの追補・2026-09-19）
波=当日高値（寄りから更新）→当日安値。戻り水準 23.6/38.2/50/61.8 に売り指値（足の高値が指値+2ティック以上で約定）。
損切り=最終弾の1段上の水準を上抜け（水準+0.2%で買戻し）。利確=-5%/-7.5%（平均比・足の安値が目標-2ティック以下）／安値更新後トレール（以後の最安値+1.5%）。
配分・時間・枚数は買い版と同じ。実行: python -X utf8 _bt_fibo_scalein_gap_short_0919.py > _log_fibo_scalein_gap_short_0919.txt"""
from __future__ import annotations
import pickle, time, sys
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
ACCOUNT = 3_000_000; RISK = ACCOUNT * 0.01
LEVELS = {"23.6": 0.236, "38.2": 0.382, "50": 0.5, "61.8": 0.618, "78.6": 0.786}; ORDER = ["23.6", "38.2", "50", "61.8", "78.6"]
ALLOC = {"A 23.6/38.2/50=1:2:3": (("23.6", 1), ("38.2", 2), ("50", 3)), "B 38.2/50=2:3": (("38.2", 2), ("50", 3)), "C 23.6/38.2=2:3": (("23.6", 2), ("38.2", 3)), "D 38.2のみ": (("38.2", 1),)}
TARGETS = ("-5%", "-7.5%", "トレール"); STOP_SLIP = 0.002; TRAIL = 0.015; T_START, T_END = "09:15", "14:30"
def tick(px):
    for lim, t in ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3e6, 1000)):
        if px <= lim: return t
    return 5000
t0 = time.time(); C = pickle.load(open("_intraday_cache_5m.pkl", "rb")); A = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
events = []
for tk, df in C.items():
    dd = A.get(tk)
    if dd is None: continue
    dd = dd[dd.Close > 0]; dt = pd.to_datetime(df["dt"]).dt.tz_localize(None)
    g_all = df.assign(dt=dt, day=dt.dt.date, hm=dt.dt.strftime("%H:%M")).sort_values("dt"); pc = dd["Close"].shift(1)
    for day, g in g_all.groupby("day"):
        ts = pd.Timestamp(day)
        if ts not in dd.index: continue
        p = pc.get(ts, np.nan); o = float(dd.loc[ts, "Open"]); c_off = float(dd.loc[ts, "Close"])
        if not (p > 0) or o <= 0: continue
        gap = (o / p - 1) * 100
        if gap < 5 or len(g) < 40: continue
        events.append((tk, day, gap, o, c_off, g[g.hm != "09:00"][["hm", "o", "h", "l", "c"]].reset_index(drop=True)))
print(f"[universe] 寄り+5%以上 {len(events)}銘柄日", flush=True)
def band(gap): return "5-10%" if gap < 10 else "10-15%" if gap < 15 else "15-20%" if gap < 20 else "20%+"


def simulate_short(bars, o, c_off, alloc, target, wave_min):
    hm = bars.hm.to_numpy(); H = bars.h.to_numpy(float); L = bars.l.to_numpy(float); Cl = bars.c.to_numpy(float); n = len(bars)
    run_h, run_l = o, o; grid = None; fills = []; planned = None; stop_px = None; tgt_px = None; trail_on = False; trail_lo = None
    exit_px = None; exit_reason = None; last_fill_k = -1; max_ret = 0.0
    stop_lv = ORDER[ORDER.index(alloc[-1][0]) + 1]
    for k in range(n):
        t = hm[k]
        if run_h > run_l and t >= T_START:
            r = (H[k] - run_l) / (run_h - run_l)           # 安値からの戻りの深さ
            if r > max_ret: max_ret = r
        if exit_px is None and T_START < t <= T_END:
            gh, gl = (run_h, run_l) if grid is None else grid; wave = (gh - gl) / gh if gh > 0 else 0
            if gh > gl and wave >= wave_min:
                prev_c = Cl[k - 1] if k > 0 else o
                for lv, ratio in alloc:
                    if any(f[3] == lv for f in fills): continue
                    px = gl + LEVELS[lv] * (gh - gl)             # 安値から r 戻した水準に売り指値
                    if grid is None and prev_c >= px: continue    # 既に水準の上＝置かない
                    if H[k] >= px + 2 * tick(px):
                        if grid is None:
                            grid = (gh, gl); lv_px = {l_: gl + LEVELS[l_] * (gh - gl) for l_, _ in alloc}; stop_px = gl + LEVELS[stop_lv] * (gh - gl)
                            wsum = sum(r_ for _, r_ in alloc); avg_plan = sum(lv_px[l_] * r_ for l_, r_ in alloc) / wsum; risk_ps = stop_px - avg_plan
                            if risk_ps <= 0: return None
                            total_sh = RISK / risk_ps; planned = {l_: (lv_px[l_], total_sh * r_ / wsum) for l_, r_ in alloc}
                        fills.append((planned[lv][0], planned[lv][1], k, lv)); last_fill_k = k
                if fills and target != "トレール":
                    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh; tgt_px = avg * (1 - (0.05 if target == "-5%" else 0.075))
        if fills and exit_px is None:
            if H[k] >= stop_px:
                exit_px = stop_px * (1 + STOP_SLIP); exit_reason = "STOP"
            elif target == "トレール":
                if not trail_on and L[k] < grid[1]: trail_on = True; trail_lo = L[k]
                if trail_on:
                    trail_lo = min(trail_lo, L[k]); ts_px = trail_lo * (1 + TRAIL)
                    if H[k] >= ts_px and k > last_fill_k: exit_px = ts_px; exit_reason = "TRAIL"
            elif k > last_fill_k and L[k] <= tgt_px - 2 * tick(tgt_px):
                exit_px = tgt_px; exit_reason = "TP"
            if exit_px is not None: break
        run_h = max(run_h, H[k]); run_l = min(run_l, L[k])
    nf = len(fills); out = dict(n_fill=nf, n_plan=len(alloc), max_ret=max_ret, pnl_yen=0.0, R=0.0, exit="不発")
    if nf == 0: return out
    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh
    if exit_px is None: exit_px = c_off; exit_reason = "CLOSE"
    pnl = (avg - exit_px) * sh; out.update(pnl_yen=pnl, R=pnl / RISK, exit=exit_reason); return out


rows = []
for wm in (0.02,):
    for tk, day, gap, o, c_off, bars in events:
        for an, alloc in ALLOC.items():
            for tg in TARGETS:
                r = simulate_short(bars, o, c_off, alloc, tg, wm)
                if r is None: continue
                r.update(tk=tk, day=str(day), gap=gap, band=band(gap), alloc=an, target=tg); rows.append(r)
R = pd.DataFrame(rows)
print("\n■ 鏡像: 戻り売り（wave_min=2%・R=許容損失3万円）")
print(f"  {'配分':<22}{'出口':<8}{'n':>5}{'全弾%':>6}{'部分%':>6}{'不発%':>6}{'勝率%':>6}{'平均R':>7}{'合計R':>7}{'合計万円':>8}{'STOP%':>6}{'TP%':>5}{'引け%':>5}")
for an in ALLOC:
    for tg in TARGETS:
        s = R[(R.alloc == an) & (R.target == tg)]; f = s[s.n_fill > 0]; ex = f.exit.value_counts(normalize=True) * 100 if len(f) else pd.Series(dtype=float)
        print(f"  {an:<22}{tg:<8}{len(s):>5}{(s.n_fill == s.n_plan).mean()*100:>6.1f}{((s.n_fill > 0) & (s.n_fill < s.n_plan)).mean()*100:>6.1f}{(s.n_fill == 0).mean()*100:>6.1f}"
              f"{(f.pnl_yen > 0).mean()*100 if len(f) else 0:>6.1f}{f.R.mean() if len(f) else 0:>+7.2f}{s.R.sum():>+7.1f}{s.pnl_yen.sum()/1e4:>+8.1f}{ex.get('STOP', 0):>6.0f}{ex.get('TP', 0) + ex.get('TRAIL', 0):>5.0f}{ex.get('CLOSE', 0):>5.0f}")
for tg in TARGETS:
    s = R[R.target == tg]; piv = s.pivot_table(index="band", columns="alloc", values="R", aggfunc="mean").reindex(["5-10%", "10-15%", "15-20%", "20%+"])
    wr = s[s.n_fill > 0].pivot_table(index="band", columns="alloc", values="pnl_yen", aggfunc=lambda x: (x > 0).mean() * 100).reindex(piv.index)
    print(f"\n▶ ヒートマップ（出口={tg}・平均R/銘柄日・( )勝率%）"); print("  " + f"{'':<8}" + "".join(f"{a[:14]:>18}" for a in piv.columns))
    for b in piv.index:
        print("  " + f"{b:<8}" + "".join(f"{piv.loc[b, a]:>+9.2f}({(wr.loc[b, a] if a in wr.columns and pd.notna(wr.loc[b, a]) else 0):>3.0f}%)   " for a in piv.columns))
base = R[(R.alloc == list(ALLOC)[0]) & (R.target == "-5%")]
print("\n▶ 安値からの最大戻り（フィボ%）の分布")
bins = [-1, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 99]; labs = ["<23.6", "23.6-38.2", "38.2-50", "50-61.8", "61.8-78.6", "78.6-100", ">100(高値更新)"]
mr = base.assign(bin=pd.cut(base.max_ret, bins, labels=labs)); print((pd.crosstab(mr.band, mr.bin, normalize="index") * 100).reindex(["5-10%", "10-15%", "15-20%", "20%+"]).round(0).to_string())
print(f"\n[done] {time.time()-t0:.0f}s")

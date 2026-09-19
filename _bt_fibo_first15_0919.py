# -*- coding: utf-8 -*-
"""_bt_fibo_first15_0919.py — 本人「昨日のテクニスコみたいな、朝15分以内に+5%以上上昇した銘柄を、初押しの23.6から2枚で買い下がり、
デイトレなのですぐ利確したい。何%で利確？できれば5%取りたい」（2026-09-19 深夜）
対象: 9:00〜9:15の高値が 前日終値比+5%以上（T1）／ 寄り値比+5%以上＝寄り後の噴き上げ（T2）
波: 寄り〜9:15の安値→高値（以後は初弾約定までその時点の高値/安値で更新）。フィボ 23.6/38.2/50/61.8。
配分: E 23.6のみ ／ F 23.6/38.2=1:1 ／ A2 23.6/38.2/50=1:1:1 ／ A 1:2:3 ／ C 23.6/38.2=2:3
出口: 利確 +1/+2/+3/+5/+7.5%（平均取得比・足の高値が目標+2ティック）／トレール(高値更新後-1.5%)。損切り=最終弾の1段下 or 当日安値割れ。
枚数: 3万円リスク÷(計画平均−損切り)。約定=2ティック下抜け・前足終値が水準より上の弾だけ。受付9:15〜14:30・大引け公式終値。
＋ 初弾(23.6)約定後の最大含み益(MFE)/最大含み損(MAE)の分布＝「何%取れるか」の実測。
実行: python -X utf8 _bt_fibo_first15_0919.py > _log_fibo_first15_0919.txt"""
from __future__ import annotations
import pickle, time, sys
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
RISK = 30_000
LEVELS = {"23.6": 0.236, "38.2": 0.382, "50": 0.5, "61.8": 0.618, "78.6": 0.786}; ORDER = ["23.6", "38.2", "50", "61.8", "78.6"]
ALLOC = {"E 23.6のみ": (("23.6", 1),), "F 23.6/38.2=1:1": (("23.6", 1), ("38.2", 1)), "A2 23.6/38.2/50=1:1:1": (("23.6", 1), ("38.2", 1), ("50", 1)),
         "A 23.6/38.2/50=1:2:3": (("23.6", 1), ("38.2", 2), ("50", 3)), "C 23.6/38.2=2:3": (("23.6", 2), ("38.2", 3))}
TPS = ("+1%", "+2%", "+3%", "+5%", "+7.5%", "トレール"); TPV = {"+1%": 0.01, "+2%": 0.02, "+3%": 0.03, "+5%": 0.05, "+7.5%": 0.075}
STOP_SLIP = 0.002; TRAIL = 0.015; T_START, T_END = "09:15", "14:30"
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
        if not (p > 0) or o <= 0 or len(g) < 40: continue
        g = g[g.hm != "09:00"].reset_index(drop=True); f15 = g[g.hm <= "09:15"]
        if len(f15) < 2: continue
        h15 = float(f15.h.max()); l15 = min(o, float(f15.l.min()))
        rise_pc = (h15 / p - 1) * 100; rise_o = (h15 / o - 1) * 100; gap = (o / p - 1) * 100
        if rise_pc < 5: continue
        events.append(dict(tk=tk, day=str(day), gap=gap, rise_pc=rise_pc, rise_o=rise_o, o=o, c=c_off, h15=h15, l15=l15, bars=g[["hm", "o", "h", "l", "c"]]))
print(f"[universe] 9:15までの高値が前日終値比+5%以上: {len(events)}銘柄日（うち寄り後に+5%以上噴いた(T2) {sum(e['rise_o'] >= 5 for e in events)}）", flush=True)


def simulate(e, alloc, target, stop_mode):
    bars = e["bars"]; o = e["o"]; c_off = e["c"]
    hm = bars.hm.to_numpy(); H = bars.h.to_numpy(float); L = bars.l.to_numpy(float); Cl = bars.c.to_numpy(float); n = len(bars)
    run_h, run_l = e["h15"], e["l15"]; grid = None; fills = []; planned = None; stop_px = None; tgt_px = None; trail_on = False; trail_hi = None
    exit_px = None; exit_reason = None; last_fill_k = -1; first_k = None; first_px = None
    stop_lv = ORDER[ORDER.index(alloc[-1][0]) + 1]
    for k in range(n):
        t = hm[k]
        if t <= "09:15": continue
        if exit_px is None and t <= T_END:
            gh, gl = (run_h, run_l) if grid is None else grid
            if gh > gl and (gh - gl) / gh >= 0.02:
                prev_c = Cl[k - 1]
                for lv, ratio in alloc:
                    if any(f[3] == lv for f in fills): continue
                    px = gh - LEVELS[lv] * (gh - gl)
                    if grid is None and prev_c <= px: continue
                    if L[k] <= px - 2 * tick(px):
                        if grid is None:
                            grid = (gh, gl); lv_px = {l_: gh - LEVELS[l_] * (gh - gl) for l_, _ in alloc}
                            stop_px = (gh - LEVELS[stop_lv] * (gh - gl)) if stop_mode == "1段下" else gl
                            wsum = sum(r_ for _, r_ in alloc); avg_plan = sum(lv_px[l_] * r_ for l_, r_ in alloc) / wsum; risk_ps = avg_plan - stop_px
                            if risk_ps <= 0: return None
                            planned = {l_: (lv_px[l_], RISK / risk_ps * r_ / wsum) for l_, r_ in alloc}; first_k = k; first_px = lv_px[alloc[0][0]]
                        fills.append((planned[lv][0], planned[lv][1], k, lv)); last_fill_k = k
                if fills and target != "トレール":
                    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh; tgt_px = avg * (1 + TPV[target])
        if fills and exit_px is None:
            if L[k] <= stop_px: exit_px = stop_px * (1 - STOP_SLIP); exit_reason = "STOP"
            elif target == "トレール":
                if not trail_on and H[k] > grid[0]: trail_on = True; trail_hi = H[k]
                if trail_on:
                    trail_hi = max(trail_hi, H[k]); ts_px = trail_hi * (1 - TRAIL)
                    if L[k] <= ts_px and k > last_fill_k: exit_px = ts_px; exit_reason = "TRAIL"
            elif k > last_fill_k and H[k] >= tgt_px + 2 * tick(tgt_px): exit_px = tgt_px; exit_reason = "TP"
            if exit_px is not None: break
        run_h = max(run_h, H[k]); run_l = min(run_l, L[k])
    nf = len(fills); out = dict(n_fill=nf, n_plan=len(alloc), pnl_yen=0.0, R=0.0, exit="不発", mfe=np.nan, mae=np.nan, first_hm=None)
    if nf == 0: return out
    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh
    if exit_px is None: exit_px = c_off; exit_reason = "CLOSE"
    # 初弾約定後の最大含み益/含み損（初弾価格比・損切り無視で引けまで）
    if first_k is not None and first_k + 1 < n:
        out["mfe"] = (H[first_k + 1:].max() / first_px - 1) * 100; out["mae"] = (L[first_k + 1:].min() / first_px - 1) * 100
    out.update(pnl_yen=(exit_px - avg) * sh, R=(exit_px - avg) * sh / RISK, exit=exit_reason, first_hm=hm[first_k]); return out


rows = []
for e in events:
    for an, alloc in ALLOC.items():
        for tg in TPS:
            for sm in ("1段下", "安値割れ"):
                r = simulate(e, alloc, tg, sm)
                if r is None: continue
                r.update(tk=e["tk"], day=e["day"], gap=e["gap"], rise_pc=e["rise_pc"], rise_o=e["rise_o"], t2=e["rise_o"] >= 5, alloc=an, target=tg, stop=sm); rows.append(r)
R = pd.DataFrame(rows); R.to_pickle("_fibo_first15_rows_0919.pkl")
print(f"  例: 上昇率の内訳 中央値 gap{R.groupby(['tk','day']).gap.first().median():+.1f}% / 9:15高値(前終比){R.groupby(['tk','day']).rise_pc.first().median():+.1f}% / 寄り後上昇{R.groupby(['tk','day']).rise_o.first().median():+.1f}%")


def block(sub, title):
    print(f"\n■ {title}（銘柄日 {sub.groupby(['tk','day']).ngroups}）")
    b = sub[(sub.alloc == "E 23.6のみ") & (sub.target == "+5%") & (sub.stop == "1段下")]; f = b[b.n_fill > 0]
    print(f"  23.6に初押しが来て約定した割合 {len(f)/max(len(b),1)*100:.0f}%（{len(f)}/{len(b)}）・初弾の時刻 中央値 {sorted(f.first_hm)[len(f)//2] if len(f) else '-'}")
    if len(f):
        print("  初弾(23.6)約定後の最大含み益MFE（損切り無視・引けまで）: " + " ".join(f"≥{x}%:{(f.mfe >= x).mean()*100:.0f}%" for x in (1, 2, 3, 5, 7.5, 10)) + f" ／ 中央値{f.mfe.median():+.1f}%")
        print("  初弾約定後の最大含み損MAE: " + " ".join(f"≤{x}%:{(f.mae <= -x).mean()*100:.0f}%" for x in (1, 2, 3, 5, 7.5)) + f" ／ 中央値{f.mae.median():+.1f}%")
        print("  引けの結果(初弾価格比): " + f"勝率{((f.pnl_yen>0)).mean()*100:.0f}%")
    print(f"  {'配分':<24}{'損切り':<6}" + "".join(f"{tp:>14}" for tp in TPS) + "   ← 平均R/銘柄日（勝率%・約定あり）")
    for an in ALLOC:
        for sm in ("1段下", "安値割れ"):
            cells = []
            for tp in TPS:
                s = sub[(sub.alloc == an) & (sub.target == tp) & (sub.stop == sm)]; f = s[s.n_fill > 0]
                cells.append(f"{s.R.mean():>+6.2f}({(f.pnl_yen > 0).mean()*100 if len(f) else 0:>3.0f}%)")
            print(f"  {an:<24}{sm:<6}" + "".join(f"{c:>14}" for c in cells))
    # 最良セル
    g = sub.groupby(["alloc", "target", "stop"]).agg(R=("R", "mean"), n=("R", "size"), tot=("R", "sum")).reset_index().sort_values("R", ascending=False)
    top = g.iloc[0]; print(f"  最良: {top.alloc} × {top.target} × 損切り{top['stop']} = 平均{top.R:+.2f}R/銘柄日・合計{top.tot:+.0f}R（{int(top.n)}銘柄日）")


block(R, "T1: 9:15までの高値が前日終値比+5%以上（ギャップ込み）")
block(R[R.t2], "T2: 寄り後15分で+5%以上噴いた（テクニスコ型・ギャップだけの銘柄を除く）")
block(R[R.t2 & (R.gap < 3)], "T2b: 寄りのギャップ3%未満で、寄り後15分に+5%以上噴いた")
print(f"\n[done] {time.time()-t0:.0f}s")

# -*- coding: utf-8 -*-
"""_bt_fibo_scalein_gap_0919.py — 寄り付きギャップ銘柄のフィボナッチ買い下がり戦略 BT（2026-09-19 本人仕様書）
データ: _intraday_cache_5m.pkl（Yahoo 5分足・2026-06-23〜09-16・1,162銘柄）＋ 立花日足（公式 始値/終値・〜09-02）
  ※立花APIは分足の遡及不可（毎分断面の日次収集は 9/17 から live_flow/minutes に稼働中）。足ラベルは終了時刻（09:05=09:00-09:05）。
  ※5分足には15:25-15:30の板寄せが無い → 大引け手仕舞いは公式終値。
対象: 公式始値が前日終値比 +5%以上 の銘柄日（gap_band 5-10/10-15/15-20/20+）
フィボ: 当日安値→当日高値（その時点の高値で更新）。水準 23.6/38.2/50/61.8。初弾約定でグリッド固定。
配分: A 23.6/38.2/50=1:2:3 ／ B 38.2/50=2:3 ／ C 23.6/38.2=2:3 ／ D 38.2のみ
損切り: 最終弾の1段下のフィボ割れ（足の安値がその水準を割ったら水準-0.2%で成行）
利確: +5% / +7.5%（平均取得比・足の高値が目標+2ティック以上で約定）/ 高値更新後トレール（グリッド高値を更新したら以後の最高値-1.5%）
枚数: 総株数 = 口座資金300万×1% ÷ (計画平均取得単価 − 損切り価格)・比率で配分（連続株数・R=3万円で正規化）
約定: 指値は「足の安値が指値を2ティック以上下回った」時だけ。前足終値が水準より上の弾だけ置く（既に下にある水準は置かない）。
時間: 受付 9:15〜14:30（14:30で未約定は取消）・大引け成行手仕舞い。
波の最小幅 wave_min（高値−安値≥価格比）を感度パラメータに（既定2%）。
実行: python -X utf8 _bt_fibo_scalein_gap_0919.py > _log_fibo_scalein_gap_0919.txt"""
from __future__ import annotations
import pickle, time, sys
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ACCOUNT = 3_000_000; RISK = ACCOUNT * 0.01
LEVELS = {"23.6": 0.236, "38.2": 0.382, "50": 0.5, "61.8": 0.618, "78.6": 0.786}
ORDER = ["23.6", "38.2", "50", "61.8", "78.6"]
ALLOC = {"A 23.6/38.2/50=1:2:3": (("23.6", 1), ("38.2", 2), ("50", 3)),
         "B 38.2/50=2:3": (("38.2", 2), ("50", 3)),
         "C 23.6/38.2=2:3": (("23.6", 2), ("38.2", 3)),
         "D 38.2のみ": (("38.2", 1),)}
TARGETS = ("+5%", "+7.5%", "トレール")
STOP_SLIP = 0.002; TRAIL = 0.015
T_START, T_END = "09:15", "14:30"


def tick(px: float) -> float:
    for lim, t in ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3e6, 1000)):
        if px <= lim: return t
    return 5000


t0 = time.time()
C = pickle.load(open("_intraday_cache_5m.pkl", "rb")); A = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
print(f"[load] 5分足{len(C)}銘柄・立花日足{len(A)}銘柄 {time.time()-t0:.0f}s", flush=True)

# ── 対象銘柄日の抽出 ──
events = []
for tk, df in C.items():
    dd = A.get(tk)
    if dd is None: continue
    dd = dd[dd.Close > 0]
    dt = pd.to_datetime(df["dt"]).dt.tz_localize(None)
    g_all = df.assign(dt=dt, day=dt.dt.date, hm=dt.dt.strftime("%H:%M")).sort_values("dt")
    pc = dd["Close"].shift(1)
    for day, g in g_all.groupby("day"):
        ts = pd.Timestamp(day)
        if ts not in dd.index: continue
        p = pc.get(ts, np.nan); o = float(dd.loc[ts, "Open"]); c_off = float(dd.loc[ts, "Close"])
        if not (p > 0) or o <= 0: continue
        gap = (o / p - 1) * 100
        if gap < 5 or len(g) < 40: continue
        g = g[g.hm != "09:00"]
        events.append((tk, day, gap, o, c_off, g[["hm", "o", "h", "l", "c"]].reset_index(drop=True)))
print(f"[universe] 寄り+5%以上の銘柄日 {len(events)}（{min(e[1] for e in events)}〜{max(e[1] for e in events)}）", flush=True)


def band(gap):
    return "5-10%" if gap < 10 else "10-15%" if gap < 15 else "15-20%" if gap < 20 else "20%+"


def simulate(bars: pd.DataFrame, o: float, c_off: float, alloc, target: str, wave_min: float):
    """1銘柄日×1配分×1出口。戻り値 dict（0本=見送りも返す）。"""
    hm = bars.hm.to_numpy(); H = bars.h.to_numpy(float); L = bars.l.to_numpy(float); Cl = bars.c.to_numpy(float)
    n = len(bars)
    run_h, run_l = o, o                       # 寄り値から開始（初足の高安で更新）
    grid = None                               # 初弾約定でフィボグリッド固定 (H, L)
    fills = []                                # (price, shares, bar_idx, level)
    planned = None                            # 弾ごとの計画 (level_name, level_price, shares)
    stop_px = None; stop_lv = None
    tgt_px = None; trail_on = False; trail_hi = None
    max_ret = 0.0                             # 最大押し戻り（フィボ%）
    exit_px = None; exit_reason = None; exit_i = None
    last_level = alloc[-1][0]; stop_lv = ORDER[ORDER.index(last_level) + 1]
    last_fill_k = -1
    for k in range(n):
        t = hm[k]
        # 押しの深さ（前足までの高安に対する当足安値の戻り）
        if run_h > run_l and t >= T_START:
            r = (run_h - L[k]) / (run_h - run_l)
            if r > max_ret: max_ret = r
        # ① 新規/追加の約定判定（受付時間内・前足終値が水準より上の弾のみ・2ティック下抜けで約定）
        if exit_px is None and T_START < t <= T_END:
            gh, gl = (run_h, run_l) if grid is None else grid
            wave = (gh - gl) / gh if gh > 0 else 0
            if gh > gl and wave >= wave_min:
                prev_c = Cl[k - 1] if k > 0 else o
                for lv, ratio in alloc:
                    if any(f[3] == lv for f in fills): continue
                    px = gh - LEVELS[lv] * (gh - gl)
                    if grid is None and prev_c <= px: continue         # 既に水準の下＝置かない
                    if L[k] <= px - 2 * tick(px):
                        if grid is None:
                            grid = (gh, gl)
                            lv_px = {l_: gh - LEVELS[l_] * (gh - gl) for l_, _ in alloc}
                            stop_px = gh - LEVELS[stop_lv] * (gh - gl)
                            wsum = sum(r_ for _, r_ in alloc); avg_plan = sum(lv_px[l_] * r_ for l_, r_ in alloc) / wsum
                            risk_ps = avg_plan - stop_px
                            if risk_ps <= 0: return None
                            total_sh = RISK / risk_ps
                            planned = {l_: (lv_px[l_], total_sh * r_ / wsum) for l_, r_ in alloc}
                        fills.append((planned[lv][0], planned[lv][1], k, lv)); last_fill_k = k
                if fills and target != "トレール":
                    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh
                    tgt_px = avg * (1 + (0.05 if target == "+5%" else 0.075))
        # ② 手仕舞い判定（損切り優先＝悲観・利確/トレールは約定足の次の足から）
        if fills and exit_px is None:
            if L[k] <= stop_px:
                exit_px = stop_px * (1 - STOP_SLIP); exit_reason = "STOP"; exit_i = k
            elif target == "トレール":
                if not trail_on and H[k] > grid[0]:
                    trail_on = True; trail_hi = H[k]
                if trail_on:
                    trail_hi = max(trail_hi, H[k]); ts_px = trail_hi * (1 - TRAIL)
                    if L[k] <= ts_px and k > last_fill_k:
                        exit_px = ts_px; exit_reason = "TRAIL"; exit_i = k
            elif k > last_fill_k and H[k] >= tgt_px + 2 * tick(tgt_px):
                exit_px = tgt_px; exit_reason = "TP"; exit_i = k
            if exit_px is not None: break
        run_h = max(run_h, H[k]); run_l = min(run_l, L[k])
    nf = len(fills)
    out = dict(n_fill=nf, max_ret=max_ret, n_plan=len(alloc), pnl_yen=0.0, R=0.0, exit=None, rebound_min=np.nan, avg=np.nan, stop=stop_px)
    if nf == 0:
        out["exit"] = "不発"; return out
    sh = sum(f[1] for f in fills); avg = sum(f[0] * f[1] for f in fills) / sh
    if exit_px is None:
        exit_px = c_off; exit_reason = "CLOSE"; exit_i = n - 1
    pnl = (exit_px - avg) * sh
    # 最終弾から反発（終値が平均取得を上回る）までの所要時間（分）
    li = fills[-1][2]; reb = np.nan
    for kk in range(li + 1, n):
        if Cl[kk] >= avg:
            reb = (kk - li) * 5; break
    out.update(pnl_yen=pnl, R=pnl / RISK, exit=exit_reason, rebound_min=reb, avg=avg, exit_px=exit_px, last_fill_hm=hm[li], first_fill_hm=hm[fills[0][2]])
    return out


def run_all(wave_min: float) -> pd.DataFrame:
    rows = []
    for tk, day, gap, o, c_off, bars in events:
        for an, alloc in ALLOC.items():
            for tg in TARGETS:
                r = simulate(bars, o, c_off, alloc, tg, wave_min)
                if r is None: continue
                r.update(tk=tk, day=str(day), gap=gap, band=band(gap), alloc=an, target=tg, wave_min=wave_min, open=o, close=c_off)
                rows.append(r)
    return pd.DataFrame(rows)


def report(R: pd.DataFrame, wave_min: float, full: bool):
    print(f"\n{'═'*100}\n■ wave_min={wave_min*100:.0f}%（銘柄日 {R.groupby(['tk','day']).ngroups}）", flush=True)
    base = R[(R.alloc == list(ALLOC)[0]) & (R.target == "+5%")]
    if full:
        print("\n▶ 最大押し戻り（フィボ%・受付時間内・その時点の高安に対する）の分布 ＝ 銘柄日ごと（配分に依存しない）")
        bins = [-1, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 99]; labs = ["<23.6", "23.6-38.2", "38.2-50", "50-61.8", "61.8-78.6", "78.6-100", ">100(安値更新)"]
        mr = base.assign(bin=pd.cut(base.max_ret, bins, labels=labs))
        tab = pd.crosstab(mr.band, mr.bin, normalize="index") * 100
        tab = tab.reindex(["5-10%", "10-15%", "15-20%", "20%+"]); tab["n"] = mr.groupby("band").size().reindex(tab.index)
        print(tab.round(0).to_string())
        drift = base.assign(oc=(base.close / base.open - 1) * 100).groupby("band").oc.agg(["mean", "median", lambda x: (x > 0).mean() * 100]).reindex(["5-10%", "10-15%", "15-20%", "20%+"])
        drift.columns = ["寄→引 平均%", "中央値%", "上昇率%"]; print("\n▶ 参考: 対象銘柄日の寄り→大引け（買い持ちの地合い）"); print(drift.round(2).to_string())
    print("\n▶ 配分×出口: 約定率・損益（R=許容損失3万円を1とした倍率・不発は0R）")
    print(f"  {'配分':<22}{'出口':<8}{'n':>5}{'全弾%':>6}{'部分%':>6}{'不発%':>6}{'勝率%':>6}{'平均R':>7}{'合計R':>7}{'合計万円':>8}{'STOP%':>6}{'TP%':>5}{'引け%':>5}{'反発中央値分':>8}")
    for an in ALLOC:
        for tg in TARGETS:
            s = R[(R.alloc == an) & (R.target == tg)]; f = s[s.n_fill > 0]
            ex = f.exit.value_counts(normalize=True) * 100 if len(f) else pd.Series(dtype=float)
            print(f"  {an:<22}{tg:<8}{len(s):>5}{(s.n_fill == s.n_plan).mean()*100:>6.1f}{((s.n_fill > 0) & (s.n_fill < s.n_plan)).mean()*100:>6.1f}{(s.n_fill == 0).mean()*100:>6.1f}"
                  f"{(f.pnl_yen > 0).mean()*100 if len(f) else 0:>6.1f}{f.R.mean() if len(f) else 0:>+7.2f}{s.R.sum():>+7.1f}{s.pnl_yen.sum()/1e4:>+8.1f}"
                  f"{ex.get('STOP', 0):>6.0f}{ex.get('TP', 0) + ex.get('TRAIL', 0):>5.0f}{ex.get('CLOSE', 0):>5.0f}{f.rebound_min.median() if len(f) else np.nan:>8.0f}")
    if full:
        print("\n▶ 約定弾数別の損益（出口=+5%・配分A/B/C/D）")
        for an in ALLOC:
            s = R[(R.alloc == an) & (R.target == "+5%")]
            g = s.groupby("n_fill").agg(n=("R", "size"), 平均R=("R", "mean"), 勝率=("pnl_yen", lambda x: (x > 0).mean() * 100), 合計R=("R", "sum"))
            print(f"  {an}: " + " | ".join(f"{int(k)}弾 n{int(v.n)} 平均{v.平均R:+.2f}R 勝率{v.勝率:.0f}% 計{v.合計R:+.1f}R" for k, v in g.iterrows()))
        print("\n▶ 最終弾から反発（終値が平均取得を上回る）までの所要時間（分・出口=+5%・約定あり）")
        for an in ALLOC:
            f = R[(R.alloc == an) & (R.target == "+5%") & (R.n_fill > 0)]
            q = f.rebound_min.dropna()
            print(f"  {an}: 反発あり{len(q)}/{len(f)}（{len(q)/max(len(f),1)*100:.0f}%） 中央値{q.median() if len(q) else np.nan:.0f}分 25%点{q.quantile(.25) if len(q) else np.nan:.0f} 75%点{q.quantile(.75) if len(q) else np.nan:.0f} ／ 反発なし{f.rebound_min.isna().mean()*100:.0f}%")
    for tg in TARGETS:
        print(f"\n▶ ヒートマップ gap_band × 配分（出口={tg}・平均R/銘柄日・( )内=約定ありの勝率%・[ ]内=n）")
        s = R[R.target == tg]
        piv = s.pivot_table(index="band", columns="alloc", values="R", aggfunc="mean").reindex(["5-10%", "10-15%", "15-20%", "20%+"])
        wr = s[s.n_fill > 0].pivot_table(index="band", columns="alloc", values="pnl_yen", aggfunc=lambda x: (x > 0).mean() * 100).reindex(piv.index)
        nn = s.pivot_table(index="band", columns="alloc", values="R", aggfunc="size").reindex(piv.index)
        print("  " + f"{'':<8}" + "".join(f"{a[:14]:>22}" for a in piv.columns))
        for b in piv.index:
            print("  " + f"{b:<8}" + "".join(f"{piv.loc[b, a]:>+8.2f}({wr.loc[b, a] if b in wr.index and a in wr.columns and pd.notna(wr.loc[b, a]) else 0:>3.0f}%)[{int(nn.loc[b, a]) if pd.notna(nn.loc[b, a]) else 0:>4}]" for a in piv.columns))


R2 = run_all(0.02); report(R2, 0.02, full=True)
R2.to_csv("_fibo_scalein_rows_0919.csv", index=False, encoding="utf-8-sig")
for wm in (0.01, 0.03):
    report(run_all(wm), wm, full=False)
print(f"\n[done] {time.time()-t0:.0f}s")

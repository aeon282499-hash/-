# -*- coding: utf-8 -*-
"""_bt_gokujo_round15_zerobase_0919.py — 極上ラウンド15「一から考え直す」（2026-09-19 夜 本人
「いまのルールが間違っているかもしれん・確証バイアス・追加買い・保有日数変更・シグナル少ないなら保有日数増やしてもいい・
 新データ(SBI/J-Q/立花)で極上をアップデートできるはず」）

土台は極み入口に固定しない。広い候補(RSI≤50/乖離≤-1%/rr1.5|vr2/代金20億/ATR≤3/≤1万円/寄指NOFILL除外・26年 立花日足)を
自前で切り直し、5日ではなく12日先までの足を付け直す（保有4〜10日を測るため）。
 0 土台の再現（R14 a1: 1×150 乗せなし・DAY1CUT = 26年+352万/PF1.30/DD-80・10年+300万/PF1.63/DD-25）
 A 保有日数×出口の総当たり（現行入口・1×150・DAY1CUT）: hold2-10 × TP × STOP × RSI出口 ＋ トレーリング ＝ 600+セル
    ＋「入口を絞る×保有を伸ばす」の表（vt5 0.8/0.9/1.0/1.09 × hold 3/5/7/10）
 B 追加買い: ナンピン3型（初日引け(-1,0]で同額／≤-1%で切らず同額／≤0で同額）・勝ち乗せ3型・長期保有時の3日目乗せ
 C 確証バイアス監査＝ウォークフォワード再選定: 入口48×出口108=5,184セルを毎年「過去だけ」で選び直して翌年を運用。
    現行ルールの順位（2001-16 vs 2017-26）・WF曲線 vs 固定ルール vs 事後最良
 D 新データ（10年）: 空売り残高報告(J-Quants)・日経VI・投資部門別(海外)・貸借倍率 → 極上に当てる（16-20/21-26・噪音床）
実行: python -X utf8 _bt_gokujo_round15_zerobase_0919.py > _log_gokujo_round15_zerobase_0919.txt
"""
from __future__ import annotations
import json, pickle, time, os, sys
import numpy as np, pandas as pd

t0 = time.time()
H2 = 12
CACHE = "_bt_gokujo_r15_cache.pkl"
ERAS4 = ((2001, 2013, "01-13"), (2014, 2026, "14-26"), (2017, 2021, "17-21"), (2022, 2026, "22-26"))


def log(*a):
    print(*a, flush=True)


# ══════════════════════════════════════════════════════════════
# 0. 土台の構築（広い候補 + 12日先の足 + vt5）
# ══════════════════════════════════════════════════════════════
if os.path.exists(CACHE):
    Z = pickle.load(open(CACHE, "rb")); C = Z["C"]; OP, HI, LO, CL, RS = (Z[k] for k in ("OP", "HI", "LO", "CL", "RS")); VT5 = Z["VT5"]
    log(f"[cache] {CACHE} 候補{len(C):,}件 {time.time()-t0:.0f}s")
else:
    P = pickle.load(open("_bt_buy_20y_wide.pkl", "rb")); C0 = P["C"]
    RSI, DEV, RR, VR, TOV, ATR, PRICE, NOFILL = (C0[c].to_numpy() for c in ("rsi", "dev", "rr", "vr", "tov", "atr", "price", "nofill"))
    E_ = C0.E.to_numpy()
    WIDE = (RSI <= 50) & (DEV <= -1.0) & ((RR >= 1.5) | (VR >= 2.0)) & (TOV >= 2e9) & (ATR <= 3.0) & (PRICE <= 10000) & ~NOFILL.astype(bool) & np.isfinite(E_) & (E_ > 0)
    C = C0[WIDE].reset_index(drop=True); n = len(C)
    log(f"[pool] 広い候補 {n:,}件（元 {len(C0):,}） {time.time()-t0:.0f}s")
    OP5, HI5, LO5, CL5, RS5 = (P[k][WIDE] for k in ("OP", "HI", "LO", "CL", "RS")); del P
    ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
    OP = np.full((n, H2), np.nan); HI = OP.copy(); LO = OP.copy(); CL = OP.copy(); RS = OP.copy(); VT5 = np.full(n, np.nan)
    need = {}
    for i, (tk, d) in enumerate(zip(C.ticker, C.entry)): need.setdefault(tk, []).append((d, i))
    bad = 0
    for tk, rows in need.items():
        df0 = ALL.get(tk)
        if df0 is None: continue
        # 足: builder と同じ掃除（dropna Close）
        df = df0.dropna(subset=["Close"])
        o = df["Open"].astype(float).to_numpy(); h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy(); c = df["Close"].astype(float)
        dlt = c.diff(); ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean(); al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2).to_numpy(); cn = c.to_numpy()
        pos = {d.strftime("%Y-%m-%d"): k for k, d in enumerate(df.index)}; N = len(cn)
        # vt5: round6 と同じ掃除（Close>0）
        df2 = df[df.Close > 0]; v = df2["Volume"].astype(float).to_numpy(); pos2 = {d.strftime("%Y-%m-%d"): k for k, d in enumerate(df2.index)}
        for d, i in rows:
            k = pos.get(d)
            if k is None: bad += 1; continue
            m = min(H2, N - k)
            OP[i, :m] = o[k:k + m]; HI[i, :m] = h[k:k + m]; LO[i, :m] = l[k:k + m]; CL[i, :m] = cn[k:k + m]; RS[i, :m] = rsi[k:k + m]
            k2 = pos2.get(d)
            if k2 is not None and k2 >= 27:
                t = k2 - 1; v20 = v[t - 24:t - 4]
                if v20.mean() > 0: VT5[i] = v[t - 4:t + 1].mean() / v20.mean()
    del ALL
    ok = np.isfinite(CL5[:, 2]) & np.isfinite(CL[:, 2])
    log(f"[fwd] 12日足 付け直し 欠損{bad}件 / 5日足との一致率 OP{np.mean(np.isclose(OP[ok, :5], OP5[ok], equal_nan=True)):.4f} CL{np.mean(np.isclose(CL[ok, :5], CL5[ok], equal_nan=True)):.4f} RS{np.mean(np.isclose(RS[ok, :5], RS5[ok], equal_nan=True, atol=0.02)):.4f} {time.time()-t0:.0f}s")
    pickle.dump(dict(C=C, OP=OP, HI=HI, LO=LO, CL=CL, RS=RS, VT5=VT5), open(CACHE, "wb"), protocol=4)

n = len(C); E0 = C.E.to_numpy(); RSI, DEV, RR, VR, TOV, ATR, PRICE = (C[c].to_numpy() for c in ("rsi", "dev", "rr", "vr", "tov", "atr", "price"))
YEARv = C.year.to_numpy(); TICKv = C.ticker.to_numpy(); DATEv = pd.to_datetime(C.entry.to_numpy())
SECMAP = json.load(open("sector33_map.json", encoding="utf-8")); SECv = np.array([SECMAP.get(t) or f"__u{t}" for t in TICKv], dtype=object)
days = sorted(C.entry.unique()); gdi = {d: i for i, d in enumerate(days)}; DAY = C.entry.map(gdi).to_numpy(); DAYS_TS = pd.to_datetime(pd.Index(days))
day_groups = [np.where(DAY == d)[0] for d in range(len(days))]
rsi_s = 1 / (1 + ((RSI - 38) / 8) ** 2); dev_s = 1 / (1 + ((DEV + 3) / 2) ** 2); turn_s = np.log10(np.maximum(TOV, 1) / 1e9 + 1) / 3
SCORE = rsi_s * 0.3 + dev_s * 0.3 + turn_s * 0.4
ENT_CUR = (RSI <= 45) & (DEV <= -1.5)
G = ENT_CUR & (VT5 <= 1.09)
log(f"[base] 現行入口 {ENT_CUR.sum():,}件 / 極上(vt5≤1.09) {G.sum():,}件")


def replay(tp=5.0, stop=3.0, hold=3, rsith=50.0, d1cut=1.0, trail=None, be=None):
    """寄り建て(k=0)。悲観順序=寄りギャップ→損切→利確→RSI/期限。tp/stop/rsith=None で無効。
    d1cut: 初日引け≤建値×(1-d1cut%) → 2日目寄りで処分。trail: 直近高値からtrail%割れで処分（安値タッチ・約定は水準）。
    be: 含み益がbe%に達した後は損切りを建値へ。戻り pnl%, exo(手仕舞いk)"""
    valid = np.isfinite(E0) & (E0 > 0) & np.isfinite(CL[:, hold - 1]); pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = E0 * (1 - stop / 100) if stop is not None else np.full(n, -np.inf); tl = E0 * (1 + tp / 100) if tp is not None else np.full(n, np.inf)
    sl = sl.copy(); hi_run = np.full(n, -np.inf)
    for k in range(hold):
        live = ~done
        if k > 0:
            op = OP[:, k]; okop = np.isfinite(op) & (op > 0)
            g = live & okop & ((op <= sl) | (op >= tl)); pnl[g] = (op[g] - E0[g]) / E0[g] * 100; exo[g] = k; done |= g; live = ~done
            if d1cut is not None and k == 1:
                m_ = live & okop & (CL[:, 0] <= E0 * (1 - d1cut / 100)); pnl[m_] = (op[m_] - E0[m_]) / E0[m_] * 100; exo[m_] = k; done |= m_; live = ~done
        s = live & (LO[:, k] <= sl); pnl[s] = (sl[s] - E0[s]) / E0[s] * 100; exo[s] = k; done |= s; live = ~done
        t_ = live & (HI[:, k] >= tl); pnl[t_] = (tl[t_] - E0[t_]) / E0[t_] * 100; exo[t_] = k; done |= t_; live = ~done
        if trail is not None:
            hi_run = np.maximum(hi_run, np.where(np.isfinite(HI[:, k]), HI[:, k], -np.inf))
            tr_lvl = hi_run * (1 - trail / 100)
            s2 = live & (k > 0) & (LO[:, k] <= tr_lvl) & (tr_lvl > sl); pnl[s2] = (tr_lvl[s2] - E0[s2]) / E0[s2] * 100; exo[s2] = k; done |= s2; live = ~done
        if be is not None:
            up = live & (CL[:, k] >= E0 * (1 + be / 100)); sl = np.where(up, np.maximum(sl, E0), sl)
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]) if rsith is not None else np.zeros(n, bool)
        r_ = live & (rc | (k == hold - 1)); pnl[r_] = (CL[r_, k] - E0[r_]) / E0[r_] * 100; exo[r_] = k; done |= r_
    return pnl, exo


def sim(mask, pnl, exo, key=SCORE, slots=1, size=1_500_000, max_sig=5, cap=3, add_cond=None, add_px=None, add_frac=0.0, exit_px=None):
    """選定=本番と同じ(1日5件・業種cap3・保有中除外)→枠。追加買い: add_cond[i] True の玉に add_px[i] で size×add_frac を追加し本玉と同時手仕舞い。"""
    ou = {}; os_ = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]: del ou[tk]; del os_[tk]
        sc = {}
        for s in os_.values(): sc[s] = sc.get(s, 0) + 1
        cnt = 0; idxs = day_groups[d]; idxs = idxs[mask[idxs]]
        if len(idxs) == 0: continue
        idxs = idxs[np.argsort(-key[idxs], kind="stable")]
        for i in idxs:
            if cnt >= max_sig: break
            if not np.isfinite(pnl[i]) or TICKv[i] in ou: continue
            s = SECv[i]
            if sc.get(s, 0) >= cap: continue
            cnt += 1; ex = min(d + int(exo[i]), len(days) - 1); ou[TICKv[i]] = ex; os_[TICKv[i]] = s; sc[s] = sc.get(s, 0) + 1; picks.append((d, ex, i))
    live = []; rows = []
    for d, ex, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= slots: continue
        sh = int(size / E0[i] / 100) * 100
        if sh <= 0: continue
        yen = pnl[i] / 100 * sh * E0[i]; expo = sh * E0[i]
        if add_cond is not None and add_cond[i] and np.isfinite(add_px[i]) and add_px[i] > 0:
            ash = int(size * add_frac / add_px[i] / 100) * 100
            yen += (exit_px[i] - add_px[i]) * ash; expo += ash * add_px[i]
        live.append(ex); rows.append((i, YEARv[i], pnl[i], yen, sh, d, ex, expo))
    return pd.DataFrame(rows, columns=["i", "y", "pnl", "yen", "sh", "d", "ex", "expo"])


def tot(R, a=2001, b=2026): s = R[(R.y >= a) & (R.y <= b)]; return s.yen.sum() / 1e4


def exit_series(R, y0=2001):
    R = R[R.y >= y0]
    if len(R) == 0: return pd.Series(dtype=float)
    return pd.Series(R.yen.to_numpy(), index=DAYS_TS[R.ex.to_numpy()]).groupby(level=0).sum().sort_index()


def dd_of(R, y0=2001):
    s = exit_series(R, y0)
    if len(s) == 0: return 0.0
    c = s.cumsum(); return (c - c.cummax()).min() / 1e4


def stat(R):
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    r10 = R[R.y >= 2017]; yy10 = r10.groupby("y").yen.sum()
    return dict(n=len(R), win=(R.pnl > 0).mean() * 100 if len(R) else 0, pf=gp / gl if gl else 9.99, tot=tot(R), a=tot(R, 2001, 2013), b=tot(R, 2014, 2026), e3=tot(R, 2017, 2021), e4=tot(R, 2022, 2026),
                wy=int((yy > 0).sum()), worst=yy.min() / 1e4 if len(yy) else 0, dd=dd_of(R), y10=tot(R, 2017, 2026), n10=len(r10),
                pf10=(r10.yen[r10.yen > 0].sum() / max(-r10.yen[r10.yen <= 0].sum(), 1)), win10=(r10.pnl > 0).mean() * 100 if len(r10) else 0,
                worst10=yy10.min() / 1e4 if len(yy10) else 0, dd10=dd_of(R, 2017), wy10=int((yy10 > 0).sum()))


def show(lab, R, cap=150, base=None):
    r = stat(R)
    j = ""
    if base is not None:
        ok = r["a"] > base["a"] and r["b"] > base["b"] and r["e3"] > base["e3"] and r["e4"] > base["e4"] and r["dd"] >= base["dd"] - 1 and r["worst"] >= base["worst"] - 1
        j = " ★" if ok else ""
    log(f"  {lab:<40} 玉{r['n']:>5}(10年{r['n10']/10:>3.0f}/年) 勝率{r['win']:>5.1f}% PF{r['pf']:>4.2f} 26年{r['tot']:>+6,.0f}万 DD{r['dd']:>+5,.0f} 最悪年{r['worst']:>+4,.0f} 4分割{r['a']:>+5,.0f}/{r['b']:>+5,.0f}/{r['e3']:>+5,.0f}/{r['e4']:>+5,.0f} | 10年{r['y10']:>+5,.0f}万 PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% DD{r['dd10']:>+4,.0f} 最悪年{r['worst10']:+.0f} 勝年{r['wy10']}/10 年利{r['y10']/cap/10*100:>4.1f}%{j}")
    return r


# ── 0 土台の再現 ──
log("\n■ 0 土台の再現（現行極上: RSI≤45/乖離≤-1.5/vt5≤1.09・TP5/STOP3/H3/RSI50/DAY1CUT-1%・1×150 乗せなし）")
PNL_B, EXO_B = replay()
B0 = show("現行（R14 a1 目標: 26年+352/PF1.30/DD-80・10年+300/PF1.63/DD-25）", sim(G, PNL_B, EXO_B))
PNL_NC, EXO_NC = replay(d1cut=None)
show("参考 DAY1CUTなし", sim(G, PNL_NC, EXO_NC))

# 長い保有の公平比較のため、12日目まで足がある候補に揃える（2026年8月中旬以降の数玉が落ちる）
V12 = np.isfinite(CL[:, H2 - 1])
Bv = show("土台（12日足あり候補に揃えた版＝以降A/B/Cの基準）", sim(G & V12, PNL_B, EXO_B))

# ══════════════════════════════════════════════════════════════
# A 保有日数 × 出口
# ══════════════════════════════════════════════════════════════
log("\n■ A 保有日数×出口の総当たり（現行入口 G・1×150・DAY1CUT-1%・12日足あり候補）")
cells = []
GV = G & V12
for hold in (2, 3, 4, 5, 7, 10):
    for tp in (3.0, 5.0, 7.0, 10.0, None):
        for stop in (2.0, 3.0, 4.0, 6.0, None):
            for rsith in (50.0, 55.0, 60.0, None):
                if hold == 2 and rsith is not None and rsith != 50.0: continue
                p, e = replay(tp=tp, stop=stop, hold=hold, rsith=rsith)
                r = stat(sim(GV, p, e)); r.update(hold=hold, tp=tp, stop=stop, rsith=rsith, kind="grid"); cells.append(r)
log(f"  グリッド {len(cells)}セル {time.time()-t0:.0f}s")
for hold in (5, 7, 10):
    for trail in (3.0, 5.0):
        for tp in (7.0, None):
            p, e = replay(tp=tp, stop=3.0, hold=hold, rsith=None, trail=trail)
            r = stat(sim(GV, p, e)); r.update(hold=hold, tp=tp, stop=3.0, rsith=None, kind=f"trail{trail:.0f}"); cells.append(r)
    for be in (2.0, 3.0):
        p, e = replay(tp=None, stop=3.0, hold=hold, rsith=None, be=be)
        r = stat(sim(GV, p, e)); r.update(hold=hold, tp=None, stop=3.0, rsith=None, kind=f"be{be:.0f}"); cells.append(r)
D = pd.DataFrame(cells)
_f = lambda v, fmt=lambda z: z: fmt(v) if pd.notna(v) else "∞"
D["lab"] = D.apply(lambda x: f"H{x.hold} TP{_f(x.tp)} ST{_f(x.stop)} RSI{_f(x.rsith, int)} {x.kind}", axis=1)
cur = D[(D.hold == 3) & (D.tp == 5) & (D.stop == 3) & (D.rsith == 50) & (D.kind == "grid")].iloc[0]
D["beat4"] = (D.a > cur.a) & (D.b > cur.b) & (D.e3 > cur.e3) & (D.e4 > cur.e4)
D["beat4dd"] = D.beat4 & (D.dd >= cur.dd - 1) & (D.worst >= cur.worst - 1)
D["rank26"] = D.tot.rank(ascending=False); D["rank10"] = D.y10.rank(ascending=False)
log(f"  現行セルの順位: 26年 {int(D[D.lab==cur.lab].rank26.iloc[0])}/{len(D)} ・10年 {int(D[D.lab==cur.lab].rank10.iloc[0])}/{len(D)}  | 4分割すべて上回るセル {int(D.beat4.sum())} ・＋DD/最悪年も悪化なし {int(D.beat4dd.sum())}")


def dline(x):
    return f"  {x.lab:<32} 玉{int(x.n):>5} 勝率{x.win:>5.1f}% PF{x.pf:4.2f} 26年{x.tot:>+6,.0f} DD{x.dd:>+5,.0f} 最悪{x.worst:>+4,.0f} 4分割{x.a:>+5,.0f}/{x.b:>+5,.0f}/{x.e3:>+5,.0f}/{x.e4:>+5,.0f} | 10年{x.y10:>+5,.0f} PF{x.pf10:.2f} 勝率{x.win10:.1f}% DD{x.dd10:>+4,.0f} 最悪{x.worst10:+.0f} 勝年{int(x.wy10)}/10{' ★' if x.beat4dd else (' ☆' if x.beat4 else '')}"


log("  --- 26年合計 上位15 ---")
for _, x in D.sort_values("tot", ascending=False).head(15).iterrows(): log(dline(x))
log("  --- 10年合計 上位10 ---")
for _, x in D.sort_values("y10", ascending=False).head(10).iterrows(): log(dline(x))
log("  --- 保有日数ごとの最良セル（26年）と 現行出口(TP5/ST3/RSI50)のまま保有だけ変えた場合 ---")
for hold in (2, 3, 4, 5, 7, 10):
    s = D[D.hold == hold]; log(dline(s.sort_values("tot", ascending=False).iloc[0]))
    q = s[(s.tp == 5) & (s.stop == 3) & (s.rsith == 50) & (s.kind == "grid")]
    if len(q): log("   (現行出口のまま)" + dline(q.iloc[0])[2:])
log("  --- 4分割すべて上回るセル（あれば） ---")
for _, x in D[D.beat4].sort_values("tot", ascending=False).head(10).iterrows(): log(dline(x))
D.to_csv("_bt_gokujo_round15_exitgrid.csv", index=False)

log("\n■ A2 「入口を絞る × 保有を伸ばす」（シグナルが減るなら保有を増やす、の直接測定）")
log("  行=vt5上限（絞るほど玉が減る）・列=保有日数。セル=26年合計万(10年合計万) [PF26]。出口は 現行(TP5/ST3/RSI50) と 期限のみ(TP∞/ST3/RSI∞)")
for ex_lab, ex_kw in (("現行出口 TP5/ST3/RSI50", dict(tp=5.0, stop=3.0, rsith=50.0)), ("期限のみ TP∞/ST3/RSI∞", dict(tp=None, stop=3.0, rsith=None)), ("広め TP10/ST4/RSI∞", dict(tp=10.0, stop=4.0, rsith=None))):
    log(f"  [{ex_lab}]")
    reps = {h: replay(hold=h, **ex_kw) for h in (3, 5, 7, 10)}
    for vt in (0.8, 0.9, 1.0, 1.09, 1.3, None):
        m = ENT_CUR & V12 & ((VT5 <= vt) if vt is not None else np.ones(n, bool))
        row = f"    vt5≤{vt if vt else '∞':<5}"
        for h in (3, 5, 7, 10):
            r = stat(sim(m, *reps[h])); row += f" | H{h:<2} {r['tot']:>+5,.0f}({r['y10']:>+4,.0f}) PF{r['pf']:.2f} n{r['n']:<4}"
        log(row)

# ══════════════════════════════════════════════════════════════
# B 追加買い
# ══════════════════════════════════════════════════════════════
log("\n■ B 追加買い（1×150土台・追加玉は2日目寄りで建て本玉と同時手仕舞い・⚠1銘柄150万MAXの制約は超える＝制約を緩めた場合の数字）")
c1 = CL[:, 0]; o2 = OP[:, 1]; ok2 = np.isfinite(o2) & (o2 > 0)
ret1 = (c1 / E0 - 1) * 100


def addon_run(lab, cond, frac, pnl=PNL_B, exo=EXO_B, mask=GV, base=Bv, size=1_500_000):
    exit_px = E0 * (1 + pnl / 100)
    cond = cond & ok2 & (exo >= 1) & np.isfinite(pnl)
    R = sim(mask, pnl, exo, size=size, add_cond=cond, add_px=o2, add_frac=frac, exit_px=exit_px)
    r = show(lab, R, cap=int(size / 1e4 * (1 + frac)), base=base)
    ii = R.i.to_numpy(); k = cond[ii]
    if k.sum():
        ap = (exit_px[ii][k] / o2[ii][k] - 1) * 100
        log(f"      追加玉 n{int(k.sum())}({k.mean()*100:.0f}%の玉) 件あたり{ap.mean():+.2f}% 勝率{(ap>0).mean()*100:.1f}% | 追加した玉の本玉{R.pnl.to_numpy()[k].mean():+.2f}% vs 追加なし本玉{R.pnl.to_numpy()[~k].mean():+.2f}%")
    return r


show("土台 1×150 乗せなし", sim(GV, PNL_B, EXO_B))
log("  [ナンピン＝負けている玉に足す]")
addon_run("n1 初日引け (-1,0] → 同額150追加（DAY1CUTはそのまま）", (ret1 > -1.0) & (ret1 <= 0.0), 1.0)
addon_run("n2 初日引け (-1,0] → 半額75追加", (ret1 > -1.0) & (ret1 <= 0.0), 0.5)
# DAY1CUTを止めて、-1%以下の玉に足す（本玉はカットしない損益に差し替え）
pnl_mix = np.where(ret1 <= -1.0, PNL_NC, PNL_B); exo_mix = np.where(ret1 <= -1.0, EXO_NC, EXO_B)
addon_run("n3 初日引け ≤-1% → 切らずに同額150追加", (ret1 <= -1.0), 1.0, pnl=pnl_mix, exo=exo_mix)
addon_run("n4 初日引け ≤0 → 切らずに同額150追加（全部ナンピン）", (ret1 <= 0.0), 1.0, pnl=PNL_NC, exo=EXO_NC)
addon_run("n5 初日引け (-3,-1] → 切らずに同額150追加・-3%以下は切る", (ret1 > -3.0) & (ret1 <= -1.0), 1.0, pnl=np.where((ret1 > -3.0) & (ret1 <= -1.0), PNL_NC, PNL_B), exo=np.where((ret1 > -3.0) & (ret1 <= -1.0), EXO_NC, EXO_B))
log("  [勝ち乗せ＝勝っている玉に足す（R14で既知・比較用）]")
addon_run("w1 初日引け >+1% → 同額150追加（1銘柄300万）", (ret1 > 1.0), 1.0)
addon_run("w2 初日引け >+1% → 100追加（1銘柄250万）", (ret1 > 1.0), 2 / 3)
addon_run("w3 初日引け >0 → 同額150追加", (ret1 > 0.0), 1.0)
addon_run("w4 初日引け >+2% → 同額150追加", (ret1 > 2.0), 1.0)
log("  [保有を伸ばした土台での乗せ（3日目寄りに、2日目引けが>+1%なら追加）]")
for hold, tp in ((5, 5.0), (5, None), (7, None)):
    p, e = replay(tp=tp, stop=3.0, hold=hold, rsith=50.0 if tp else None)
    o3 = OP[:, 2]; ok3 = np.isfinite(o3) & (o3 > 0); c2 = CL[:, 1]; cond = (c2 > E0 * 1.01) & ok3 & (e >= 2)
    exit_px = E0 * (1 + p / 100)
    Rb = sim(GV, p, e); rb = stat(Rb)
    R = sim(GV, p, e, add_cond=cond, add_px=o3, add_frac=1.0, exit_px=exit_px)
    show(f"h{hold} TP{tp if tp else '∞'}: 乗せなし", Rb)
    show(f"h{hold} TP{tp if tp else '∞'}: 3日目寄りに同額乗せ(2日目引け>+1%)", R, cap=300, base=rb)

# ══════════════════════════════════════════════════════════════
# C 確証バイアス監査 ＝ ウォークフォワード再選定
# ══════════════════════════════════════════════════════════════
log("\n■ C 確証バイアス監査＝ウォークフォワード再選定（入口48×出口108＝5,184セル・1×150・12日足あり候補）")
log("  入口: RSI≤{40,45,50} × 乖離≤{-1,-1.5,-2.5,-4} × vt5≤{0.9,1.09,1.3,∞}  出口: 保有{3,5,7} × TP{3,5,∞} × 損切{2,3,5} × RSI出口{50,∞} × DAY1CUT{あり,なし}")
EXITS = [(h, tp, st, rs, dc) for h in (3, 5, 7) for tp in (3.0, 5.0, None) for st in (2.0, 3.0, 5.0) for rs in (50.0, None) for dc in (1.0, None)]
ENTS = [(rsi, dev, vt) for rsi in (40, 45, 50) for dev in (-1.0, -1.5, -2.5, -4.0) for vt in (0.9, 1.09, 1.3, None)]
YEARS = np.arange(2001, 2027)
YEN = np.zeros((len(ENTS), len(EXITS), len(YEARS))); GP = YEN.copy(); GL = YEN.copy(); NN = YEN.copy()
tC = time.time()
for j, (h, tp, st, rs, dc) in enumerate(EXITS):
    p, e = replay(tp=tp, stop=st, hold=h, rsith=rs, d1cut=dc)
    for i_, (rsi_t, dev_t, vt) in enumerate(ENTS):
        m = (RSI <= rsi_t) & (DEV <= dev_t) & V12 & ((VT5 <= vt) if vt is not None else np.ones(n, bool))
        R = sim(m, p, e)
        g = R.groupby("y")
        yy = g.yen.sum(); YEN[i_, j, yy.index.to_numpy() - 2001] = yy.to_numpy()
        gp = R[R.yen > 0].groupby("y").yen.sum(); GP[i_, j, gp.index.to_numpy() - 2001] = gp.to_numpy()
        gl = -R[R.yen <= 0].groupby("y").yen.sum(); GL[i_, j, gl.index.to_numpy() - 2001] = gl.to_numpy()
        nn_ = g.size(); NN[i_, j, nn_.index.to_numpy() - 2001] = nn_.to_numpy()
    if j % 20 == 0: log(f"    出口{j+1}/{len(EXITS)} {time.time()-tC:.0f}s")
pickle.dump(dict(ENTS=ENTS, EXITS=EXITS, YEN=YEN, GP=GP, GL=GL, NN=NN), open("_bt_gokujo_round15_wf_cells.pkl", "wb"))
ci = ENTS.index((45, -1.5, 1.09)); cj = EXITS.index((3, 5.0, 3.0, 50.0, 1.0))
F = YEN.reshape(-1, len(YEARS)); FGP = GP.reshape(-1, len(YEARS)); FGL = GL.reshape(-1, len(YEARS)); FN = NN.reshape(-1, len(YEARS)); cur_k = ci * len(EXITS) + cj
labels = [f"RSI{a}/乖離{b}/vt5{c if c else '∞'} H{h}/TP{tp if tp else '∞'}/ST{st}/RSI{int(rs) if rs else '∞'}/DC{'有' if dc else '無'}" for (a, b, c) in ENTS for (h, tp, st, rs, dc) in EXITS]


def yrs(a, b): return (YEARS >= a) & (YEARS <= b)


def pf_of(k, msk): gl = FGL[k][msk].sum(); return FGP[k][msk].sum() / gl if gl > 0 else 9.99


for a, b in ((2001, 2016), (2017, 2026), (2011, 2026), (2001, 2026)):
    m = yrs(a, b); tots = F[:, m].sum(1) / 1e4; rank = int((tots > tots[cur_k]).sum()) + 1
    pfs = np.array([pf_of(k, m) for k in range(len(F))]); rpf = int((pfs > pfs[cur_k]).sum()) + 1
    log(f"  現行セルの順位 {a}-{b}: 合計 {rank}/{len(F)}（{tots[cur_k]:+,.0f}万・最良{tots.max():+,.0f}万={labels[int(tots.argmax())]}）・PF {rpf}/{len(F)}（{pfs[cur_k]:.2f}・最良{pfs.max():.2f}）")

log("  --- ウォークフォワード（毎年、その年より前のデータだけで最良セルを選び、翌年を運用） ---")


def walk(sel, y_from=2011, win=None, lab=""):
    oos = []; chosen = []
    for Y in range(y_from, 2027):
        lo = 2001 if win is None else Y - win
        m = yrs(lo, Y - 1); k = sel(m); oos.append(F[k, Y - 2001]); chosen.append(k)
    oos = np.array(oos) / 1e4; ch = pd.Series(chosen)
    same = (ch == cur_k).mean() * 100; nun = ch.nunique()
    fixed = F[cur_k, yrs(y_from, 2026)] / 1e4
    log(f"  {lab:<44} WF合計{oos.sum():>+6,.0f}万 勝年{int((oos>0).sum())}/{len(oos)} 最悪年{oos.min():+.0f} | 固定(現行){fixed.sum():>+6,.0f}万 | 選ばれたセル{nun}種・現行と同一{same:.0f}% | 年別 " + " ".join(f"{Y}:{v:+.0f}" for Y, v in zip(range(y_from, 2027), oos)))
    log(f"      選ばれたセル(最頻3): " + " / ".join(f"{labels[k]}×{c}" for k, c in ch.value_counts().head(3).items()))
    return oos


def sel_tot(m): return int((F[:, m].sum(1)).argmax())
def sel_pf(m):
    pfs = np.array([pf_of(k, m) for k in range(len(F))]); ok = FN[:, m].sum(1) >= 100; pfs[~ok] = -1; return int(pfs.argmax())
def sel_totdd(m):
    # 合計 / 最悪年 の折衷: 合計 − 2×|最悪年|
    Fy = F[:, m]; sc = Fy.sum(1) - 2 * np.abs(np.minimum(Fy.min(1), 0)); return int(sc.argmax())


walk(sel_tot, lab="拡張窓(2001〜前年)・合計最大")
walk(sel_pf, lab="拡張窓・PF最大(n≥100)")
walk(sel_totdd, lab="拡張窓・合計−2×最悪年")
walk(sel_tot, win=10, lab="直近10年窓・合計最大（＝俺たちの『10年BT』流儀）")
walk(sel_pf, win=10, lab="直近10年窓・PF最大")
walk(sel_tot, win=5, lab="直近5年窓・合計最大")
m1126 = yrs(2011, 2026); tots = F[:, m1126].sum(1) / 1e4
log(f"  事後最良(2011-26を見て選んだ上限) {tots.max():+,.0f}万 = {labels[int(tots.argmax())]} / 現行 {tots[cur_k]:+,.0f}万（上位{(tots>tots[cur_k]).mean()*100:.1f}%）/ 全セル中央値 {np.median(tots):+,.0f}万・PF>1のセル {np.mean([pf_of(k, m1126)>1 for k in range(len(F))])*100:.0f}%")
log("  --- 部品ごとの限界寄与（2001-16 / 2017-26 別に、その部品だけ動かした時の中央値）: 現行の各値が両期間で頂点かどうか ---")
EA = np.array(ENTS, dtype=object); EX = np.array(EXITS, dtype=object)
for nm, pos, vals, is_ent in (("RSI", 0, (40, 45, 50), True), ("乖離", 1, (-1.0, -1.5, -2.5, -4.0), True), ("vt5", 2, (0.9, 1.09, 1.3, None), True), ("保有", 0, (3, 5, 7), False), ("TP", 1, (3.0, 5.0, None), False), ("損切", 2, (2.0, 3.0, 5.0), False), ("RSI出口", 3, (50.0, None), False), ("DAY1CUT", 4, (1.0, None), False)):
    row = f"    {nm:<8}"
    for v in vals:
        if is_ent: ks = [i_ * len(EXITS) + j for i_ in range(len(ENTS)) if ENTS[i_][pos] == v for j in range(len(EXITS))]
        else: ks = [i_ * len(EXITS) + j for i_ in range(len(ENTS)) for j in range(len(EXITS)) if EXITS[j][pos] == v]
        ks = np.array(ks); a1 = np.median(F[ks][:, yrs(2001, 2016)].sum(1)) / 1e4; a2 = np.median(F[ks][:, yrs(2017, 2026)].sum(1)) / 1e4
        row += f" | {v if v is not None else '∞'}: 01-16 {a1:>+5,.0f} / 17-26 {a2:>+5,.0f}"
    log(row)

# ══════════════════════════════════════════════════════════════
# D 新データ（10年・2016-09〜）
# ══════════════════════════════════════════════════════════════
log("\n■ D 新データを極上に当てる（10年・1×150・DAY1CUT・判定=16-20/21-26両方改善＋PF↑＋噪音床99%超え）")
V10 = (DATEv >= pd.Timestamp("2016-09-05"))
G10 = G & V10 & V12
rng = np.random.default_rng(0)


def stat10(R):
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    return dict(n=len(R), win=(R.pnl > 0).mean() * 100 if len(R) else 0, pf=gp / gl if gl else 9.99, tot=R.yen.sum() / 1e4, h1=R[R.y <= 2020].yen.sum() / 1e4, h2=R[R.y >= 2021].yen.sum() / 1e4, wy=int((yy > 0).sum()), ny=len(yy), worst=yy.min() / 1e4 if len(yy) else 0, dd=dd_of(R))


def line10(lab, mask, base=None, noise=None, mult=None):
    R = sim(mask, PNL_B, EXO_B)
    if mult is not None:
        ii = R.i.to_numpy(); R = R.copy(); R["yen"] = R.yen * mult[ii]
    r = stat10(R); j = ""
    if base is not None:
        j = " ★" if (r["h1"] > base["h1"] and r["h2"] > base["h2"] and r["pf"] >= base["pf"] and r["worst"] >= base["worst"] - 1 and (noise is None or r["tot"] > noise)) else ""
    log(f"  {lab:<44} 玉{r['n']:>4} 勝率{r['win']:>5.1f}% PF{r['pf']:>4.2f} 10年{r['tot']:>+6,.0f}万 DD{r['dd']:>+4,.0f} | 16-20{r['h1']:>+5,.0f} 21-26{r['h2']:>+5,.0f} | 勝年{r['wy']}/{r['ny']} 最悪年{r['worst']:>+4,.0f}" + j + (f"  (噪音床99%={noise:+,.0f})" if noise is not None else ""))
    return r


def noise_floor(base_mask, frac, seeds=30):
    idx = np.where(base_mask)[0]; k = int(len(idx) * frac); tots = []
    if k <= 0: return None
    for s in range(seeds):
        drop = rng.choice(idx, size=k, replace=False); m = base_mask.copy(); m[drop] = False; tots.append(stat10(sim(m, PNL_B, EXO_B))["tot"])
    return float(np.quantile(tots, 0.99))


def pm(lab, m):
    m = m & G10
    log(f"    {lab:<30} n={int(m.sum()):>5} 件あたり{np.nanmean(PNL_B[m]):>+6.2f}% 勝率{np.mean(PNL_B[m]>0)*100:>5.1f}%")


def excl(lab, drop, base):
    m = G10 & ~drop; frac = 1 - m.sum() / G10.sum()
    nf = noise_floor(G10, frac) if frac > 0.01 else None
    return line10(f"{lab}（除外{frac*100:.0f}%）", m, base, nf)


B10 = line10("土台（2016-09〜・1×150・DAY1CUT）", G10)

# D1 空売り残高報告（J-Quants /markets/short_selling_positions・ファンド別報告→銘柄合計を前日as-ofで）
log("  [D1 空売り残高報告＝大口空売りの合計比率（DiscDate<エントリー日・各ファンド最新報告を合算）]")
try:
    SS = pd.read_pickle("_short_sale_10y.pkl")
    SS = SS[["DiscDate", "Code", "SSName", "ShrtPosToSO"]].copy(); SS["tk"] = SS.Code.astype(str).str[:4] + ".T"; SS["DiscDate"] = pd.to_datetime(SS.DiscDate)
    SS = SS.sort_values("DiscDate")
    SHORT = np.full(n, np.nan); NFUND = np.full(n, np.nan)
    need = {}
    for i in np.where(G10)[0]: need.setdefault(TICKv[i], []).append(i)
    for tk, g in SS.groupby("tk"):
        rows = need.get(tk)
        if not rows: continue
        # 日次にファンド別最新値を forward-fill → 合計
        piv = g.pivot_table(index="DiscDate", columns="SSName", values="ShrtPosToSO", aggfunc="last").sort_index().ffill()
        # 90日以上報告が無いファンドは消えたとみなす（最終報告が0.5%割れで来るので通常は不要だが保険）
        tot_ = piv.sum(1); cnt_ = (piv > 0.005).sum(1); idx = tot_.index.to_numpy()
        for i in rows:
            p_ = np.searchsorted(idx, np.datetime64(DATEv[i]), side="left") - 1
            if p_ >= 0 and (DATEv[i] - pd.Timestamp(idx[p_])).days <= 120: SHORT[i] = tot_.iloc[p_]; NFUND[i] = cnt_.iloc[p_]
            else: SHORT[i] = 0.0; NFUND[i] = 0
    cov = np.isfinite(SHORT[G10]).mean(); has = (SHORT[G10] > 0).mean()
    log(f"    結合 {cov*100:.0f}%・報告あり {has*100:.0f}% ・合計比率の中央値(報告あり) {np.nanmedian(SHORT[G10][SHORT[G10]>0])*100:.2f}%")
    q = np.nanquantile(SHORT[G10][SHORT[G10] > 0], [1 / 3, 2 / 3])
    pm("報告なし", SHORT == 0); pm("下位1/3", (SHORT > 0) & (SHORT <= q[0])); pm("中位", (SHORT > q[0]) & (SHORT <= q[1])); pm("上位1/3(大口空売り多)", SHORT > q[1])
    excl("報告なしを除外", SHORT == 0, B10); excl("上位1/3を除外", SHORT > q[1], B10); excl("下位1/3+なしを除外", SHORT <= q[0], B10)
    excl("合計≥2%を除外", SHORT >= 0.02, B10); excl("合計≥1%だけ撃つ(他を除外)", ~(SHORT >= 0.01), B10)
    excl("ファンド数≥3を除外", NFUND >= 3, B10)
except Exception as ex:
    log(f"    D1 skip: {ex!r}")

# D2 日経VI（前々日・オプションBaseVol代替 vi30）
log("  [D2 日経VI（前々日）＝サイズ傾斜（1×150の上限があるので下げる側だけが実装可能）]")
try:
    VI = pd.read_pickle("_nk225_iv_daily.pkl"); VI["Date"] = pd.to_datetime(VI.Date); vi = VI.set_index("Date").vi30.sort_index()
    vidx = vi.index.to_numpy(); viv = vi.to_numpy()
    VIL = np.full(n, np.nan)
    for i in np.where(G10)[0]:
        p_ = np.searchsorted(vidx, np.datetime64(DATEv[i] - pd.Timedelta(days=2)), side="right") - 1
        if p_ >= 0: VIL[i] = viv[p_]
    for lo_, hi_ in ((0, 15), (15, 20), (20, 25), (25, 99)): pm(f"VI {lo_}-{hi_}", (VIL > lo_) & (VIL <= hi_))
    line10("VI≤15 → 半額75万", G10, B10, mult=np.where(VIL <= 15, 0.5, 1.0))
    line10("VI≤15 → 見送り", G10 & ~(VIL <= 15), B10, noise_floor(G10, np.mean(VIL[G10] <= 15)))
    line10("参考 VI≥20 → 1.3倍（上限超・実装不可）", G10, B10, mult=np.where(VIL >= 20, 1.3, np.where(VIL <= 15, 0.7, 1.0)))
    line10("VI>30 → 見送り", G10 & ~(VIL > 30), B10, noise_floor(G10, np.mean(VIL[G10] > 30)))
except Exception as ex:
    log(f"    D2 skip: {ex!r}")

# D3 投資部門別（海外投資家の週次買越し・PubDate<エントリー日・4週計）
log("  [D3 投資部門別売買状況＝海外投資家の直近4週の買越し（TokyoNagoya・PubDate<エントリー日）]")
try:
    IT = pd.read_pickle("_investor_types.pkl"); IT = IT[IT.Section == "TokyoNagoya"].copy(); IT["PubDate"] = pd.to_datetime(IT.PubDate); IT = IT.sort_values("PubDate")
    IT["f4"] = IT.FrgnBal.rolling(4).sum(); IT["f1"] = IT.FrgnBal; IT["i4"] = IT.IndBal.rolling(4).sum()
    pidx = IT.PubDate.to_numpy(); F4 = np.full(n, np.nan); F1 = np.full(n, np.nan); I4 = np.full(n, np.nan)
    for i in np.where(G10)[0]:
        p_ = np.searchsorted(pidx, np.datetime64(DATEv[i]), side="left") - 1
        if p_ >= 0: F4[i] = IT.f4.iat[p_]; F1[i] = IT.f1.iat[p_]; I4[i] = IT.i4.iat[p_]
    q = np.nanquantile(F4[G10], [1 / 3, 2 / 3])
    pm("海外4週 下位1/3(売り越し)", F4 <= q[0]); pm("中位", (F4 > q[0]) & (F4 <= q[1])); pm("上位1/3(買い越し)", F4 > q[1])
    excl("海外4週 上位1/3(買い越し週)を除外", F4 > q[1], B10); excl("海外4週 下位1/3(売り越し週)を除外", F4 <= q[0], B10)
    q1 = np.nanquantile(F1[G10], [1 / 3, 2 / 3]); excl("海外1週 上位1/3を除外", F1 > q1[1], B10); excl("海外1週 下位1/3を除外", F1 <= q1[0], B10)
    qi = np.nanquantile(I4[G10], [1 / 3, 2 / 3]); excl("個人4週 上位1/3(個人買い越し)を除外", I4 > qi[1], B10); excl("個人4週 下位1/3を除外", I4 <= qi[0], B10)
except Exception as ex:
    log(f"    D3 skip: {ex!r}")

# D4 週次信用残（貸借倍率・買残の前週比）as-of
log("  [D4 週次信用残（J-Quants）＝貸借倍率・買残前週比（公表週の翌週から有効）]")
try:
    MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
    RATIO = np.full(n, np.nan); LCHG = np.full(n, np.nan)
    need = {}
    for i in np.where(G10)[0]: need.setdefault(TICKv[i][:4], []).append(i)
    for code, rows in need.items():
        df = MG.get(code)
        if df is None or len(df) < 3: continue
        df = df.sort_index(); idx = pd.to_datetime(df.index).to_numpy(); L = df.LongVol.to_numpy(float); S_ = df.ShrtVol.to_numpy(float)
        for i in rows:
            p_ = np.searchsorted(idx, np.datetime64(DATEv[i] - pd.Timedelta(days=4)), side="right") - 1   # 金曜基準→翌週火曜公表を想定して4日ラグ
            if p_ >= 1:
                RATIO[i] = L[p_] / S_[p_] if S_[p_] > 0 else np.nan; LCHG[i] = L[p_] / L[p_ - 1] - 1 if L[p_ - 1] > 0 else np.nan
    log(f"    結合 {np.isfinite(RATIO[G10]).mean()*100:.0f}%")
    for lo_, hi_ in ((0, 1), (1, 3), (3, 6), (6, 1e9)): pm(f"貸借倍率 {lo_}-{hi_ if hi_<1e9 else '∞'}", (RATIO > lo_) & (RATIO <= hi_))
    excl("貸借倍率>6を除外", RATIO > 6, B10); excl("貸借倍率>10を除外", RATIO > 10, B10); excl("貸借倍率<1(売り長)を除外", RATIO < 1, B10)
    q = np.nanquantile(LCHG[G10], [1 / 3, 2 / 3]); pm("買残前週比 下位1/3(減)", LCHG <= q[0]); pm("上位1/3(増)", LCHG > q[1])
    excl("買残前週比 上位1/3(急増)を除外", LCHG > q[1], B10); excl("買残前週比 下位1/3を除外", LCHG <= q[0], B10)
except Exception as ex:
    log(f"    D4 skip: {ex!r}")

log(f"\n[done] {time.time()-t0:.0f}s")

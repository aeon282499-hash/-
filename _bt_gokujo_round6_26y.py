# -*- coding: utf-8 -*-
"""_bt_gokujo_round6_26y.py（round5の土台をコピー・下部が本体）
元: — 極上ラウンド5: 暦(10月除外/曜日)・2日連続候補・同業種クラスタ（新ルール土台）: 玉数を増やす(絞り緩和/枠数/スコア)と何が起きるか・新ルール土台: DAY1CUT+勝ち乗せ土台で並び順(スコア+vt5小)を再計測: DAY1CUT土台で 2日目勝ち乗せ / VIX30の2枠目 / 並び順 を26年検証（2026-09-14）。
 A 市場ヘッジ（銘柄買い＋市場を同額/β倍ショート・保有窓=シグナル日引け→手仕舞い日引け）
 B 確認待ちの入り方（t+1の足を見てからt+2寄りで建てる）
 C 損切り位置（一律-3% → シグナル日安値ベース / -2% / -5%）
 D 長期RSI（週足RSI14 / RSI7 / RSI28）
 E 株価帯（100株丸め込み）
 F 業種cap 1/2/3/なし
エンジン=_bt_kiwami_score_axes_26y_0905.py と同一（_bt_buy_20y_wide.pkl・3枠×100万・1日5件・業種cap3）。本番無変更。
実行: python -X utf8 _bt_kiwami_hedge_entry_stop_26y.py > _log_kiwami_hedge_entry_stop_26y.txt
"""
from __future__ import annotations

import json, pickle, time
import numpy as np, pandas as pd

MAX_SIG, SECTOR_CAP, SLOTS, SIZE = 5, 3, 3, 1_000_000
ERAS = ((2001, 2008, "01-08"), (2009, 2016, "09-16"), (2017, 2021, "17-21"), (2022, 2026, "22-26"))
P = pickle.load(open("_bt_buy_20y_wide.pkl", "rb")); C = P["C"]
OP, HI, LO, CL, RS = (P[k] for k in ("OP", "HI", "LO", "CL", "RS"))
E = C.E.to_numpy(); n = len(C)
RSI, DEV, RR, VR, TOV, ATR, PRICE, NOFILL, DEV75 = (C[c].to_numpy() for c in ("rsi", "dev", "rr", "vr", "tov", "atr", "price", "nofill", "dev75"))


def replay(tp=5.0, stop=3.0, hold=3, rsith=50.0):
    valid = np.isfinite(E) & np.isfinite(CL[:, hold - 1]); pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = E * (1 - stop / 100); tl = E * (1 + tp / 100)
    for k in range(hold):
        live = ~done
        if k > 0:
            op = OP[:, k]; g = live & np.isfinite(op) & (op > 0) & ((op <= sl) | (op >= tl))
            pnl[g] = (op[g] - E[g]) / E[g] * 100; exo[g] = k; done |= g; live = ~done
        s = live & (LO[:, k] <= sl); pnl[s] = -stop; exo[s] = k; done |= s; live = ~done
        t = live & (HI[:, k] >= tl); pnl[t] = tp; exo[t] = k; done |= t; live = ~done
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]); r = live & (rc | (k == hold - 1))
        pnl[r] = (CL[r, k] - E[r]) / E[r] * 100; exo[r] = k; done |= r
    return pnl, exo


PNL, EXO = replay()
BASE = ((RSI <= 45) & (DEV <= -1.5) & ((RR >= 1.5) | (VR >= 2.0)) & (TOV >= 2e9) & (ATR <= 3.0) & (PRICE <= 10000) & ~NOFILL.astype(bool) & np.isfinite(PNL))
idx_all = np.where(BASE)[0]
C = C.iloc[idx_all].reset_index(drop=True); PNL = PNL[idx_all]; EXO = EXO[idx_all]; E = E[idx_all]; n = len(C)
OP, HI, LO, CL, RS = (a[idx_all] for a in (OP, HI, LO, CL, RS))
print(f"[pool] 候補 {n:,}件", flush=True)

SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))
TICK = C.ticker.to_numpy(); YEAR = C.year.to_numpy(); SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
days = sorted(C.entry.unique()); gdi = {d: i for i, d in enumerate(days)}; DAY = C.entry.map(gdi).to_numpy()
day_groups = [np.where(DAY == d)[0] for d in range(len(days))]


def run(order_key):
    ou = {}; os_ = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]:
            del ou[tk]; del os_[tk]
        sc = {}
        for s in os_.values(): sc[s] = sc.get(s, 0) + 1
        cnt = 0; idxs = day_groups[d]; idxs = idxs[np.argsort(-order_key[idxs], kind="stable")]
        for i in idxs:
            if cnt >= MAX_SIG: break
            if not np.isfinite(PNL[i]): continue
            if TICK[i] in ou: continue
            s = SEC[i]
            if sc.get(s, 0) >= SECTOR_CAP: continue
            cnt += 1; ex = min(d + int(EXO[i]), len(days) - 1); ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1
            picks.append((d, ex, i))
    live = []; rows = []
    for d, ex, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= SLOTS: continue
        sh = int(SIZE / E[i] / 100) * 100
        if sh <= 0: continue
        live.append(ex); rows.append((YEAR[i], PNL[i], PNL[i] / 100 * sh * E[i]))
    return pd.DataFrame(rows, columns=["y", "pnl", "yen"])


def line(lab, R):
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    row = f"  {lab:<30}{len(R):>6}{(R.pnl>0).mean()*100:>6.1f}%{gp/gl:>6.2f}{R.yen.sum()/1e4:>+9,.0f}万"
    for a, b, nm in ERAS:
        s = R[(R.y >= a) & (R.y <= b)]; g1 = s.yen[s.yen > 0].sum(); g2 = -s.yen[s.yen <= 0].sum()
        row += f"{s.yen.sum()/1e4:>+8,.0f}万/{g1/g2 if g2 else 9.99:4.2f}"
    row += f"{int((yy>0).sum()):>4}/26{yy.min()/1e4:>+7,.0f}万{(R.yen.sum()-R.nlargest(20,'yen').yen.sum())/1e4:>+8,.0f}万"
    print(row, flush=True)


def tot(R, a=2001, b=2026):
    s = R[(R.y >= a) & (R.y <= b)]; return s.yen.sum() / 1e4


def replay_gen(Ev, k0, stop_price, tp=5.0, hold=3, rsith=50.0):
    """k0=建て日(0=t+1寄り,1=t+2寄り)。stop_price=損切価格配列。悲観順序=寄りギャップ→損切→利確→RSI/期限。"""
    valid = np.isfinite(Ev) & (Ev > 0) & np.isfinite(CL[:, k0 + hold - 1]) & np.isfinite(stop_price); pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = stop_price; tl = Ev * (1 + tp / 100)
    for j in range(hold):
        k = k0 + j; live = ~done
        if j > 0:
            op = OP[:, k]; g = live & np.isfinite(op) & (op > 0) & ((op <= sl) | (op >= tl))
            pnl[g] = (op[g] - Ev[g]) / Ev[g] * 100; exo[g] = k; done |= g; live = ~done
        s = live & (LO[:, k] <= sl); pnl[s] = (sl[s] - Ev[s]) / Ev[s] * 100; exo[s] = k; done |= s; live = ~done
        t_ = live & (HI[:, k] >= tl); pnl[t_] = tp; exo[t_] = k; done |= t_; live = ~done
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]); r_ = live & (rc | (j == hold - 1))
        pnl[r_] = (CL[r_, k] - Ev[r_]) / Ev[r_] * 100; exo[r_] = k; done |= r_
    return pnl, exo





# ══════════════════════════════════════════════════════════════════════
# 極上ラウンド2（2026-09-14）: DAY1CUT(初日引け-1%→翌寄り処分)を新土台に
#  A 2日目勝ち乗せ（初日終値>建値 or >建値+1% → 2日目寄りに同額/半額追加・本玉と同時手仕舞い）
#  B VIX≥30の日だけ2枠目を開ける（別枠1×150万・本玉と同じ銘柄は除外）
#  C 並び順 スコア+vt5小(w0.3) の重ね
#  D 噪音床（ランダム除外30シード）
# ══════════════════════════════════════════════════════════════════════
t0 = time.time()
E0 = E.copy(); PNL0 = PNL.copy(); EXO0 = EXO.copy(); ent = pd.to_datetime(C.entry); YEARv = C.year.to_numpy()
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
VT5 = np.full(n, np.nan); AV20 = np.full(n, np.nan); need = {}
for i in range(n): need.setdefault(C.ticker.iat[i], []).append((C.entry.iat[i], i))
for tk, rows in need.items():
    df = ALL.get(tk)
    if df is None: continue
    df = df.dropna(subset=["Close"]); df = df[df.Close > 0]; v = df["Volume"].astype(float).to_numpy(); pos = {d.strftime("%Y-%m-%d"): k for k, d in enumerate(df.index)}
    for entd, i in rows:
        k = pos.get(entd)
        if k is None or k < 27: continue
        t = k - 1; v20 = v[t - 24:t - 4]
        if v20.mean() > 0: VT5[i] = v[t - 4:t + 1].mean() / v20.mean(); AV20[i] = v20.mean()
del ALL
VIX = pd.read_pickle("_vix_yf.pkl")
vix_lvl = np.array([VIX[:d - pd.Timedelta(days=1)].iloc[-1] if len(VIX[:d - pd.Timedelta(days=1)]) else np.nan for d in ent])
G = VT5 <= 1.09; SLOTS = 1; SIZE = 1_500_000
rsi_s = 1 / (1 + ((C.rsi.to_numpy() - 38) / 8) ** 2); dev_s = 1 / (1 + ((C.dev.to_numpy() + 3) / 2) ** 2); turn_s = np.log10(np.maximum(C.tov.to_numpy(), 1) / 1e9 + 1) / 3
SCORE = rsi_s * 0.3 + dev_s * 0.3 + turn_s * 0.4
print(f"[feat] {time.time()-t0:.0f}s", flush=True)

# DAY1CUT の損益: 初日で決着していない玉で初日終値≤建値×0.99 → 2日目寄りで処分
def day1cut(pnl_in, exo_in, x=1.0):
    pnl = pnl_in.copy(); exo = exo_in.copy(); c1 = CL[:, 0]; o2 = OP[:, 1]
    m = np.isfinite(pnl) & (exo_in >= 1) & (c1 <= E0 * (1 - x / 100)) & np.isfinite(o2) & (o2 > 0)
    pnl[m] = (o2[m] - E0[m]) / E0[m] * 100; exo[m] = 1
    return pnl, exo, m
PNL1, EXO1, CUTM = day1cut(PNL0, EXO0)


def run_idx(mask, key, pnl, exo, slots=1, size=1_500_000, max_sig=5, cap=3):
    """run() と同じ選定で、玉の index も返す。"""
    TICK = C.ticker.to_numpy(); SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
    ou = {}; os_ = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]: del ou[tk]; del os_[tk]
        sc = {}
        for s in os_.values(): sc[s] = sc.get(s, 0) + 1
        cnt = 0; idxs = day_groups[d]; idxs = idxs[mask[idxs]]; idxs = idxs[np.argsort(-key[idxs], kind="stable")]
        for i in idxs:
            if cnt >= max_sig: break
            if not np.isfinite(pnl[i]) or TICK[i] in ou: continue
            s = SEC[i]
            if sc.get(s, 0) >= cap: continue
            cnt += 1; ex = min(d + int(exo[i]), len(days) - 1); ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1; picks.append((d, ex, i))
    live = []; rows = []
    for d, ex, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= slots: continue
        sh = int(size / E0[i] / 100) * 100
        if sh <= 0: continue
        live.append(ex); rows.append((i, YEARv[i], pnl[i], pnl[i] / 100 * sh * E0[i], sh))
    return pd.DataFrame(rows, columns=["i", "y", "pnl", "yen", "sh"])


def tot(R, a=2001, b=2026): s = R[(R.y >= a) & (R.y <= b)]; return s.yen.sum() / 1e4
def stat(R):
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    return dict(n=len(R), win=(R.pnl > 0).mean() * 100, pf=gp / gl if gl else 9.99, tot=tot(R), a=tot(R, 2001, 2013), b=tot(R, 2014, 2026), e3=tot(R, 2017, 2021), e4=tot(R, 2022, 2026), wy=int((yy > 0).sum()), worst=yy.min() / 1e4, y10=tot(R, 2017, 2026), pf10=(lambda s: s.yen[s.yen > 0].sum() / max(-s.yen[s.yen <= 0].sum(), 1))(R[R.y >= 2017]), win10=(R[R.y >= 2017].pnl > 0).mean() * 100, worst10=(R[R.y >= 2017].groupby("y").yen.sum().min() / 1e4))
def judge(r, b): return " ★" if (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["worst"] >= b["worst"] - 1) else ""
def line(lab, R, base=None):
    r = stat(R); print(f"  {lab:<36}{r['n']:>5}{r['win']:>6.1f}%{r['pf']:>6.2f}{r['tot']:>+8,.0f}万 | 01-13{r['a']:>+6,.0f} 14-26{r['b']:>+6,.0f} 17-21{r['e3']:>+6,.0f} 22-26{r['e4']:>+6,.0f} | 10年{r['y10']:>+6,.0f} PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% 最悪{r['worst10']:+.0f} | 勝年{r['wy']}/26 最悪{r['worst']:>+4,.0f}" + (judge(r, base) if base else ""), flush=True); return r



hdr = f"  {'構成':<36}{'n':>5}{'勝率':>7}{'PF':>6}{'26年':>9} | {'4分割(万)':<32} | {'10年/PF/勝率/最悪年':<30} | 勝年 最悪年"
def with_addon(R, frac=1.0, th=1.0, size=1_500_000):
    exit_px = E0 * (1 + PNL1 / 100); o2 = OP[:, 1]
    add_ok = (CL[:, 0] > E0 * (1 + th / 100)) & (EXO1 >= 1) & np.isfinite(o2) & (o2 > 0)
    add_pct = np.where(add_ok, (exit_px / o2 - 1) * 100, 0.0)
    Rb = R.copy(); ii = Rb.i.to_numpy(); ok_i = add_ok[ii]; add_sh = np.where(ok_i, (size * frac / o2[ii] // 100 * 100), 0).astype(int)
    Rb["yen"] = Rb.yen + np.where(ok_i, add_pct[ii] / 100 * add_sh * o2[ii], 0.0); Rb["pnl"] = Rb.pnl + np.where(ok_i, add_pct[ii] * frac, 0.0)
    return Rb
def rk(x, low_good=True):
    r = np.zeros(n)
    for g in day_groups:
        gg = g[G[g] & np.isfinite(x[g])]
        if len(gg) > 1: r[gg] = pd.Series(x[gg]).rank(ascending=low_good, pct=True).to_numpy()
    return r

def show(lab, mask, slots, size, key=None):
    R = with_addon(run_idx(mask, SCORE if key is None else key, PNL1, EXO1, slots=slots, size=size), size=size); r = stat(R); cap = slots * size
    dly = R.assign(date=pd.to_datetime(C.entry.to_numpy()[R.i.to_numpy()])).groupby("date").yen.sum().sort_index(); eq = dly.cumsum(); dd = (eq - eq.cummax()).min() / 1e4
    print(f"  {lab:<30} 玉{r['n']:>5}({r['n']/26:>3.0f}/年) 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 26年{r['tot']:>+6,.0f}万 資金{cap//10000:>4}万 年利{r['tot']/cap*1e4/26*100:>+5.1f}% DD{dd:>+5,.0f} | 4分割 {r['a']:>+5,.0f}/{r['b']:>+5,.0f}/{r['e3']:>+5,.0f}/{r['e4']:>+5,.0f} | 10年{r['y10']:>+5,.0f} PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% 最悪年{r['worst10']:+.0f} | 勝年{r['wy']}/26", flush=True)


# ════════════════════════════════════════════════════════
# 極上ラウンド6（2026-09-14 本人「極上をさらに良くする未検証をさがして」）
#  未検証3軸: A 決算日近接(保有中に決算跨ぎ/決算直後) 10年  B 週次信用残の売り側(貸借倍率/売残回転/売残増減) 10年  C ATRでサイズ正規化 26年
# ════════════════════════════════════════════════════════
rng = np.random.default_rng(0)
entd = pd.to_datetime(C.entry); dayidx = {d: i for i, d in enumerate(days)}; DI = np.array([dayidx[d] for d in C.entry])
def exit_day(i): return pd.Timestamp(days[min(DI[i] + 3, len(days) - 1)])
V10 = (entd >= pd.Timestamp("2016-08-01")).to_numpy()
def stat10(R):
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    h1 = R[(R.y <= 2020)].yen.sum() / 1e4; h2 = R[(R.y >= 2021)].yen.sum() / 1e4
    return dict(n=len(R), win=(R.pnl > 0).mean() * 100, pf=gp / gl if gl else 9.99, tot=R.yen.sum() / 1e4, h1=h1, h2=h2, wy=int((yy > 0).sum()), ny=len(yy), worst=yy.min() / 1e4)
def run10(mask): return with_addon(run_idx(mask & V10, SCORE, PNL1, EXO1))
def line10(lab, mask, base=None, noise=None):
    R = run10(mask); r = stat10(R)
    j = ""
    if base is not None:
        j = " ★" if (r["h1"] > base["h1"] and r["h2"] > base["h2"] and r["pf"] >= base["pf"] and r["worst"] >= base["worst"] - 1 and (noise is None or r["tot"] > noise)) else ""
    print(f"  {lab:<34} 玉{r['n']:>4} 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 10年{r['tot']:>+6,.0f}万 | 16-20{r['h1']:>+5,.0f} 21-26{r['h2']:>+5,.0f} | 勝年{r['wy']}/{r['ny']} 最悪年{r['worst']:>+4,.0f}" + j, flush=True); return r
def noise_floor(base_mask, frac, seeds=30):
    idx = np.where(base_mask & V10)[0]; k = int(len(idx) * frac); tots = []
    for s in range(seeds):
        drop = rng.choice(idx, size=k, replace=False); m = base_mask.copy(); m[drop] = False; tots.append(stat10(run10(m))["tot"])
    return float(np.quantile(tots, 0.99)), float(np.std(tots))
def pm(lab, m): print(f"    {lab:<28} n={int(np.sum(m)):>5} 件あたり{np.nanmean(PNL1[m]):>+6.2f}% 勝率{np.mean(PNL1[m]>0)*100:>5.1f}%")

GV = G & V10
print("\n■ 土台（極上 新ルール・2016-08〜2026・1×150万）", flush=True)
B10 = line10("土台", G)

# ── A 決算日近接 ──
print("\n■ A 決算日近接（TDnet決算カレンダー 2016-07〜）", flush=True)
EC = {}
for f in ("earnings_calendar_2016_2021.json", "earnings_calendar.json"):
    for tk, ds in json.load(open(f, encoding="utf-8")).items(): EC.setdefault(tk, set()).update(ds)
EC = {tk: np.array(sorted(pd.to_datetime(list(ds)))) for tk, ds in EC.items()}
d_next = np.full(n, np.nan); d_prev = np.full(n, np.nan); cross = np.zeros(n, dtype=bool)
for i in np.where(GV)[0]:
    arr = EC.get(C.ticker.iat[i])
    if arr is None or len(arr) == 0: continue
    e = entd.iat[i]; k = np.searchsorted(arr, e)
    if k < len(arr): d_next[i] = (arr[k] - e).days; cross[i] = arr[k] <= exit_day(i)
    if k > 0: d_prev[i] = (e - arr[k - 1]).days
known = np.isfinite(d_next) | np.isfinite(d_prev)
post = np.isfinite(d_prev) & (d_prev <= 7)
near = np.isfinite(d_next) & (d_next <= 14)
print(f"  カレンダー既知 {known[GV].sum()}/{GV.sum()}件・決算跨ぎ {cross[GV].sum()}件・決算直後7日 {post[GV].sum()}件・決算前14日 {near[GV].sum()}件")
pm("決算跨ぎ(保有中に発表)", GV & cross); pm("跨がない", GV & known & ~cross); pm("決算直後≤7日", GV & post); pm("決算前≤14日", GV & near & ~cross); pm("それ以外", GV & known & ~cross & ~post & ~near)
for lab, m in (("決算跨ぎを除外", G & ~cross), ("決算直後7日を除外", G & ~post), ("決算前14日(跨ぎ含む)を除外", G & ~near & ~cross), ("決算直後7日のみ", G & post)):
    frac = 1 - (GV & m).sum() / GV.sum(); nf, sd = noise_floor(G, frac) if 0 < frac < 0.9 else (None, 0)
    r = line10(lab, m, B10, nf); print(f"      （除外率{frac*100:.0f}%・同率ランダム除外の99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）")

# ── B 週次信用残の売り側 ──
print("\n■ B 週次信用残（J-Quants weekly margin 2016-07〜・入手可能な直近週=建て日-5日以前）", flush=True)
MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
ratio = np.full(n, np.nan); sturn = np.full(n, np.nan); schg = np.full(n, np.nan); lturn = np.full(n, np.nan)
for i in np.where(GV)[0]:
    df = MG.get(C.ticker.iat[i].replace(".T", ""))
    if df is None or len(df) < 3: continue
    cut = entd.iat[i] - pd.Timedelta(days=5); k = df.index.searchsorted(cut, side="right") - 1
    if k < 1: continue
    L, S = df.LongVol.iat[k], df.ShrtVol.iat[k]; Sp = df.ShrtVol.iat[k - 1]
    if np.isfinite(L) and np.isfinite(S) and S > 0: ratio[i] = L / S
    if np.isfinite(S) and np.isfinite(AV20[i]) and AV20[i] > 0: sturn[i] = S / AV20[i]
    if np.isfinite(L) and np.isfinite(AV20[i]) and AV20[i] > 0: lturn[i] = L / AV20[i]
    if np.isfinite(S) and np.isfinite(Sp) and Sp > 0: schg[i] = S / Sp - 1
for nm, x in (("貸借倍率(買残/売残)", ratio), ("売残回転(売残/20日平均出来高)", sturn), ("売残 前週比", schg), ("買残回転(参考)", lturn)):
    ok = GV & np.isfinite(x); q1, q2 = np.nanquantile(x[ok], [1/3, 2/3])
    lo, mid, hi = ok & (x <= q1), ok & (x > q1) & (x <= q2), ok & (x > q2)
    print(f"  {nm}: 既知{ok.sum()}件 三分位境界 {q1:.2f}/{q2:.2f}")
    pm("下位1/3", lo); pm("中位", mid); pm("上位1/3", hi)
    for lab, m in ((f"{nm} 上位1/3を除外", G & ~hi), (f"{nm} 下位1/3を除外", G & ~lo)):
        frac = 1 - (GV & m).sum() / GV.sum(); nf, sd = noise_floor(G, frac)
        line10(lab, m, B10, nf); print(f"      （除外率{frac*100:.0f}%・ランダム除外99%点 {nf:.0f}万 sd{sd:.0f}）")

# ── C ATRサイズ正規化（26年） ──
print("\n■ C ATRでサイズ正規化（26年・1×150万×係数・追加玉も同じ株数）", flush=True)
ATRv = C.atr.to_numpy()
def run_sized(mask, mult, slots=1, size=1_500_000, max_sig=5, cap=3):
    TICK = C.ticker.to_numpy(); SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
    ou = {}; os_ = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]: del ou[tk]; del os_[tk]
        sc = {}
        for s in os_.values(): sc[s] = sc.get(s, 0) + 1
        cnt = 0; idxs = day_groups[d]; idxs = idxs[mask[idxs]]; idxs = idxs[np.argsort(-SCORE[idxs], kind="stable")]
        for i in idxs:
            if cnt >= max_sig: break
            if not np.isfinite(PNL1[i]) or TICK[i] in ou: continue
            s = SEC[i]
            if sc.get(s, 0) >= cap: continue
            cnt += 1; ex = min(d + int(EXO1[i]), len(days) - 1); ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1; picks.append((d, ex, i))
    live = []; rows = []
    exit_px = E0 * (1 + PNL1 / 100); o2 = OP[:, 1]
    add_ok = (CL[:, 0] > E0 * 1.01) & (EXO1 >= 1) & np.isfinite(o2) & (o2 > 0)
    for d, ex, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= slots: continue
        sh = int(size * mult[i] / E0[i] / 100) * 100
        if sh <= 0: continue
        live.append(ex); yen = PNL1[i] / 100 * sh * E0[i]
        if add_ok[i]: yen += (exit_px[i] - o2[i]) * sh
        rows.append((i, YEARv[i], PNL1[i], yen, sh))
    return pd.DataFrame(rows, columns=["i", "y", "pnl", "yen", "sh"])
def line26(lab, R):
    r = stat(R); dly = R.assign(date=pd.to_datetime(C.entry.to_numpy()[R.i.to_numpy()])).groupby("date").yen.sum().sort_index(); eq = dly.cumsum(); dd = (eq - eq.cummax()).min() / 1e4
    print(f"  {lab:<30} 玉{r['n']:>5} 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 26年{r['tot']:>+6,.0f}万 | 4分割 {r['a']:>+5,.0f}/{r['b']:>+5,.0f}/{r['e3']:>+5,.0f}/{r['e4']:>+5,.0f} | 10年{r['y10']:>+5,.0f} PF{r['pf10']:.2f} 最悪年{r['worst10']:+.0f} | 勝年{r['wy']}/26 最悪{r['worst']:>+4,.0f} DD{dd:>+5,.0f}", flush=True); return r
line26("固定150万(現行)", run_sized(G, np.ones(n)))
for ref in (2.4, 2.8):
    for lo_, hi_ in ((0.7, 1.3), (0.5, 1.5), (0.5, 2.0)):
        line26(f"ATR基準{ref}% 係数[{lo_},{hi_}]", run_sized(G, np.clip(ref / np.where(np.isfinite(ATRv) & (ATRv > 0), ATRv, ref), lo_, hi_)))
line26("逆: ATR高いほど厚く[0.7,1.3]", run_sized(G, np.clip(np.where(np.isfinite(ATRv) & (ATRv > 0), ATRv, 2.8) / 2.8, 0.7, 1.3)))
print(f"\n[done] {time.time()-t0:.0f}s")

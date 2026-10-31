# -*- coding: utf-8 -*-
"""chk4: J-Quants bulk(生値+AdjFactor・上場廃止込み・2021-10〜2026-10-30)で極上を独立再現。
   V1 全銘柄(廃止込み)・株価上限は当時の生値 / V2 現存銘柄だけ(立花にある銘柄) / V3 V2+株価上限を調整後価格(=BTと同じ)
   → V3 を harness(2022-01〜2026-08-27)と突き合わせ、V2-V3=株価上限の先読み、V1-V2=生存者バイアス。
   さらに 2026-09-01〜10-28 の毎日の1位を紙台帳と突き合わせる。"""
import os, sys, json, pickle, time
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project"); sys.path.insert(0, ".")
from screener import is_etf_ticker, yose_limit_price
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo"
t0 = time.time()


def log(*a):
    print(*a, flush=True)


B = pickle.load(open("_bars_daily_5y_1024.pkl", "rb"))
M = pickle.load(open("_master_1024.pkl", "rb")); NAME = dict(zip(M.Code, M.CoName)); MKT = dict(zip(M.Code, M.MktNm))
T = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]; SURV = {k[:4] for k in T}; del T
F = pd.read_pickle("_fins_history.pkl"); F = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)]
DISC = {c: set(pd.to_datetime(g.DiscDate).dt.normalize()) for c, g in F.groupby(F.Code.astype(str).str[:4])}
FINS_END = pd.to_datetime(F.DiscDate).max(); del F
JS = json.load(open("jpx_earnings_schedule.json", encoding="utf-8"))["schedule"]
for d, lst in JS.items():
    if pd.Timestamp(d) > FINS_END:
        for it in lst:
            DISC.setdefault(it["code"], set()).add(pd.Timestamp(d))
MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
log(f"読込 {time.time()-t0:.0f}s  fins末尾 {FINS_END.date()}  bulk {B.Date.min().date()}〜{B.Date.max().date()} codes {B.Code.nunique()}")
B = B.sort_values(["Code", "Date"])
recs = []; BARS = {}
for code, g in B.groupby("Code", sort=False):
    c4 = code[:4]; tk = c4 + ".T"
    nm = NAME.get(code)
    if nm is None:
        if is_etf_ticker(tk, None):
            continue          # 廃止銘柄は名前が無い→コード帯で判定
    else:
        if is_etf_ticker(tk, nm):
            continue
        if MKT.get(code) not in ("プライム", "スタンダード", "グロース"):
            continue
    g = g.dropna(subset=["C"]); g = g[g.C > 0]
    if len(g) < 60:
        continue
    af = g.AdjFactor.fillna(1.0).to_numpy(dtype=float)
    f = np.cumprod(af[::-1])[::-1] / af              # Π_{s>t} AF(s)
    rawC = g.C.to_numpy(dtype=float); rawO = g.O.to_numpy(dtype=float); rawV = g.Vo.to_numpy(dtype=float)
    o = pd.Series(rawO * f); h = pd.Series(g.H.to_numpy(dtype=float) * f); l = pd.Series(g.L.to_numpy(dtype=float) * f)
    c = pd.Series(rawC * f); v = pd.Series(rawV / f)
    d = c.diff(); ag = d.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean(); al = (-d).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    ma = c.rolling(25).mean(); dev = (c - ma) / ma * 100
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1); atr = tr.rolling(14).mean(); atrp = atr / c * 100
    rr = (h - l).shift(1) / atr.shift(1); vr = v.shift(1) / v.shift(2).rolling(20).mean(); tov = c.shift(1) * v.shift(1)
    vt5 = v.rolling(5).mean() / v.shift(5).rolling(20).mean()
    cn = c.to_numpy(); vn = v.to_numpy()
    cand = ((rsi <= 45) & (dev <= -1.5) & ((rr >= 1.5) | (vr >= 2.0)) & (tov >= 2e9) & (atrp <= 3.0) & (vt5 <= 1.09)).fillna(False).to_numpy()
    dates = g.Date.to_numpy()
    adv20 = (pd.Series(rawC * rawV)).rolling(20).mean().to_numpy()
    m = MG.get(c4); mi = pd.to_datetime(m.index).values if m is not None and len(m) else None
    lv = m.LongVol.to_numpy(dtype=float) if mi is not None else None
    BARS[code] = (dates, o.to_numpy(), h.to_numpy(), l.to_numpy(), cn, rsi.to_numpy(), rawO, rawC)
    rsin = rsi.to_numpy(); devn = dev.to_numpy(); tovn = tov.to_numpy()
    for t in np.where(cand)[0]:
        if t + 1 >= len(cn) or t < 25:
            continue
        rrr = np.diff(cn[t - 20:t + 1]); vv = vn[t - 19:t + 1]; up, dn = vv[rrr > 0], vv[rrr < 0]
        if len(up) < 2 or len(dn) < 2 or dn.mean() <= 0:
            continue
        uv = up.mean() / dn.mean()
        if not uv < 1.091:
            continue
        sd = pd.Timestamp(dates[t]); ed = pd.Timestamp(dates[t + 1])
        ds = DISC.get(c4, set()); earn = any(abs((x - ed).days) <= 3 for x in ds)
        dc = np.nan
        if mi is not None:
            q = np.searchsorted(mi, np.datetime64(sd - pd.Timedelta(days=4)), side="right") - 1
            if q >= 0 and lv[q] > 0 and (np.datetime64(sd) - mi[q]) <= np.timedelta64(21, "D") and adv20[t] > 0:
                dc = lv[q] * rawC[t] / adv20[t]
        sc = (1 / (1 + ((rsin[t] - 38) / 8) ** 2)) * 0.3 + (1 / (1 + ((devn[t] + 3) / 2) ** 2)) * 0.3 + np.log10(max(tovn[t], 1) / 1e9 + 1) / 3 * 0.4
        recs.append((ed, sd, code, t, sc, rawC[t], cn[t], earn, dc, c4 in SURV, rsin[t], devn[t]))
CD = pd.DataFrame(recs, columns=["entry", "sig", "code", "t", "score", "rawC", "adjC", "earn", "dc", "surv", "rsi", "dev"])
log(f"候補 {len(CD):,}  {time.time()-t0:.0f}s  廃止銘柄の候補 {(~CD.surv).sum()}  生値>1万 {(CD.rawC>10000).sum()} 調整後>1万 {(CD.adjC>10000).sum()}")
CD.to_pickle(os.path.join(OUT, "jq_cands.pkl"))


def trade(code, t, size=1_500_000, cost=0.13):
    dates, o, h, l, c, r, rawO, rawC = BARS[code]
    e = t + 1; lp = yose_limit_price(rawC[t])
    if not (rawO[e] > 0) or (lp and rawO[e] > lp):
        return dict(typ="NOFILL", exit_i=e, pnl=np.nan, yen=0.0, sh=0, E=o[e])
    E = o[e]; sl = E * 0.97; tp = E * 1.05; sh = int(size / E / 100) * 100

    def fin(px, j, typ):
        p = (px - E) / E * 100 - cost
        return dict(typ=typ, exit_i=j, pnl=p, yen=p / 100 * sh * E, sh=sh, E=E, xpx=px)
    for k in range(3):
        j = e + k
        if j >= len(c):
            return dict(typ="保有中", exit_i=len(c) - 1, pnl=np.nan, yen=0.0, sh=sh, E=E)
        if k > 0:
            if o[j] <= sl or o[j] >= tp:
                return fin(o[j], j, "GAP")
            if k == 1 and c[e] <= E * 0.99:
                return fin(o[j], j, "初日処分")
        if l[j] <= sl:
            return fin(sl, j, "STOP")
        if h[j] >= tp:
            return fin(tp, j, "TP")
        if k == 2:
            return fin(c[j], j, "期限")
        if r[j] >= 50:
            return fin(c[j], j, "RSI引け")


def run(mask, a, b, verbose=False):
    D = CD[mask & (CD.entry >= a) & (CD.entry <= b)].sort_values(["entry", "score"], ascending=[True, False])
    busy_until = None; held = None; rows = []; miss = 0
    for ed, grp in D.groupby("entry", sort=True):
        if busy_until is not None and ed <= busy_until:
            if verbose:
                log(f"  {ed.date()} 保有中({held})")
            continue
        pick = next(iter(grp.itertuples()))
        tr = trade(pick.code, pick.t)
        dates = BARS[pick.code][0]
        xd = pd.Timestamp(dates[tr["exit_i"]])
        if verbose:
            nm = NAME.get(pick.code, pick.code); others = ", ".join(NAME.get(x.code, x.code)[:6] for x in list(grp.itertuples())[1:4])
            pn = f"{tr['pnl']:+.2f}" if np.isfinite(tr["pnl"]) else ""
            log(f"  {ed.date()} 1位 {pick.code[:4]} {nm[:10]:<10} 前日終値{pick.rawC:,.1f} 指値{yose_limit_price(pick.rawC)} 寄り{BARS[pick.code][6][pick.t+1]:,.1f} → {tr['typ']} {pn:>7} 決済{xd.date()}  次点: {others}")
        if tr["typ"] == "NOFILL":
            miss += 1; continue
        rows.append(dict(entry=ed, exit=xd, code=pick.code, typ=tr["typ"], pnl=tr["pnl"], yen=tr["yen"], notional=tr["sh"] * tr["E"]))
        busy_until = xd; held = pick.code
    R = pd.DataFrame(rows); R.attrs["miss"] = miss
    return R


def st(R, lab):
    if not len(R):
        log(lab, "no trades"); return
    X = R[np.isfinite(R.pnl)]
    gp = X.yen[X.yen > 0].sum(); gl = -X.yen[X.yen <= 0].sum(); cum = X.groupby("exit").yen.sum().cumsum()
    yy = (X.groupby(X.entry.dt.year).yen.sum() / 1e4).round(1).to_dict()
    log(f"{lab:<46} 玉{len(X):>3} 不成立{R.attrs['miss']:>2} 勝率{(X.pnl>0).mean()*100:4.1f}% PF{gp/gl:4.2f} 損益{X.yen.sum()/1e4:+6.1f}万 DD{(cum-cum.cummax()).min()/1e4:+5.1f}  年別{yy}")


A, Bq = pd.Timestamp("2022-01-01"), pd.Timestamp("2026-08-27")
base = (~CD.earn) & ~(np.isfinite(CD.dc) & (CD.dc > 1.2))
R3 = run(base & CD.surv & (CD.adjC <= 10000), A, Bq); st(R3, "V3 現存のみ・株価上限=調整後(BTと同じ土俵)")
R2 = run(base & CD.surv & (CD.rawC <= 10000), A, Bq); st(R2, "V2 現存のみ・株価上限=当時の生値(本番)")
R1 = run(base & (CD.rawC <= 10000), A, Bq); st(R1, "V1 上場廃止込み・株価上限=当時の生値")
R0 = run(CD.surv & (CD.adjC <= 10000), A, Bq); st(R0, "参考 V3で決算/買残除外なし")
chg = CD[base & CD.surv & ((CD.adjC <= 10000) != (CD.rawC <= 10000)) & (CD.entry >= A) & (CD.entry <= Bq)]
log(f"株価上限の判定が調整後/生値で食い違う候補(2022〜) {len(chg)}件: " + ", ".join(f"{r.code[:4]}@{r.entry.date()} 生{r.rawC:,.0f}/調{r.adjC:,.0f}" for r in chg.head(12).itertuples()))
dl = CD[base & ~CD.surv & (CD.rawC <= 10000) & (CD.entry >= A)]
log(f"上場廃止銘柄の候補(2022〜) {len(dl)}件 銘柄{dl.code.nunique()}: {dl.code.str[:4].value_counts().head(10).to_dict()}")
pickle.dump(dict(R1=R1, R2=R2, R3=R3), open(os.path.join(OUT, "jq_runs.pkl"), "wb"))
log("\n■ 2026-09-01〜10-28 毎日の1位（V1条件・決算は fins〜9/18 + JPX予定表）")
Rl = run(base & (CD.rawC <= 10000), pd.Timestamp("2026-09-01"), pd.Timestamp("2026-10-28"), verbose=True); st(Rl, "2026-09-01〜10-28")
log(f"done {time.time()-t0:.0f}s")

# -*- coding: utf-8 -*-
"""_bt_gold_rn_break_0927.py — G1の深掘り: 金の「$50キリ番を抜けたら順張り」（2026-09-27）
第1段(_bt_gold_levels_0927.log・550セル)で、キリ番$50/$100だけ「初タッチ後の抜けに逆指値で順張り」が偽の格子より
+1.0〜+2.3$/oz良く(両方向・δ0.5/1/2・保有15〜60分すべて)、逆張りは偽より-1〜-2.8$悪い＝Osler型の損切り連鎖。
ただし前半(2015-20)はコスト$0.593後マイナス。→ ここでは同じイベントを細かい情報つきで作り直し、
  ①%で見た効果(値段とボラの時代差を消す) ②ボラ(20日実現ボラ)別・今の金AMと同じボラ門 ③自己ゲート(直近40回)
  ④$X00 と $X50 の違い ⑤時間帯 ⑥偽の格子との差の前後半 ⑦実スプレッド＋手数料 を測る。
イベント: その日(サーバー日)の$50キリ番の初タッチ(02:00-22:59)。方向=直前足の終値の側。
建て: 初タッチから60分以内に 水準±δ に触れたら逆指値で約定(下から=買い・上から=売り)。約定値=max/min(水準±δ, その足の始値)。
手仕舞い: 約定足の時刻+h分の足の始値。偽の格子: 水準 = 50k + 0.37×50 / 0.63×50。
実行: python -X utf8 _bt_gold_rn_break_0927.py [xm|duka] > _bt_gold_rn_break_0927_<src>.log
"""
import pandas as pd, numpy as np, sys, time
t0 = time.time()
SRC = sys.argv[1] if len(sys.argv) > 1 else "xm"
COMM = 0.453          # 手数料(往復・$/oz)。スプレッドは足のspread列(XM)を使う。Dukaは合計0.593固定
ROLL_LO, ROLL_HI = 120, 1380
if SRC == "xm":
    d = pd.read_pickle("_xm_m1_GOLD._0926.pkl"); d = d[d.index >= "2015-01-01"]
    idx = d.index; SP = d["spread"].to_numpy(np.float64) * 0.01
else:
    d = pd.read_pickle("_fx_xauusd_m1.pkl")
    if not isinstance(d.index, pd.DatetimeIndex):
        tc = [c for c in d.columns if "time" in c.lower() or "date" in c.lower()]
        d = d.set_index(tc[0])
    d = d.sort_index(); d.columns = [c.lower() for c in d.columns]
    d = d[(d.index >= "2016-01-01") & (d["vol"] > 0)]            # ティックの無い埋め足(週末・閉場)を除く
    utc = d.index.tz_localize("UTC") if d.index.tz is None else d.index.tz_convert("UTC")
    idx = utc.tz_convert("America/New_York").tz_localize(None) + pd.Timedelta(hours=7)
    SP = np.full(len(d), 0.14)
O = d["open"].to_numpy(np.float64); H = d["high"].to_numpy(np.float64); L = d["low"].to_numpy(np.float64); C = d["close"].to_numpy(np.float64)
MIN = (idx.hour * 60 + idx.minute).to_numpy()
ny = idx - pd.Timedelta(hours=7)
lon = ny.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").tz_convert("Europe/London").tz_localize(None)
LMIN = np.where(pd.isna(lon), -1, lon.hour * 60 + lon.minute)
DAY = idx.normalize()
days, dstart = np.unique(DAY.values, return_index=True); dend = np.append(dstart[1:], len(d)); days = pd.DatetimeIndex(days)
# 20日実現ボラ(日足=サーバー日の最後の終値・前日までで計算＝先読みなし)
dclose = pd.Series([C[b - 1] for b in dend], index=days)
rv20 = (np.log(dclose).diff().rolling(20).std() * np.sqrt(252) * 100).shift(1).to_numpy()
print(f"[{SRC}] {len(d):,}本 {idx.min()}〜{idx.max()} {len(days)}日 {time.time()-t0:.0f}s", flush=True)

HOLDS = (5, 15, 30, 60, 120)
rows = []
for k in range(len(days)):
    a, b = dstart[k], dend[k]
    mpos = np.full(1440, -1); mpos[MIN[a:b]] = np.arange(a, b)
    lo = L[a:b].min(); hi = H[a:b].max()
    for off, grid in ((0.0, "本物"), (18.5, "偽37"), (31.5, "偽63")):
        n0 = int(np.floor((lo - off) / 50)); n1 = int(np.ceil((hi - off) / 50))
        for n in range(n0, n1 + 1):
            lv = n * 50 + off
            touch = (L[a:b] <= lv) & (H[a:b] >= lv)
            pos = np.flatnonzero(touch)
            if len(pos) == 0: continue
            i = a + pos[0]
            if i == a or not (ROLL_LO <= MIN[i] < ROLL_HI): continue
            prev = C[i - 1]
            if prev == lv: continue
            up = prev < lv
            big = (grid == "本物") and (round(lv) % 100 == 0)
            for dl in (0.5, 1.0, 2.0):
                trg = lv + dl if up else lv - dl
                je = min(b, i + 61)
                hit = np.flatnonzero(H[i:je] >= trg) if up else np.flatnonzero(L[i:je] <= trg)
                if len(hit) == 0: continue
                j = i + hit[0]
                px = trg if j == i else (max(trg, O[j]) if up else min(trg, O[j]))
                side = 1 if up else -1
                m0 = MIN[j]
                if not (ROLL_LO <= m0 < ROLL_HI): continue
                for h in HOLDS:
                    mx = m0 + h
                    if mx >= ROLL_HI: continue
                    e = -1
                    for q in range(3):
                        if mpos[mx + q] >= 0: e = mpos[mx + q]; break
                    if e < 0: continue
                    g = side * (O[e] - px)
                    cost = SP[j] + COMM if SRC == "xm" else 0.593
                    rows.append((k, grid, lv, big, "下から" if up else "上から", dl, h, side, px, g, cost, rv20[k], LMIN[j], j - i))
E = pd.DataFrame(rows, columns=["k", "grid", "lv", "big", "dir", "dl", "hold", "side", "px", "gross", "cost", "rv20", "lmin", "wait"])
E["day"] = days[E.k.values]; E["yr"] = E.day.dt.year
E["gpct"] = E.gross / E.px * 100
E.to_pickle(f"_bt_gold_rn_break_0927_{SRC}.pkl")
print(f"行 {len(E):,}  {time.time()-t0:.0f}s", flush=True)
ndays = len(days)


def ct(k, x):
    if len(x) < 30: return len(x), np.nan, np.nan
    S = np.bincount(k, weights=x, minlength=ndays); N = np.bincount(k, minlength=ndays)
    u = N > 0; S = S[u]; N = N[u]; G = len(S); m = S.sum() / N.sum()
    se = np.sqrt(((S - m * N) ** 2).sum() * G / max(G - 1, 1)) / N.sum()
    return len(x), m, (m / se if se > 0 else np.nan)


def line(g, lab, cost_col="cost", mult=1.0):
    k = g.k.to_numpy(); gr = g.gross.to_numpy(); net = gr - g[cost_col].to_numpy() * mult; yr = g.yr.to_numpy(); gp = g.gpct.to_numpy()
    n, mg, tg = ct(k, gr); _, mn, tn = ct(k, net); _, mp, tp = ct(k, gp)
    _, m1, t1 = ct(k[yr <= 2020], net[yr <= 2020]); _, m2, t2 = ct(k[yr >= 2021], net[yr >= 2021])
    _, gp1, tp1 = ct(k[yr <= 2020], gp[yr <= 2020]); _, gp2, tp2 = ct(k[yr >= 2021], gp[yr >= 2021])
    ys = pd.Series(net).groupby(yr).sum()
    Sd = pd.Series(net).groupby(k).sum(); drop = set(Sd.nlargest(3).index); msk = ~np.isin(k, list(drop))
    _, _, t3 = ct(k[msk], net[msk])
    return (f"{lab:<40} n{n:5d} 粗{mg:+6.3f}$(t{tg:+5.2f}) 粗%{mp:+.4f}(t{tp:+5.2f}・前{tp1:+.2f}/後{tp2:+.2f}) | 実コスト後{mn:+6.3f}$ t{tn:+5.2f} "
            f"前{m1:+.3f}(t{t1:+.2f}) 後{m2:+.3f}(t{t2:+.2f}) 勝ち年{(ys>0).sum()}/{len(ys)} 上位3日除去t{t3:+.2f}")


R = E[E.grid == "本物"]; Pc = E[E.grid != "本物"]
print("\n■ 本物の$50キリ番・抜けたら順張り (保有×δ・両方向合算) と偽の格子")
for h in HOLDS:
    for dl in (0.5, 1.0, 2.0):
        g = R[(R.hold == h) & (R.dl == dl)]; p = Pc[(Pc.hold == h) & (Pc.dl == dl)]
        print("  " + line(g, f"本物 h{h} δ{dl}"))
        print("  " + line(p, f"偽   h{h} δ{dl}"))
print("\n■ 代表(h30・δ1)の分解")
g0 = R[(R.hold == 30) & (R.dl == 1.0)]; p0 = Pc[(Pc.hold == 30) & (Pc.dl == 1.0)]
for lab, g in (("$X00(=$100)", g0[g0.big]), ("$X50だけ", g0[~g0.big]), ("下から(買い)", g0[g0.dir == "下から"]), ("上から(売り)", g0[g0.dir == "上から"])):
    print("  " + line(g, lab))
print("  年別(本物 h30 δ1・実コスト後の合計$/oz・件数):", " ".join(f"{y}:{v:+.0f}({n})" for y, v, n in zip(*[g0.groupby('yr').apply(lambda x: (x.gross - x.cost).sum()).index, g0.groupby('yr').apply(lambda x: (x.gross - x.cost).sum()).values, g0.groupby('yr').size().values])))
print("  年別(偽 h30 δ1・実コスト後の合計$/oz):", " ".join(f"{y}:{v:+.0f}" for y, v in p0.groupby('yr').apply(lambda x: (x.gross - x.cost).sum()).items()))
print("\n■ ボラ(20日実現ボラ・前日まで)別 h30 δ1")
for lo_, hi_ in ((0, 12), (12, 16), (16, 22), (22, 30), (30, 999)):
    g = g0[(g0.rv20 >= lo_) & (g0.rv20 < hi_)]; p = p0[(p0.rv20 >= lo_) & (p0.rv20 < hi_)]
    if len(g) > 30: print("  " + line(g, f"本物 ボラ{lo_}-{hi_}%")); print("  " + line(p, f"偽   ボラ{lo_}-{hi_}%"))
print("\n■ 今の金AMと同じボラ門(20日ボラ>22%)だけ")
for h in (15, 30, 60):
    for dl in (0.5, 1.0, 2.0):
        g = R[(R.hold == h) & (R.dl == dl) & (R.rv20 > 22)]; p = Pc[(Pc.hold == h) & (Pc.dl == dl) & (Pc.rv20 > 22)]
        print("  " + line(g, f"本物 ボラ>22 h{h} δ{dl}")); print("  " + line(p, f"偽   ボラ>22 h{h} δ{dl}"))
print("\n■ 時間帯(ロンドン時刻) h30 δ1")
for a_, b_, lab in ((0, 420, "アジア 00-07"), (420, 720, "ロンドン午前 07-12"), (720, 1020, "NY重なり 12-17"), (1020, 1440, "NY午後 17-24")):
    g = g0[(g0.lmin >= a_) & (g0.lmin < b_)]; p = p0[(p0.lmin >= a_) & (p0.lmin < b_)]
    print("  " + line(g, "本物 " + lab)); print("  " + line(p, "偽   " + lab))
print("\n■ 自己ゲート(同じ設定の直近40回の実コスト後平均>0の時だけ・前日までの玉で判定) h30 δ1")
def gated(g, N=40):
    g = g.sort_values(["k", "px"]).copy(); net = (g.gross - g.cost).to_numpy(); kk = g.k.to_numpy()
    keep = np.zeros(len(g), bool); hist = []
    cur_k = -1; buf = []
    for i in range(len(g)):
        if kk[i] != cur_k:
            hist += buf; buf = []; cur_k = kk[i]
        if len(hist) >= N * 0.8 and np.mean(hist[-N:]) > 0: keep[i] = True
        buf.append(net[i])
    return g[keep]
for lab, g in (("本物", g0), ("偽", p0)):
    print("  " + line(gated(g), f"{lab} 自己ゲートN40"))
print("\n■ コスト1.5倍(実スプレッド+手数料)×1.5 h30 δ1: " + line(g0, "本物", mult=1.5))
print(f"\n完了 {time.time()-t0:.0f}s")

# -*- coding: utf-8 -*-
"""_bt_gold_x00_robust_0927.py — 候補「大台($X00)に初めて下から触れたら、+δの逆指値で買い→h分で手仕舞い」の頑健性（2026-09-27・G1）
①格子ずらし: $100格子を +0,+5,…,+95 ずらした水準で同じルール(ちょうど大台だけに効くか)
②年別(今のコスト%・実際の$コスト・件数) ③時間帯(ロンドン時刻)と今の金AM(10:19-10:32)/PM(14:53-15:03 London)との重なり
④損切り$5/$10/なし ⑤下からの接近の定義(直前1本/直前5本の平均)の違い
データ: xm(_xm_m1_GOLD._0926.pkl 2015〜) と duka(_fx_xauusd_m1.pkl 2016〜・ティック0の足は除外)。
実行: python -X utf8 _bt_gold_x00_robust_0927.py [xm|duka]
"""
import pandas as pd, numpy as np, sys, time
t0 = time.time()
SRC = sys.argv[1] if len(sys.argv) > 1 else "xm"
ROLL_LO, ROLL_HI = 120, 1380
CNOW = 0.593 / 4285 * 100
if SRC == "xm":
    d = pd.read_pickle("_xm_m1_GOLD._0926.pkl"); d = d[d.index >= "2015-01-01"]; idx = d.index
    SP = d["spread"].to_numpy(np.float64) * 0.01
else:
    d = pd.read_pickle("_fx_xauusd_m1.pkl").sort_index(); d = d[(d.index >= "2016-01-01") & (d["vol"] > 0)]
    utc = d.index.tz_localize("UTC"); idx = utc.tz_convert("America/New_York").tz_localize(None) + pd.Timedelta(hours=7)
    SP = np.full(len(d), 0.14)
O = d["open"].to_numpy(np.float64); H = d["high"].to_numpy(np.float64); L = d["low"].to_numpy(np.float64); C = d["close"].to_numpy(np.float64)
MIN = (idx.hour * 60 + idx.minute).to_numpy()
ny = idx - pd.Timedelta(hours=7)
lon = ny.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").tz_convert("Europe/London").tz_localize(None)
LMIN = np.where(pd.isna(lon), -1, lon.hour * 60 + lon.minute)
DAY = idx.normalize(); days, dstart = np.unique(DAY.values, return_index=True); dend = np.append(dstart[1:], len(d)); days = pd.DatetimeIndex(days)
ndays = len(days)
print(f"[{SRC}] {len(d):,}本 {len(days)}日 {time.time()-t0:.0f}s", flush=True)


def events(off, step=100.0, dls=(0.5, 1.0, 2.0), holds=(15, 30), sls=(None,), appr="prev1"):
    rows = []
    for k in range(ndays):
        a, b = dstart[k], dend[k]
        mpos = np.full(1440, -1); mpos[MIN[a:b]] = np.arange(a, b)
        lo = L[a:b].min(); hi = H[a:b].max()
        for n in range(int(np.floor((lo - off) / step)), int(np.ceil((hi - off) / step)) + 1):
            lv = n * step + off
            pos = np.flatnonzero((L[a:b] <= lv) & (H[a:b] >= lv))
            if len(pos) == 0: continue
            i = a + pos[0]
            if i - a < 5 or not (ROLL_LO <= MIN[i] < ROLL_HI): continue
            ref = C[i - 1] if appr == "prev1" else C[i - 5:i].mean()
            if not (ref < lv): continue                                 # 下からだけ
            for dl in dls:
                trg = lv + dl; je = min(b, i + 61)
                hit = np.flatnonzero(H[i:je] >= trg)
                if len(hit) == 0: continue
                j = i + hit[0]; px = trg if j == i else max(trg, O[j]); m0 = MIN[j]
                if not (ROLL_LO <= m0 < ROLL_HI): continue
                for h in holds:
                    mx = m0 + h
                    if mx >= ROLL_HI: continue
                    e = -1
                    for q in range(3):
                        if mpos[mx + q] >= 0: e = mpos[mx + q]; break
                    if e < 0: continue
                    for sl in sls:
                        exitp = O[e]; hitsl = False
                        if sl is not None:
                            lows = L[j:e]
                            if j < e and lows.size and (lows <= px - sl).any(): exitp = px - sl; hitsl = True
                        g = exitp - px
                        rows.append((k, lv, dl, h, sl, px, g, SP[j] + 0.453 if SRC == "xm" else 0.593, LMIN[j], hitsl))
    E = pd.DataFrame(rows, columns=["k", "lv", "dl", "hold", "sl", "px", "gross", "cost", "lmin", "hitsl"])
    E["gpct"] = E.gross / E.px * 100; E["yr"] = days[E.k.values].year
    return E


def ct(k, x):
    if len(x) < 20: return len(x), np.nan, np.nan
    S = np.bincount(k, weights=x, minlength=ndays); N = np.bincount(k, minlength=ndays)
    u = N > 0; S = S[u]; N = N[u]; G = len(S); m = S.sum() / N.sum()
    se = np.sqrt(((S - m * N) ** 2).sum() * G / max(G - 1, 1)) / N.sum()
    return len(x), m, (m / se if se > 0 else np.nan)


# ① 格子ずらし(δ1・h15)
print("\n■ ①$100格子をずらした時の成績(下から・δ1・h15・今のコスト%後)  ※ずらし0=本物の大台")
prof = []
for off in range(0, 100, 5):
    E = events(float(off), dls=(1.0,), holds=(15,))
    n, m, t = ct(E.k.to_numpy(), (E.gpct - CNOW).to_numpy())
    prof.append((off, n, m, t))
    print(f"  ずらし+{off:2d}: n{n:4d} 今コスト後{m:+.4f}% t{t:+5.2f}", flush=True)
others = [p for p in prof if p[0] != 0]
print(f"  ずらし5〜95の平均 {np.nanmean([p[2] for p in others]):+.4f}% / t の最大 {np.nanmax([p[3] for p in others]):+.2f}  ← 本物(0) {prof[0][2]:+.4f}% t{prof[0][3]:+.2f}")

# ②〜⑤ 本物
E = events(0.0, dls=(0.5, 1.0, 2.0), holds=(15, 30), sls=(None, 5.0, 10.0))
print(f"\n本物のイベント行 {len(E):,}  {time.time()-t0:.0f}s")
B = E[(E.dl == 1.0) & (E.hold == 15) & (E.sl.isna())]
print("\n■ ②年別(δ1・h15): 件数 / 今コスト%後の合計 / 実際の$コスト後の合計($/oz)")
for y, g in B.groupby("yr"):
    print(f"  {y}: {len(g):3d}件  {(g.gpct - CNOW).sum():+.3f}%  {(g.gross - g.cost).sum():+7.2f}$  勝率{((g.gpct - CNOW) > 0).mean()*100:3.0f}%")
g2 = B[B.yr < 2025]
n, m, t = ct(g2.k.to_numpy(), (g2.gpct - CNOW).to_numpy()); print(f"  2025-26を除く: n{n} {m:+.4f}% t{t:+.2f}")
print("\n■ ③時間帯(ロンドン時刻・δ1・h15)")
for a_, b_, lab in ((0, 420, "00-07 アジア"), (420, 720, "07-12 ロンドン午前"), (720, 1020, "12-17 NY重なり"), (1020, 1440, "17-24 NY午後")):
    g = B[(B.lmin >= a_) & (B.lmin < b_)]; n, m, t = ct(g.k.to_numpy(), (g.gpct - CNOW).to_numpy())
    print(f"  {lab}: n{n:4d} {m:+.4f}% t{t:+.2f}")
am = ((B.lmin + 15 > 10 * 60 + 19) & (B.lmin < 10 * 60 + 32)); pm = ((B.lmin + 15 > 14 * 60 + 53) & (B.lmin < 15 * 60 + 3))
print(f"  今の金AM窓(10:19-10:32)と保有が重なる: {int(am.sum())}件 / 金PM窓(14:53-15:03): {int(pm.sum())}件 / 全{len(B)}件")
print("\n■ ④損切り(δ1・h15/h30)")
for h in (15, 30):
    for sl in (None, 5.0, 10.0):
        g = E[(E.dl == 1.0) & (E.hold == h) & ((E.sl.isna()) if sl is None else (E.sl == sl))]
        n, m, t = ct(g.k.to_numpy(), (g.gpct - CNOW).to_numpy())
        print(f"  h{h} 損切り{('なし' if sl is None else f'${sl:g}')}: n{n} {m:+.4f}% t{t:+.2f} 損切り発動{g.hitsl.mean()*100:.1f}%")
print("\n■ ⑤接近の定義を「直前5本の平均<水準」に変えた時(δ1・h15)")
E5 = events(0.0, dls=(1.0,), holds=(15,), appr="prev5")
n, m, t = ct(E5.k.to_numpy(), (E5.gpct - CNOW).to_numpy()); print(f"  n{n} {m:+.4f}% t{t:+.2f}")
print(f"\n完了 {time.time()-t0:.0f}s")

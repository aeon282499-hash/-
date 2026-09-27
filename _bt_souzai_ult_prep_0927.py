# -*- coding: utf-8 -*-
"""_bt_souzai_ult_prep_0927.py — 割安B「究極版」探索の追加データ（2026-09-27）
_bt_souzai_opt_slim_0927.pkl に無いものだけを作る:
 AEV(上方修正イベント: 開示日s・銘柄・修正率・直近決算の前年同期比) / EQR(自己資本比率・前営業日) / ROE1(実績EPS÷BPS・前営業日)
 権利日: 銘柄ごとに決算月(期末)と中間の有無 → 権利確定日(月末の最終営業日) → 権利付き最終日(2019-07-18以降は2営業日前・それ以前は3営業日前)
   CUMN[t,j] = t より後で最初の権利付き最終日の営業日番号（無ければ大きな数）
   EXD = {(権利落ち日t, j): その回に払う割合(年間配当のうち)}  … 権利落ち日をまたいだ玉に配当を足す(信用買いの配当落調整金≒配当×0.85)ための材料
   割合: 権利落ち日より前の最新の開示で、同じ行の 期末予想÷年間予想（分割の前後で単位がずれない）。無ければ期末0.5/中間0.5。
   実行時の配当額 = 予想配当利回り(前営業日の値)×割合×前日終値。
 出力: _bt_souzai_ult_extra_0927.pkl
"""
import pickle, time, gc, numpy as np, pandas as pd

t0 = time.time()
SL = pickle.load(open("_bt_souzai_opt_slim_0927.pkl", "rb"))
dates, codes = SL["dates"], SL["codes"]; RAWR = SL["RAWR"]
T, N = len(dates), len(codes); cix = {c: j for j, c in enumerate(codes)}
del SL; gc.collect()
S = pickle.load(open("_bt_souzai_sig2_0927.pkl", "rb"))
AEV = S["AEV"]; EQR = S["EQR"].astype(np.float32)
del S; gc.collect()
print(f"sig2 {time.time()-t0:.0f}s AEV {len(AEV)}", flush=True)
P = pickle.load(open("_bt_souzai_panel_0927.pkl", "rb"))
with np.errstate(invalid="ignore", divide="ignore"):
    ROE = np.where(P["VAL"]["BPS"] > 0, P["VAL"]["EPS"] / P["VAL"]["BPS"], np.nan).astype(np.float32)
ROE1 = np.vstack([np.full((1, N), np.nan, np.float32), ROE[:-1]])
del P, ROE; gc.collect()
print(f"panel {time.time()-t0:.0f}s", flush=True)

F = pd.read_pickle("_fins_history.pkl")
F = F[F.Code.str.len() == 5].copy(); F["c4"] = F.Code.str[:4]; F = F[F.c4.isin(cix)]
for c in ("FDivFY", "FDivAnn", "NxFDivAnn"): F[c] = pd.to_numeric(F[c], errors="coerce")
F["dt"] = pd.to_datetime(F.DiscDate)
F = F.sort_values(["c4", "dt"])
dv = dates.values
def last_bd_on_or_before(ts):
    i = int(np.searchsorted(dv, np.datetime64(ts), side="right")) - 1
    return i
CUMN = np.full((T, N), 10 ** 6, np.int32)
EXD = {}
n_cum = 0
for c4, g in F.groupby("c4", sort=False):
    j = cix[c4]
    fy = g[g.DocType.str.startswith("FY") & g.DocType.str.contains("FinancialStatements")].CurFYEn.dropna()
    fy = fy[fy.str.len() == 10]
    if not len(fy): continue
    mon = pd.to_datetime(fy).dt.month.mode().iloc[0]
    # 予想の時系列(開示日→値): 期末予想・年間予想
    tl = g[["dt", "FDivFY", "FDivAnn", "NxFDivAnn", "DocType"]].copy()
    tl["ann"] = np.where(np.isfinite(tl.FDivAnn), tl.FDivAnn, np.where(tl.DocType.str.startswith("FY"), tl.NxFDivAnn, np.nan))
    cums = []
    for y in range(2016, 2027):
        for m in (mon, (mon - 7) % 12 + 1):
            me = pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0)
            r = last_bd_on_or_before(me)
            if r < 5 or r + 1 >= T or dates[r].month != m or dates[r + 1].month == m: continue   # データの最終日を月末と取り違えない
            k = 2 if me >= pd.Timestamp("2019-07-18") else 3
            cum = r - k; ex = cum + 1
            if cum < 1 or ex >= T: continue
            known = tl[tl.dt < dates[ex]]
            cums.append(cum)
            both = known[np.isfinite(known.FDivFY) & np.isfinite(known.FDivAnn) & (known.FDivAnn > 0)]
            if len(both):                                    # 同じ開示の期末予想÷年間予想＝期末の割合（分割の前後で単位がずれない）
                shf = float(np.clip(both.FDivFY.iloc[-1] / both.FDivAnn.iloc[-1], 0, 1))
            else:
                shf = 0.5
            share = shf if m == mon else 1.0 - shf
            if share > 0:
                EXD[(ex, j)] = share                          # その回に払う割合（配当額は実行時に 予想利回り×割合×前日終値 で出す）
    cums = sorted(set(cums))
    n_cum += len(cums)
    nxt = 10 ** 6; ci = len(cums) - 1
    col = np.full(T, 10 ** 6, np.int32)
    for t in range(T - 1, -1, -1):
        while ci >= 0 and cums[ci] > t:
            nxt = cums[ci]; ci -= 1
        col[t] = nxt
    CUMN[:, j] = col
print(f"権利日 {n_cum}件 / 配当あり権利落ち {len(EXD)}件 {time.time()-t0:.0f}s", flush=True)
pickle.dump(dict(AEV=AEV, EQR=EQR, ROE1=ROE1, CUMN=CUMN, EXD=EXD), open("_bt_souzai_ult_extra_0927.pkl", "wb"), protocol=4)
print(f"保存 {time.time()-t0:.0f}s")

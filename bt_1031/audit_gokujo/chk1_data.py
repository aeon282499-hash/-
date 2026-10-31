# -*- coding: utf-8 -*-
"""chk1: データ土台の点検（カレンダー・分割調整・決算日・信用残）読み取り専用"""
import os, sys, pickle, time
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project")
t0 = time.time()
Z = pickle.load(open("_bt_gokujo_cache_0926.pkl", "rb"))
C, NF, DAYS = Z["C"], Z["NF"], Z["DAYS"]
print("C", len(C), "NF", len(NF), "DAYS", len(DAYS), DAYS.min().date(), DAYS.max().date())
print("C.entry range", C.entry.min(), C.entry.max(), " tickers", C.ticker.nunique(), " NF tickers", NF.ticker.nunique())
print("C columns", list(C.columns))
# ── 決算 ──
F = pd.read_pickle("_fins_history.pkl")
print("fins columns:", list(F.columns)[:20], "rows", len(F))
F2 = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)].copy()
F2["DiscDate"] = pd.to_datetime(F2.DiscDate)
print("fins FS rows", len(F2), "DiscDate", F2.DiscDate.min().date(), F2.DiscDate.max().date(), "codes", F2.Code.astype(str).str[:4].nunique())
print("fins FS rows by year:", F2.groupby(F2.DiscDate.dt.year).size().to_dict())
for c in ("DisclosedTime", "DiscTime", "DisclosedDate", "CurrentPeriodEndDate", "CurPerEnd"):
    if c in F.columns: print("  col", c, F[c].head(3).tolist())
# 開示日 vs 期末日の確認（期末から開示まで中央値が30〜45日ならDiscDateは開示日）
pe = [c for c in F.columns if "PeriodEnd" in c or "PerEnd" in c]
if pe:
    d = (F2.DiscDate - pd.to_datetime(F2[pe[0]], errors="coerce")).dt.days
    print("  開示日-期末日 中央値", d.median(), "分位", d.quantile([.1,.5,.9]).tolist())
# ── 信用残 ──
MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
k0 = next(iter(MG)); m = MG[k0]
print("margin keys", len(MG), "sample", k0, type(m), list(m.columns) if hasattr(m, "columns") else None)
idx = pd.to_datetime(m.index)
print("  sample idx", idx.min().date(), idx.max().date(), "weekday counts", pd.Series(idx.weekday).value_counts().to_dict())
allidx = pd.DatetimeIndex(np.concatenate([pd.to_datetime(v.index).values for v in MG.values() if len(v)]))
print("  all margin dates", allidx.min().date(), allidx.max().date(), "weekday", pd.Series(allidx.weekday).value_counts().to_dict())
# ── 立花日足 ──
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
print("tachibana tickers", len(ALL), f"{time.time()-t0:.0f}s")
ends = pd.Series({tk: df.index.max() for tk, df in ALL.items() if df is not None and len(df)})
starts = pd.Series({tk: df.index.min() for tk, df in ALL.items() if df is not None and len(df)})
print("  last bar: max", ends.max().date(), " 末尾が最終日より前の銘柄(上場廃止の痕跡?)", (ends < ends.max() - pd.Timedelta(days=10)).sum())
print("  start dates: 2001以前", (starts <= "2001-01-10").sum(), " 2017以降に始まる", (starts >= "2017-01-01").sum())
# カレンダー: 流動性の高い銘柄の index の和集合 vs DAYS
uni = None
for tk in ("7203.T", "8306.T", "6758.T", "9432.T", "8035.T", "6501.T", "7267.T", "9984.T"):
    df = ALL.get(tk)
    if df is None: continue
    ix = df.dropna(subset=["Close"]).index
    uni = ix if uni is None else uni.union(ix)
uni = uni[(uni >= DAYS.min()) & (uni <= DAYS.max())]
missing_in_days = uni.difference(DAYS); extra_in_days = DAYS.difference(uni)
print("  取引日(主力8銘柄の和集合)", len(uni), " DAYSに無い取引日", len(missing_in_days), list(missing_in_days[:10].date), " DAYSにあるが主力が取引していない日", len(extra_in_days), list(extra_in_days[:10].date))
# 分割調整の確認: 既知分割の前後で出来高が連続か（調整済なら 価格×出来高 が連続）
for tk, d0, f in (("7974.T", "2022-10-01", 10), ("9432.T", "2023-07-01", 25), ("6758.T", "2024-10-01", 5), ("6501.T", "2024-07-01", 5), ("7011.T", "2024-04-01", 10)):
    df = ALL.get(tk)
    if df is None: print("  no", tk); continue
    df = df.dropna(subset=["Close"])
    b = df[(df.index < d0)].tail(10); a = df[(df.index >= d0)].head(10)
    print(f"  {tk} 分割{f}:1 前10日 終値中央{b.Close.median():.0f} 出来高中央{b.Volume.median():,.0f} / 後10日 終値{a.Close.median():.0f} 出来高{a.Volume.median():,.0f} → 価格比{a.Close.median()/b.Close.median():.2f} 出来高比{a.Volume.median()/b.Volume.median():.2f}")
# 非整数の終値(=調整済みの痕跡)の割合
nonint = 0; tot = 0
for tk in list(ALL)[:300]:
    df = ALL[tk]
    if df is None or not len(df): continue
    c = df.Close.dropna().to_numpy(dtype=float); tot += len(c); nonint += (np.abs(c - np.round(c)) > 1e-6).sum()
print(f"  終値が整数でない割合(先頭300銘柄) {nonint/tot*100:.1f}%")
pickle.dump(dict(DAYS=DAYS, uni=uni), open(r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo\cal.pkl", "wb"))
print(f"done {time.time()-t0:.0f}s")

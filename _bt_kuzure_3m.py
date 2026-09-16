# 直近3ヶ月「本日崩れ」(📉モメンタム終了・アプリ表示条件そのまま)を翌日寄成ショート→引けで集計
import io, sys, pickle, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import numpy as np, pandas as pd
from screener import _jquants_id_token, batch_download_jquants, fetch_tse_universe, is_etf_ticker
from crash_short_watch import _iss_map
CACHE = "_bt_kuzure_3m_cache.pkl"
tok = _jquants_id_token()
if os.path.exists(CACHE):
    data, uni, iss = pickle.load(open(CACHE, "rb"))
else:
    data = batch_download_jquants(tok, start="2026-05-01", end="2026-09-16")
    uni = fetch_tse_universe(tok); iss = _iss_map(tok)
    pickle.dump((data, uni, iss), open(CACHE, "wb"))
name_map = dict(uni)
rows = []
for tk, df in data.items():
    nm = name_map.get(tk)
    if nm is None or is_etf_ticker(tk, nm) or len(df) < 30: continue
    df = df.sort_index()
    o = df["Open"].astype(float); c = df["Close"].astype(float); v = df["Volume"].astype(float)
    ma5 = c.rolling(5).mean()
    below = (c < ma5)
    for i in range(26, len(df) - 1):
        if c.iloc[i] <= 0 or c.iloc[i-1] <= 0: continue
        r1 = (c.iloc[i] / c.iloc[i-1] - 1) * 100
        ma5_dev = (c.iloc[i] / ma5.iloc[i] - 1) * 100
        down = c.iloc[i] < o.iloc[i]
        v20p = v.iloc[i-20:i].mean(); vol_x = v.iloc[i] / v20p if v20p > 0 else 0
        tov = c.iloc[i] * v.iloc[i-24:i+1].mean() / 1e8
        peak20 = c.iloc[i-19:i+1].max(); base = c.iloc[i-20]
        runup = (peak20 / base - 1) * 100 if base > 0 else 0
        if not (runup >= 15 and ma5_dev < 0 and (down or r1 <= -2) and vol_x >= 1.3 and tov >= 5 and c.iloc[i] >= 100): continue
        b5 = 0
        for back in range(0, 9):
            if i-back >= 0 and bool(below.iloc[i-back]): b5 += 1
            else: break
        o1, c1 = o.iloc[i+1], c.iloc[i+1]
        if not (o1 > 0 and c1 > 0): continue
        gap = (o1 / c.iloc[i] - 1) * 100
        ret = (o1 - c1) / o1 * 100
        r1n = (c1 / c.iloc[i] - 1) * 100
        rows.append(dict(d0=df.index[i].strftime("%Y-%m-%d"), code=tk[:4], name=nm, r1=r1, vol_x=vol_x, runup=runup,
                         below5=b5, iss=iss.get(tk[:4], "?"), gap=gap, ret=ret, tov=tov,
                         crash=(runup >= 30 and vol_x >= 2 and r1 <= -5 and b5 == 1)))
R = pd.DataFrame(rows)
R = R[R.d0 >= "2026-06-16"]
def summ(x, label):
    if len(x) == 0: print(f"{label}: 0件"); return
    g = x.ret[x.ret > 0].sum(); l = -x.ret[x.ret < 0].sum()
    print(f"{label}: n={len(x)} 勝率{(x.ret>0).mean()*100:.0f}% 平均{x.ret.mean():+.2f}%/件 中央{x.ret.median():+.2f}% PF{(g/l if l>0 else 99):.2f} 30万/件で{x.ret.sum()*3000:+,.0f}円 最悪{x.ret.min():+.1f}% 最良{x.ret.max():+.1f}%")
print("=== 直近3ヶ月(2026-06-16〜09-15検出・翌日寄成S→引け) ===")
summ(R, "本日崩れ+割れN日目 全件")
F = R[R.below5 <= 1]
summ(F, "「本日崩れ」(割れ初日) 全件")
summ(F[F.iss == "2"], "  うち貸借○(制度で撃てる)")
summ(F[F.iss != "2"], "  うち貸借✕/不明(HYPER頼み)")
summ(F[F.gap > -3], "  本日崩れ×寄りGD-3%より上(見送りルール適用)")
summ(F[F.gap <= -3], "  本日崩れ×寄りGD-3%以下(見送り分)")
summ(R[R.crash], "💥崩壊ショート条件(貸借問わず)")
summ(R[R.crash & (R.iss == "2")], "  💥×貸借○")
print("\n--- 月別(本日崩れ全件) ---")
F2 = F.copy(); F2["m"] = F2.d0.str[:7]
for m, g in F2.groupby("m"): summ(g, m)
print("\n--- 1日3件まで(出来高倍率順・アプリの並び)・30万/件 ---")
day = F.sort_values(["d0", "vol_x"], ascending=[True, False]).groupby("d0").head(3)
summ(day, "3件/日")
eq = day.groupby("d0").ret.sum() * 3000
print("日別損益の最悪日", f"{eq.min():+,.0f}円", "最良日", f"{eq.max():+,.0f}円", "シグナル日数", len(eq))
print("\n--- ワースト10(本日崩れ) ---")
print(F.sort_values("ret").head(10)[["d0","code","name","r1","gap","ret","iss"]].to_string(index=False))
print("\n--- ベスト10 ---")
print(F.sort_values("ret", ascending=False).head(10)[["d0","code","name","r1","gap","ret","iss"]].to_string(index=False))
R.to_csv("_bt_kuzure_3m_events.csv", index=False, encoding="utf-8-sig")

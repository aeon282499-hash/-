# -*- coding: utf-8 -*-
"""_bt_gokujo_round10b_holiday_0919.py — R10の追試: 「保有窓に祝日が入る玉」と「日経3日連続陰線」（2026-09-19）
R10 F段: 保有中の暦日数 5日以上(=祝日を跨ぐ) n=978 +0.70%/勝率57.8% vs 2日 -0.03% / 週末のみ -0.13%。祝日は事前に分かる＝先読みではない。
検証: 時代別・年別の安定性 / 祝日窓だけ建てる / 祝日窓なしを除外 / サイズ×1.5・×0.5 / 祝日の位置(3連休前=木金建て vs 平日中の祝日) / 噪音床
実行: python -X utf8 _bt_gokujo_round10b_holiday_0919.py > _log_gokujo_round10b_holiday_0919.txt"""
import numpy as np, pandas as pd, json, time, pickle, re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read()
exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))

# ── 暦: 実際の営業日カレンダー＝立花全銘柄の日付和集合（候補日でなく市場の営業日） ──
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
tdays = set()
for tk, df in ALL.items():
    if len(df) > 1000: tdays.update(df.index[df.Close > 0].normalize().to_list())
del ALL
tdays = np.array(sorted(tdays)); tpos = {d: k for k, d in enumerate(tdays)}
ent_ts = pd.to_datetime(C.entry.to_numpy())
ent_k = np.array([tpos.get(d, -1) for d in ent_ts])
ok = ent_k >= 0
exit_ts = np.array([tdays[min(k + 2, len(tdays) - 1)] if k >= 0 else pd.NaT for k in ent_k], dtype="datetime64[ns]")
sig_ts = np.array([tdays[k - 1] if k >= 1 else pd.NaT for k in ent_k], dtype="datetime64[ns]")
holdcal = np.where(ok, (exit_ts - ent_ts.to_numpy()).astype("timedelta64[D]").astype(float), np.nan)
gapdays = np.where(ok, (ent_ts.to_numpy() - sig_ts).astype("timedelta64[D]").astype(float), np.nan)
wd = ent_ts.weekday.to_numpy()
# 祝日を跨ぐ = 暦日数が「平日だけの窓」より長い。平日窓: 月火水建て=2日・木金建て=4日
plain = np.where(wd <= 2, 2, 4)
HOL = ok & (holdcal > plain)              # 保有窓に祝日(または年末年始)が入る
HOL3 = HOL & (wd >= 3)                    # 3連休を跨ぐ（木/金建て）
HOLW = HOL & (wd <= 2)                    # 平日の祝日を跨ぐ
print(f"[cal] 営業日{len(tdays)}日 候補の建て日一致{ok.sum()}/{n} 極上候補{G.sum()} 祝日跨ぎ{(G&HOL).sum()} (3連休{(G&HOL3).sum()}/平日祝{(G&HOLW).sum()})", flush=True)

def pm(lab, m, arr=None):
    a = PNL1 if arr is None else arr
    print(f"    {lab:<34} n={int(np.sum(m)):>5} 件あたり{np.nanmean(a[m]):>+6.2f}% 勝率{np.mean(a[m]>0)*100:>5.1f}%", flush=True)
def jl(r, b): return " ★" if (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["dd"] >= b["dd"] - 1 and r["worst"] >= b["worst"] - 1) else ""
def base_run(mask=None, mult=None, pnl=None, exo=None, size=2_000_000):
    return run_gen(G if mask is None else mask, SCORE, slots=1, size=size, addon_frac=1.0, mult=mult, pnl=pnl, exo=exo)
rng26 = np.random.default_rng(2)
def noise26(frac, seeds=20):
    idx = np.where(G)[0]; k = int(len(idx) * frac); tots = []
    for s in range(seeds):
        mm = G.copy(); mm[rng26.choice(idx, size=k, replace=False)] = False; tots.append(metrics(base_run(mm))["tot"])
    return float(np.quantile(tots, 0.99)), float(np.std(tots))

print("\n■ 土台", flush=True); B0 = show("土台 1×200＋同額乗せ", base_run(), 400)

print("\n■ 候補レベル（極上候補 3,778件）", flush=True)
pm("祝日跨ぎなし", G & ok & ~HOL); pm("祝日跨ぎあり", G & HOL); pm("  うち3連休(木金建て)", G & HOL3); pm("  うち平日の祝日", G & HOLW)
pm("週末跨ぎ(木金建て・祝日なし)", G & ok & ~HOL & (wd >= 3)); pm("平日のみ(月火水建て・祝日なし)", G & ok & ~HOL & (wd <= 2))
print("  時代別（祝日跨ぎ あり/なし）")
for a, b, era in ((2001, 2008, "01-08"), (2009, 2016, "09-16"), (2017, 2021, "17-21"), (2022, 2026, "22-26")):
    ye = (YEARv >= a) & (YEARv <= b); h = G & HOL & ye; o = G & ok & ~HOL & ye
    print(f"    {era}: あり n={h.sum():>4} {np.nanmean(PNL1[h]):+.2f}% (勝率{np.mean(PNL1[h]>0)*100:.0f}%) / なし n={o.sum():>4} {np.nanmean(PNL1[o]):+.2f}% (勝率{np.mean(PNL1[o]>0)*100:.0f}%)")
print("  年別（あり−なし の差・%）: " + " ".join(f"{y}:{np.nanmean(PNL1[G&HOL&(YEARv==y)])-np.nanmean(PNL1[G&ok&~HOL&(YEARv==y)]):+.1f}" for y in range(2001, 2027) if (G&HOL&(YEARv==y)).sum() >= 5))
# 祝日の種類: 年末年始 / GW / その他
mon = ent_ts.month.to_numpy(); day_ = ent_ts.day.to_numpy()
YE = HOL & ((mon == 12) | ((mon == 1) & (day_ <= 6))); GW = HOL & (((mon == 4) & (day_ >= 26)) | ((mon == 5) & (day_ <= 7))); OT = HOL & ~YE & ~GW
pm("年末年始跨ぎ", G & YE); pm("GW跨ぎ", G & GW); pm("その他の祝日跨ぎ", G & OT)
# 祝日「前日」建て（保有初日の翌暦日が休み）と 祝日「明け」建て（gapdays>plain）
plain_gap = np.where(wd == 0, 3, 1); AFTER = ok & (gapdays > plain_gap)
pm("祝日明けに建てる", G & AFTER); pm("祝日明けでない", G & ok & ~AFTER)

print("\n■ 枠シム（26年・1×200＋同額乗せ）", flush=True)
for lab, keep in (("祝日跨ぎの玉だけ建てる", HOL), ("祝日跨ぎなしを除外(=同じ)", HOL), ("祝日明け建てを除外", ~AFTER), ("3連休跨ぎだけ", HOL3)):
    mm = G & keep; frac = 1 - mm.sum() / G.sum(); r = show(lab, base_run(mm), 400)
    nf, sdv = noise26(frac) if 0 < frac < 0.9 else (np.nan, 0)
    print(f"      判定:{jl(r, B0) or ' —'} 除外率{frac*100:.0f}%・ランダム99%点{nf:+.0f}万(sd{sdv:.0f}) → {'噪音超え' if r['tot'] > nf else '噪音内'}", flush=True)
    if lab.startswith("祝日跨ぎなしを除外"): break
print("  ── サイズ×（祝日跨ぎ ×1.5 / なし ×0.5 など・年利は最大建玉で）", flush=True)
for lab, hi_m, lo_m in (("祝日跨ぎ×1.5/なし×1", 1.5, 1.0), ("祝日跨ぎ×1/なし×0.5", 1.0, 0.5), ("祝日跨ぎ×1.5/なし×0.5", 1.5, 0.5), ("祝日跨ぎ×2/なし×0.5", 2.0, 0.5)):
    mult = np.where(HOL, hi_m, lo_m); r = show(lab, base_run(mult=mult), 400)
    print(f"      年利(最大建玉{r['maxexpo']:.0f}万基準){r['tot']/r['maxexpo']/26*100:+.1f}% 利益/DD {r['tot']/-r['dd']:.2f}(土台{B0['tot']/-B0['dd']:.2f}) 判定:{jl(r, B0) or ' —'}", flush=True)

print("\n■ 日経 3日以上連続陰線（R10 C: -0.15%/件）", flush=True)
NK = pd.read_pickle("_nk225_yf.pkl").dropna(); NK.index = pd.to_datetime(NK.index); nkc = NK.to_numpy(float)
nk_ds = np.zeros(len(nkc))
for k in range(1, len(nkc)): nk_ds[k] = nk_ds[k - 1] + 1 if nkc[k] < nkc[k - 1] else 0
nkpos = {d: k for k, d in enumerate(NK.index)}
DS = np.array([nk_ds[nkpos[pd.Timestamp(s)]] if (s == s and pd.Timestamp(s) in nkpos) else np.nan for s in sig_ts])
for th in (3, 4):
    keep = ~(np.isfinite(DS) & (DS >= th)); mm = G & keep; frac = 1 - mm.sum() / G.sum()
    pm(f"日経連続陰線{th}+ の候補", G & ~keep)
    r = show(f"日経連続陰線{th}+ の日は見送り", base_run(mm), 400); nf, sdv = noise26(frac)
    print(f"      判定:{jl(r, B0) or ' —'} 除外率{frac*100:.0f}%・ランダム99%点{nf:+.0f}万(sd{sdv:.0f}) → {'噪音超え' if r['tot'] > nf else '噪音内'}", flush=True)
print(f"\n[done] {time.time()-t0:.0f}s")

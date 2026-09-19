# -*- coding: utf-8 -*-
"""_bt_gokujo_round10_newviews_0919.py — 極上ラウンド10「見たことない視点」（2026-09-19 本人「未検証もあるでしょ・死ぬよ・改善しないと」）
これまで=銘柄の指標(スコア/財務/信用/BB/ストキャス…)・市場の水準(VIX/TOPIXゲート)・出口セル・サイズ。
今回=**まだ一度も当てていない切り口**を26年（立花日足・_bt_buy_20y_wide.pkl・極上新ルール土台）で:
 A シグナル日の「足の形」: 終値の位置(CLV=(C-L)/(H-L))・当日リターン・ギャップ・実体・下ヒゲ・連続陰線・20日安値更新・5日リターン
 A2 全市場の横断順位: シグナル日の当日リターンが全上場銘柄の中で何%点か（個別ショック vs 市場ぐるみ）
 B 損切りの機構: 日中タッチ(-3%・現行) → 終値ベース(-3%引け判定→翌寄り処分) / 日中-5%＋終値-3% / 勝ち乗せ後は建値で撤退
 C 市場の短期売られすぎ(日経225・26年): 1日/3日/5日リターン・RSI3・20日高値からのDD・連続陰線 → 三分位/ゲート/サイズ×
 D 公募増資・届出書(EDINET 2022-26のみ): 有価証券届出書(030)を直近10日に出した銘柄の除外 / 臨時報告書(180)直近3日
 E 資産曲線サイズ: 直近20玉の合計がマイナスなら半額 / プラスなら1.5倍
 F 連休明け・休日跨ぎ: 建て日と前営業日の暦日差(3日以上=連休明け)・保有中の暦日数
判定=4分割(01-13/14-26/17-21/22-26)すべて現行超え＋DD/最悪年が悪化しない＋除外系は同率ランダム除外の99%点(26年・20シード)超え。
実行: python -X utf8 _bt_gokujo_round10_newviews_0919.py > _log_gokujo_round10_newviews_0919.txt"""
import numpy as np, pandas as pd, json, time, pickle, gc
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read()
exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
exec(r9.split("TICKv = C.ticker.to_numpy()")[1].split("print(\"\\n■ A 資金構成")[0].replace("", ""), globals()) if False else None
# run_gen/metrics/show を round9 から取り出す
import re
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))

t1 = time.time()
# ══ 特徴量: シグナル日の足の形（立花20年日足） ══
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
F = {k: np.full(n, np.nan) for k in ("clv", "r1", "gap", "body", "lshadow", "dstreak", "low20", "r5", "rng_ratio", "vol_r1")}
need = {}
for i in range(n): need.setdefault(C.ticker.iat[i], []).append((C.entry.iat[i], i))
big_date = []; big_code = []; big_ret = []
for tk, rows in need.items():
    df = ALL.get(tk)
    if df is None: continue
    df = df.dropna(subset=["Close"]); df = df[df.Close > 0]
    o = df.Open.to_numpy(float); h = df.High.to_numpy(float); l = df.Low.to_numpy(float); c = df.Close.to_numpy(float)
    pos = {d.strftime("%Y-%m-%d"): k for k, d in enumerate(df.index)}
    for entd_, i in rows:
        k = pos.get(entd_)
        if k is None or k < 25: continue
        t = k - 1
        rng = h[t] - l[t]
        F["clv"][i] = (c[t] - l[t]) / rng if rng > 0 else 0.5
        F["r1"][i] = c[t] / c[t - 1] - 1
        F["gap"][i] = o[t] / c[t - 1] - 1 if o[t] > 0 else np.nan
        F["body"][i] = (c[t] - o[t]) / o[t] if o[t] > 0 else np.nan
        F["lshadow"][i] = (min(o[t], c[t]) - l[t]) / c[t]
        s_ = 0
        for j in range(t, max(t - 10, 0), -1):
            if c[j] < c[j - 1]: s_ += 1
            else: break
        F["dstreak"][i] = s_
        F["low20"][i] = 1.0 if l[t] <= l[t - 19:t].min() else 0.0
        F["r5"][i] = c[t] / c[t - 5] - 1
        atr5 = np.mean((h[t - 4:t + 1] - l[t - 4:t + 1]) / c[t - 4:t + 1]); atr20 = np.mean((h[t - 19:t + 1] - l[t - 19:t + 1]) / c[t - 19:t + 1])
        F["rng_ratio"][i] = atr5 / atr20 if atr20 > 0 else np.nan
# 全市場の横断順位（当日リターン）: 全銘柄×全日の1日リターンを積んで日ごとのパーセンタイル
for tk, df in ALL.items():
    df = df.dropna(subset=["Close"]); df = df[df.Close > 0]
    if len(df) < 30: continue
    c = df.Close.to_numpy(np.float32); r = c[1:] / c[:-1] - 1
    big_date.append(df.index.to_numpy()[1:]); big_ret.append(r); big_code.append(np.full(len(r), hash(tk) & 0x7fffffff, dtype=np.int64))
del ALL; gc.collect()
BIG = pd.DataFrame({"date": np.concatenate(big_date), "code": np.concatenate(big_code), "ret": np.concatenate(big_ret)})
del big_date, big_code, big_ret; gc.collect()
BIG["pct"] = BIG.groupby("date").ret.rank(pct=True).astype(np.float32)
BIG = BIG.set_index(["code", "date"]).pct
sig_dates = pd.to_datetime(C.entry.to_numpy())   # entry=建て日 → シグナル日は前営業日
# シグナル日の日付: days の1つ前
prev_day = {days[k]: days[k - 1] for k in range(1, len(days))}
sd = pd.to_datetime([prev_day.get(d, d) for d in C.entry.to_numpy()])
codes = np.array([hash(t) & 0x7fffffff for t in C.ticker.to_numpy()], dtype=np.int64)
key_idx = pd.MultiIndex.from_arrays([codes, sd])
F["xpct"] = BIG.reindex(key_idx).to_numpy(np.float64)
del BIG; gc.collect()
print(f"[feat] 足の形 既知{np.isfinite(F['clv'][G]).sum()}/{G.sum()} 横断順位 既知{np.isfinite(F['xpct'][G]).sum()} ({time.time()-t1:.0f}s)", flush=True)

# ══ 日経225（26年・yfinance）: シグナル日時点 ══
NK = pd.read_pickle("_nk225_yf.pkl").dropna(); NK.index = pd.to_datetime(NK.index)
nkc = NK.to_numpy(float); nkd = NK.index
nk_r1 = pd.Series(nkc).pct_change().to_numpy(); nk_r3 = pd.Series(nkc).pct_change(3).to_numpy(); nk_r5 = pd.Series(nkc).pct_change(5).to_numpy()
nk_dd20 = (nkc / pd.Series(nkc).rolling(20).max().to_numpy() - 1)
dlt = np.diff(nkc, prepend=np.nan); up = pd.Series(np.where(dlt > 0, dlt, 0)).rolling(3).mean(); dn = pd.Series(np.where(dlt < 0, -dlt, 0)).rolling(3).mean()
nk_rsi3 = (100 - 100 / (1 + up / dn.replace(0, np.nan))).to_numpy()
nk_ds = np.zeros(len(nkc))
for k in range(1, len(nkc)): nk_ds[k] = nk_ds[k - 1] + 1 if nkc[k] < nkc[k - 1] else 0
nkpos = {d: k for k, d in enumerate(nkd)}
def nk_at(arr):
    out = np.full(n, np.nan)
    for i in range(n):
        k = nkpos.get(sd[i])
        if k is not None: out[i] = arr[k]
    return out
M = {"nk_r1": nk_at(nk_r1), "nk_r3": nk_at(nk_r3), "nk_r5": nk_at(nk_r5), "nk_dd20": nk_at(nk_dd20), "nk_rsi3": nk_at(nk_rsi3), "nk_ds": nk_at(nk_ds)}
print(f"[feat] 日経 既知{np.isfinite(M['nk_r1'][G]).sum()}/{G.sum()}", flush=True)

# ══ 暦: 建て日と前営業日の暦日差・保有中の暦日数 ══
ent_ts = pd.to_datetime(C.entry.to_numpy()); gapdays = (ent_ts - pd.to_datetime(sd)).days.to_numpy()
exit3 = pd.to_datetime([days[min(dayidx[d] + 2, len(days) - 1)] for d in C.entry.to_numpy()]); holdcal = (exit3 - ent_ts).days.to_numpy()

# ══ EDINET 2022-26 ══
ED = json.load(open("edinet_docs_2022_2026.json", encoding="utf-8"))
ed030 = {}; ed180 = {}
for d_, lst in ED.items():
    for x in lst:
        sc = x.get("secCode") or ""
        if len(sc) < 4: continue
        tk = sc[:4] + ".T"
        (ed030 if x.get("docTypeCode") == "030" else ed180 if x.get("docTypeCode") == "180" else {}).setdefault(tk, []).append(pd.Timestamp(d_))
for dct in (ed030, ed180):
    for k in dct: dct[k] = np.array(sorted(dct[k]))
def recent(dct, within):
    out = np.zeros(n, dtype=bool)
    for i in range(n):
        arr = dct.get(C.ticker.iat[i])
        if arr is None: continue
        e = sd[i]; k = np.searchsorted(arr, e, side="right")
        if k > 0 and (e - arr[k - 1]).days <= within: out[i] = True
    return out
E030 = recent(ed030, 10); E180 = recent(ed180, 3)
V22 = np.asarray(ent_ts >= pd.Timestamp("2022-01-15"))
print(f"[feat] EDINET 2022-26 極上候補{(G&V22).sum()}件 届出書10日内{(G&V22&E030).sum()} 臨報3日内{(G&V22&E180).sum()}", flush=True)

# ══ 共通 ══
def base_run(mask=None, mult=None, pnl=None, exo=None, size=2_000_000):
    return run_gen(G if mask is None else mask, SCORE, slots=1, size=size, addon_frac=1.0, mult=mult, pnl=pnl, exo=exo)
rng26 = np.random.default_rng(1)
def noise26(frac, seeds=20):
    idx = np.where(G)[0]; k = int(len(idx) * frac); tots = []
    for s in range(seeds):
        m = G.copy(); m[rng26.choice(idx, size=k, replace=False)] = False; tots.append(metrics(base_run(m))["tot"])
    return float(np.quantile(tots, 0.99)), float(np.std(tots))
def pm(lab, m):
    print(f"    {lab:<30} n={int(np.sum(m)):>5} 件あたり{np.nanmean(PNL1[m]):>+6.2f}% 勝率{np.mean(PNL1[m]>0)*100:>5.1f}%", flush=True)
def jl(r, b):
    return " ★" if (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["dd"] >= b["dd"] - 1 and r["worst"] >= b["worst"] - 1) else ""
def excl_test(lab, keep):
    """keep=True の候補だけ残す（未知は残す）→ 26年枠シム＋同率ランダム除外の噪音床"""
    m = G & keep; frac = 1 - m.sum() / G.sum()
    r = show(lab, base_run(m), 400); j = jl(r, B0)
    if 0 < frac < 0.9 and (r["tot"] > B0["tot"]):
        nf, sdv = noise26(frac); j += f"  除外率{frac*100:.0f}%・ランダム99%点{nf:+.0f}万(sd{sdv:.0f}) → {'噪音超え' if r['tot'] > nf else '噪音内'}"
    elif 0 < frac:
        j += f"  除外率{frac*100:.0f}%"
    print(f"      判定:{j or ' —'}", flush=True); return r
def tercile(nm, x, low_lab="下位1/3", hi_lab="上位1/3"):
    ok = G & np.isfinite(x); q1, q2 = np.nanquantile(x[ok], [1/3, 2/3])
    lo, mid, hi = ok & (x <= q1), ok & (x > q1) & (x <= q2), ok & (x > q2)
    print(f"\n■ {nm}: 既知{ok.sum()}件 三分位 {q1:+.3g}/{q2:+.3g}", flush=True)
    pm(low_lab, lo); pm("中位", mid); pm(hi_lab, hi)
    for a, b, era in ((2001, 2013, "01-13"), (2014, 2026, "14-26")):
        ye = (YEARv >= a) & (YEARv <= b)
        print(f"      {era}: 下{np.nanmean(PNL1[lo&ye]):+.2f}% 中{np.nanmean(PNL1[mid&ye]):+.2f}% 上{np.nanmean(PNL1[hi&ye]):+.2f}%")
    excl_test(f"{nm} 上位1/3を除外", ~hi); excl_test(f"{nm} 下位1/3を除外", ~lo)

print("\n■ 土台（極上 新ルール・1×200＋同額乗せ・26年）", flush=True)
B0 = show("土台", base_run(), 400)

print("\n════ A シグナル日の足の形 ════", flush=True)
tercile("CLV 終値の位置(0=安値引け,1=高値引け)", F["clv"], "安値引け側", "高値引け側")
tercile("当日リターン r1", F["r1"], "大きく下げた", "小さい下げ/上げ")
tercile("寄りギャップ(シグナル日)", F["gap"])
tercile("実体 (C-O)/O", F["body"], "大陰線", "陽線側")
tercile("下ヒゲ", F["lshadow"], "ヒゲなし", "長い下ヒゲ")
tercile("5日リターン r5", F["r5"], "5日で大きく下げ", "5日では下げてない")
tercile("レンジ比 ATR5/ATR20", F["rng_ratio"], "静か", "荒れている")
print("\n■ 連続陰線数・20日安値更新（離散）", flush=True)
for s_ in (0, 1, 2, 3): pm(f"連続陰線={s_}{'+' if s_==3 else ''}", G & (F["dstreak"] >= s_ if s_ == 3 else F["dstreak"] == s_))
pm("20日安値を更新", G & (F["low20"] == 1)); pm("更新なし", G & (F["low20"] == 0))
excl_test("連続陰線0(=当日は上げ)を除外", ~(F["dstreak"] == 0)); excl_test("連続陰線3+を除外", ~(F["dstreak"] >= 3)); excl_test("20日安値更新を除外", ~(F["low20"] == 1)); excl_test("20日安値更新のみ", (F["low20"] == 1) | ~np.isfinite(F["low20"]))

print("\n════ A2 全市場の横断順位（シグナル日の当日リターンの%点・0=全市場最弱） ════", flush=True)
xp = F["xpct"]
tercile("横断順位 xpct", xp, "全市場で最弱側", "市場並み")
for lab, m in (("下位1%", xp <= 0.01), ("下位1-5%", (xp > 0.01) & (xp <= 0.05)), ("下位5-20%", (xp > 0.05) & (xp <= 0.2)), ("20%超", xp > 0.2)): pm(lab, G & m)
excl_test("下位1%(個別ショック)を除外", ~(xp <= 0.01)); excl_test("下位5%を除外", ~(xp <= 0.05)); excl_test("20%超(市場ぐるみでない)を除外", ~(xp > 0.2))

print("\n════ B 損切りの機構（26年・1×200＋同額乗せ） ════", flush=True)
def replay_stop(mode, stop=3.0, dstop=5.0, tp=5.0, hold=3, rsith=50.0, be_after_addon=False):
    """mode: 'intraday'=現行(安値タッチ) / 'close'=終値が-stop%以下→翌寄り処分(最終日は引け) / 'both'=日中-dstop%タッチ＋終値-stop%
    be_after_addon: 乗せ条件成立(初日終値>建値+1%)後、2日目以降に安値が建値以下→建値で撤退(本玉±0・乗せ玉は建値-乗せ値)。"""
    valid = np.isfinite(E0) & np.isfinite(CL[:, hold - 1]); pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = E0 * (1 - stop / 100); dl = E0 * (1 - dstop / 100); tl = E0 * (1 + tp / 100)
    pend = np.zeros(n, dtype=bool)   # 終値割れ→翌寄り処分待ち
    addon_on = (CL[:, 0] > E0 * 1.01)
    for k in range(hold):
        live = ~done
        if k > 0:
            op = OP[:, k]; okop = np.isfinite(op) & (op > 0)
            g = live & pend & okop; pnl[g] = (op[g] - E0[g]) / E0[g] * 100; exo[g] = k; done |= g; live = ~done; pend[:] = False
            g = live & okop & ((op <= (sl if mode == "intraday" else dl)) | (op >= tl)); pnl[g] = (op[g] - E0[g]) / E0[g] * 100; exo[g] = k; done |= g; live = ~done
            if be_after_addon:
                b = live & addon_on & (LO[:, k] <= E0); pnl[b] = 0.0; exo[b] = k; done |= b; live = ~done
        if mode == "intraday":
            s = live & (LO[:, k] <= sl); pnl[s] = -stop; exo[s] = k; done |= s; live = ~done
        elif mode == "both":
            s = live & (LO[:, k] <= dl); pnl[s] = -dstop; exo[s] = k; done |= s; live = ~done
        t_ = live & (HI[:, k] >= tl); pnl[t_] = tp; exo[t_] = k; done |= t_; live = ~done
        if mode in ("close", "both"):
            cbad = live & (CL[:, k] <= sl)
            if k == hold - 1: pnl[cbad] = (CL[cbad, k] - E0[cbad]) / E0[cbad] * 100; exo[cbad] = k; done |= cbad; live = ~done
            else: pend |= cbad
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]); r_ = live & (rc | (k == hold - 1)) & ~pend
        pnl[r_] = (CL[r_, k] - E0[r_]) / E0[r_] * 100; exo[r_] = k; done |= r_
        # pend で最終日以外は翌寄りへ持ち越し（RSI回復していても引けで損失確定より翌寄り処分を優先）
    return pnl, exo
def d1(pnl, exo): p, e, _ = day1cut(pnl, exo); return p, e
for lab, kw in (("現行 日中-3%タッチ", dict(mode="intraday")), ("終値-3%→翌寄り処分", dict(mode="close")), ("終値-2%→翌寄り処分", dict(mode="close", stop=2.0)),
                ("日中-5%＋終値-3%", dict(mode="both")), ("日中-4%＋終値-3%", dict(mode="both", dstop=4.0)), ("日中-6%＋終値-2.5%", dict(mode="both", dstop=6.0, stop=2.5)),
                ("現行＋乗せ後は建値で撤退", dict(mode="intraday", be_after_addon=True)), ("日中-5%＋終値-3%＋乗せ後建値撤退", dict(mode="both", be_after_addon=True))):
    p, e = d1(*replay_stop(**kw)); r = show(lab, base_run(pnl=p, exo=e), 400); print(f"      判定:{jl(r, B0) or ' —'}")

print("\n════ C 市場の短期売られすぎ（日経225・シグナル日引け時点） ════", flush=True)
tercile("日経 当日リターン", M["nk_r1"], "日経が大きく下げた日", "日経は下げてない")
tercile("日経 3日リターン", M["nk_r3"]); tercile("日経 5日リターン", M["nk_r5"])
tercile("日経 20日高値からのDD", M["nk_dd20"], "深い", "高値圏")
tercile("日経 RSI3", M["nk_rsi3"], "売られすぎ", "買われすぎ")
print("\n■ 日経 連続陰線（離散）");
for s_ in (0, 1, 2, 3): pm(f"日経連続陰線={s_}{'+' if s_==3 else ''}", G & (M["nk_ds"] >= s_ if s_ == 3 else M["nk_ds"] == s_))
print("\n■ サイズ×（三分位で ×0.5/×1/×1.5・最大建玉が変わるので年利は最大建玉で見る）", flush=True)
for nm, x, low_big in (("日経RSI3 低いほど厚く", M["nk_rsi3"], True), ("日経5日リターン 低いほど厚く", M["nk_r5"], True), ("日経DD20 深いほど厚く", M["nk_dd20"], True), ("日経当日 下げた日ほど厚く", M["nk_r1"], True)):
    ok = np.isfinite(x); q1, q2 = np.nanquantile(x[G & ok], [1/3, 2/3]); mult = np.ones(n)
    mult[ok & (x <= q1)] = 1.5 if low_big else 0.5; mult[ok & (x > q2)] = 0.5 if low_big else 1.5
    r = show(nm, base_run(mult=mult), 400); print(f"      年利(最大建玉基準){r['tot']/r['maxexpo']/26*100:+.1f}% 判定:{jl(r, B0) or ' —'}")

print("\n════ D 公募増資・届出書（EDINET・2022-26のみ＝観察） ════", flush=True)
G22 = G & V22
pm("届出書(030)10日内あり", G22 & E030); pm("なし", G22 & ~E030); pm("臨時報告書(180)3日内あり", G22 & E180); pm("なし", G22 & ~E180)
def show22(lab, mask):
    R = base_run(mask); R = R[R.y >= 2022]; r = metrics(R); print(f"  {lab:<34} 玉{r['n']:>4} 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 22-26{r['tot']:>+6,.0f}万 DD{r['dd']:>+5,.0f} 最悪月{r['wm']:>+4,.0f}", flush=True)
show22("土台 22-26", G); show22("届出書10日内を除外", G & ~E030); show22("臨時報告書3日内を除外", G & ~E180); show22("両方除外", G & ~E030 & ~E180)

print("\n════ E 資産曲線サイズ（直近20玉の合計で ×0.5 / ×1.5・逐次） ════", flush=True)
R0 = base_run(); yen_seq = R0.yen.to_numpy(); ii = R0.i.to_numpy()
for lab, lo_m, hi_m in (("負けてたら半額", 0.5, 1.0), ("勝ってたら1.5倍", 1.0, 1.5), ("両方", 0.5, 1.5), ("逆張り: 負けてたら1.5倍", 1.5, 1.0)):
    mult = np.ones(n)
    for k in range(20, len(ii)):
        s20 = yen_seq[k - 20:k].sum(); mult[ii[k]] = lo_m if s20 < 0 else hi_m
    r = show(f"直近20玉 {lab}", base_run(mult=mult), 400); print(f"      年利(最大建玉基準){r['tot']/r['maxexpo']/26*100:+.1f}% 判定:{jl(r, B0) or ' —'}")

print("\n════ F 暦（連休明け・保有中の暦日数） ════", flush=True)
for lab, m in (("建て日=通常(前営業日と1日差)", gapdays == 1), ("連休明け(3日差)", gapdays == 3), ("4日以上", gapdays >= 4)): pm(lab, G & m)
for lab, m in (("保有中の暦日=2(平日3日)", holdcal == 2), ("週末を跨ぐ(4日)", holdcal == 4), ("5日以上", holdcal >= 5)): pm(lab, G & m)
excl_test("連休明け(3日以上)を除外", ~(gapdays >= 3)); excl_test("週末跨ぎ(4日以上)を除外", ~(holdcal >= 4))
print(f"\n[done] {time.time()-t0:.0f}s")

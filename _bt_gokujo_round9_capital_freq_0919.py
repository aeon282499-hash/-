# -*- coding: utf-8 -*-
"""_bt_gokujo_round9_capital_freq_0919.py — 極上ラウンド9（2026-09-19 本人「極上を仕上げたい・理想は3日1回×150万・買いで使えるのは300〜400万」）
 A 資金構成: 最大建玉≤400万に収まる構成を総当たり（1枠×サイズ／勝ち乗せ有無・比率／2〜3枠）
 B 頻度: 現行37玉/年→「3日1回(≈80玉/年)」に近づける方法（vt5緩和／枠数／極上優先＋空き日は極み候補で埋める／埋め玉は半額）
 C 部品の重ね掛け: 26年データがある★候補＝利確3%(9/14)・ストキャス%K>50除外(R8)を土台と積む
 D BとCの組み合わせ
土台＝round6〜8と同一（極上 新ルール: DAY1CUT＋2日目勝ち乗せ同額・1×150万）。26年=_bt_buy_20y_wide.pkl・立花日足。
判定＝現行(1×150+同額乗せ)に対し 4分割(01-13/14-26/17-21/22-26)すべて上回り・DD/最悪年が悪化しない・同資金なら年利で比較。
実行: python -X utf8 _bt_gokujo_round9_capital_freq_0919.py > _log_gokujo_round9_capital_freq_0919.txt"""
import numpy as np, pandas as pd, json, time, pickle
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read()
head = src.split("# ── A 決算日近接 ──")[0]
exec(head)

t1 = time.time()
# ── ストキャス%K（R8と同じ定義・シグナル日引け） ──
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
SK = np.full(n, np.nan); need = {}
for i in range(n): need.setdefault(C.ticker.iat[i], []).append((C.entry.iat[i], i))
for tk, rows in need.items():
    df = ALL.get(tk)
    if df is None: continue
    df = df.dropna(subset=["Close"]); df = df[df.Close > 0]
    c = df.Close.to_numpy(float); h = df.High.to_numpy(float); l = df.Low.to_numpy(float)
    hh = pd.Series(h).rolling(14).max().to_numpy(); ll = pd.Series(l).rolling(14).min().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        fk = (c - ll) / np.where(hh - ll > 0, hh - ll, np.nan) * 100
    sk = pd.Series(fk).rolling(3).mean().to_numpy()
    pos = {d.strftime("%Y-%m-%d"): k for k, d in enumerate(df.index)}
    for entd_, i in rows:
        k = pos.get(entd_)
        if k is None or k < 25: continue
        SK[i] = sk[k - 1]
del ALL
K50 = ~(np.isfinite(SK) & (SK > 50))     # %K>50 を除外（未知は残す）
print(f"[feat] %K 既知{np.isfinite(SK[G]).sum()}/{G.sum()}件 ({time.time()-t1:.0f}s)", flush=True)

# ── 利確3%版の損益（同じ悲観順序で再生 → DAY1CUT） ──
PNL_T3_raw, EXO_T3_raw = replay(tp=3.0)
PNL3, EXO3, _ = day1cut(PNL_T3_raw, EXO_T3_raw)

TICKv = C.ticker.to_numpy(); SECv = np.array([SECMAP.get(t) or f"__u{t}" for t in TICKv], dtype=object)
DATEv = pd.to_datetime(C.entry.to_numpy())

def run_gen(mask, key, slots, size, mult=None, addon_frac=0.0, th=1.0, pnl=None, exo=None, max_sig=5, cap=3):
    """選定=run_idx と同一（1日5件・業種cap3・保有中除外）。枠=slots・1玉=size×mult[i]・勝ち乗せ=size×mult×addon_frac（初日終値>建値+th%で2日目寄り・本玉と同時手仕舞い）。"""
    pnl = PNL1 if pnl is None else pnl; exo = EXO1 if exo is None else exo
    mult = np.ones(n) if mult is None else mult
    ou = {}; os_ = {}; picks = []
    for d in range(len(days)):
        for tk in [t for t, u in ou.items() if u < d]: del ou[tk]; del os_[tk]
        sc = {}
        for s in os_.values(): sc[s] = sc.get(s, 0) + 1
        cnt = 0; idxs = day_groups[d]; idxs = idxs[mask[idxs]]; idxs = idxs[np.argsort(-key[idxs], kind="stable")]
        for i in idxs:
            if cnt >= max_sig: break
            if not np.isfinite(pnl[i]) or TICKv[i] in ou: continue
            s = SECv[i]
            if sc.get(s, 0) >= cap: continue
            cnt += 1; ex = min(d + int(exo[i]), len(days) - 1); ou[TICKv[i]] = ex; os_[TICKv[i]] = s; sc[s] = sc.get(s, 0) + 1; picks.append((d, ex, i))
    exit_px = E0 * (1 + pnl / 100); o2 = OP[:, 1]
    add_ok = (CL[:, 0] > E0 * (1 + th / 100)) & (exo >= 1) & np.isfinite(o2) & (o2 > 0)
    live = []; rows = []
    for d, ex, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= slots: continue
        sh = int(size * mult[i] / E0[i] / 100) * 100
        if sh <= 0: continue
        live.append(ex); yen = pnl[i] / 100 * sh * E0[i]; expo = sh * E0[i]
        if addon_frac > 0 and add_ok[i]:
            ash = int(size * mult[i] * addon_frac / o2[i] / 100) * 100
            yen += (exit_px[i] - o2[i]) * ash; expo += ash * o2[i]
        rows.append((i, YEARv[i], pnl[i], yen, sh, d, ex, expo))
    return pd.DataFrame(rows, columns=["i", "y", "pnl", "yen", "sh", "d", "ex", "expo"])

def metrics(R):
    r = stat(R)
    dly = R.assign(date=DATEv[R.i.to_numpy()]).groupby("date").yen.sum().sort_index(); eq = dly.cumsum(); r["dd"] = (eq - eq.cummax()).min() / 1e4
    mo = R.assign(ym=DATEv[R.i.to_numpy()].strftime("%Y-%m")).groupby("ym").yen.sum(); r["wm"] = mo.min() / 1e4
    r10 = R[R.y >= 2017]; r["n10"] = len(r10)
    # 同時建玉の最大（建て日〜手仕舞い日の重なり）
    ev = []
    for d, ex, e_ in zip(R.d, R.ex, R.expo): ev.append((d, e_)); ev.append((ex + 1, -e_))
    ev.sort(); cur = 0; mx = 0
    for _, v in ev: cur += v; mx = max(mx, cur)
    r["maxexpo"] = mx / 1e4
    return r

def show(lab, R, cap):
    r = metrics(R)
    print(f"  {lab:<34} 玉{r['n']:>5}({r['n']/26:>3.0f}/年・10年{r['n10']/10:>3.0f}/年) 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 26年{r['tot']:>+6,.0f}万 資金{cap:>4}万 年利{r['tot']/cap/26*100:>+5.1f}% DD{r['dd']:>+5,.0f} 最悪月{r['wm']:>+4,.0f} 最大建玉{r['maxexpo']:>4,.0f}万 | 4分割 {r['a']:>+5,.0f}/{r['b']:>+5,.0f}/{r['e3']:>+5,.0f}/{r['e4']:>+5,.0f} | 10年{r['y10']:>+5,.0f} PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% 最悪年{r['worst10']:+.0f} | 勝年{r['wy']}/26 最悪年{r['worst']:>+4,.0f}", flush=True)
    return r

def judge(r, b, cap, capb):
    """4分割すべて上回り・DD/最悪年が1万以上悪化しない・年利(同資金換算)が上"""
    ok = (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["dd"] >= b["dd"] - 1 and r["worst"] >= b["worst"] - 1)
    return " ★" if ok else ""

print("\n■ A 資金構成（極上 vt5≤1.09・1日5件業種cap3・DAY1CUT・買いで使えるのは300〜400万）", flush=True)
A = {}
for lab, kw, cap in (
    ("a1 現行 1×150＋同額乗せ(最大300)", dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("a2 1×200＋同額乗せ(最大400)", dict(slots=1, size=2_000_000, addon_frac=1.0), 400),
    ("a3 1×300 乗せなし", dict(slots=1, size=3_000_000, addon_frac=0.0), 300),
    ("a4 1×300＋乗せ1/3=100(最大400)", dict(slots=1, size=3_000_000, addon_frac=1/3), 400),
    ("a5 1×400 乗せなし", dict(slots=1, size=4_000_000, addon_frac=0.0), 400),
    ("a6 1×200＋乗せ半額100(最大300)", dict(slots=1, size=2_000_000, addon_frac=0.5), 300),
    ("a7 1×250＋乗せ150(最大400)", dict(slots=1, size=2_500_000, addon_frac=0.6), 400),
    ("a8 2×150 乗せなし(300)", dict(slots=2, size=1_500_000, addon_frac=0.0), 300),
    ("a9 2×200 乗せなし(400)", dict(slots=2, size=2_000_000, addon_frac=0.0), 400),
    ("a10 2×150＋同額乗せ(最大600)", dict(slots=2, size=1_500_000, addon_frac=1.0), 600),
    ("a11 3×130 乗せなし(390)", dict(slots=3, size=1_300_000, addon_frac=0.0), 390),
):
    A[lab] = show(lab, run_gen(G, SCORE, **kw), cap)
B0 = A["a1 現行 1×150＋同額乗せ(最大300)"]

print("\n■ B 頻度（現行≈37玉/年 → 3日1回≈80玉/年へ）・1×150＋同額乗せ(最大300)で比較", flush=True)
KEY_PRIO = SCORE + 10.0 * G          # 極上候補を必ず先に取り、無い日だけ極み候補（vt5>1.09）で埋める
ALLM = np.ones(n, dtype=bool)
Bres = {}
for lab, mask, key, kw, cap in (
    ("b1 現行 vt5≤1.09", G, SCORE, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b2 vt5≤1.20", VT5 <= 1.20, SCORE, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b3 vt5≤1.30", VT5 <= 1.30, SCORE, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b4 vt5≤1.50", VT5 <= 1.50, SCORE, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b5 vt5なし(極み入口top1)", ALLM, SCORE, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b6 極上優先→空き日は極み候補で埋める", ALLM, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("b7 b6の埋め玉は半額75万", ALLM, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0, mult=np.where(G, 1.0, 0.5)), 300),
    ("b8 b6の埋め玉は乗せなし", ALLM, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=0.0), 300),
    ("b9 2×150 乗せなし(300)", G, SCORE, dict(slots=2, size=1_500_000, addon_frac=0.0), 300),
    ("b10 極上優先埋め 2×150 乗せなし", ALLM, KEY_PRIO, dict(slots=2, size=1_500_000, addon_frac=0.0), 300),
    ("b11 極上1×150乗せ＋別枠:極み候補1×150乗せ(最大600)", ALLM, KEY_PRIO, dict(slots=2, size=1_500_000, addon_frac=1.0), 600),
):
    Bres[lab] = show(lab, run_gen(mask, key, **kw), cap)
print("  ※b8: 埋め玉に勝ち乗せを付けない版は本玉にも乗せが無い（本玉だけ乗せる版は b6 の内数として D で分ける）")

print("\n■ C 部品の重ね掛け（26年データあり）・現行 1×150＋同額乗せ", flush=True)
Cres = {}
for lab, mask, pnl, exo in (
    ("c1 現行", G, PNL1, EXO1),
    ("c2 +利確3%", G, PNL3, EXO3),
    ("c3 +%K>50除外", G & K50, PNL1, EXO1),
    ("c4 +利確3%+%K>50除外", G & K50, PNL3, EXO3),
):
    r = show(lab, run_gen(mask, SCORE, slots=1, size=1_500_000, addon_frac=1.0, pnl=pnl, exo=exo), 300); Cres[lab] = r
    print(f"      判定 vs 現行: {judge(r, B0, 300, 300) or '—'}")
print("  ── 同じ重ね掛けを 1×200＋同額乗せ(最大400) で", flush=True)
for lab, mask, pnl, exo in (
    ("c5 1×200乗せ", G, PNL1, EXO1),
    ("c6 1×200乗せ+利確3%", G, PNL3, EXO3),
    ("c7 1×200乗せ+%K除外", G & K50, PNL1, EXO1),
    ("c8 1×200乗せ+利確3%+%K除外", G & K50, PNL3, EXO3),
):
    show(lab, run_gen(mask, SCORE, slots=1, size=2_000_000, addon_frac=1.0, pnl=pnl, exo=exo), 400)

print("\n■ D 頻度×部品（極上優先＋空き日は極み候補で埋める・本玉/埋め玉とも乗せ同額）", flush=True)
for lab, mask, key, kw, cap in (
    ("d1 b6 埋め＋%K除外(埋め玉にも)", K50, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("d2 b6 埋め＋利確3%", ALLM, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0, pnl=PNL3, exo=EXO3), 300),
    ("d3 b6 埋め 1×200乗せ(最大400)", ALLM, KEY_PRIO, dict(slots=1, size=2_000_000, addon_frac=1.0), 400),
    ("d4 b7 半額埋め 1×200乗せ(最大400)", ALLM, KEY_PRIO, dict(slots=1, size=2_000_000, addon_frac=1.0, mult=np.where(G, 1.0, 0.5)), 400),
    ("d5 埋め玉=極み候補のうち%K≤50だけ", G | K50, KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("d6 埋め玉=vt5≤1.3だけ", G | (VT5 <= 1.3), KEY_PRIO, dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
):
    show(lab, run_gen(mask, key, **kw), cap)

# ── 頻度の実感: 現行と b6 のシグナル間隔（営業日）と月あたり玉数（2017-26） ──
print("\n■ 参考: シグナル間隔（2017-26・営業日）", flush=True)
for lab, mask, key in (("現行", G, SCORE), ("b6 極上優先＋埋め", ALLM, KEY_PRIO)):
    R = run_gen(mask, key, slots=1, size=1_500_000, addon_frac=1.0); R = R[R.y >= 2017]
    gap = np.diff(np.sort(R.d.to_numpy()))
    print(f"  {lab:<16} 玉{len(R)}/10年={len(R)/10:.0f}/年・月{len(R)/120:.1f}玉・間隔中央値{np.median(gap):.0f}営業日・平均{gap.mean():.1f}・7日超の空き{(gap>7).mean()*100:.0f}%")

# ── 噪音床: b6 の埋め玉部分だけ（極み候補 vt5>1.09 を空き日に入れる）の期待値 ──
R6 = run_gen(ALLM, KEY_PRIO, slots=1, size=1_500_000, addon_frac=1.0)
fill = R6[~G[R6.i.to_numpy()]]; core = R6[G[R6.i.to_numpy()]]
print(f"\n■ b6 の分解: 極上玉 {len(core)}件 {core.yen.sum()/1e4:+,.0f}万(件+{core.pnl.mean():.2f}%) / 埋め玉 {len(fill)}件 {fill.yen.sum()/1e4:+,.0f}万(件{fill.pnl.mean():+.2f}% 勝率{(fill.pnl>0).mean()*100:.1f}%)")
for a, b, nm in ((2001, 2013, "01-13"), (2014, 2026, "14-26"), (2017, 2021, "17-21"), (2022, 2026, "22-26")):
    f_ = fill[(fill.y >= a) & (fill.y <= b)]; c_ = core[(core.y >= a) & (core.y <= b)]
    print(f"    {nm}: 極上{len(c_)}件{c_.yen.sum()/1e4:+,.0f}万 / 埋め{len(f_)}件{f_.yen.sum()/1e4:+,.0f}万 (件{f_.pnl.mean():+.2f}%)")
print(f"\n[done] {time.time()-t0:.0f}s")

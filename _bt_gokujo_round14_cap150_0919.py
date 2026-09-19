# -*- coding: utf-8 -*-
"""_bt_gokujo_round14_cap150_0919.py — 極上ラウンド14（2026-09-19 本人「買いで使える資金は300万・分散でもいい・1銘柄は150万MAX・現金余力50万も加味」）
制約: 合計建玉≤300万・1銘柄≤150万（＝同額乗せは不可）・現金余力50万（確定損の受け皿）。
 A 制約内の構成を総当たり: 1×150／2×150／2×150(1日1本まで)／1×150＋2枠目は「1枠目の初日勝ち」の時だけ（乗せを別銘柄で）／
   1×150＋2枠目は「1枠目が含み益」の時だけ／2×150(別業種)／3×100／2×(75+75乗せ)／150+100／現金ラダー(50万→100万で2枠目)
 B 現金50万の耐久: 12か月ローリングの最小累積（-30万/-50万割れの起点比率）・最悪月・最悪年
 C 2枠目の玉だけの質（件あたり・4時代）
土台＝R9〜R13と同一（極上 新ルール DAY1CUT・26年=_bt_buy_20y_wide.pkl 立花日足・10年=2017-26の内数）。
実行: python -X utf8 _bt_gokujo_round14_cap150_0919.py > _log_gokujo_round14_cap150_0919.txt"""
import numpy as np, pandas as pd, time, re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read(); exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))
Y17 = YEARv >= 2017
DAYS_TS = pd.to_datetime(pd.Index(days))


def run_gen2(mask, key, slots, size, size2=None, addon_frac=0.0, th=1.0, pnl=None, exo=None, max_sig=5, cap=3,
             one_per_day=False, gate2=None, diff_sector=False, ladder=None, cash0=500_000):
    """run_gen と同じ選定（1日5件・業種cap3・保有中除外）。枠の埋め方だけ拡張:
    size2=2枠目以降のサイズ / one_per_day=新規は1日1本まで / gate2='day2win'(昨日建てた玉の初日終値>建値+th%の時だけ2枠目)
    'unreal'(保有玉のどれかが前日終値で含み益の時だけ2枠目) / diff_sector=保有玉と別業種だけ2枠目 /
    ladder=閾値円: 現金(cash0+確定損益累計)がこれ以上の時だけ2枠目。"""
    pnl = PNL1 if pnl is None else pnl; exo = EXO1 if exo is None else exo
    size2 = size if size2 is None else size2
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
    live = []; rows = []; closed = []
    for d, ex, i in picks:
        live = [x for x in live if x[0] >= d]
        if len(live) >= slots: continue
        slot = len(live) + 1
        if slot >= 2:
            if one_per_day and any(x[2] == d for x in live): continue
            if diff_sector and SECv[i] in {SECv[x[1]] for x in live}: continue
            if gate2 == "day2win" and not any(x[2] == d - 1 and CL[x[1], 0] > E0[x[1]] * (1 + th / 100) for x in live): continue
            if gate2 == "unreal":
                ok = False
                for x in live:
                    k = d - 1 - x[2]
                    if 0 <= k <= 2 and np.isfinite(CL[x[1], k]) and CL[x[1], k] > E0[x[1]]: ok = True
                if not ok: continue
            if ladder is not None:
                realized = sum(y_ for e_, y_ in closed if e_ < d)
                if cash0 + realized < ladder: continue
        sz = size if slot == 1 else size2
        sh = int(sz / E0[i] / 100) * 100
        if sh <= 0: continue
        yen = pnl[i] / 100 * sh * E0[i]; expo = sh * E0[i]
        if addon_frac > 0 and add_ok[i]:
            ash = int(sz * addon_frac / o2[i] / 100) * 100
            yen += (exit_px[i] - o2[i]) * ash; expo += ash * o2[i]
        live.append((ex, i, d))
        closed.append((ex, yen))
        rows.append((i, YEARv[i], pnl[i], yen, sh, d, ex, expo, slot))
    return pd.DataFrame(rows, columns=["i", "y", "pnl", "yen", "sh", "d", "ex", "expo", "slot"])


def exit_series(R, y0=2017):
    """確定損益を手仕舞い日に付けた日次系列（候補日カレンダー）"""
    R = R[R.y >= y0]
    s = pd.Series(R.yen.to_numpy(), index=DAYS_TS[R.ex.to_numpy()]).groupby(level=0).sum().sort_index()
    return s


def cash_risk(R, y0=2017, thr=(-300_000, -500_000)):
    """12か月ローリング: 各月初を起点に、その後12か月の最小累積を取る → thr割れの起点比率と最悪値"""
    s = exit_series(R, y0)
    if len(s) == 0: return {"b30": np.nan, "b50": np.nan, "w12": np.nan, "wm": np.nan}
    cum = s.cumsum(); idx = cum.index
    starts = pd.date_range(idx.min().to_period("M").to_timestamp(), idx.max() - pd.DateOffset(months=12), freq="MS")
    mins = []
    for st in starts:
        base = cum[cum.index < st].iloc[-1] if (cum.index < st).any() else 0.0
        w = cum[(cum.index >= st) & (cum.index < st + pd.DateOffset(months=12))]
        mins.append((w.min() - base) if len(w) else 0.0)
    mins = np.array(mins)
    mo = s.groupby(s.index.to_period("M")).sum()
    return {"b30": (mins < thr[0]).mean() * 100, "b50": (mins < thr[1]).mean() * 100, "w12": mins.min() / 1e4, "wm": mo.min() / 1e4}


def row(lab, R, cap):
    r = metrics(R); R10 = R[R.y >= 2017]
    dd10 = (lambda s: (s.cumsum() - s.cumsum().cummax()).min() / 1e4)(exit_series(R, 2017))
    cr = cash_risk(R)
    n2 = int((R10.slot >= 2).sum())
    print(f"  {lab:<32} 月{len(R10)/117:>4.1f}本(2枠目{n2/117:>3.1f}) | 26年{r['tot']:>+6,.0f}万 PF{r['pf']:.2f} DD{r['dd']:>+5,.0f} 最悪年{r['worst']:>+4,.0f} 4分割{r['a']:>+4,.0f}/{r['b']:>+4,.0f}/{r['e3']:>+4,.0f}/{r['e4']:>+4,.0f} | 10年{r['y10']:>+5,.0f}万 PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% DD{dd10:>+4,.0f} 最悪年{r['worst10']:+.0f} 最悪月{cr['wm']:+.0f} 年利{r['y10']/cap/10*100:>4.1f}% | 12か月最悪{cr['w12']:+.0f} -30万割れ{cr['b30']:>3.0f}% -50万割れ{cr['b50']:>3.0f}% | 最大建玉{r['maxexpo']:.0f}", flush=True)
    return r


print("■ A 制約内の構成（合計≤300万・1銘柄≤150万・26年/10年・現金50万の耐久は2017-26の12か月ローリング）", flush=True)
RES = {}
CFG = (
    ("参考 現行 1×200＋乗せ200(400)", dict(slots=1, size=2_000_000, addon_frac=1.0), 400),
    ("参考 1×150＋乗せ150(300・1銘柄300)", dict(slots=1, size=1_500_000, addon_frac=1.0), 300),
    ("a1 1×150 乗せなし", dict(slots=1, size=1_500_000), 150),
    ("a2 2×150 乗せなし", dict(slots=2, size=1_500_000), 300),
    ("a3 2×150 新規は1日1本", dict(slots=2, size=1_500_000, one_per_day=True), 300),
    ("a4 150＋2枠目=1枠目の初日勝ち時だけ", dict(slots=2, size=1_500_000, gate2="day2win"), 300),
    ("a5 150＋2枠目=1枠目が含み益の時だけ", dict(slots=2, size=1_500_000, gate2="unreal"), 300),
    ("a6 2×150 別業種だけ", dict(slots=2, size=1_500_000, diff_sector=True), 300),
    ("a7 3×100 乗せなし", dict(slots=3, size=1_000_000), 300),
    ("a8 2×(75＋同額乗せ75)", dict(slots=2, size=750_000, addon_frac=1.0), 300),
    ("a9 150＋2枠目100", dict(slots=2, size=1_500_000, size2=1_000_000), 250),
    ("a10 150＋2枠目75", dict(slots=2, size=1_500_000, size2=750_000), 225),
    ("a11 2×150 別業種＋1日1本", dict(slots=2, size=1_500_000, one_per_day=True, diff_sector=True), 300),
)
for lab, kw, cap in CFG:
    RES[lab] = row(lab, run_gen2(G, SCORE, **kw), cap)

print("\n■ B 現金ラダー（2017-01起点・現金=50万+確定損益累計・閾値以上の時だけ2枠目・1枠目150万は常時）", flush=True)
for L in (700_000, 1_000_000, 1_500_000, 2_000_000):
    row(f"b 2枠目は現金≥{L//10000}万の時だけ", run_gen2(G & Y17, SCORE, slots=2, size=1_500_000, ladder=L), 300)
row("b 参考 1×150(2017起点)", run_gen2(G & Y17, SCORE, slots=1, size=1_500_000), 150)
row("b 参考 2×150(2017起点)", run_gen2(G & Y17, SCORE, slots=2, size=1_500_000), 300)

print("\n■ C 2枠目の玉だけの質（2×150 乗せなし・26年）", flush=True)
R2 = run_gen2(G, SCORE, slots=2, size=1_500_000)
for lab, mk in (("1枠目", R2.slot == 1), ("2枠目", R2.slot == 2)):
    S = R2[mk]
    parts = " ".join(f"{nm}{S[(S.y >= a) & (S.y <= b)].pnl.mean():+.2f}%(n{len(S[(S.y >= a) & (S.y <= b)])})" for a, b, nm in ((2001, 2013, "01-13"), (2014, 2026, "14-26"), (2017, 2021, "17-21"), (2022, 2026, "22-26")))
    print(f"  {lab} n{len(S)} 件あたり{S.pnl.mean():+.3f}% 勝率{(S.pnl>0).mean()*100:.1f}% 円{S.yen.sum()/1e4:+,.0f}万 | {parts}")
same = R2[R2.slot == 2]
d1 = R2[R2.slot == 1].set_index("d")
sd = same[same.d.isin(d1.index)]
print(f"  2枠目のうち1枠目と同日建て {len(sd)}/{len(same)}件 件あたり{sd.pnl.mean():+.3f}% / 別日建て {same[~same.d.isin(d1.index)].pnl.mean():+.3f}%")

print("\n■ D 参考: 2×150 の年別（2017-26・確定損益）", flush=True)
yy = R2[R2.y >= 2017].groupby("y").yen.sum() / 1e4
print("  " + " ".join(f"{y}:{v:+.0f}" for y, v in yy.items()))
R1 = run_gen2(G, SCORE, slots=1, size=1_500_000)
yy1 = R1[R1.y >= 2017].groupby("y").yen.sum() / 1e4
print("  1×150 " + " ".join(f"{y}:{v:+.0f}" for y, v in yy1.items()))
print(f"\n[done] {time.time()-t0:.0f}s")

# -*- coding: utf-8 -*-
"""_bt_souzai_ult_0927.py — 割安B「究極版」: 手で実際に置ける注文だけで探す（2026-09-27）
本人「割安B究極にして」。前回の R_opt は「毎日の候補すべてに指値」の理想化で、本番の置き方(空き枠の数だけ上位に指値)では C1 に負けた。
■ 実運用の制約（エンジンに最初から組み込む）
 毎晩、空き枠(=枠数−持ち玉)の数だけ、並べ方の上位の銘柄に注文を置く(予備なし)。注文は ①翌朝の寄りの成行 ②終値×(1−x)の指値(有効d営業日) だけ。
 成行が寄りS高張り付きで約定しない枠は空いたまま(翌晩また上位から)。指値は毎晩、生きている信号(有効期限内)の中からスコア上位を空き枠の数だけ置き直す。
 1枠50万・50万で100株に届かない銘柄(シグナル日の生の終値>5,000円)は候補から外す。前営業日の値・決算10営業日前は建てない・コスト片道0.1%＋金利2.8%。
 配当: J-Quantsの株価は配当を調整していない＝権利落ち日をまたいだ玉は配当分の値下がりが損に見える。信用買いは配当落調整金(≒配当×0.85)を受け取るので、
   その分を足す(配当=前営業日の予想利回り×その回の割合×前日終値・利回り15%超はデータの誤りとして足さない)。C1の本番の数字(配当なし)とも並べて出す。
■ 事前登録（ISを見る前に固定）
 選ぶ期間(IS)=2016-07〜2021-12 / 確かめる期間(OOS)=2022-01〜2026-09-18（空の口座から）。基準=C1(本番)を同じエンジンで。
 段階A: 割安の定義53(しきい値 PBR≤{0.8,1.0,1.2}×PER≤{8,10,12,15}×利回り≥{2.5,3,3.5,4} / 合成スコア上位{5,10,15,20,30}%)
        × 入り方5(T1・成行 / 初日・成行 / 無条件・成行 / T4押し目・指値2%3日 / 無条件・指値2%3日) = 265セル。合成スコア順・3枠×50万・既定の出口。
   選び方: 隣(しきい値は各軸±1段・合成は隣の%)が全部そろったセルで「隣を含めた最小のIS月次シャープ」。条件=月平均の新規2件以上・新規ゼロの月2割以下(中心)。
 段階B: 段階Aの上位4 × 並べ方3(合成/利回り/ランダム10回) × 枠3(3/4/5) × 出口8(既定/PBR>1で利確/最長10日/最長30日/トレール3%/トレール8%/損切り5%/損切り10%)
        ＋指値の入り方は x∈{1,2,3}%×d∈{1,2,3} の9通り。選び方: 同じ並べ方・出口で枠3/4/5の最小のIS月次シャープが最大のもの(枠の高原)。
 段階C: 段階Bの1位に足す要素(フィルタ=満たす銘柄だけ / 優先=満たす銘柄を先に): 上方修正(直近20営業日に+5%以上) / 12-1か月モメンタム(上半分・下半分)
        / 低ボラ(ATR20÷株価が全体の中央値以下・以上) / 質(ROE≥8%かつ自己資本比率≥40%) / 権利取り(次の権利付き最終日が20〜40営業日先・最終日の前日に売る・保有は最長45日)
        ／出口の追加: 権利付き最終日の前日に売る(配当落ちをまたがない)。採用=IS月次シャープが+0.10以上良くなり、OOSで崩れないもの。
 確かめ: IS/OOS/10年/2026年・月の安定・同じ置き方でランダムに選ぶ30回・上位3玉除去・4時代・隣の定義。
実行: python -X utf8 _bt_souzai_ult_0927.py > _bt_souzai_ult_0927.log
"""
import pickle, time, gc, heapq, numpy as np, pandas as pd

t0 = time.time()
D = pickle.load(open("_bt_souzai_opt_slim_0927.pkl", "rb"))
dates, codes = D["dates"], D["codes"]
O, H, L, C = D["O"], D["H"], D["L"], D["C"]
PBRd, PBR1, PER1, DY, COMPZ, VOL20, TOV20 = (D[k] for k in ("PBRd", "PBR1", "PER1", "DY", "COMPZ", "VOL20", "TOV20"))
NEXT_E, RAWR, LIQ, T1, T4, bad, si = (D[k] for k in ("NEXT_E", "RAWR", "LIQ", "T1", "T4", "bad", "si"))
del D["EP1"], D["F6"]
X = pickle.load(open("_bt_souzai_ult_extra_0927.pkl", "rb"))
AEV, EQR, ROE1, CUMN, EXD = X["AEV"], X["EQR"], X["ROE1"], X["CUMN"], X["EXD"]
del X; gc.collect()
T, N = C.shape; yr = dates.year.values
cal = dates.values.astype("datetime64[D]").astype(np.int64)
IS_END = int(np.searchsorted(dates, pd.Timestamp("2022-01-01")))
I2026 = int(np.searchsorted(dates, pd.Timestamp("2026-01-01")))
MON = dates.to_period("M")
SIZE0, SIDE, RATE = 500_000, 0.001, 0.028
with np.errstate(invalid="ignore"):
    VALID = LIQ & (C > 0)
    AFF = (C * RAWR) <= SIZE0 / 100                        # 50万で100株買える(シグナル日の生の終値≤5,000円)
print(f"load {time.time()-t0:.0f}s  {dates[0].date()}〜{dates[-1].date()}  IS〜{dates[IS_END-1].date()}  利回り15%超のBの日×銘柄 "
      f"{int(((DY > 15) & (PBR1 <= 1) & (PBR1 > 0) & (PER1 > 0) & (PER1 <= 10) & LIQ).sum())}", flush=True)

LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
       (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000),
       (200000, 40000), (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000), (1e18, 300000)]
LB = np.array([b for b, _ in LIM]); LW = np.array([w for _, w in LIM], float)
def stuck_up(t, j):
    pc = C[t - 1, j]
    if not (np.isfinite(pc) and np.isfinite(O[t, j]) and np.isfinite(H[t, j])): return False
    raw = pc * RAWR[t - 1, j]
    if not (raw > 0): raw = pc
    w = LW[np.searchsorted(LB, raw, side="right")] / raw
    return bool((O[t, j] / pc - 1 >= w - 0.003) and (H[t, j] <= O[t, j] * 1.0005))

def entry_day(M):
    prior = pd.DataFrame(M.astype(np.float32)).shift(1).rolling(20, min_periods=1).max().fillna(0).to_numpy() > 0
    return M & ~prior

def to_cands(mask, score, prio=None):
    """日ごとの候補 [(j, 並べ方の値)] を並べ方の順に。prio(ブール)があれば満たすものを先に"""
    s_, j_ = np.nonzero(mask)
    sc = score[s_, j_]
    sc = np.where(np.isfinite(sc), sc, -1e9)
    if prio is not None:
        sc = sc + np.where(prio[s_, j_], 1e6, 0.0)
    out = {}
    for s, j, v in zip(s_.tolist(), j_.tolist(), sc.tolist()):
        out.setdefault(s, []).append((j, v))
    for s in out: out[s].sort(key=lambda x: -x[1])
    return out

STAT = {"stuck": 0}
def real_run(cands, slots=3, size=SIZE0, order=("mkt",), stop=0.07, trail=0.05, act=0.05, maxhold=20, pbr_tp=None, earn_gap=10,
             t_start=1, t_end=None, seed=None, div=True, cum_exit=False, afford=True):
    t_end = T if t_end is None else t_end
    rng = np.random.default_rng(seed) if seed is not None else None
    held = {}; tr = []; eq = np.zeros(T); realized = 0.0
    orders = []; pool = {}
    is_lim = order[0] == "lim"
    for t in range(max(1, t_start), t_end):
        # ① 朝: 前の晩に置いた注文の約定
        for (j, lim, sc, ne) in orders:
            if j in held or len(held) >= slots: continue
            o = O[t, j]
            if not (o > 0): continue
            if lim is None:
                if stuck_up(t, j): STAT["stuck"] += 1; continue
                px = o
            else:
                l = L[t, j]
                if not (l > 0 and l <= lim): continue
                px = min(o, lim); pool.pop(j, None)
            held[j] = dict(e=t, px=px, peak=px, ne=ne, last=px, pb0=PBRd[t - 1, j], divc=0.0, cum=int(CUMN[t - 1, j]), miss=0)
        # ② 配当落ちと出口
        for j in list(held):
            p = held[j]
            o, h, l, c = O[t, j], H[t, j], L[t, j], C[t, j]
            if div and p["e"] < t:
                sh = EXD.get((t, j))
                if sh:
                    y = DY[t, j]
                    if np.isfinite(y) and 0 < y <= 15 and C[t - 1, j] > 0:
                        cr = y / 100 * sh * C[t - 1, j] / p["px"] * 0.85
                        p["divc"] += cr; realized += size * cr
            if not (c > 0):
                p["miss"] += 1
                if p["miss"] > 5:
                    r = p["last"] / p["px"] - 1 - 2 * SIDE - RATE * (cal[t] - cal[p["e"]]) / 365
                    tr.append((p["e"], t, j, r + p["divc"], "廃止等")); realized += size * r; del held[j]
                continue
            p["miss"] = 0
            x = why = None
            if p.get("pbrx") and o > 0: x, why = o, "PBR>1"
            ls = p["px"] * (1 - stop)
            lt = p["peak"] * (1 - trail) if (trail and p["peak"] >= p["px"] * (1 + act)) else -1e18
            lvl, lw = (lt, "トレーリング") if lt > ls else (ls, "損切り")
            if x is None and p["e"] < t and o > 0 and o <= lvl: x, why = o, lw
            if x is None and l <= lvl: x, why = lvl, lw
            if x is None:
                if (t - p["e"] + 1) >= maxhold: x, why = c, "期限"
                elif t >= p["ne"] - 1: x, why = c, "決算前"
                elif cum_exit and t == p["cum"] - 1: x, why = c, "権利前"
            if x is not None:
                r = x / p["px"] - 1 - 2 * SIDE - RATE * (cal[t] - cal[p["e"]]) / 365
                tr.append((p["e"], t, j, r + p["divc"], why)); realized += size * r; del held[j]
            else:
                if h > p["peak"]: p["peak"] = h
                p["last"] = c
                if pbr_tp is not None and p["pb0"] <= pbr_tp and PBRd[t, j] > pbr_tp: p["pbrx"] = True
        # ③ 晩: 空き枠の数だけ上位に注文
        free = slots - len(held)
        orders = []
        cl = cands.get(t)
        if cl:
            if rng is not None: cl = [(j, rng.random()) for j, _ in cl]; cl.sort(key=lambda x: -x[1])
            for j, sc in cl:
                if j in held: continue
                if afford and not AFF[t, j]: continue
                ne = NEXT_E[t, j]
                if ne - t <= earn_gap: continue
                if j in bad and t <= si + 1 <= t + maxhold + 1: continue
                if is_lim:
                    pool[j] = (C[t, j] * (1 - order[1]), t + order[2], sc, ne)
                else:
                    orders.append((j, None, sc, ne))
                    if len(orders) >= free: break
        if is_lim:
            for j in [k for k, v in pool.items() if v[1] < t + 1 or k in held]: pool.pop(j)
            live = heapq.nlargest(max(free, 0), pool.items(), key=lambda kv: kv[1][2]) if free > 0 else []
            orders = [(j, v[0], v[2], v[3]) for j, v in live]
        elif free <= 0:
            orders = []
        eq[t] = realized + sum(size * (p["last"] / p["px"] - 1) for p in held.values())
    return tr, eq

ERAS = [(2016, 2018, "16-18"), (2019, 2020, "19-20"), (2021, 2023, "21-23"), (2024, 2026, "24-26")]
def metrics(res, size=SIZE0, t_start=1, t_end=None):
    tr, eq = res
    t_end = T if t_end is None else t_end
    lo = max(1, t_start)
    e_ = pd.Series(eq[lo:t_end], index=dates[lo:t_end])
    me = e_.groupby(MON[lo:t_end]).last(); mp = me.diff(); mp.iloc[0] = me.iloc[0]
    out = dict(months=len(me))
    R = pd.DataFrame(tr, columns=["e", "x", "j", "r", "why"]) if tr else pd.DataFrame(columns=["e", "x", "j", "r", "why"])
    R = R[(R.e >= lo) & (R.e < t_end)]
    if not len(R): return dict(out, n=0, sharpe=-9, total=0, feas=False, epm=0, zero=100)
    r = R.r.values.astype(float)
    ent = pd.Series(1, index=MON[R.e.values.astype(int)]).groupby(level=0).sum().reindex(me.index, fill_value=0)
    dd = (e_ - e_.cummax()).min(); roll12 = mp.rolling(12).sum(); y = yr[R.e.values.astype(int)]
    out.update(n=len(r), win=(r > 0).mean() * 100, avg=r.mean() * 100, pf=r[r > 0].sum() / max(1e-9, -r[r <= 0].sum()),
               total=mp.sum() / 1e4, top3=(np.sort(r)[:-3].sum() * size) / 1e4 if len(r) > 3 else 0, dd=dd / 1e4,
               sharpe=(mp.mean() / mp.std() * np.sqrt(12)) if mp.std() > 0 else 0, mwin=(mp > 0).mean() * 100, mworst=mp.min() / 1e4,
               m12worst=(roll12.min() / 1e4) if roll12.notna().any() else np.nan, mavg=mp.mean() / 1e4, epm=ent.mean(), zero=(ent == 0).mean() * 100,
               eras=" ".join(f"{lab}:{(r[(y >= a) & (y <= b)].sum() * size) / 1e4:+.0f}" for a, b, lab in ERAS),
               hold=(R.x - R.e + 1).mean(), why=" ".join(f"{k}{v:.0f}%" for k, v in (R.why.value_counts(normalize=True) * 100).items()))
    out["feas"] = (out["epm"] >= 2.0) and (out["zero"] <= 20.0)
    return out
def line(lab, m):
    if m.get("n", 0) == 0: return f"   {lab:<6} 玉0"
    return (f"   {lab:<6} {m['total']:+6.1f}万 玉{m['n']:4d}(月{m['epm']:.1f}件・撃たない月{m['zero']:.0f}%) 勝率{m['win']:.0f}% PF{m['pf']:.2f} DD{m['dd']:+5.1f} "
            f"月平均{m['mavg']:+5.2f}万 月勝率{m['mwin']:.0f}% 最悪月{m['mworst']:+5.1f} 12か月最悪{m['m12worst']:+6.1f} 月次シャープ{m['sharpe']:.2f} 上位3玉除く{m['top3']:+.0f} 平均保有{m['hold']:.1f}日")
PERIODS = (("IS", 1, IS_END), ("OOS", IS_END, T), ("10年", 1, T), ("2026年", I2026, T))
def evals(cd, **kw):
    return {lab: metrics(real_run(cd, t_start=a, t_end=b, **kw), kw.get("size", SIZE0), a, b) for lab, a, b in PERIODS}

# ================= 定義 =================
COMPP = np.full((T, N), np.nan, np.float32)
for t in range(T):
    m = np.isfinite(COMPZ[t]); k = int(m.sum())
    if k < 100: continue
    idx = np.nonzero(m)[0]; order_ = np.argsort(-COMPZ[t, idx], kind="stable")
    pr = np.empty(k, np.float32); pr[order_] = (np.arange(k) + 0.5) / k; COMPP[t, idx] = pr
PBS, PES, DYS = (0.8, 1.0, 1.2), (8, 10, 12, 15), (2.5, 3.0, 3.5, 4.0)
DEFS = {}
with np.errstate(invalid="ignore"):
    for pb in PBS:
        for pe in PES:
            for dy in DYS:
                DEFS[("thr", pb, pe, dy)] = (PBR1 > 0) & (PBR1 <= pb) & (PER1 > 0) & (PER1 <= pe) & (DY >= dy)
    for x in (5, 10, 15, 20, 30):
        DEFS[("comp", x)] = COMPP <= x / 100
def dname(k): return f"PBR≤{k[1]}・PER≤{k[2]}・利回り≥{k[3]}%" if k[0] == "thr" else f"合成スコア上位{k[1]}%"
def neighbors(k):
    if k[0] == "comp":
        xs = [5, 10, 15, 20, 30]; i = xs.index(k[1])
        return [("comp", xs[j]) for j in (i - 1, i, i + 1) if 0 <= j < len(xs)]
    out = [k]
    for ax, vals in ((1, PBS), (2, PES), (3, DYS)):
        i = vals.index(k[ax])
        for j in (i - 1, i + 1):
            if 0 <= j < len(vals):
                kk = list(k); kk[ax] = vals[j]; out.append(tuple(kk))
    return out
ENTRIES = {"T1・成行": (lambda M: M & T1 & VALID, dict(order=("mkt",), stop=0.07)),
           "初日・成行": (lambda M: entry_day(M) & VALID, dict(order=("mkt",), stop=0.10)),
           "無条件・成行": (lambda M: M & VALID, dict(order=("mkt",), stop=0.07)),
           "T4・指値2%3日": (lambda M: M & T4 & VALID, dict(order=("lim", 0.02, 3), stop=0.07)),
           "無条件・指値2%3日": (lambda M: M & VALID, dict(order=("lim", 0.02, 3), stop=0.07))}
SCORES = {"合成": COMPZ, "利回り": DY}

# ================= 基準: C1(本番) =================
C1M = DEFS[("thr", 1.0, 10, 3.0)] & T1 & VALID
cdC1 = to_cands(C1M, DY)
print("\n■ 基準 C1（本番: PBR≤1・PER≤10・利回り≥3%・T1・翌寄り成行・利回り順・3枠×50万・値がさ見送り・既定の出口）")
c1_nodiv = real_run(cdC1, div=False); mC = metrics(c1_nodiv)
Rr = pd.DataFrame(c1_nodiv[0], columns=["e", "x", "j", "r", "why"])
print(f"   再現の確認(配当なし・10年): 実現{Rr.r.sum() * SIZE0 / 1e4:+.1f}万 {len(Rr)}玉 PF{mC['pf']:.2f} DD{mC['dd']:+.1f}  ← 本番の監査 +204.3万/323玉/PF1.71/DD-34.4万")
EC1_nodiv = evals(cdC1, div=False); EC1 = evals(cdC1, div=True)
for lab in ("IS", "OOS", "10年", "2026年"): print(line(lab + "(配当なし)", EC1_nodiv[lab]))
for lab in ("IS", "OOS", "10年", "2026年"): print(line(lab + "(配当込み)", EC1[lab]))

# ================= 段階A =================
rowsA = []
for dk, M in DEFS.items():
    for en, (mk, kw) in ENTRIES.items():
        cd = to_cands(mk(M), COMPZ)
        m = metrics(real_run(cd, t_end=IS_END, **kw), SIZE0, 1, IS_END)
        rowsA.append(dict(key=dk, entry=en, **{k: m.get(k) for k in ("n", "epm", "zero", "win", "pf", "total", "dd", "sharpe", "mwin", "mworst", "m12worst", "feas")}))
    print(f"  A {dname(dk)} {time.time()-t0:.0f}s", flush=True)
RA = pd.DataFrame(rowsA)
ix = {(r.key, r.entry): r for r in RA.itertuples()}
RA["s_min"] = [np.min([ix[(nk, r.entry)].sharpe for nk in neighbors(r.key)]) for r in RA.itertuples()]
RA["feas_nb"] = [np.mean([bool(ix[(nk, r.entry)].feas) for nk in neighbors(r.key)]) for r in RA.itertuples()]
RA["name"] = [dname(k) for k in RA.key]
okA = RA[RA.feas.astype(bool) & (RA.feas_nb >= 0.5)].sort_values("s_min", ascending=False)
pd.set_option("display.width", 280); pd.set_option("display.max_columns", 40); pd.set_option("display.float_format", "{:.2f}".format)
colsA = ["name", "entry", "n", "epm", "zero", "win", "pf", "total", "dd", "sharpe", "s_min", "mwin", "mworst", "m12worst"]
print(f"\n■ 段階A（IS・3枠×50万・合成スコア順・既定の出口・配当込み）{len(RA)}セル。条件を満たす {len(okA)}セル。隣を含めた最小シャープの上位15:")
print(okA[colsA].head(15).to_string(index=False))
print("  入り方ごとの最良:")
print(okA.groupby("entry").head(1)[colsA].to_string(index=False))
TOPA = []
for r in okA.itertuples():
    if (r.key, r.entry) not in TOPA: TOPA.append((r.key, r.entry))
    if len(TOPA) >= 4: break
pickle.dump(dict(RA=RA), open("_bt_souzai_ult_0927_A.pkl", "wb"))

# ================= 段階B =================
EXITS = {"既定": {}, "PBR>1で利確": dict(pbr_tp=1.0), "最長10日": dict(maxhold=10), "最長30日": dict(maxhold=30),
         "トレール3%": dict(trail=0.03), "トレール8%": dict(trail=0.08), "損切り5%": dict(stop=0.05), "損切り10%": dict(stop=0.10)}
rowsB = []
for dk, en in TOPA:
    mk, kw0 = ENTRIES[en]; M = mk(DEFS[dk])
    for oname in ("合成", "利回り", "ランダム"):
        cd = to_cands(M, SCORES.get(oname, COMPZ))
        for sl in (3, 4, 5):
            for ename, ekw in EXITS.items():
                kw = dict(kw0, **ekw, slots=sl)
                if oname == "ランダム":
                    ms = [metrics(real_run(cd, t_end=IS_END, seed=sd, **kw), SIZE0, 1, IS_END) for sd in range(10)]
                    m = {k: float(np.mean([x.get(k, 0) for x in ms])) for k in ("n", "epm", "zero", "win", "pf", "total", "dd", "sharpe", "mwin", "mworst", "m12worst")}
                    m["feas"] = m["epm"] >= 2 and m["zero"] <= 20
                else:
                    m = metrics(real_run(cd, t_end=IS_END, **kw), SIZE0, 1, IS_END)
                rowsB.append(dict(key=dk, entry=en, order=oname, slots=sl, exit=ename, lim=None, **{k: m.get(k) for k in ("n", "epm", "zero", "win", "pf", "total", "dd", "sharpe", "mwin", "mworst", "m12worst", "feas")}))
    if kw0["order"][0] == "lim":
        cd = to_cands(M, COMPZ)
        for xx in (0.01, 0.02, 0.03):
            for dd_ in (1, 2, 3):
                kw = dict(kw0, order=("lim", xx, dd_))
                m = metrics(real_run(cd, t_end=IS_END, **kw), SIZE0, 1, IS_END)
                rowsB.append(dict(key=dk, entry=en, order="合成", slots=3, exit="既定", lim=(xx, dd_), **{k: m.get(k) for k in ("n", "epm", "zero", "win", "pf", "total", "dd", "sharpe", "mwin", "mworst", "m12worst", "feas")}))
    print(f"  B {dname(dk)} {en} {time.time()-t0:.0f}s", flush=True)
RB = pd.DataFrame(rowsB); RB["name"] = [dname(k) for k in RB.key]
main = RB[RB.lim.isna()].copy()
g = main.groupby(["key", "entry", "order", "exit"])
agg = g.agg(s_min=("sharpe", "min"), s3=("sharpe", "first"), feas_all=("feas", "all")).reset_index()
agg["name"] = [dname(k) for k in agg.key]
okB = agg[agg.feas_all].sort_values("s_min", ascending=False)
print(f"\n■ 段階B（IS）{len(RB)}セル。枠3/4/5の最小シャープの上位15:")
print(okB.head(15).to_string(index=False))
limrows = RB[RB.lim.notna()]
if len(limrows):
    print("  指値の幅と日数（合成・3枠・既定の出口）:")
    print(limrows[["name", "entry", "lim", "n", "epm", "zero", "total", "dd", "sharpe", "mwin"]].to_string(index=False))
for ax in ("order", "slots", "exit"):
    print(f"  軸ごとの平均シャープ({ax}): " + " / ".join(f"{k}:{v:.2f}" for k, v in main.groupby(ax).sharpe.mean().items()))
pickle.dump(dict(RA=RA, RB=RB), open("_bt_souzai_ult_0927_AB.pkl", "wb"))

bB = okB.iloc[0]
cand_sl = main[(main.key == bB.key) & (main.entry == bB.entry) & (main.order == bB.order) & (main.exit == bB.exit)].sort_values("slots")
sl_best = int(cand_sl.slots.iloc[0]) if not ((cand_sl.sharpe.max() - cand_sl[cand_sl.slots == 3].sharpe.iloc[0]) > 0.10) else int(cand_sl.sort_values("sharpe", ascending=False).slots.iloc[0])
BASE = dict(key=bB.key, entry=bB.entry, order=bB.order, exit=bB.exit, slots=sl_best)
print(f"\n段階Bの1位: {dname(BASE['key'])} {BASE['entry']} 並べ方={BASE['order']} 出口={BASE['exit']} 枠={BASE['slots']}（枠3/4/5のIS: " +
      " / ".join(f"{int(r.slots)}枠 {r.sharpe:.2f}" for r in cand_sl.itertuples()) + "）")

# ================= 段階C =================
def kw_of(base, **over):
    mk, kw0 = ENTRIES[base["entry"]]
    kw = dict(kw0, **EXITS[base["exit"]], slots=base["slots"]); kw.update(over); return mk, kw
rev = np.zeros((T, N), bool)
for s, j, r_, y_, same in AEV:
    if r_ >= 0.05: rev[s: min(T, s + 20), j] = True
with np.errstate(invalid="ignore", divide="ignore"):
    M12 = C / np.vstack([np.full((252, N), np.nan, np.float32), C[:-252]])
    M12 = np.vstack([np.full((21, N), np.nan, np.float32), M12[:-21]])      # 12-1か月: t-252 → t-21
    def med_mask(A, upper=True):
        out = np.zeros((T, N), bool)
        for t in range(T):
            m = VALID[t] & np.isfinite(A[t])
            if m.sum() < 100: continue
            md = np.median(A[t, m])
            out[t] = (A[t] >= md) if upper else (A[t] < md)
        return out & np.isfinite(A)
    MOMU = med_mask(M12, True); MOMD = med_mask(M12, False)
    LVOL = med_mask(-VOL20, True); HVOL = med_mask(-VOL20, False)
    QUAL = (ROE1 >= 0.08) & (EQR >= 0.4)
    DIVW = ((CUMN - np.arange(T)[:, None]) >= 20) & ((CUMN - np.arange(T)[:, None]) <= 40) & (DY >= 3.0)
FACT = {"上方修正(直近20日に+5%以上)": rev, "モメンタム上半分": MOMU, "モメンタム下半分": MOMD, "低ボラ(中央値以下)": LVOL,
        "高ボラ(中央値超)": HVOL, "質(ROE≥8%・自己資本比率≥40%)": QUAL, "権利取り(権利付き最終日の20〜40営業日前)": DIVW}
mk, kw = kw_of(BASE)
MB = mk(DEFS[BASE["key"]]); scB = SCORES.get(BASE["order"], COMPZ)
cdB = to_cands(MB, scB)
EB = evals(cdB, **kw) if BASE["order"] != "ランダム" else None
mB_is = metrics(real_run(cdB, t_end=IS_END, **kw), SIZE0, 1, IS_END)
print(f"\n■ 段階C（段階Bの1位に足す要素・IS月次シャープ。1位そのもの {mB_is['sharpe']:.2f}）")
rowsC = []
for fname, FM in FACT.items():
    for mode in ("フィルタ", "優先"):
        extra = {}
        if fname.startswith("権利取り"): extra = dict(cum_exit=True, maxhold=45)
        cd = to_cands(MB & FM, scB) if mode == "フィルタ" else to_cands(MB, scB, prio=FM)
        m = metrics(real_run(cd, t_end=IS_END, **dict(kw, **extra)), SIZE0, 1, IS_END)
        mo = metrics(real_run(cd, t_start=IS_END, **dict(kw, **extra)), SIZE0, IS_END, T)
        rowsC.append(dict(factor=fname, mode=mode, is_sh=m["sharpe"], is_tot=m["total"], is_epm=m.get("epm", 0), is_zero=m.get("zero", 100),
                          oos_sh=mo["sharpe"], oos_tot=mo["total"], feas=m["feas"]))
        print(f"  {fname:<30} {mode:<4} IS {m['sharpe']:.2f}({m['total']:+.0f}万・月{m.get('epm', 0):.1f}件・ゼロ月{m.get('zero', 0):.0f}%) ｜ OOS {mo['sharpe']:.2f}({mo['total']:+.0f}万)", flush=True)
for lab, extra in (("出口に「権利付き最終日の前日に売る」を追加", dict(cum_exit=True)), ("配当を足さない(本番の帳簿と同じ数え方)", dict(div=False))):
    m = metrics(real_run(cdB, t_end=IS_END, **dict(kw, **extra)), SIZE0, 1, IS_END)
    mo = metrics(real_run(cdB, t_start=IS_END, **dict(kw, **extra)), SIZE0, IS_END, T)
    print(f"  {lab:<30}      IS {m['sharpe']:.2f}({m['total']:+.0f}万) ｜ OOS {mo['sharpe']:.2f}({mo['total']:+.0f}万)", flush=True)
RC = pd.DataFrame(rowsC)
adopt = RC[(RC.is_sh >= mB_is["sharpe"] + 0.10) & RC.feas.astype(bool)].sort_values("is_sh", ascending=False)
print(f"  採用の条件(IS月次シャープ+0.10以上・回数の条件)を満たす要素: {len(adopt)}本" + ("" if not len(adopt) else " → " + ", ".join(f"{r.factor}({r.mode}) IS{r.is_sh:.2f}/OOS{r.oos_sh:.2f}" for r in adopt.itertuples())))
FINAL = dict(BASE)
FINAL_FACT = None
if len(adopt):
    a0 = adopt.iloc[0]
    if a0.oos_sh >= metrics(real_run(cdB, t_start=IS_END, **kw), SIZE0, IS_END, T)["sharpe"] - 0.10:
        FINAL_FACT = (a0.factor, a0["mode"])
print(f"  段階Cの結論: {'要素なし(段階Bの1位のまま)' if FINAL_FACT is None else '採用 ' + FINAL_FACT[0] + '(' + FINAL_FACT[1] + ')'}")

# ================= 確かめ =================
if FINAL_FACT is None:
    cdF = cdB; kwF = dict(kw)
else:
    FM = FACT[FINAL_FACT[0]]; extra = dict(cum_exit=True, maxhold=45) if FINAL_FACT[0].startswith("権利取り") else {}
    cdF = to_cands(MB & FM, scB) if FINAL_FACT[1] == "フィルタ" else to_cands(MB, scB, prio=FM)
    kwF = dict(kw, **extra)
EF = evals(cdF, **kwF); EFn = evals(cdF, **dict(kwF, div=False))
print(f"\n■ 究極版の候補: {dname(FINAL['key'])} {FINAL['entry']} 並べ方={FINAL['order']} {FINAL['slots']}枠×50万 出口={FINAL['exit']}" + ("" if FINAL_FACT is None else f" ＋{FINAL_FACT[0]}({FINAL_FACT[1]})"))
for lab in ("IS", "OOS", "10年", "2026年"): print(line(lab + "(配当込み)", EF[lab]))
for lab in ("IS", "OOS", "10年", "2026年"): print(line(lab + "(配当なし)", EFn[lab]))
print(f"   10年の時代別(配当込み) {EF['10年']['eras']} ／ 出口 {EF['10年']['why']}")
print("   C1(同じエンジン)の時代別(配当込み)", EC1["10年"]["eras"])
# 対照: 同じ置き方で、同じ日に同じ数の候補を全銘柄(流動性・値がさ見送り)からランダムに
cnt = np.zeros(T, int)
for s, lst in cdF.items(): cnt[s] = len(lst)
def rand_c(seed):
    rng = np.random.default_rng(seed); out = {}
    for t in np.nonzero(cnt)[0]:
        pool_ = np.nonzero(VALID[t] & AFF[t])[0]; k = min(int(cnt[t]), len(pool_))
        out[int(t)] = [(int(j), 0.0) for j in rng.choice(pool_, k, replace=False)]
    return out
for lab, a, b in (("10年", 1, T), ("OOS", IS_END, T), ("2026年", I2026, T)):
    u = np.array([metrics(real_run(rand_c(300 + sd), t_start=a, t_end=b, **kwF), SIZE0, a, b)["total"] for sd in range(30)])
    o = np.array([metrics(real_run(cdF, t_start=a, t_end=b, seed=400 + sd, **kwF), SIZE0, a, b)["total"] for sd in range(30)])
    real = EF[lab]["total"]
    print(f"   対照({lab}): 同じ置き方で全銘柄からランダム 平均{u.mean():+.1f}万 sd{u.std():.1f} → z{(real-u.mean())/max(u.std(),1e-9):+.1f} ／ 同じ候補をランダム順 平均{o.mean():+.1f}万 sd{o.std():.1f} → z{(real-o.mean())/max(o.std(),1e-9):+.1f}", flush=True)
print("   隣の定義(同じ入り方・並べ方・枠・出口): IS月次シャープ(損益) / OOS月次シャープ(損益)")
for nk in neighbors(FINAL["key"]):
    Mn = ENTRIES[FINAL["entry"]][0](DEFS[nk])
    cdn = to_cands(Mn, scB) if FINAL_FACT is None else (to_cands(Mn & FACT[FINAL_FACT[0]], scB) if FINAL_FACT[1] == "フィルタ" else to_cands(Mn, scB, prio=FACT[FINAL_FACT[0]]))
    mi = metrics(real_run(cdn, t_end=IS_END, **kwF), SIZE0, 1, IS_END); mo = metrics(real_run(cdn, t_start=IS_END, **kwF), SIZE0, IS_END, T)
    print(f"     {dname(nk):<30} IS {mi['sharpe']:.2f}({mi['total']:+.0f}万) / OOS {mo['sharpe']:.2f}({mo['total']:+.0f}万) 月{mo.get('epm', 0):.1f}件")
def monthly(cd, kw_, a):
    tr, eq = real_run(cd, t_start=a, **kw_)
    R = pd.DataFrame(tr, columns=["e", "x", "j", "r", "why"])
    e_ = pd.Series(eq[a:], index=dates[a:]); me = e_.groupby(MON[a:]).last(); mp = me.diff(); mp.iloc[0] = me.iloc[0]
    ent = pd.Series(1, index=MON[R.e.values.astype(int)]).groupby(level=0).sum() if len(R) else pd.Series(dtype=float)
    return " ".join(f"{str(k)[5:]}月{v/1e4:+.1f}({int(ent.get(k, 0))})" for k, v in mp.items())
print(f"\n   2026年の月別(配当込み・万円・(件)) 究極版の候補: {monthly(cdF, kwF, I2026)}")
print(f"   2026年の月別(配当込み・万円・(件)) C1          : {monthly(cdC1, dict(div=True), I2026)}")
def yearly(cd, kw_):
    tr, eq = real_run(cd, **kw_)
    e10 = pd.Series(eq, index=dates); ye = e10.groupby(dates.year).last(); yp = ye.diff(); yp.iloc[0] = ye.iloc[0]
    return " ".join(f"{k}:{v/1e4:+.0f}" for k, v in yp.items())
print(f"   10年の年別(配当込み・万円) 究極版の候補: {yearly(cdF, kwF)}")
print(f"   10年の年別(配当込み・万円) C1          : {yearly(cdC1, dict(div=True))}")
print(f"\nセル数: 段階A {len(RA)} ＋ 段階B {len(RB)} ＋ 段階C {len(RC) + 2} = {len(RA) + len(RB) + len(RC) + 2}（IS）。寄りS高で約定しなかった成行 {STAT['stuck']}回(全実行の累計)。 {time.time()-t0:.0f}s")
pickle.dump(dict(FINAL=FINAL, FINAL_FACT=FINAL_FACT, EF=EF, EFn=EFn, EC1=EC1, EC1_nodiv=EC1_nodiv), open("_bt_souzai_ult_0927_final.pkl", "wb"))

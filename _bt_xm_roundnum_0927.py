# -*- coding: utf-8 -*-
"""_bt_xm_roundnum_0927.py — 金の「大台ブレイク」はXMの他の銘柄でも効くか（2026-09-27）
金で合格した形(親の独立再現 _verify_gold_x00_0927.py と同じ定義)を、指数6・為替4・銀に当てる。金は対照として同じエンジンで再現。
定義(サーバー時刻・サーバー日):
  上抜け: その足がその日の新高値で、その日のそれより前の高値 < L ≤ その足の高値 (L=刻みSの格子+ずらしO) → アーム
          → アームの足を含む60本以内で高値 ≥ L+δ の最初の足で買い(始値がL+δ以上なら始値=窓) → その足からH本後の始値で手仕舞い
  下抜け: 鏡像(新安値・L−δで売り)
  時間帯: アーム/建てはサーバー02:00〜22:44、手仕舞いの足が23:00以降・日をまたぐ玉は除外。同時保有1(先に建てた玉が優先)。
  ※親の検証は「保有中に来たアームは期限内なら保有明けに建てる」が、ここは保有中のアームは捨てる(差は小さい)。
刻み: 銘柄ごとに一番キリの良い水準とその半分。δ=刻みの0.5%/1%/2%。保有15/30本。偽の格子=刻みの25%/50%/75%ずらし。
コスト: 今の実勢の往復%(指数=スプレッドのみ・為替=スプレッド＋Zero手数料0.007%・USDJPYは実測1.9pip・銀=スプレッド＋0.0105%)。
判定: キリ番(ずらし0)のセルが ①コスト抜きtが全セルの向きランダム(20日ブロック符号反転)の最大tの95%点(床)を超える
      ②今のコスト後 t>2 ③前半(〜2020)/後半(2021〜)とも t>1.5 ④勝ち年7割 ⑤上位3玉除去 t>2.5 ⑥偽の格子(25%/75%ずらし)との差 t>2
実行: python -X utf8 _bt_xm_roundnum_0927.py > _bt_xm_roundnum_0927.log
"""
import pandas as pd, numpy as np, time, sys

SYMS = {   # 銘柄: (ファイル, [刻み(大),刻み(小)], 今の往復コスト%, 対照か)
    'GOLD.':     ('_xm_m1_GOLD._0926.pkl',     [100.0, 50.0],   0.0138, True),
    'US100Cash': ('_xm_m1_US100Cash_0926.pkl', [1000.0, 500.0], 0.0095, False),
    'US500Cash': ('_xm_m1_US500Cash_0926.pkl', [100.0, 50.0],   0.0103, False),
    'US30Cash':  ('_xm_m1_US30Cash_0926.pkl',  [1000.0, 500.0], 0.0115, False),
    'GER40Cash': ('_xm_m1_GER40Cash_0926.pkl', [1000.0, 500.0], 0.0101, False),
    'JP225Cash': ('_xm_m1_JP225Cash_0926.pkl', [1000.0, 500.0], 0.0120, False),
    'UK100Cash': ('_xm_m1_UK100Cash_0926.pkl', [100.0, 50.0],   0.0223, False),
    'USDJPY.':   ('_xm_m1_USDJPY._0926.pkl',   [1.0, 0.5],      0.0121, False),
    'EURUSD.':   ('_xm_m1_EURUSD._0926.pkl',   [0.01, 0.005],   0.0105, False),
    'GBPUSD.':   ('_xm_m1_GBPUSD._0926.pkl',   [0.01, 0.005],   0.0160, False),
    'AUDUSD.':   ('_xm_m1_AUDUSD._0926.pkl',   [0.01, 0.005],   0.0197, False),
    'SILVER.':   ('_xm_m1_SILVER._0926.pkl',   [1.0, 0.5],      0.0775, False),
}
OFFS = [0.0, 0.25, 0.5, 0.75]
DELS = [0.005, 0.01, 0.02]
HOLDS = [15, 30]
ARM = 60
W0, W1, XEND = 120, 1364, 1380


def tstat(x):
    x = np.asarray(x, float)
    if len(x) < 20: return np.nan
    s = x.std(ddof=1)
    return x.mean() / (s / np.sqrt(len(x))) if s > 0 else np.nan


def load(fn):
    d = pd.read_pickle(fn)
    d = d[d.index >= '2015-01-01']
    day = d.index.normalize()
    mins = (d.index.hour * 60 + d.index.minute).values
    o = d['open'].to_numpy(float); h = d['high'].to_numpy(float); l = d['low'].to_numpy(float)
    dayc = pd.factorize(day)[0]
    hs = pd.Series(h); ls = pd.Series(l)
    dhb = hs.groupby(dayc).cummax().groupby(dayc).shift(1).to_numpy()   # その日のそれより前の高値
    dlb = ls.groupby(dayc).cummin().groupby(dayc).shift(1).to_numpy()
    return d.index, dayc, mins, o, h, l, dhb, dlb


def cell_trades(idx, dayc, mins, o, h, l, dhb, dlb, S, O, direction, deltas, holds):
    """戻り: {(δfrac, hold): DataFrame(t, gross%)}"""
    n = len(o); eps = S * 1e-7
    inwin = (mins >= W0) & (mins <= W1)
    if direction == 'up':
        kb = np.floor((dhb - O) / S + 1e-9)
        L = (kb + 1) * S + O
        arm = np.isfinite(dhb) & inwin & (h >= L - eps)
    else:
        ka = np.ceil((dlb - O) / S - 1e-9)
        L = (ka - 1) * S + O
        arm = np.isfinite(dlb) & inwin & (l <= L + eps)
    ai = np.flatnonzero(arm)
    Ls = L[ai]
    out = {}
    if len(ai) == 0:
        return out
    fw = ai[:, None] + np.arange(ARM)[None, :]
    fw = np.clip(fw, 0, n - 1)
    ok = (dayc[fw] == dayc[ai][:, None]) & (mins[fw] <= W1) & (fw >= ai[:, None])
    for df_ in deltas:
        dlt = S * df_
        if direction == 'up':
            T = Ls + dlt
            hit = ok & (h[fw] >= T[:, None] - eps)
        else:
            T = Ls - dlt
            hit = ok & (l[fw] <= T[:, None] + eps)
        has = hit.any(1)
        first = np.where(has, hit.argmax(1), -1)
        j = np.where(has, fw[np.arange(len(ai)), np.maximum(first, 0)], -1)
        sel = j >= 0
        jj = j[sel]; TT = T[sel]
        if direction == 'up':
            ent = np.where(o[jj] >= TT, o[jj], TT)
        else:
            ent = np.where(o[jj] <= TT, o[jj], TT)
        for H in holds:
            x = jj + H
            good = (x < n)
            x = np.minimum(x, n - 1)
            good &= (dayc[x] == dayc[jj]) & (mins[x] < XEND)
            je, xe, en = jj[good], x[good], ent[good]
            if direction == 'up':
                g = (o[xe] - en) / en * 100
            else:
                g = (en - o[xe]) / en * 100
            # 同時保有1: 建てた順に、前の玉の手仕舞いより後の建てだけ残す
            order = np.argsort(je, kind='stable')
            keep = []; last = -1
            for k in order:
                if je[k] > last:
                    keep.append(k); last = xe[k]
            keep = np.array(keep, int)
            out[(df_, H)] = pd.DataFrame({'t': idx[je[keep]], 'gross': g[keep]})
    return out


def summarize(tr, cost):
    g = tr.gross.to_numpy(); netv = g - cost
    y = tr.t.dt.year.to_numpy()
    ys = pd.Series(netv).groupby(y).sum()
    yrs = (tr.t.max() - tr.t.min()).days / 365.25 if len(tr) > 1 else np.nan
    return dict(n=len(g), per_yr=len(g) / yrs if yrs and yrs > 0 else np.nan, gross=g.mean() if len(g) else np.nan, t_gross=tstat(g),
                net=netv.mean() if len(g) else np.nan, t_net=tstat(netv), t1=tstat(netv[y <= 2020]), t2=tstat(netv[y >= 2021]),
                yrs_pos=(ys > 0).mean() if len(ys) else np.nan, n_yrs=len(ys), t_top3=tstat(np.sort(netv)[:-3]) if len(netv) > 23 else np.nan)


rows = []; cells = {}
t0 = time.time()
for sym, (fn, steps, cost, is_ctrl) in SYMS.items():
    idx, dayc, mins, o, h, l, dhb, dlb = load(fn)
    for S in steps:
        for of in OFFS:
            for dr in ('up', 'down'):
                res = cell_trades(idx, dayc, mins, o, h, l, dhb, dlb, S, S * of, dr, DELS, HOLDS)
                for (dfrac, H), tr in res.items():
                    if len(tr) < 20: continue
                    st = summarize(tr, cost)
                    key = (sym, S, of, dr, dfrac, H)
                    cells[key] = tr
                    rows.append(dict(sym=sym, step=S, off=of, dir=dr, delta=S * dfrac, hold=H, **st))
    print(f"[{sym}] done {time.time()-t0:.0f}s", flush=True)
    del idx, dayc, mins, o, h, l, dhb, dlb

R = pd.DataFrame(rows)
# ── ノイズ床: 20日ブロックの符号反転(全セル同じ乱数)→各セルのコスト抜きt→最大tの95%点(全体・銘柄別)
allt = pd.concat([tr.t for tr in cells.values()])
t_min = allt.min()
keys = list(cells)
blocks = [((cells[k].t - t_min).dt.days // 20).to_numpy() for k in keys]
gro = [cells[k].gross.to_numpy() for k in keys]
nb = int(max(b.max() for b in blocks)) + 1
rng = np.random.default_rng(27)
mx_all = []; mx_sym = {s: [] for s in SYMS}
for _ in range(300):
    sg = rng.choice([-1.0, 1.0], size=nb)
    best_all = -9; best_s = {s: -9 for s in SYMS}
    for k, b, g in zip(keys, blocks, gro):
        v = g * sg[b]
        tt = v.mean() / (v.std(ddof=1) / np.sqrt(len(v)))
        if tt > best_all: best_all = tt
        if tt > best_s[k[0]]: best_s[k[0]] = tt
    mx_all.append(best_all)
    for s in SYMS: mx_sym[s].append(best_s[s])
floor_all = float(np.percentile(mx_all, 95))
floor_sym = {s: float(np.percentile(v, 95)) for s, v in mx_sym.items()}
R['floor_sym'] = R.sym.map(floor_sym); R['floor_all'] = floor_all

# ── 偽の格子との差(キリ番の玉 vs 25%/75%ずらしの玉を合わせたもの・コスト抜き)
def diff_t(a, b):
    a = np.asarray(a); b = np.asarray(b)
    if len(a) < 20 or len(b) < 20: return np.nan, np.nan
    d = a.mean() - b.mean()
    se = np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
    return d, d / se
dcol, tcol, pcol = [], [], []
for r in R.itertuples():
    if r.off != 0.0:
        dcol.append(np.nan); tcol.append(np.nan); pcol.append(np.nan); continue
    a = cells[(r.sym, r.step, 0.0, r.dir, r.delta / r.step, r.hold)].gross
    bs = [cells.get((r.sym, r.step, of, r.dir, r.delta / r.step, r.hold)) for of in (0.25, 0.75)]
    bs = [x.gross for x in bs if x is not None]
    b = pd.concat(bs) if bs else pd.Series(dtype=float)
    d, t = diff_t(a, b)
    dcol.append(d); tcol.append(t); pcol.append(b.mean() if len(b) else np.nan)
R['placebo_gross'] = pcol; R['diff'] = dcol; R['t_diff'] = tcol
R['PASS'] = ((R.off == 0.0) & (R.t_gross > R.floor_all) & (R.t_net > 2) & (R.t1 > 1.5) & (R.t2 > 1.5) & (R.yrs_pos >= 0.7) & (R.t_top3 > 2.5) & (R.t_diff > 2)).fillna(False)
R['PASS_symfloor'] = ((R.off == 0.0) & (R.t_gross > R.floor_sym) & (R.t_net > 2) & (R.t1 > 1.5) & (R.t2 > 1.5) & (R.yrs_pos >= 0.7) & (R.t_top3 > 2.5) & (R.t_diff > 2)).fillna(False)

pd.set_option('display.width', 280); pd.set_option('display.max_columns', 40); pd.set_option('display.float_format', '{:.4f}'.format)
print(f"\nセル数 {len(R)}  ノイズ床(全体・コスト抜き最大tの95%点)={floor_all:.2f}  銘柄別: " + ' '.join(f"{s}:{v:.2f}" for s, v in floor_sym.items()))
cols = ['sym', 'step', 'dir', 'delta', 'hold', 'n', 'per_yr', 'gross', 't_gross', 'placebo_gross', 't_diff', 'net', 't_net', 't1', 't2', 'yrs_pos', 't_top3', 'PASS', 'PASS_symfloor']
K = R[R.off == 0.0].copy()
for sym in SYMS:
    sub = K[K.sym == sym].sort_values('t_gross', ascending=False)
    print(f"\n■ {sym}（今のコスト {SYMS[sym][2]:.4f}%・床 全体{floor_all:.2f}/銘柄{floor_sym[sym]:.2f}）キリ番セルの上位")
    print(sub[cols].head(6).to_string(index=False))
    # 偽の格子の一覧(同じ刻み・向き・δ1%・保有15)
    for S in SYMS[sym][1]:
        for dr in ('up', 'down'):
            pr = R[(R.sym == sym) & (R.step == S) & (R.dir == dr) & (np.isclose(R.delta, S * 0.01)) & (R.hold == 15)].sort_values('off')
            if len(pr):
                print(f"   刻み{S:g} {dr} δ1% 保有15: " + ' / '.join(f"ずらし{o:.0%} t{t:+.2f}({g:+.4f}%)" for o, t, g in zip(pr.off, pr.t_gross, pr.gross)))
P = R[R.PASS]
print(f"\n合格(全体の床) {len(P)}本 / 銘柄別の床なら {int(R.PASS_symfloor.sum())}本")
if len(P): print(P[cols].to_string(index=False))
P2 = R[R.PASS_symfloor & ~R.PASS]
if len(P2): print('\n銘柄別の床でだけ合格:\n' + P2[cols].to_string(index=False))
R.to_csv('_bt_xm_roundnum_0927.csv', index=False, encoding='utf-8-sig')
pd.to_pickle(cells, '_bt_xm_roundnum_0927_cells.pkl')
print(f"\n完了 {time.time()-t0:.0f}s")

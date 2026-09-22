# -*- coding: utf-8 -*-
"""_bt_rem_bound_0922.py — REM Bound Sniper型(逆張り)の本測。REM系統で唯一未検証の面。

Bound Sniper の公開説明文から読み取れる構造:
  ・「ボリンジャーバンド周辺での価格の行き過ぎや反発」に着目 = 逆張り
  ・「極所的リバーサル検知(特殊ヒゲ判定)」= ピンバー/ヒゲの長さでの反転検出
  ・「独自のトレンドフィルター」
  ・「ドル円(USD/JPY)の5分足専用に設計・チューニング」
Pullback Rider(順張り押し目)の720セルとは別の型なので、ここだけが手つかずだった。

土俵は完全に同じ: 10.7年USDJPY M5・XM実コスト・無選別プラセボ10シード・同じ6条件。
プラセボは「同じフィルタが立っている足の中でランダムに同数建てる」= BB行き過ぎ+ヒゲの寄与を分離。
"""
import pandas as pd, numpy as np, itertools, time
from multiprocessing import Pool
from _bt_rem_family_0922 import build, simulate, grade, sma, PIP, COST_PIPS, EXITS, TRAILS

T0 = time.time()
PSEEDS = 10
BBS = [(20, 2.0), (20, 2.5), (20, 3.0)]
WICKS = [0.0, 0.33, 0.50]          # ヒゲが足の全幅に占める割合の下限(0.0=ヒゲ判定なし)
FILTERS = ['none', 'range', 'trend']
_G = {}


def parts(b5):
    c, o, h, l = b5.c, b5.o, b5.h, b5.l
    rng = (h - l).replace(0, np.nan)
    lw = (np.minimum(o, c) - l) / rng        # 下ヒゲ比率
    uw = (h - np.maximum(o, c)) / rng        # 上ヒゲ比率
    s20, s50, s200 = sma(c, 20), sma(c, 50), sma(c, 200)
    return lw.fillna(0), uw.fillna(0), s20, s50, s200


def make(b5, bbn, bbk, wick, filt, pre):
    lw, uw, s20, s50, s200 = pre
    c, o, h, l = b5.c, b5.o, b5.h, b5.l
    mid = sma(c, bbn); sd = c.rolling(bbn).std(ddof=0)
    lo, hi = mid - bbk * sd, mid + bbk * sd
    # 行き過ぎ = バンドの外に出た足
    over_lo, over_hi = l <= lo, h >= hi
    if wick > 0:
        over_lo = over_lo & (lw >= wick)     # 下ヒゲで拒否 = 反転の兆候
        over_hi = over_hi & (uw >= wick)
    if filt == 'range':                      # レンジ(低ボラ・MA横ばい)でのみ逆張り
        bw = (hi - lo) / mid
        calm = bw < bw.rolling(500, min_periods=100).median()
        flat = (s20 - s50).abs() / s50 < 0.001
        pool_up = pool_dn = (calm & flat).fillna(False)
    elif filt == 'trend':                    # 大局トレンド方向にだけ逆張り
        pool_up = (c > s200).fillna(False)
        pool_dn = (c < s200).fillna(False)
    else:
        pool_up = pool_dn = pd.Series(True, index=b5.index)
    sig = np.where(over_lo & pool_up, 1, np.where(over_hi & pool_dn, -1, 0))
    return sig.astype(np.int8), pool_up.values, pool_dn.values


def init(o, h, l, dates, years):
    _G.update(o=o, h=h, l=l, dates=dates, years=years)


def work(job):
    bb, wick, filt, sig_arr, pu, pd_ = job
    o, h, l, dates, years = _G['o'], _G['h'], _G['l'], _G['dates'], _G['years']
    N = len(o); iu, idn = np.flatnonzero(pu), np.flatnonzero(pd_)
    out = []
    for (tp, slp), (ta, to) in itertools.product(EXITS, TRAILS):
        for dname in ('both', 'long', 'short'):
            s = (np.where(sig_arr > 0, sig_arr, 0) if dname == 'long'
                 else np.where(sig_arr < 0, sig_arr, 0) if dname == 'short' else sig_arr).astype(np.int8)
            nL, nS = int((s > 0).sum()), int((s < 0).sum())
            if nL + nS < 100:
                continue
            ti, pn, _ = simulate(o, h, l, s, tp, slp, ta, to)
            if len(pn) < 100:
                continue
            yy = years[ti]
            era = {}
            for nm, a, b in (('16-19', 2016, 2019), ('20-21', 2020, 2021),
                             ('22-24', 2022, 2024), ('25-26', 2025, 2026)):
                m = (yy >= a) & (yy <= b)
                era[f'e{nm}'] = float(pn[m].mean()) if m.sum() >= 20 else np.nan
            ps = []
            for sd_ in range(PSEEDS):
                rng = np.random.default_rng(hash((bb, wick, filt, tp, slp, ta, dname, sd_)) % (2**31))
                z = np.zeros(N, dtype=np.int8)
                if nL and len(iu):
                    z[rng.choice(iu, size=min(nL, len(iu)), replace=False)] = 1
                if nS and len(idn):
                    pk = rng.choice(idn, size=min(nS, len(idn)), replace=False)
                    z[pk] = np.where(z[pk] == 0, -1, z[pk])
                _, p, _ = simulate(o, h, l, z, tp, slp, ta, to)
                if len(p):
                    ps.append(p.mean())
            ps = np.array(ps)
            lab = f'BB{bb[1]}|ヒゲ{wick}|{filt}|TP{tp}/SL{slp}|TR{ta}-{to}|{dname}'
            r = grade(dates, ti, pn, lab)
            if r is None:
                continue
            r.update(real=pn.mean(), pbo=float(ps.mean()), psd=float(ps.std()),
                     delta=float(pn.mean() - ps.mean()),
                     z=float((pn.mean() - ps.mean()) / ps.std()) if ps.std() > 0 else 0.0,
                     bb=bb[1], wick=wick, filt=filt, dirn=dname, trail=ta > 0, **era)
            out.append(r)
    return out


if __name__ == '__main__':
    b5, b15 = build()
    o, h, l = b5.o.values, b5.h.values, b5.l.values
    dates, years = b5.index.values, b5.index.year.values
    pre = parts(b5)
    print(f'M5 {len(b5):,}本 ({time.time()-T0:.0f}s)・Bound Sniper型(逆張り) 開始', flush=True)

    jobs = []
    for bb, wick, filt in itertools.product(BBS, WICKS, FILTERS):
        s, pu, pdn = make(b5, bb[0], bb[1], wick, filt, pre)
        jobs.append((bb, wick, filt, s, pu, pdn))

    with Pool(12, initializer=init, initargs=(o, h, l, dates, years)) as p:
        res = [r for ch in p.imap_unordered(work, jobs) for r in ch]

    D = pd.DataFrame(res); D.to_csv('_bt_rem_bound_0922.csv', index=False)
    NF = np.sqrt(2 * np.log(max(len(D), 2)))
    need = (D.ny * 0.8).round()
    ok = D[(D.t > NF) & (D.t1 > 1.5) & (D.t2 > 1.5) & (D.wy >= need) & (D.tc2 > 2) & (D.tt3 > 2.5)]
    print(f'\n{"="*92}\n全{len(D)}セル・ノイズ床 {NF:.2f}  ({time.time()-T0:.0f}s)')
    print(f'合格(6条件): {len(ok)}   最大t = {D.t.max():+.2f}')

    print('\n【1】BB行き過ぎ+ヒゲの寄与 Δ = 実 − 同フィルタ内ランダム')
    for g, s in [('全セル', D)] + [(f'{d}のみ', D[D.dirn == d]) for d in ('long', 'short', 'both')]:
        tt = s.delta.mean() / s.delta.std() * np.sqrt(len(s)) if s.delta.std() > 0 else 0
        print(f'  {g:9s} n={len(s):4d}  Δ中央値{s.delta.median():+6.3f}p  Δ>0{(s.delta>0).mean()*100:5.1f}%  対応t{tt:+6.2f}')

    print('\n【2】ヒゲ判定(特殊ヒゲ)は効くか')
    for w, s in D.groupby('wick'):
        print(f'  ヒゲ≥{w:.2f}: n={len(s):3d}セル 実{s.real.median():+6.2f}p  Δ{s.delta.median():+6.3f}p  最大t{s.t.max():+5.2f}  年間玉数中央{s.n.median()/10.7:5.0f}')

    print('\n【3】トレンドフィルタの型')
    for f, s in D.groupby('filt'):
        print(f'  {f:6s}: n={len(s):3d}セル 実{s.real.median():+6.2f}p  Δ{s.delta.median():+6.3f}p  最大t{s.t.max():+5.2f}')

    print('\n【4】コスト別・1玉あたりプラスのセル数(BTは0.8pips)')
    for c in [0.0, 0.8, 1.25, 1.6]:
        a = D.real + 0.8 - c
        print(f'  {c:4.2f}pips: {int((a>0).sum()):3d}/{len(D)}  中央値{a.median():+6.3f}p  最大{a.max():+5.2f}p')

    print('\n【5】t 上位10')
    for _, r in D.sort_values('t', ascending=False).head(10).iterrows():
        print(f'  {r.label:46s} t{r.t:+5.2f} 実{r.real:+6.2f} 乱数{r.pbo:+6.2f} Δ{r.delta:+5.2f} '
              f'n{int(r.n):5d} PF{r.pf:4.2f} 勝率{r.win:4.1f}% 勝年{int(r.wy)}/{int(r.ny)}')

    print('\n【6】時代分解(全セル中央値)')
    for c in ('e16-19', 'e20-21', 'e22-24', 'e25-26'):
        v = D[c].dropna()
        print(f'  {c}: {v.median():+6.2f}p  正のセル{int((v>0).sum())}/{len(v)}')
    print(f'\n完了 {time.time()-T0:.0f}s')

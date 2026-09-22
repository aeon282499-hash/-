# -*- coding: utf-8 -*-
"""_bt_rem_full_0922.py — REM型の決定版: 全720セルで無選別のプラセボ検定 + 時代分解。

上位セルだけのプラセボは「720から選んだ最大値」なので信用できない。
そこで全セルについて (実 − 同トレンド環境ランダム) を取り、分布ごと正に寄っているかを見る。
  ・BBタッチに情報があるなら Δ の分布が全体として正にずれるはず
  ・上位セルだけ正で中央値がゼロなら、それは選択効果
さらに、上位セルが USDJPY の 2022-24 円安トレンドだけの現象でないかを時代別に割る。
プラセボ10シード/セル・建玉数とTP/SL/コストは実シグナルと完全一致。
"""
import pandas as pd, numpy as np, itertools, time
from multiprocessing import Pool
from _bt_rem_family_0922 import (build, signals, simulate, grade, sma, PIP, COST_PIPS,
                                 SMAS, BBS, EXITS, TRAILS, MTFS)

T0 = time.time()
PSEEDS = 10
_G = {}


def trend_mask(b5, b15, smas, slope_n, use_mtf):
    s, m, l = smas
    c = b5.c
    a, bb_, cc = sma(c, s), sma(c, m), sma(c, l)
    up = (a > bb_) & (bb_ > cc) & (a.diff(slope_n) > 0)
    dn = (a < bb_) & (bb_ < cc) & (a.diff(slope_n) < 0)
    if use_mtf:
        a15, c15 = sma(b15.c, s), sma(b15.c, l)
        t15 = np.sign(a15 - c15).reindex(b5.index, method='ffill')
        up = up & (t15 > 0); dn = dn & (t15 < 0)
    return up.fillna(False).values, dn.fillna(False).values


def init(o, h, l, dates, years):
    _G.update(o=o, h=h, l=l, dates=dates, years=years)


def work(job):
    smas, bb, mtf, sig_arr, up, dn = job
    o, h, l, dates, years = _G['o'], _G['h'], _G['l'], _G['dates'], _G['years']
    N = len(o)
    iu, idn = np.flatnonzero(up), np.flatnonzero(dn)
    out = []
    for (tp, slp), (ta, to) in itertools.product(EXITS, TRAILS):
        for dname in ('both', 'long', 'short'):
            if dname == 'long':
                s = np.where(sig_arr > 0, sig_arr, 0).astype(np.int8)
            elif dname == 'short':
                s = np.where(sig_arr < 0, sig_arr, 0).astype(np.int8)
            else:
                s = sig_arr
            nL, nS = int((s > 0).sum()), int((s < 0).sum())
            if nL + nS < 100:
                continue
            ti, pn, _ = simulate(o, h, l, s, tp, slp, ta, to)
            if len(pn) < 100:
                continue
            real = pn.mean()
            # 時代分解(決済年ベース)
            yy = years[ti]
            era = {}
            for name, lo, hi in (('16-19', 2016, 2019), ('20-21', 2020, 2021),
                                 ('22-24', 2022, 2024), ('25-26', 2025, 2026)):
                m = (yy >= lo) & (yy <= hi)
                era[name] = float(pn[m].mean()) if m.sum() >= 20 else np.nan
            # プラセボ: 同じトレンド環境の中でランダムに同数建てる
            ps = []
            for sd in range(PSEEDS):
                rng = np.random.default_rng(hash((smas, bb, mtf, tp, slp, ta, dname, sd)) % (2**31))
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
            lab = (f'SMA{smas[0]}/{smas[1]}/{smas[2]}|BB{bb[1]}|MTF{int(mtf)}|'
                   f'TP{tp}/SL{slp}|TR{ta}-{to}|{dname}')
            r = grade(dates, ti, pn, lab)
            if r is None:
                continue
            r.update(real=real, pbo=float(ps.mean()), psd=float(ps.std()),
                     delta=float(real - ps.mean()),
                     z=float((real - ps.mean()) / ps.std()) if ps.std() > 0 else 0.0,
                     dirn=dname, trail=ta > 0, **{f'e{k}': v for k, v in era.items()})
            out.append(r)
    return out


if __name__ == '__main__':
    b5, b15 = build()
    o, h, l = b5.o.values, b5.h.values, b5.l.values
    dates = b5.index.values
    years = b5.index.year.values
    print(f'M5 {len(b5):,}本 ({time.time()-T0:.0f}s)・全セル×プラセボ{PSEEDS}シード開始', flush=True)

    jobs = []
    for smas, bb, mtf in itertools.product(SMAS, BBS, MTFS):
        s = signals(b5, b15, smas, bb[0], bb[1], 10, mtf).values.astype(np.int8)
        up, dn = trend_mask(b5, b15, smas, 10, mtf)
        jobs.append((smas, bb, mtf, s, up, dn))

    with Pool(12, initializer=init, initargs=(o, h, l, dates, years)) as p:
        res = [r for chunk in p.imap_unordered(work, jobs) for r in chunk]

    D = pd.DataFrame(res)
    D.to_csv('_bt_rem_full_0922.csv', index=False)
    print(f'\n{"="*96}\n全{len(D)}セル  ({time.time()-T0:.0f}s)')

    print('\n【検定1】BBタッチの寄与 Δ = 実 − 同トレンド環境ランダム (単位pips/玉)')
    for g, sub in [('全セル', D)] + [(f'{d}のみ', D[D.dirn == d]) for d in ('long', 'short', 'both')]:
        w = (sub.delta > 0).mean() * 100
        tt = sub.delta.mean() / sub.delta.std() * np.sqrt(len(sub)) if sub.delta.std() > 0 else 0
        print(f'  {g:10s} n={len(sub):4d}  Δ中央値{sub.delta.median():+6.3f}  Δ平均{sub.delta.mean():+6.3f}  '
              f'Δ>0の割合{w:5.1f}%  対応t{tt:+6.2f}  z中央値{sub.z.median():+5.2f}')

    print('\n【検定2】実際に儲かるか(コスト込み・1玉あたりpips)')
    print(f'  実の平均pips>0 のセル: {int((D.real>0).sum())}/{len(D)}  中央値{D.real.median():+.3f}pips')
    print(f'  ランダムの平均pips>0 : {int((D.pbo>0).sum())}/{len(D)}  中央値{D.pbo.median():+.3f}pips')

    print('\n【検定3】時代分解 — 上位20セル(t順)の時代別 平均pips/玉')
    top = D.sort_values('t', ascending=False).head(20)
    for c in ('e16-19', 'e20-21', 'e22-24', 'e25-26'):
        v = top[c].dropna()
        print(f'  {c}: 中央値{v.median():+6.2f}pips  正のセル{int((v>0).sum())}/{len(v)}')
    print('\n  全セルでも同じことを見る:')
    for c in ('e16-19', 'e20-21', 'e22-24', 'e25-26'):
        v = D[c].dropna()
        print(f'  {c}: 中央値{v.median():+6.2f}pips  正のセル{int((v>0).sum())}/{len(v)}')

    print('\n【検定4】Δ(BBタッチの寄与)の上位10セル — 選択前の素の姿')
    for _, r in D.sort_values('delta', ascending=False).head(10).iterrows():
        print(f'  {r.label:56s} 実{r.real:+6.2f} 乱数{r.pbo:+6.2f} Δ{r.delta:+5.2f} z{r.z:+5.2f} '
              f'n{int(r.n):5d} t{r.t:+5.2f} 勝年{int(r.wy)}/{int(r.ny)}')
    print(f'\n完了 {time.time()-T0:.0f}s')

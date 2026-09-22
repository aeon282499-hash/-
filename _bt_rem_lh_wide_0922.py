# -*- coding: utf-8 -*-
"""_bt_rem_lh_wide_0922.py — Line Hunter型のフロンティアを閉じる。

_bt_rem_linehunter_0922.py で ピボット幅 5→10→20 に対し Δ が -0.044→+0.116→+0.350 と
【単調に上昇】したまま端で打ち切られた。単調な傾向を端で切ると「未検証」であって「棄却」ではない。
そこで幅 40/60/100 まで伸ばし、頭打ちになる点を特定してフロンティアを閉じる。
(REM系統の他2型では絞り込みがΔを下げたので、Line Hunterだけ逆挙動という点も確認する)

土俵・判定・プラセボは _bt_rem_linehunter_0922.py と完全に同一。
"""
import pandas as pd, numpy as np, itertools, time
from multiprocessing import Pool
import _bt_rem_linehunter_0922 as LH
from _bt_rem_family_0922 import build, sma

T0 = time.time()
WIDE = [(40, 40), (60, 60), (100, 100)]

if __name__ == '__main__':
    b5, b15 = build()
    o, h, l = b5.o.values, b5.h.values, b5.l.values
    dates, years = b5.index.values, b5.index.year.values
    c = b5.c
    s50, s200 = sma(c, 50).values, sma(c, 200).values
    mid = sma(c, 20); sd = c.rolling(20).std(ddof=0)
    tlo = (b5.l <= (mid - 2.5 * sd)).fillna(False).values
    thi = (b5.h >= (mid + 2.5 * sd)).fillna(False).values
    pre = (s50, s200, tlo, thi)
    print(f'M5 {len(b5):,}本・ピボット幅 40/60/100 で打ち切り点を探す ({time.time()-T0:.0f}s)', flush=True)

    PL = {pv: LH.pivot_levels(b5, pv[0], pv[1]) for pv in WIDE}
    jobs = []
    for pv, st, ma_on, bb_on in itertools.product(WIDE, LH.SIGTYPES, LH.MAFILT, LH.BBFILT):
        R, S = PL[pv]
        sg, pu, pdn = LH.make(b5, R, S, st, ma_on, bb_on, pre)
        jobs.append((pv, st, ma_on, bb_on, sg, pu, pdn))
    print(f'  シグナル{len(jobs)}通り ({time.time()-T0:.0f}s)', flush=True)

    with Pool(12, initializer=LH.init, initargs=(o, h, l, dates, years)) as p:
        res = [r for ch in p.imap_unordered(LH.work, jobs) for r in ch]

    W = pd.DataFrame(res); W.to_csv('_bt_rem_lh_wide_0922.csv', index=False)
    N = pd.read_csv('_bt_rem_linehunter_0922.csv')
    D = pd.concat([N, W], ignore_index=True)
    D.to_csv('_bt_rem_lh_all_0922.csv', index=False)

    NF = np.sqrt(2 * np.log(max(len(D), 2)))
    need = (D.ny * 0.8).round()
    ok = D[(D.t > NF) & (D.t1 > 1.5) & (D.t2 > 1.5) & (D.wy >= need) & (D.tc2 > 2) & (D.tt3 > 2.5)]
    print(f'\n{"="*90}\nLine Hunter型 合計 {len(D)}セル(幅5〜100)・ノイズ床 {NF:.2f}  ({time.time()-T0:.0f}s)')
    print(f'合格(6条件): {len(ok)}   最大t = {D.t.max():+.2f}')

    print('\n【フロンティア】ピボット幅ごとの Δ と成績 — 頭打ちはどこか')
    print(f'  {"幅":>5s} {"セル":>5s} {"Δ中央値":>9s} {"実1玉":>8s} {"最大t":>7s} {"年間玉数":>9s} {"コスト1.25で正":>13s}')
    for v, s in D.groupby('pv'):
        a = s.real + 0.8 - 1.25
        print(f'  {int(v):5d} {len(s):5d} {s.delta.median():+8.3f}p {s.real.median():+7.2f}p '
              f'{s.t.max():+6.2f} {s.n.median()/10.7:8.0f}玉 {int((a>0).sum()):6d}/{len(s)}')

    print('\n【機構別】幅を伸ばしても2機構の序列は変わるか')
    for st, s in D.groupby('sigtype'):
        print(f'  {st:9s}:')
        for v, ss in s.groupby('pv'):
            print(f'     幅{int(v):3d}: Δ{ss.delta.median():+7.3f}p 実{ss.real.median():+6.2f}p 最大t{ss.t.max():+5.2f} 年{ss.n.median()/10.7:5.0f}玉')

    print('\n【コスト別】全{}セル中で1玉あたりプラス'.format(len(D)))
    for cst in [0.0, 0.8, 1.25, 1.6]:
        a = D.real + 0.8 - cst
        print(f'  {cst:4.2f}pips: {int((a>0).sum()):3d}/{len(D)}  中央値{a.median():+6.3f}p  最大{a.max():+5.2f}p')

    print('\n【t 上位10(幅5〜100の全体)】')
    for _, r in D.sort_values('t', ascending=False).head(10).iterrows():
        print(f'  {r.label:48s} t{r.t:+5.2f} 実{r.real:+6.2f} Δ{r.delta:+5.2f} n{int(r.n):5d} '
              f'PF{r.pf:4.2f} 勝率{r.win:4.1f}% 勝年{int(r.wy)}/{int(r.ny)}')

    print('\n【最良セルの実力(コスト1.25pips)】')
    b = D.sort_values('t', ascending=False).iloc[0]
    avg = b.real + 0.8 - 1.25
    print(f'  {b.label}')
    print(f'  平均{avg:+.2f}pips/玉 × 年{b.n/10.7:.0f}玉 = 年{avg*b.n/10.7:+.0f}pips '
          f'= 1lot(10万通貨)で年{avg*b.n/10.7*1000:+,.0f}円')
    print(f'  t={b.t:.2f}(ノイズ床{NF:.2f})・勝ち年{int(b.wy)}/{int(b.ny)}')

    print('\n【時代分解(全セル中央値)】')
    for cc in ('e16-19', 'e20-21', 'e22-24', 'e25-26'):
        v = D[cc].dropna()
        print(f'  {cc}: {v.median():+6.2f}p  正のセル{int((v>0).sum())}/{len(v)}')
    print(f'\n完了 {time.time()-T0:.0f}s')

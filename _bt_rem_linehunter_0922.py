# -*- coding: utf-8 -*-
"""_bt_rem_linehunter_0922.py — REM Line Hunter型の本測。REM系統で最後の未検証機構。

REM BB Line Hunter の公開説明文から読み取れる4つのシグナル:
  1. ピボットから算出したS&Rラインの【ブレイクアウト】(上位足15分の確定をフィルタに使える) ←未検証
  2. MA50/200でトレンド判定 → トレンド方向のBB2.5σタッチで押し目/戻り        ←720セルで測定済
  3. 【レジサポ転換(Role Reversal)】過去のレジスタンスがサポートに変わる点+BB条件 ←未検証
  4. トレンドに沿った3σ極端な反発(逆行する逆張りはしない)                      ←810セルで測定済
本スクリプトは未検証の 1 と 3 を測る。
指標既定値: BB短期2.5σ / BB長期3σ / MA50(短期トレンド) / MA200(大局)・USDJPY 5分足。

■ look-ahead の潰し方(ここが本件の肝)
Pineの ta.pivothigh(left,right) は「右にright本」見て初めて確定する。
つまり bar i のピボットは bar i+right になるまで存在を知れない。
素直に rolling(center=True) で作ると未来を見てしまうので、
各足では「i-right 以前に確定したピボット」しか参照しない実装にしている。

土俵は前2本と完全に同じ: 10.7年USDJPY M5・XM実コスト・無選別プラセボ10シード・判定6条件。
"""
import pandas as pd, numpy as np, itertools, time
from multiprocessing import Pool
from _bt_rem_family_0922 import build, simulate, grade, sma, PIP, COST_PIPS, EXITS, TRAILS

T0 = time.time()
PSEEDS = 10
PIVOTS = [(5, 5), (10, 10), (20, 20)]
SIGTYPES = ['breakout', 'reversal']
MAFILT = [False, True]
BBFILT = [False, True]
REV_WINDOW = 50          # ブレイク後、何本以内の戻りをレジサポ転換とみなすか
_G = {}


def pivot_levels(b, left, right):
    """各足で参照できる『直近の確定ピボット』の高値R・安値S。未来は見ない。"""
    h, l = b.h.values, b.l.values
    n = len(h)
    w = left + right + 1
    hm = pd.Series(h).rolling(w, center=True).max().values
    lm = pd.Series(l).rolling(w, center=True).min().values
    is_ph = (h == hm) & ~np.isnan(hm)
    is_pl = (l == lm) & ~np.isnan(lm)
    R = np.full(n, np.nan); S = np.full(n, np.nan)
    lastR = np.nan; lastS = np.nan
    for i in range(n):
        j = i - right                      # ここで初めて j のピボットが確定する
        if j >= 0:
            if is_ph[j]: lastR = h[j]
            if is_pl[j]: lastS = l[j]
        R[i] = lastR; S[i] = lastS
    return R, S


def make(b5, R, S, sigtype, ma_on, bb_on, pre):
    s50, s200, tlo, thi = pre
    c, h, l = b5.c.values, b5.h.values, b5.l.values
    n = len(c)
    up_ok = (s50 > s200) if ma_on else np.ones(n, bool)
    dn_ok = (s50 < s200) if ma_on else np.ones(n, bool)
    sig = np.zeros(n, dtype=np.int8)

    if sigtype == 'breakout':
        brk_up = (c > R) & (np.roll(c, 1) <= R) & ~np.isnan(R)
        brk_dn = (c < S) & (np.roll(c, 1) >= S) & ~np.isnan(S)
        brk_up[0] = brk_dn[0] = False
        if bb_on:                           # ブレイク足がBB外に出ていること
            brk_up = brk_up & thi
            brk_dn = brk_dn & tlo
        sig = np.where(brk_up & up_ok, 1, np.where(brk_dn & dn_ok, -1, 0)).astype(np.int8)
    else:                                   # レジサポ転換
        prevc = np.roll(c, 1); prevc[0] = c[0]
        bu = (c > R) & (prevc <= R) & ~np.isnan(R)
        bd = (c < S) & (prevc >= S) & ~np.isnan(S)
        pend_lv, pend_end, pend_dir = np.nan, -1, 0
        for i in range(1, n):
            if i <= pend_end and pend_dir != 0:
                if pend_dir > 0:            # 上抜けた旧レジスタンスに戻って支持されたら買い
                    if l[i] <= pend_lv and c[i] > pend_lv and up_ok[i] and (tlo[i] or not bb_on):
                        sig[i] = 1; pend_dir = 0
                else:
                    if h[i] >= pend_lv and c[i] < pend_lv and dn_ok[i] and (thi[i] or not bb_on):
                        sig[i] = -1; pend_dir = 0
            if bu[i]:
                pend_lv, pend_end, pend_dir = R[i], i + REV_WINDOW, 1
            elif bd[i]:
                pend_lv, pend_end, pend_dir = S[i], i + REV_WINDOW, -1
    return sig, up_ok, dn_ok


def init(o, h, l, dates, years):
    _G.update(o=o, h=h, l=l, dates=dates, years=years)


def work(job):
    pv, st, ma_on, bb_on, sig_arr, pu, pdn = job
    o, h, l, dates, years = _G['o'], _G['h'], _G['l'], _G['dates'], _G['years']
    N = len(o); iu, idn = np.flatnonzero(pu), np.flatnonzero(pdn)
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
            yy = years[ti]; era = {}
            for nm, a, bq in (('16-19', 2016, 2019), ('20-21', 2020, 2021),
                              ('22-24', 2022, 2024), ('25-26', 2025, 2026)):
                m = (yy >= a) & (yy <= bq)
                era[f'e{nm}'] = float(pn[m].mean()) if m.sum() >= 20 else np.nan
            ps = []
            for sd_ in range(PSEEDS):
                rng = np.random.default_rng(hash((pv, st, ma_on, bb_on, tp, slp, ta, dname, sd_)) % (2**31))
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
            lab = f'P{pv[0]}|{st}|MA{int(ma_on)}|BB{int(bb_on)}|TP{tp}/SL{slp}|TR{ta}-{to}|{dname}'
            r = grade(dates, ti, pn, lab)
            if r is None:
                continue
            r.update(real=pn.mean(), pbo=float(ps.mean()), psd=float(ps.std()),
                     delta=float(pn.mean() - ps.mean()),
                     z=float((pn.mean() - ps.mean()) / ps.std()) if ps.std() > 0 else 0.0,
                     pv=pv[0], sigtype=st, mafilt=ma_on, bbfilt=bb_on, dirn=dname, trail=ta > 0, **era)
            out.append(r)
    return out


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
    print(f'M5 {len(b5):,}本 ({time.time()-T0:.0f}s)・Line Hunter型(ピボットS&R) 開始', flush=True)

    PL = {pv: pivot_levels(b5, pv[0], pv[1]) for pv in PIVOTS}
    print(f'  ピボット3種を確定ベースで構築 ({time.time()-T0:.0f}s)', flush=True)

    jobs = []
    for pv, st, ma_on, bb_on in itertools.product(PIVOTS, SIGTYPES, MAFILT, BBFILT):
        R, S = PL[pv]
        sg, pu, pdn = make(b5, R, S, st, ma_on, bb_on, pre)
        jobs.append((pv, st, ma_on, bb_on, sg, pu, pdn))
    print(f'  シグナル{len(jobs)}通り・玉数中央 {int(np.median([np.count_nonzero(j[4]) for j in jobs]))} '
          f'({time.time()-T0:.0f}s)', flush=True)

    with Pool(12, initializer=init, initargs=(o, h, l, dates, years)) as p:
        res = [r for ch in p.imap_unordered(work, jobs) for r in ch]

    D = pd.DataFrame(res); D.to_csv('_bt_rem_linehunter_0922.csv', index=False)
    NF = np.sqrt(2 * np.log(max(len(D), 2)))
    need = (D.ny * 0.8).round()
    ok = D[(D.t > NF) & (D.t1 > 1.5) & (D.t2 > 1.5) & (D.wy >= need) & (D.tc2 > 2) & (D.tt3 > 2.5)]
    print(f'\n{"="*94}\n全{len(D)}セル・ノイズ床 {NF:.2f}  ({time.time()-T0:.0f}s)')
    print(f'合格(6条件): {len(ok)}   最大t = {D.t.max():+.2f}')

    print('\n【1】ピボットS&Rの寄与 Δ = 実 − 同フィルタ内ランダム')
    for g, s in [('全セル', D)] + [(f'{x}', D[D.sigtype == x]) for x in SIGTYPES] + \
                [(f'{d}のみ', D[D.dirn == d]) for d in ('long', 'short', 'both')]:
        tt = s.delta.mean() / s.delta.std() * np.sqrt(len(s)) if len(s) > 2 and s.delta.std() > 0 else 0
        print(f'  {g:10s} n={len(s):4d}  Δ中央値{s.delta.median():+6.3f}p  Δ>0{(s.delta>0).mean()*100:5.1f}%  対応t{tt:+6.2f}')

    print('\n【2】2つの機構の比較')
    for st, s in D.groupby('sigtype'):
        print(f'  {st:9s}: n={len(s):3d}セル 実{s.real.median():+6.2f}p Δ{s.delta.median():+6.3f}p '
              f'最大t{s.t.max():+5.2f} 年間玉数中央{s.n.median()/10.7:6.0f}')

    print('\n【3】ピボット幅 / フィルタ')
    for k, nm in [('pv', 'ピボット幅'), ('mafilt', 'MA50-200フィルタ'), ('bbfilt', 'BB条件')]:
        for v, s in D.groupby(k):
            print(f'  {nm}={str(v):6s}: n={len(s):3d} 実{s.real.median():+6.2f}p Δ{s.delta.median():+6.3f}p 最大t{s.t.max():+5.2f}')

    print('\n【4】コスト別・1玉あたりプラスのセル数(BTは0.8pips)')
    for cst in [0.0, 0.8, 1.25, 1.6]:
        a = D.real + 0.8 - cst
        print(f'  {cst:4.2f}pips: {int((a>0).sum()):3d}/{len(D)}  中央値{a.median():+6.3f}p  最大{a.max():+5.2f}p')

    print('\n【5】t 上位10')
    for _, r in D.sort_values('t', ascending=False).head(10).iterrows():
        print(f'  {r.label:48s} t{r.t:+5.2f} 実{r.real:+6.2f} 乱数{r.pbo:+6.2f} Δ{r.delta:+5.2f} '
              f'n{int(r.n):5d} PF{r.pf:4.2f} 勝率{r.win:4.1f}% 勝年{int(r.wy)}/{int(r.ny)}')

    print('\n【6】時代分解(全セル中央値)')
    for cc in ('e16-19', 'e20-21', 'e22-24', 'e25-26'):
        v = D[cc].dropna()
        print(f'  {cc}: {v.median():+6.2f}p  正のセル{int((v>0).sum())}/{len(v)}')
    print(f'\n完了 {time.time()-T0:.0f}s')

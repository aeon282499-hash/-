# -*- coding: utf-8 -*-
"""_bt_rem_placebo_0922.py — REM型の上位セルに対するプラセボ検定とドリフト分解。

_bt_rem_family_0922.py の上位セルは全部 long / TP30-SL30 に偏っていた。
USDJPYは2016年120円→2026年156円(+3,580pips)なので「買っただけ」の疑いがある。
そこで3つを分離する:
  (A) 実シグナル           = トレンドフィルタ + BBタッチ
  (B) プラセボ-トレンド内  = 同じトレンド環境の中でランダムに建てる(BBタッチを壊す)
  (C) プラセボ-全期間      = トレンドフィルタも外して全足からランダム(ドリフトだけ)
(A) > (B) でなければ BBタッチに情報は無い。(B) > (C) ならトレンドフィルタだけが効いている。
各プラセボ30シード。建玉数・TP/SL・コストは実シグナルと完全に揃える。
"""
import pandas as pd, numpy as np, time
from _bt_rem_family_0922 import build, signals, simulate, grade, sma, PIP, COST_PIPS

T0 = time.time()
SEEDS = 30

# _bt_rem_family_0922.csv の t 上位から代表を取る(全部longに寄っていたので both/short も1本ずつ)
CONFIGS = [
    dict(smas=(5, 20, 60),  bb=(20, 2.5), mtf=True,  tp=30, sl=30, tr=(0, 0), dirn='long'),
    dict(smas=(10, 25, 75), bb=(20, 3.0), mtf=True,  tp=30, sl=30, tr=(0, 0), dirn='long'),
    dict(smas=(10, 25, 75), bb=(20, 3.0), mtf=False, tp=30, sl=30, tr=(0, 0), dirn='long'),
    dict(smas=(20, 50, 200), bb=(20, 3.0), mtf=False, tp=30, sl=30, tr=(0, 0), dirn='long'),
    dict(smas=(10, 25, 75), bb=(20, 3.0), mtf=False, tp=30, sl=30, tr=(0, 0), dirn='both'),
    dict(smas=(20, 50, 200), bb=(20, 2.5), mtf=False, tp=20, sl=20, tr=(0, 0), dirn='short'),
]


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


def run(o, h, l, dates, sig, cfg):
    ti, pn, _ = simulate(o, h, l, sig, cfg['tp'], cfg['sl'], cfg['tr'][0], cfg['tr'][1])
    return ti, pn


def random_sig(pool_up, pool_dn, n_long, n_short, rng, N):
    s = np.zeros(N, dtype=np.int8)
    iu = np.flatnonzero(pool_up); idn = np.flatnonzero(pool_dn)
    if n_long and len(iu):
        s[rng.choice(iu, size=min(n_long, len(iu)), replace=False)] = 1
    if n_short and len(idn):
        pick = rng.choice(idn, size=min(n_short, len(idn)), replace=False)
        s[pick] = np.where(s[pick] == 0, -1, s[pick])
    return s


if __name__ == '__main__':
    b5, b15 = build()
    o, h, l, dates = b5.o.values, b5.h.values, b5.l.values, b5.index.values
    N = len(b5)
    allmask = np.ones(N, dtype=bool)
    # 全期間の平均ドリフト(pips/5分足) — 参考値
    drift = (b5.c.iloc[-1] - b5.c.iloc[0]) / PIP / N
    print(f'M5 {N:,}本  USDJPY {b5.c.iloc[0]:.2f} → {b5.c.iloc[-1]:.2f}  '
          f'ドリフト {drift:+.5f} pips/足 ({time.time()-T0:.0f}s)\n')
    print(f'{"設定":52s} {"実":>8s} {"B:トレンド内乱数":>18s} {"C:全期間乱数":>16s} {"z(実vsB)":>9s}')
    print('-' * 112)

    for cfg in CONFIGS:
        sig = signals(b5, b15, cfg['smas'], cfg['bb'][0], cfg['bb'][1], 10, cfg['mtf']).values.astype(np.int8)
        if cfg['dirn'] == 'long':
            sig = np.where(sig > 0, sig, 0).astype(np.int8)
        elif cfg['dirn'] == 'short':
            sig = np.where(sig < 0, sig, 0).astype(np.int8)
        ti, pn = run(o, h, l, dates, sig, cfg)
        real = pn.mean()
        nL = int((sig > 0).sum()); nS = int((sig < 0).sum())

        up, dn = trend_mask(b5, b15, cfg['smas'], 10, cfg['mtf'])
        pb, pc = [], []
        for sd in range(SEEDS):
            rng = np.random.default_rng(1000 + sd)
            sB = random_sig(up, dn, nL, nS, rng, N)
            _, p = run(o, h, l, dates, sB, cfg)
            if len(p): pb.append(p.mean())
            rng2 = np.random.default_rng(5000 + sd)
            sC = random_sig(allmask if nL else np.zeros(N, bool),
                            allmask if nS else np.zeros(N, bool), nL, nS, rng2, N)
            _, p2 = run(o, h, l, dates, sC, cfg)
            if len(p2): pc.append(p2.mean())
        pb, pc = np.array(pb), np.array(pc)
        z = (real - pb.mean()) / pb.std() if pb.std() > 0 else 0.0
        lab = (f"SMA{'/'.join(map(str,cfg['smas']))}|BB{cfg['bb'][1]}|MTF{int(cfg['mtf'])}|"
               f"TP{cfg['tp']}/SL{cfg['sl']}|{cfg['dirn']}")
        print(f'{lab:52s} {real:+7.2f}p  {pb.mean():+7.2f}±{pb.std():4.2f}p  '
              f'{pc.mean():+7.2f}±{pc.std():4.2f}p  {z:+8.2f}')
    print(f'\n(単位=1玉あたりpips・コスト{COST_PIPS}pips込み・プラセボ{SEEDS}シード)')
    print(f'完了 {time.time()-T0:.0f}s')

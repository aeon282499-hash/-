# -*- coding: utf-8 -*-
"""_bt_rem_family_0922.py — REMシリーズ(Ren-1904)の「型」がUSDJPY 5分足に実在するかの本測。

REM BB Pullback Rider の公開説明文から読み取れる構造:
  ・3本SMAのパーフェクトオーダー + 傾きフィルタでトレンドを定義
  ・トレンド方向のボリンジャーバンド(既定2.5σ)へのタッチで押し目買い/戻り売り
  ・15分足を上位足フィルタに使う(MTF)
  ・固定TP/SL(pips) + トレール(発動距離/オフセット)
ソースは非公開なので「REM本体の再現」ではなく【この型に素性があるか】を測る。
型に何も無ければV1/V3/ZEROの違いを議論する意味が無い、という足切りが目的。

データ: _fx_usdjpy_m1_10y.pkl (2016-01 〜 2026-09 / 229万本) → M5・M15
コスト: 往復0.8pips(XM Zero: spread0.1 + 手数料相当0.7)。2倍(1.6pips)でも見る。
約定: シグナル足の次の足の始値で建て。足の高値安値でSL/TP/トレールを判定。
      同一足でSLとTPの両方に触れたらSL優先(悲観側)。
判定: 9/20・9/22と同じ物差し(t>ノイズ床 x 前後半 x 勝ち年 x コスト2倍 x 上位3日除去)
      + プラセボ(同じトレンド環境でランダムに建てる)30シードとの比較。
"""
import pandas as pd, numpy as np, itertools, os, time
from multiprocessing import Pool

PIP = 0.01
COST_PIPS = 0.8
T0 = time.time()


# ---------------- データ ----------------
def build():
    m1 = pd.read_pickle('_fx_usdjpy_m1_10y.pkl')
    m1 = m1.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c'})

    def rs(rule):
        r = m1.resample(rule)
        b = pd.DataFrame({'o': r.o.first(), 'h': r.h.max(), 'l': r.l.min(), 'c': r.c.last()}).dropna()
        return b[b.index.dayofweek < 5]
    return rs('5min'), rs('15min')


def sma(x, n): return x.rolling(n).mean()


def signals(b5, b15, smas, bbn, bbk, slope_n, use_mtf):
    """REM型のエントリー: +1=買い, -1=売り, 0=無し"""
    s, m, l = smas
    c = b5.c
    a, bb_, cc = sma(c, s), sma(c, m), sma(c, l)
    up = (a > bb_) & (bb_ > cc) & (a.diff(slope_n) > 0)      # パーフェクトオーダー+傾き
    dn = (a < bb_) & (bb_ < cc) & (a.diff(slope_n) < 0)
    mid = sma(c, bbn); sd = c.rolling(bbn).std(ddof=0)
    lo, hi = mid - bbk * sd, mid + bbk * sd
    touch_lo = b5.l <= lo                                     # 押し目(下限タッチ)
    touch_hi = b5.h >= hi                                     # 戻り(上限タッチ)
    if use_mtf:                                               # 上位足15分の同意
        a15, c15 = sma(b15.c, s), sma(b15.c, l)
        t15 = np.sign(a15 - c15).reindex(b5.index, method='ffill')
        up = up & (t15 > 0); dn = dn & (t15 < 0)
    sig = np.where(up & touch_lo, 1, np.where(dn & touch_hi, -1, 0))
    return pd.Series(sig, index=b5.index)


# ---------------- 約定シミュレーション ----------------
def simulate(o, h, l, sig, tp_p, sl_p, tr_act, tr_off, max_bars=288):
    """戻り: (決済足index配列, 損益pips配列, 方向配列)"""
    n = len(o)
    ti, pn, sd = [], [], []
    i = 0
    while i < n - 2:
        s = sig[i]
        if s == 0:
            i += 1; continue
        j0 = i + 1
        ep = o[j0]
        tp = ep + s * tp_p * PIP
        sl = ep - s * sl_p * PIP
        best = ep
        j = j0
        out = None
        while j < n and j - j0 < max_bars:
            if s > 0:
                if l[j] <= sl: out = (j, (sl - ep) / PIP); break
                if h[j] >= tp: out = (j, (tp - ep) / PIP); break
                if tr_act:
                    if h[j] > best: best = h[j]
                    if best - ep >= tr_act * PIP:
                        ts = best - tr_off * PIP
                        if l[j] <= ts and ts > sl: out = (j, (ts - ep) / PIP); break
            else:
                if h[j] >= sl: out = (j, (ep - sl) / PIP); break
                if l[j] <= tp: out = (j, (ep - tp) / PIP); break
                if tr_act:
                    if l[j] < best: best = l[j]
                    if ep - best >= tr_act * PIP:
                        ts = best + tr_off * PIP
                        if h[j] >= ts and ts < sl: out = (j, (ep - ts) / PIP); break
            j += 1
        if out is None:
            if j >= n: break
            out = (j - 1, s * (o[min(j, n - 1)] - ep) / PIP)
        ti.append(out[0]); pn.append(out[1] - COST_PIPS); sd.append(s)
        i = out[0] + 1
    return np.array(ti, dtype=np.int64), np.array(pn), np.array(sd, dtype=np.int8)


# ---------------- 採点 ----------------
def _t(x):
    return float(x.mean() / x.std() * np.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0


def grade(dates, ti, pn, label, min_n=100):
    if len(pn) < min_n:
        return None
    d = pd.Series(pn, index=pd.DatetimeIndex(dates[ti]).date)
    day = d.groupby(level=0).sum()
    d2 = pd.to_datetime(day.index); yr = day.groupby(d2.year).sum()
    mid = len(day) // 2
    pn2 = pn - COST_PIPS                                       # コスト2倍
    day2 = pd.Series(pn2, index=pd.DatetimeIndex(dates[ti]).date).groupby(level=0).sum()
    g = pn[pn > 0].sum(); ls = -pn[pn < 0].sum()
    return dict(label=label, n=len(pn), t=_t(day), t1=_t(day.iloc[:mid]), t2=_t(day.iloc[mid:]),
                wy=int((yr > 0).sum()), ny=len(yr), ann=float(yr.mean()), tc2=_t(day2),
                tt3=_t(day.sort_values(ascending=False).iloc[3:]),
                pf=float(g / ls) if ls > 0 else np.inf, win=float((pn > 0).mean() * 100),
                avg=float(pn.mean()), tot=float(pn.sum()))


# ---------------- グリッド ----------------
SMAS = [(20, 50, 200), (10, 25, 75), (25, 75, 200), (5, 20, 60)]
BBS = [(20, 2.0), (20, 2.5), (20, 3.0)]
EXITS = [(20, 20), (15, 15), (30, 30), (20, 10), (10, 20)]
TRAILS = [(0, 0), (5, 3)]
MTFS = [False, True]

_G = {}


def init(o, h, l, dates):
    _G['o'], _G['h'], _G['l'], _G['dates'] = o, h, l, dates


def work(job):
    smas, bb, mtf, sig_arr = job
    o, h, l, dates = _G['o'], _G['h'], _G['l'], _G['dates']
    out = []
    for (tp, sl), (ta, to) in itertools.product(EXITS, TRAILS):
        for dname, mask in (('both', sig_arr), ('long', np.where(sig_arr > 0, sig_arr, 0)),
                            ('short', np.where(sig_arr < 0, sig_arr, 0))):
            ti, pn, sd = simulate(o, h, l, mask, tp, sl, ta, to)
            lab = (f'SMA{smas[0]}/{smas[1]}/{smas[2]}|BB{bb[0]},{bb[1]}|MTF{int(mtf)}|'
                   f'TP{tp}/SL{sl}|TR{ta}-{to}|{dname}')
            r = grade(dates, ti, pn, lab)
            if r:
                r.update(smas=str(smas), bb=bb[1], mtf=mtf, tp=tp, sl=sl, trail=ta > 0, dirn=dname)
                out.append(r)
    return out


if __name__ == '__main__':
    print('データ構築中...', flush=True)
    b5, b15 = build()
    print(f'  M5 {len(b5):,}本  {b5.index.min().date()} 〜 {b5.index.max().date()}  ({time.time()-T0:.0f}s)', flush=True)
    o, h, l = b5.o.values, b5.h.values, b5.l.values
    dates = b5.index.values

    jobs = []
    for smas, bb, mtf in itertools.product(SMAS, BBS, MTFS):
        s = signals(b5, b15, smas, bb[0], bb[1], 10, mtf).values.astype(np.int8)
        jobs.append((smas, bb, mtf, s))
    print(f'シグナル{len(jobs)}通り生成 ({time.time()-T0:.0f}s)・約定シミュ開始', flush=True)

    with Pool(12, initializer=init, initargs=(o, h, l, dates)) as p:
        res = [r for chunk in p.imap_unordered(work, jobs) for r in chunk]

    D = pd.DataFrame(res)
    D.to_csv('_bt_rem_family_0922.csv', index=False)
    NF = np.sqrt(2 * np.log(max(len(D), 2)))
    need = (D.ny * 0.8).round()
    ok = D[(D.t > NF) & (D.t1 > 1.5) & (D.t2 > 1.5) & (D.wy >= need) & (D.tc2 > 2) & (D.tt3 > 2.5)]
    print(f'\n{"="*78}\n総セル {len(D)}・ノイズ床(期待最大t) = {NF:.2f}  ({time.time()-T0:.0f}s)')
    print(f'合格(t>床 x 前後半t>1.5 x 勝ち年>=8割 x コスト2倍t>2 x 上位3日除去t>2.5): {len(ok)}')
    print(f'\n== t 上位12 ==')
    for _, r in D.sort_values('t', ascending=False).head(12).iterrows():
        print(f'  {r.label:58s} t{r.t:+5.2f} 前{r.t1:+4.1f}/後{r.t2:+4.1f} 勝年{int(r.wy):2d}/{int(r.ny)} '
              f'PF{r.pf:4.2f} 勝率{r.win:4.1f}% n{int(r.n):6d} 平均{r.avg:+5.2f}pips 年{r.ann:+7.0f}pips')
    print(f'\n== 方向別 ==')
    print(D.groupby('dirn').agg(cells=('t', 'size'), max_t=('t', 'max'), med_pf=('pf', 'median'),
                                med_avg=('avg', 'median')).round(3))
    print(f'\n== 全セルの素性 ==')
    print(f'  平均pips>0のセル: {int((D.avg>0).sum())}/{len(D)}   PF>1: {int((D.pf>1).sum())}/{len(D)}')
    print(f'  平均pipsの中央値: {D.avg.median():+.3f}   最大t={D.t.max():.2f}  最小t={D.t.min():.2f}')

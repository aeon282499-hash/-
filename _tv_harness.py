# -*- coding: utf-8 -*-
"""_tv_harness.py — TradingViewのストラテジーを1本足すだけで同じ土俵に乗せる共通ハーネス。

使い方:
    import _tv_harness as T
    b = T.load('GOLD', 'H1')
    pos = my_strategy(b)                    # -1/0/+1 の Series
    print(T.gauntlet(b, pos, 'MyStrat|GOLD|H1', T.cost('GOLD')))

SL/TP付きの戦略は run_events() を使う（足の高値安値で日中約定を判定）。

判定基準は 9/20・9/22 と同じ:
  t > ノイズ床(√(2lnN))  x 前後半t>1.5  x 勝ち年>=8割  x コスト2倍t>2  x 上位3日除去t>2.5
  さらに alpha-t (買い持ちドリフトを引いた残り) > ノイズ床 を「本物」の条件とする。
約定は全て「シグナル足の次の足の始値」建て・その次の足の始値決済を基準(look-ahead防止)。
"""
import pandas as pd, numpy as np, os

DIR = os.path.dirname(os.path.abspath(__file__))
_P = lambda f: os.path.join(DIR, f)

# 往復コスト(価格%)。XMの実測spreadから。金は$0.21/oz→%は価格で割って都度算出。
COSTPCT = {
    'EURUSD': 0.0012, 'USDJPY': 0.0013, 'GBPUSD': 0.0018, 'AUDUSD': 0.0020,
    'USDCHF': 0.0020, 'NZDUSD': 0.0030, 'US500Cash': 0.0104, 'JP225Cash': 0.0122,
    'GER40Cash': 0.0102, 'US30Cash': 0.0110, 'UK100Cash': 0.0130,
    'SILVER.': 0.0900, 'OILCash': 0.0500, 'NGASCash': 0.1500, 'GOLD.': 0.0065,
}
GOLD_ABS = 0.21  # $/oz 往復

_RULE = {'M5': ('5min', None), 'M15': ('15min', None), 'M30': ('30min', None),
         'H1': ('1h', None), 'H4': ('4h', '22h'), 'D1': ('24h', '22h')}

_cache = {}


def _resample(x, tf):
    rule, off = _RULE[tf]
    r = x.resample(rule, offset=off) if off else x.resample(rule)
    b = pd.DataFrame({'o': r.o.first(), 'h': r.h.max(), 'l': r.l.min(),
                      'c': r.c.last(), 'v': r.v.sum()}).dropna()
    b = b[~((b.h == b.l) & (b.v == 0))]          # 板が動いていない足を落とす
    return b[b.index.dayofweek < 5]


def load(sym, tf):
    """sym: 'GOLD' または _xm_multi_hist.pkl のキー('US500Cash','EURUSD',...)"""
    key = (sym, tf)
    if key in _cache:
        return _cache[key]
    if sym.upper() in ('GOLD', 'XAUUSD', 'GOLD.'):
        m1 = pd.read_pickle(_P('_fx_xauusd_m1.pkl'))
        m1 = m1[m1.vol.notna()]
        x = m1.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'vol': 'v'})
        x = x[['o', 'h', 'l', 'c', 'v']]
        b = _resample(x, tf)
        b = b[b.index.dayofweek < 6]             # 金だけ日曜足も残す規約に合わせる
    else:
        H = pd.read_pickle(_P('_xm_multi_hist.pkl'))['h1']
        if sym not in H:
            raise KeyError(f'{sym} が _xm_multi_hist.pkl に無い。{list(H.keys())}')
        x = H[sym].copy()
        x['t'] = pd.to_datetime(x['time'], unit='s')
        x = x.set_index('t').sort_index()
        x = x.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c',
                              'tick_volume': 'v'})[['o', 'h', 'l', 'c', 'v']]
        if tf in ('M5', 'M15', 'M30'):
            raise ValueError(f'{sym} のH1データからは {tf} を作れない（H1が最小足）')
        b = _resample(x, tf)
    _cache[key] = b
    return b


def cost(sym, price=None):
    """往復コストを価格%で返す。金は$0.21/ozを実価格で割る。"""
    if sym.upper() in ('GOLD', 'XAUUSD', 'GOLD.'):
        return GOLD_ABS / (price if price else 2000.0) * 100
    return COSTPCT[sym]


# ---------------- 指標(Pineの既定と同じ定義) ----------------
def ema(x, n): return x.ewm(span=n, adjust=False).mean()
def rma(x, n): return x.ewm(alpha=1 / n, adjust=False).mean()
def sma(x, n): return x.rolling(n).mean()


def atr(b, n):
    tr = pd.concat([b.h - b.l, (b.h - b.c.shift()).abs(), (b.l - b.c.shift()).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def rsi(c, n):
    d = c.diff()
    return 100 - 100 / (1 + rma(d.clip(lower=0), n) / rma((-d).clip(lower=0), n).replace(0, np.nan))


def stdev(x, n): return x.rolling(n).std(ddof=0)


def bb(c, n=20, k=2.0):
    m = sma(c, n); s = stdev(c, n)
    return m - k * s, m, m + k * s


# ---------------- 判定 ----------------
def _t(x):
    return (x.mean() / x.std() * np.sqrt(len(x))) if len(x) > 2 and x.std() > 0 else 0.0


def gauntlet(b, pos, label, cpct, min_days=200):
    """pos: -1/0/+1 の Series(その足の終値時点の判断)。次足始値建て・その次の始値で評価。"""
    pos = pos.reindex(b.index).fillna(0).astype(float)
    o = b.o
    ret = ((o.shift(-2) / o.shift(-1) - 1) * 100).fillna(0)
    pnl = pos * ret
    chg = pos.diff().abs().fillna(pos.abs())
    cp = chg * (cpct / 2)
    net = pnl - cp
    mu = ret[ret != 0].mean()                    # 買い持ちドリフト
    d = pd.to_datetime(b.index).date
    day = net.groupby(d).sum(); day = day[day != 0]
    if len(day) < min_days:
        return None
    alpha = (net - pos * mu).groupby(d).sum(); alpha = alpha[alpha != 0]
    n2 = (pnl - 2 * cp).groupby(d).sum(); n2 = n2[n2 != 0]
    mid = len(day) // 2
    d2 = pd.to_datetime(day.index); yr = day.groupby(d2.year).sum()
    return dict(label=label, t=_t(day), ta=_t(alpha), t1=_t(day.iloc[:mid]), t2=_t(day.iloc[mid:]),
                wy=int((yr > 0).sum()), ny=len(yr), ann=yr.mean(), tc2=_t(n2),
                tt3=_t(day.sort_values(ascending=False).iloc[3:]),
                y25=day[d2 >= pd.Timestamp('2025-01-01')].sum(),
                trades=int((chg > 0).sum()), expo=pos.abs().mean())


def run_events(b, entry, cpct, sl_pct=None, tp_pct=None, max_bars=None, exit_sig=None):
    """SL/TP付きの戦略。entry: +1/-1/0 の Series。次足始値で建て、足のH/LでSL/TPを判定。
    同一足でSLとTPの両方に触れた場合はSL優先(悲観側)。戻り値は玉ごとのDataFrame。"""
    e = entry.reindex(b.index).fillna(0).astype(float).values
    o, h, l = b.o.values, b.h.values, b.l.values
    idx = b.index
    xs = exit_sig.reindex(b.index).fillna(0).astype(bool).values if exit_sig is not None else None
    trades, i, n = [], 0, len(b)
    while i < n - 2:
        if e[i] == 0:
            i += 1; continue
        side = int(np.sign(e[i]))
        j0 = i + 1
        ep = o[j0]                                # 次足の始値で建て
        sl = ep * (1 - side * sl_pct / 100) if sl_pct else None
        tp = ep * (1 + side * tp_pct / 100) if tp_pct else None
        xp, xi, why = None, None, ''
        for j in range(j0, n):
            if sl is not None and ((side > 0 and l[j] <= sl) or (side < 0 and h[j] >= sl)):
                xp, xi, why = sl, j, 'SL'; break
            if tp is not None and ((side > 0 and h[j] >= tp) or (side < 0 and l[j] <= tp)):
                xp, xi, why = tp, j, 'TP'; break
            if xs is not None and xs[j] and j > j0:
                xp, xi, why = o[j + 1] if j + 1 < n else o[j], j, 'SIG'; break
            if max_bars and j - j0 >= max_bars:
                xp, xi, why = o[j + 1] if j + 1 < n else o[j], j, 'TIME'; break
        if xp is None:
            break
        trades.append(dict(t_in=idx[j0], t_out=idx[xi], side=side, ep=ep, xp=xp, why=why,
                           ret=(xp / ep - 1) * 100 * side - cpct, bars=xi - j0))
        i = xi + 1
    return pd.DataFrame(trades)


def grade_trades(tr, label):
    """run_events の結果を gauntlet と同じ物差しで採点。"""
    if len(tr) < 30:
        return None
    day = tr.groupby(pd.to_datetime(tr.t_out).dt.date).ret.sum()
    d2 = pd.to_datetime(day.index); yr = day.groupby(d2.year).sum()
    mid = len(day) // 2
    g = tr.ret[tr.ret > 0].sum(); ls = -tr.ret[tr.ret < 0].sum()
    return dict(label=label, n=len(tr), t=_t(day), t1=_t(day.iloc[:mid]), t2=_t(day.iloc[mid:]),
                wy=int((yr > 0).sum()), ny=len(yr), ann=yr.mean(),
                pf=(g / ls if ls > 0 else np.inf), win=(tr.ret > 0).mean() * 100,
                avg=tr.ret.mean(), bars=tr.bars.median(),
                tt3=_t(day.sort_values(ascending=False).iloc[3:]))


def verdict(rows, name='', pass_wy=0.8):
    """セル表を受け取り、合格セル数とノイズ床を印字。Dを返す。"""
    D = pd.DataFrame([r for r in rows if r])
    if not len(D):
        print('セルゼロ'); return D
    NF = np.sqrt(2 * np.log(max(len(D), 2)))
    need_wy = (D.ny * pass_wy).round()
    ok = D[(D.t > NF) & (D.t1 > 1.5) & (D.t2 > 1.5) & (D.wy >= need_wy) &
           (D.get('tc2', D.t) > 2) & (D.tt3 > 2.5)]
    real = ok[ok.ta > NF] if 'ta' in D else ok
    print(f'\n{name} 総セル {len(D)}・ノイズ床(期待最大t)= {NF:.2f}')
    print(f'  合格(t>床 x 前後半t>1.5 x 勝ち年>={pass_wy:.0%} x コスト2倍t>2 x 上位3日除去t>2.5): {len(ok)}')
    print(f'  うち alpha-t>床(買い持ちを引いても残る) = {len(real)}')
    print('\n== t 上位10 ==')
    for _, r in D.sort_values('t', ascending=False).head(10).iterrows():
        at = f' a-t{r.ta:+5.2f}' if 'ta' in D else ''
        print(f'  {r.label:46s} t{r.t:+5.2f}{at} 前{r.t1:+4.1f}/後{r.t2:+4.1f} '
              f'勝年{int(r.wy)}/{int(r.ny)} 年率{r.ann:+6.1f}%')
    return D

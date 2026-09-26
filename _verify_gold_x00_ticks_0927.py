# -*- coding: utf-8 -*-
"""_verify_gold_x00_ticks_0927.py — 大台ブレイクの約定をティックで再現して「滑り」を測る（2026-09-27）
1分足BT(_verify_gold_x00_0927.py と同じ定義)で出た 2023-01以降の玉について、XMサーバーのティック(GOLD.)を取り、
EAと同じ手順で再現する: 大台に触れた足の中で最初に BID≥L になったティックでアーム → 買い逆指値 = L+δ+その時のスプレッド(ASK基準)
→ ASK≥逆指値 の最初のティックで約定(約定値=そのティックのASK＝飛んだ分は不利に) → 約定の15分後以降の最初のティックのBIDで決済。
比較: 同じ玉の1分足BTの損益。コストは手数料$0.453/oz(スプレッドはティックのASK-BIDで実際に払う)。
"""
import MetaTrader5 as mt5, pandas as pd, numpy as np, datetime as dt, subprocess, sys
assert mt5.initialize()
SYM = 'GOLD.'; DELTA = 1.0; HOLD = 15; COMM = 0.453
# 1分足BTの玉を作り直す(検証スクリプトと同じ定義)
d = pd.read_pickle('_xm_m1_GOLD._0926.pkl'); d = d[d.index >= '2022-12-20']
t = d.index; op = d.open.values.astype(float); hi = d.high.values.astype(float); cl = d.close.values.astype(float); sp = d.spread.values * 0.01
day = t.normalize(); mins = (t.hour * 60 + t.minute).values; n = len(d)
rows = []; i = 1; busy = -1; cur = day[0]; dh = -np.inf; armed = {}; touched = set()
while i < n:
    if day[i] != cur: cur = day[i]; dh = -np.inf; armed = {}; touched = set()
    m = mins[i]; win = 120 <= m <= 1364
    if win and day[i - 1] == cur:
        L = np.floor(cl[i - 1] / 100) * 100 + 100
        while L <= hi[i]:
            if L > dh and L not in touched and cl[i - 1] < L: armed[L] = (i + 59, i); touched.add(L)
            L += 100
    if win and i > busy:
        for L in sorted(armed):
            e, ai = armed[L]
            if i > e: continue
            if hi[i] >= L + DELTA:
                j = i + HOLD
                if j >= n or day[j] != cur or mins[j] >= 1380: armed.pop(L); break
                entry = max(L + DELTA, op[i]) if op[i] >= L + DELTA else L + DELTA
                rows.append(dict(arm_t=t[ai], ent_t=t[i], L=L, bar_entry=entry, bar_exit=op[j], bar_pnl=op[j] - entry - sp[i] - COMM))
                busy = j; armed.pop(L); break
        armed = {L: v for L, v in armed.items() if v[0] >= i}
    dh = max(dh, hi[i]); i += 1
B = pd.DataFrame(rows); B = B[B.arm_t >= '2023-01-03'].reset_index(drop=True)
print(f"1分足BTの玉(2023-01〜): {len(B)}件  平均 ${B.bar_pnl.mean():+.3f}/oz", flush=True)

# ティック取得(サーバー時刻のまま)
def srv_to_utc(ts):
    # MT5は足もティックも「サーバー時刻をそのままエポック秒にした値」で持つ＝要求もサーバー時刻のまま渡す(UTCに直すと2〜3時間ずれる)
    return pd.Timestamp(ts)
out = []
for r in B.itertuples():
    a_utc = srv_to_utc(r.arm_t)
    if pd.isna(a_utc): continue
    a_dt = a_utc.to_pydatetime().replace(tzinfo=dt.timezone.utc)   # 時刻なし(naive)は日本時間と解釈されて9時間ずれる→サーバー時刻をUTC扱いで渡す
    tk = mt5.copy_ticks_range(SYM, a_dt - dt.timedelta(seconds=5), a_dt + dt.timedelta(minutes=60 + HOLD + 5), mt5.COPY_TICKS_ALL)
    if tk is None or len(tk) == 0: out.append(dict(L=r.L, note='ティック無し')); continue
    T = pd.DataFrame(tk); T['ts'] = pd.to_datetime(T['time_msc'], unit='ms')
    T = T[(T.bid > 0) & (T.ask > 0)]
    arm_end = a_utc + pd.Timedelta(minutes=1)
    ax = T[(T.ts >= a_utc) & (T.ts < arm_end) & (T.bid >= r.L)]
    if ax.empty: out.append(dict(L=r.L, note='足の中でBID≥Lのティック無し')); continue
    a0 = ax.iloc[0]
    trig = r.L + DELTA + (a0.ask - a0.bid)
    fx = T[(T.ts >= a0.ts) & (T.ts <= a_utc + pd.Timedelta(minutes=60)) & (T.ask >= trig)]
    if fx.empty: out.append(dict(L=r.L, note='60分以内に逆指値に届かず')); continue
    f0 = fx.iloc[0]
    xx = T[T.ts >= f0.ts + pd.Timedelta(minutes=HOLD)]
    if xx.empty: out.append(dict(L=r.L, note='決済ティック無し')); continue
    x0 = xx.iloc[0]
    pnl = x0.bid - f0.ask - COMM
    out.append(dict(L=r.L, arm=a0.ts, fill=f0.ts, trig=trig, fill_ask=f0.ask, slip=f0.ask - trig, spread_fill=f0.ask - f0.bid,
                    exit_bid=x0.bid, tick_pnl=pnl, bar_pnl=r.bar_pnl, note=''))
O = pd.DataFrame(out)
ok = O[O.note == '']
print(f"ティック再現できた {len(ok)}/{len(O)}件  除外: " + ', '.join(f"{k}{v}" for k, v in O[O.note != ''].note.value_counts().items()))
print(f"  ティック再現の損益  平均 ${ok.tick_pnl.mean():+.3f}/oz  中央 ${ok.tick_pnl.median():+.3f}  t{ok.tick_pnl.mean()/(ok.tick_pnl.std(ddof=1)/np.sqrt(len(ok))):+.2f}")
print(f"  同じ玉の1分足BT     平均 ${ok.bar_pnl.mean():+.3f}/oz  中央 ${ok.bar_pnl.median():+.3f}  t{ok.bar_pnl.mean()/(ok.bar_pnl.std(ddof=1)/np.sqrt(len(ok))):+.2f}")
print(f"  逆指値の滑り(約定ASK−逆指値) 平均 ${ok.slip.mean():.3f} 中央 ${ok.slip.median():.3f} 90%点 ${ok.slip.quantile(.9):.3f} 最大 ${ok.slip.max():.3f}")
print(f"  約定時のスプレッド 平均 ${ok.spread_fill.mean():.3f} / 1分足BTとの差(ティック−足) 平均 ${ (ok.tick_pnl - ok.bar_pnl).mean():+.3f}")
yr = pd.to_datetime(ok.arm).dt.year
print('  年別(ティック再現): ' + ' '.join(f"{y}: n{len(g)} ${g.tick_pnl.mean():+.2f}/oz" for y, g in ok.groupby(yr)))
O.to_csv('_verify_gold_x00_ticks_0927.csv', index=False, encoding='utf-8-sig')

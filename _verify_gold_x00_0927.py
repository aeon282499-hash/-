# -*- coding: utf-8 -*-
"""_verify_gold_x00_0927.py — G1の「大台(＄100)に初めて下から触れた後、+δ上抜けで買い→15分」を別コードで一から再現する（2026-09-27）
G1のスクリプトは読まずに、報告の仕様だけから書く(独立検証)。先読みしない:
  ・「その日の高値がまだLに届いていない」は、その足より前の足だけで判定(サーバー日の0時から)
  ・触れた足(アーム)は「直前の足の終値<L かつ その足の高値≥L」
  ・アームから60分以内(アームの足を含む)に高値≥L+δ の足で買い: 約定=max(L+δ, その足の始値)＋その足のスプレッド＋手数料
    (アームの足の中でLを通ってL+δに達するのは、直前の終値<L から連続で上がる経路なので有効)
  ・15分後の足の始値(bid)で手仕舞い・同時保有1・サーバー02:00〜22:44に建てる・手仕舞いが23:00以降になる玉は除外
コスト: その足のスプレッド(pt×$0.01)＋手数料$0.453/oz往復。%は約定価格比。
"""
import pandas as pd, numpy as np, sys
d = pd.read_pickle('_xm_m1_GOLD._0926.pkl')
d = d[(d.index >= '2015-01-01')]
COMM = 0.453
DELTA = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
HOLD = int(sys.argv[2]) if len(sys.argv) > 2 else 15
STEP = float(sys.argv[3]) if len(sys.argv) > 3 else 100.0
OFFSET = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
t = d.index; op = d.open.values.astype(float); hi = d.high.values.astype(float); cl = d.close.values.astype(float); sp = d.spread.values.astype(float) * 0.01
day = t.normalize(); mins = (t.hour * 60 + t.minute).values
n = len(d); rows = []
i = 1; busy_until = -1
day_start = 0
cur_day = day[0]; day_hi = -np.inf      # その足より前の、同じサーバー日の高値
armed = {}                               # L -> 期限index
touched_today = set()
while i < n:
    if day[i] != cur_day:
        cur_day = day[i]; day_hi = -np.inf; armed = {}; touched_today = set()
    m = mins[i]
    in_window = 120 <= m <= 22 * 60 + 44
    # 1) アーム判定: 直前の足の終値<L≤この足の高値、かつこの足より前の当日高値<L
    if in_window and i >= 1 and day[i - 1] == cur_day:
        lo_L = np.floor((cl[i - 1] - OFFSET) / STEP) * STEP + OFFSET + STEP   # 直前終値の上の最初の格子
        L = lo_L
        while L <= hi[i]:
            if L > day_hi and L not in touched_today and cl[i - 1] < L:
                armed[L] = i + 59                                             # アームの足を含めて60分
                touched_today.add(L)
            L += STEP
    # 2) 建て: アーム中の水準で高値≥L+δ(保有していない時)
    if in_window and i > busy_until:
        for L in sorted(armed):
            if i > armed[L]:
                continue
            trig = L + DELTA
            if hi[i] >= trig:
                j = i + HOLD
                if j >= n or day[j] != cur_day or mins[j] >= 23 * 60:
                    armed.pop(L); break
                entry = max(trig, op[i]) if op[i] >= trig else trig
                # アームの足で始値がすでにL+δ以上＝直前終値<L なので窓空け上昇→始値で約定
                cost = sp[i] + COMM
                ret = (op[j] - entry - cost) / entry * 100
                gross = (op[j] - entry) / entry * 100
                rows.append((t[i], L, entry, op[j], gross, ret, cost))
                busy_until = j
                armed.pop(L)
                break
        # 期限切れを掃除
        armed = {L: e for L, e in armed.items() if e >= i}
    day_hi = max(day_hi, hi[i])
    i += 1
R = pd.DataFrame(rows, columns=['t', 'L', 'entry', 'exit', 'gross', 'net', 'cost'])
now_px = 4300.0
cost_now_pct = 0.593 / now_px * 100
R['net_now'] = R.gross - cost_now_pct
def tt(x): x = np.asarray(x); return x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))
yrs = (R.t.max() - R.t.min()).days / 365.25
y = R.t.dt.year
print(f"STEP${STEP:.0f}+{OFFSET:.1f} δ${DELTA} 保有{HOLD}分: n={len(R)} 年{len(R)/yrs:.1f}回 | コスト抜き{R.gross.mean():+.4f}% t{tt(R.gross):+.2f} | 実コスト後{R.net.mean():+.4f}% t{tt(R.net):+.2f} | 今のコスト(0.0138%)後{R.net_now.mean():+.4f}% t{tt(R.net_now):+.2f}"
      f" | 前半(〜2020)t{tt(R.net_now[y<=2020]):+.2f} 後半t{tt(R.net_now[y>=2021]):+.2f} | 勝ち年{(R.groupby(y).net_now.sum()>0).sum()}/{y.nunique()} | 上位3玉除去t{tt(np.sort(R.net_now.values)[:-3]):+.2f}")
print('  年別(今コスト後の合計%):', ' '.join(f"{k}:{v:+.2f}({c})" for (k, v), c in zip(R.groupby(y).net_now.sum().items(), R.groupby(y).size())))

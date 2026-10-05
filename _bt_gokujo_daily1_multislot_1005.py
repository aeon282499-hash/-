# -*- coding: utf-8 -*-
"""_bt_gokujo_daily1_multislot_1005.py — 極上「保有中でも毎日その日の1位を買う」(本人案 2026-10-05)
新エンジン(_bt_gokujo_engine_0924)の1枠シムを K枠に拡張。1日に建てるのは1位の1本だけ・次点繰り上げなし・保有中の同一銘柄は見送り。"""
import sys
import numpy as np, pandas as pd
import _bt_gokujo_engine_0924 as G

def run_multi(slots, size=1_500_000):
    PNL, EXO, TYP = G.EXITS["close"]
    mk = G.NEW & ~G.EARN_EX & ~G.DC_EX; nfm = G.NFOK & ~G.nf_earn & ~G.NF_DC_EX
    rows = []; open_ = []   # (exit_idx, ticker)
    for d in range(len(G.DAYS)):
        open_ = [o for o in open_ if o[0] >= d]
        if len(open_) >= slots: continue
        cands = []
        g = G.groups[d]
        if len(g):
            g = g[mk[g]]; g = g[np.isfinite(PNL[g])]
            cands += [(G.SCORE[i], "F", i) for i in g]
        cands += [(G.nf_score[j], "N", j) for j in G.nf_groups.get(d, []) if nfm[j]]
        if not cands: continue
        s, kind, i = max(cands, key=lambda x: x[0])
        if kind == "N": continue
        if any(o[1] == G.TICKv[i] for o in open_): continue
        sh = int(size / G.E0[i] / 100) * 100
        if sh <= 0: continue
        ex = min(d + int(EXO[i]), len(G.DAYS) - 1)
        rows.append((G.DAYS[d], G.DAYS[ex], G.TICKv[i], G.YEARv[i], PNL[i], PNL[i] / 100 * sh * G.E0[i], sh, TYP[i], len(open_)))
        open_.append((ex, G.TICKv[i]))
    return pd.DataFrame(rows, columns=["entry", "exit", "ticker", "year", "pnl", "yen", "sh", "typ", "nopen"])

def extra(R, a, b):
    X = R[(R.entry >= a) & (R.entry <= b)]
    m = X.groupby(pd.to_datetime(X.exit).dt.to_period("M")).yen.sum()
    m = m.reindex(pd.period_range(m.index.min(), m.index.max(), freq="M"), fill_value=0)
    r12 = m.rolling(12).sum().min() / 1e4
    # 同日決済の最悪日・同時損切り
    dly = X.groupby("exit").yen.sum()
    print(f"      最悪月{m.min()/1e4:+.0f}万 12か月最悪{r12:+.0f}万 最悪日{dly.min()/1e4:+.1f}万 / 2本目以降の玉: n={int((X.nopen>0).sum())} 平均{X.pnl[X.nopen>0].mean():+.2f}% 勝率{(X.pnl[X.nopen>0]>0).mean()*100:.1f}% (1本目 {X.pnl[X.nopen==0].mean():+.2f}%)")

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    for per, a, b in (("26年", "2001-01-01", "2026-12-31"), ("10年 2017〜", "2017-01-01", "2026-12-31"), ("直近5年", "2021-09-01", "2026-12-31"), ("2001-16", "2001-01-01", "2016-12-31")):
        print(f"\n■ {per}（150万/玉・今の本番ルール・費用なし）")
        for k in (1, 2, 3):
            R = run_multi(k); G.summary(R, a, b, f"{k}枠（1日1本まで）"); extra(R, a, b)
    R = run_multi(3)
    print("\n3枠 年別(万):"); print((R.groupby("year").yen.sum() / 1e4).round(0).to_frame("万").T.to_string())
    R1 = run_multi(1)
    print("1枠 年別(万):"); print((R1.groupby("year").yen.sum() / 1e4).round(0).to_frame("万").T.to_string())

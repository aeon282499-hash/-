# -*- coding: utf-8 -*-
"""bb_compare.py — 生き残った「逆張り売り×GU/急騰土俵×10:00〜」が BB 固有の価値か、指標なしの型(固定時刻・担がれ・新高値)と同じものかを比べる。
   コスト2種: 本検証(往復0.10%+不利2tick) と 9/16と同じ(往復0.10%のみ)。
使い方: python -X utf8 bb_compare.py 5m
"""
from __future__ import annotations
import sys, os, time
import numpy as np, pandas as pd
import bb_engine as B

HERE = os.path.dirname(os.path.abspath(__file__))


def st(r):
    r = np.asarray(r, float); n = len(r)
    if n == 0: return "n0"
    gp = r[r > 0].sum(); gl = -r[r < 0].sum()
    return f"{n} / {(r>0).mean()*100:.0f}% / {gp/gl if gl>0 else np.inf:.2f} / {r.mean():+.3f}"


def main(kind):
    D = B.Data(kind); W = D.windows(); rng = np.random.default_rng(7)
    ma, up, lo = D.bb(40, 2.0)
    sig_bb = D.signals(up, lo, "upper", "D2_外0.5%")
    sig_bb20 = D.signals(*D.bb(20, 2.0)[1:], "upper", "D1_外")
    day_open = D.o[D.row_dstart]
    sig_katsu = D.c >= day_open * 1.03                         # 寄値+3%超え(担がれ)
    runmax = pd.Series(D.h).groupby(D.grp).cummax().to_numpy()
    sig_high = (D.c >= runmax * 0.999)                         # 終値が当日高値圏(新高値)
    sig_fixed = np.ones(D.n, bool)                             # 窓の最初の足
    sigs = {"固定時刻(窓の最初の足)": sig_fixed, "寄値+3%超え(担がれ)": sig_katsu, "寄値+5%超え": D.c >= day_open * 1.05, "寄値+7%超え": D.c >= day_open * 1.07,
            "40本MA比+3%(σ不使用)": D.c >= ma * 1.03, "40本MA比+5%(σ不使用)": D.c >= ma * 1.05, "当日高値更新": sig_high, "BB40/2σ 上0.5%外": sig_bb, "BB20/2σ 上外": sig_bb20,
            "BB40/2σ上0.5%外 かつ 寄値+3%": sig_bb & sig_katsu, "寄値+3% かつ BBの外でない": sig_katsu & ~sig_bb20}
    exits = [("引け", None), ("N12本", None), ("MA回帰", None)]
    L = []; P = L.append
    P(f"## {kind} 指標なしの型との比較（売り・次足始値で建て・前半/後半）")
    P("数字 = n / 勝率 / PF / 1件平均%。コストA = 往復0.10%+不利2tick（本検証）、コストB = 往復0.10%のみ（9/16と同じ）。")
    P("")
    for un in ("U3_GU+3%", "U4_前日+8%", "U2_代金20億+"):
        for wn in ("W2_10:00〜", "W1_9:30〜"):
            P(f"### {un} × {wn} × 決算除外")
            P("| 入口 | 出口 | 全期間 A | 前半 A | 後半 A | 全期間 B | 前半 B | 後半 B |")
            P("|---|---|---|---|---|---|---|---|")
            for nm, sg in sigs.items():
                e = D.first_in_window(sg, W[wn]); g = D.grp[e]
                m = D.uni[un][g] & ~D.g_earn[g]; e = e[m]; g = g[m]
                if len(e) < 20: P(f"| {nm} | - | n={len(e)} | | | | | |"); continue
                ep, res = D.simulate(e, ma, up, lo, "upper")
                R, C = D.returns(e, ep, res, -1, with_cost=True); half = D.half[g]
                for bx, sl in exits:
                    ci = B.EXITS.index((bx, sl)); a = R[:, ci]; b = a + C[:, ci] - B.COST_RT
                    P(f"| {nm} | {bx} | {st(a)} | {st(a[half==1])} | {st(a[half==2])} | {st(b)} | {st(b[half==1])} | {st(b[half==2])} |")
            P("")
    # ---- 代表セルのコスト感度と再現(同じ土俵・10:00〜・決算込/除外) ----
    P(f"### {kind} 代表セル BB40/2σ 上0.5%外 × 10:00〜 × 売り のコスト感度（U3_GU+3%・決算除外・出口 MA回帰／引け）")
    P("| 出口 | コスト0 | 往復0.10% | 往復0.10%+2tick(本検証) | 往復0.30%+2tick | 往復0.50%+2tick |")
    P("|---|---|---|---|---|---|")
    e = D.first_in_window(sig_bb, W["W2_10:00〜"]); g = D.grp[e]; m = D.uni["U3_GU+3%"][g] & ~D.g_earn[g]; e = e[m]
    ep, res = D.simulate(e, ma, up, lo, "upper"); R, C = D.returns(e, ep, res, -1, with_cost=True)
    for bx in ("MA回帰", "引け"):
        ci = B.EXITS.index((bx, None)); gross = R[:, ci] + C[:, ci]; tk = C[:, ci] - B.COST_RT
        P(f"| {bx} | {st(gross)} | {st(gross-0.10)} | {st(R[:,ci])} | {st(gross-tk-0.30)} | {st(gross-tk-0.50)} |")
    P("")
    # ---- 時刻分布と日別集中 ----
    tr, _ = D.trades(40, 2.0, "D2_外0.5%", "upper", "W2_10:00〜", -1, "U3_GU+3%", True)
    col = "MA回帰|0"; byday = tr.groupby("date")[col].agg(["size", "sum"]).sort_values("sum", ascending=False)
    P(f"### {kind} 代表セルの日別集中（上位5日 / 合計 {tr[col].sum():+.0f}%・{len(tr)}件・{tr.date.nunique()}日）")
    P("| 日付 | 件数 | 合計% |"); P("|---|---|---|")
    for d, r in byday.head(5).iterrows(): P(f"| {d} | {int(r['size'])} | {r['sum']:+.1f} |")
    P(f"- シグナル足の時刻分布: " + ", ".join(f"{k} {v}" for k, v in tr.tod.str[:2].value_counts().sort_index().items()))
    P(f"- プラスの日の割合 {(byday['sum']>0).mean()*100:.0f}%・日別合計の中央値 {byday['sum'].median():+.2f}%")
    P("")
    out = os.path.join(HERE, f"compare_{kind}.md"); open(out, "w", encoding="utf-8").write("\n".join(L)); print("->", out)


if __name__ == "__main__":
    main(sys.argv[1])

# -*- coding: utf-8 -*-
"""bb_period_compare_1122.py — BB売りの期間 20 と 40 はどっちが勝つか（2026-11-22）。

本人「20と40どっちがかてる」。bb_filters_1122.py と同じデータ・同じ規約で、期間だけを 20/40 に変えて並べる。
  共通: 2σ・上バンドの0.5%以上外・10:00〜14:25 の最初の点灯・次足始値で売り・GU+3%・決算除外・往復0.10%＋不利1tick×2
  出口: 中心線回帰（その期間のMA）／大引け
  追加条件: なし／寄値+2%以上
  期間: 選定（8/25〜10/22）・確認（10/23〜11/20）、全時間帯と前場（10:00〜11:25）
実行: cd kabuai && python bb_period_compare_1122.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, DEV, SIG, SPLIT, W_HI, W_LO, is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PERIODS = (20, 40)


def trades(code, s, cand, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    full = float(pd.Series(s.index.normalize()).value_counts().median())
    c, o, v = s.c.to_numpy(float), s.o.to_numpy(float), s.v.to_numpy(float)
    ser = pd.Series(c); is5 = is500_of(c)
    bands = {p: (ser.rolling(p).mean().to_numpy(), (ser.rolling(p).mean() + SIG * ser.rolling(p).std(ddof=0)).to_numpy()) for p in PERIODS}
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum())
            ds = dd.strftime("%Y-%m-%d")
            if (100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and o[st] / pc - 1 >= 0.03 \
                    and (code, ds) in cand and (code, ds) not in earn:
                for p, (ma, up) in bands.items():
                    for filt in (False, True):
                        e = next((k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI and np.isfinite(up[k])
                                  and c[k] > up[k] and (c[k] - up[k]) / c[k] >= DEV
                                  and (not filt or c[k] >= o[st] * 1.02)), None)
                        if e is None:
                            continue
                        ep = o[e + 1] - tick_of(o[e + 1], is5)
                        px = c[en - 1]
                        for k in range(e + 1, en):
                            if c[k] <= ma[k]:
                                px = o[k + 1] if k + 1 < en else c[en - 1]; break
                        eod = c[en - 1]
                        for exit_name, x in (("中心線回帰", px), ("大引け", eod)):
                            r = -(x + tick_of(x, is5) - ep) / ep * 100 - COST_RT
                            out.append({"p": p, "filt": filt, "exit": exit_name, "date": ds, "ret": r,
                                        "fwd": ds >= SPLIT, "am": tmin[e] <= 11 * 60 + 25})
        prev = (st, en)
    return out


def st(r: pd.Series) -> str:
    if len(r) == 0:
        return "n=  0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return f"n={len(r):>3} 勝率{(r>0).mean()*100:>3.0f}% PF{(gp/gl if gl else np.inf):>5.2f} 1件{r.mean():>+5.2f}%"


def main() -> None:
    token = _jquants_key()
    dl = _recent_trading_days(62)
    D = jq_daily(token, dl).sort_values(["code", "date"])
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1); D["gu"] = D.AdjO / D.pc - 1
    C = D[(D.gu >= 0.025) & (D.pva >= 1e8) & (D.pc >= 100) & (D.pc <= 100000)]
    cand = set(zip(C.code, C.date.astype(str).str[:10]))
    earn = jq_fins(token, dl)
    dd = sorted(set(D.date.astype(str).str[:10])); nxt = {dd[i]: dd[i + 1] for i in range(len(dd) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(set(C.code))
    T = pd.DataFrame([x for code, s in M.items() for x in trades(code, s, cand, earn)])
    for scope, m in (("全時間帯（10:00〜14:25）", np.ones(len(T), bool)), ("前場だけ（10:00〜11:25）", T.am.to_numpy())):
        print(f"\n################ {scope} ################")
        for filt in (False, True):
            print(f"\n  ◆ 追加条件: {'寄値+2%以上' if filt else 'なし'}")
            for p in PERIODS:
                for ex in ("中心線回帰", "大引け"):
                    d = T[m & (T.p == p) & (T.filt == filt) & (T.exit == ex)]
                    print(f"    BB{p} {ex:<5}  選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
    print("\n読み方: 判断は「確認」（10/23〜・どちらにも使っていない日）。件数が少ないので差が小さければ「どちらでも同じ」。")


if __name__ == "__main__":
    main()

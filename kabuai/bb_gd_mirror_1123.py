# -*- coding: utf-8 -*-
"""bb_gd_mirror_1123.py — 今のBB売りルールを裏返した「GD株のBB下抜け買い」の検証（2026-11-23）。

本人「GDはどう？」。今の売りルールの鏡像:
  対象 GD−3%（寄値÷前日終値 ≤ −3%）・前日代金1億+・決算除外
  合図 終値 ≤ −2σ×0.995 かつ 終値 ≤ 寄値×0.98（BB40/2σ・前日から連続）。時間帯 10:00〜11:25（前場）と 10:00〜14:25（全）
  買い 合図の次足始値（不利1tick）
  出口 中心線回帰（終値 ≥ 40本MA → 次足始値）／中心線 or +3%利確の早い方。どちらも無ければ大引け
  損切り なし／−7%
  参考: 同じデータで今の売りルール（GU+3%）も出す
コスト 往復0.10%＋不利1tick×2。期間 選定（8/25〜10/22）・確認（10/23〜11/20）。
実行: cd kabuai && python bb_gd_mirror_1123.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import SPLIT, is500_of, jq_daily, jq_fins, yf_5m
from bb_touch_reversal_1123 import P, SIG, hm, run_exit, st
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def trades(code, s, cand, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    full = float(pd.Series(s.index.normalize()).value_counts().median())
    o, h, l, c, v = (s[k].to_numpy(float) for k in ("o", "h", "l", "c", "v"))
    ser = pd.Series(c); is5 = is500_of(c)
    ma = ser.rolling(P).mean().to_numpy(); sd = ser.rolling(P).std(ddof=0).to_numpy()
    up, lo = ma + SIG * sd, ma - SIG * sd
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st_, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum()); ds = dd.strftime("%Y-%m-%d")
            gu = o[st_] / pc - 1
            if (100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and (code, ds) in cand and (code, ds) not in earn:
                for side, ok in ((1, gu <= -0.03), (-1, gu >= 0.03)):
                    if not ok:
                        continue
                    for win, (wlo, whi) in (("前場", (hm(10, 0), hm(11, 25))), ("全", (hm(10, 0), hm(14, 25)))):
                        if side > 0:
                            e = next((k for k in range(st_, en - 1) if wlo <= tmin[k] <= whi and np.isfinite(lo[k])
                                      and c[k] <= lo[k] * 0.995 and c[k] <= o[st_] * 0.98), None)
                        else:
                            e = next((k for k in range(st_, en - 1) if wlo <= tmin[k] <= whi and np.isfinite(up[k])
                                      and c[k] >= up[k] * 1.005 and c[k] >= o[st_] * 1.02), None)
                        if e is None:
                            continue
                        for ex in ("ma", "both"):
                            for sl in (None, 0.07):
                                r = run_exit(side, e + 1, en, o, h, l, c, ma, ex, sl, is5)
                                out.append({"rule": "GD−3%の買い（鏡像）" if side > 0 else "GU+3%の売り（今のルール）",
                                            "win": win, "exit": ex, "sl": sl or 0, "date": ds, "ret": r, "fwd": ds >= SPLIT})
        prev = (st_, en)
    return out


def main() -> None:
    token = _jquants_key()
    dl = _recent_trading_days(62)
    D = jq_daily(token, dl).sort_values(["code", "date"])
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1); D["gu"] = D.AdjO / D.pc - 1
    C = D[(D.pc >= 100) & (D.pc <= 100000) & (D.pva >= 1e8) & (D.gu.abs() >= 0.025)]
    cand = set(zip(C.code, C.date.astype(str).str[:10]))
    earn = jq_fins(token, dl)
    dd = sorted(set(D.date.astype(str).str[:10])); nxt = {dd[i]: dd[i + 1] for i in range(len(dd) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(set(C.code))
    T = pd.DataFrame([x for code, s in M.items() for x in trades(code, s, cand, earn)])
    EXN = {"both": "中心線 or 3%利確", "ma": "中心線のみ"}
    for rule, g in T.groupby("rule", sort=False):
        print(f"\n################ {rule} ################")
        for win in ("前場", "全"):
            for ex in ("both", "ma"):
                for sl in (0, 0.07):
                    d = g[(g.win == win) & (g.exit == ex) & (g.sl == sl)]
                    print(f"  [{win:<2}] {EXN[ex]:<10} 損切り{'なし' if not sl else '7%':<3}  "
                          f"選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
    print("\n読み方: 判断は「確認」（10/23〜）。買いは在庫・売り禁の心配がない。")


if __name__ == "__main__":
    main()

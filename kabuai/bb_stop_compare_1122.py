# -*- coding: utf-8 -*-
"""bb_stop_compare_1122.py — BB売り（今のルール）に損切りを入れると成績はどう変わるか（2026-11-22）。

本人「損切り設定教えて」。ルールは bb_sell_live.py と同じ:
  GU+3%・前日代金1億+・決算除外・BB40/2σ・終値≥上バンド×1.005・終値≥寄値×1.02・合図の次足始値で売り・
  中心線(40本MA)回帰で次足始値買戻し／大引け。往復0.10%＋不利1tick×2。
損切り: なし／売値+2%／+3%／+5%／+7%（逆指値の買戻し）。売った足以降の5分足の高値が届いたら損切り値で約定、
        足の始値で既に越えていたら始値で約定（窓で飛び越え）。同じ足で中心線回帰の確定と重なっても高値が先とみなす（保守側）。
期間: 選定（8/25〜10/22）・確認（10/23〜11/20）、全時間帯と前場（合図 10:00〜11:25）。
実行: cd kabuai && python bb_stop_compare_1122.py
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

P = 40
STOPS = (None, 0.02, 0.03, 0.05, 0.07)


def trades(code, s, cand, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    full = float(pd.Series(s.index.normalize()).value_counts().median())
    o, h, c, v = (s[k].to_numpy(float) for k in ("o", "h", "c", "v"))
    ser = pd.Series(c); is5 = is500_of(c)
    ma = ser.rolling(P).mean().to_numpy(); up = ma + SIG * ser.rolling(P).std(ddof=0).to_numpy()
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum()); ds = dd.strftime("%Y-%m-%d")
            if (100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and o[st] / pc - 1 >= 0.03 \
                    and (code, ds) in cand and (code, ds) not in earn:
                e = next((k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI and np.isfinite(up[k])
                          and c[k] >= up[k] * (1 + DEV) and c[k] >= o[st] * 1.02), None)
                if e is not None:
                    ep_raw = o[e + 1]; ep = ep_raw - tick_of(ep_raw, is5)
                    # 中心線回帰の決済足（次足始値）
                    x_idx, x_px = en - 1, c[en - 1]
                    for k in range(e + 1, en):
                        if c[k] <= ma[k]:
                            x_idx, x_px = (k + 1, o[k + 1]) if k + 1 < en else (en - 1, c[en - 1]); break
                    mae = (h[e + 1:x_idx + 1].max() / ep_raw - 1) * 100 if x_idx >= e + 1 else 0.0
                    for sl in STOPS:
                        px = x_px; hit = False
                        if sl is not None:
                            lvl = ep_raw * (1 + sl)
                            for k in range(e + 1, x_idx + 1 if x_idx < en else en):
                                if k > e + 1 and o[k] >= lvl:
                                    px, hit = o[k], True; break
                                if h[k] >= lvl:
                                    px, hit = lvl, True; break
                        r = -(px + tick_of(px, is5) - ep) / ep * 100 - COST_RT
                        out.append({"sl": "なし" if sl is None else f"+{sl*100:.0f}%", "date": ds, "ret": r, "hit": hit,
                                    "fwd": ds >= SPLIT, "am": tmin[e] <= 11 * 60 + 25, "mae": mae})
        prev = (st, en)
    return out


def st(r: pd.Series) -> str:
    if len(r) == 0:
        return "n=  0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return (f"n={len(r):>3} 勝率{(r>0).mean()*100:>3.0f}% PF{(gp/gl if gl else np.inf):>5.2f} 1件{r.mean():>+5.2f}% "
            f"最大負け{r.min():>+6.2f}%")


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
    for scope, m in (("全時間帯（合図 10:00〜14:25）", np.ones(len(T), bool)), ("前場だけ（合図 10:00〜11:25）", T.am.to_numpy())):
        print(f"\n################ {scope} ################")
        for sl in ("なし", "+2%", "+3%", "+5%", "+7%"):
            d = T[m & (T.sl == sl)]
            print(f"  損切り{sl:<4}  選定: {st(d[~d.fwd].ret)} 損切り発動{d[~d.fwd].hit.mean()*100:>3.0f}%"
                  f"  ／ 確認: {st(d[d.fwd].ret)} 損切り発動{d[d.fwd].hit.mean()*100:>3.0f}%")
        b = T[m & (T.sl == "なし")]
        if len(b):
            q = np.percentile(b.mae, [50, 75, 90, 95])
            print(f"  参考: 売った後に逆行した最大幅（買戻しまでの高値÷売値−1）中央値{q[0]:.1f}% / 75%点{q[1]:.1f}% / 90%点{q[2]:.1f}% / 95%点{q[3]:.1f}%")
    print("\n読み方: 損切りを入れて PF と1件平均が「確認」でもあまり落ちず、最大負けが小さくなる幅が使える設定。")


if __name__ == "__main__":
    main()

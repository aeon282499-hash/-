# -*- coding: utf-8 -*-
"""bb_repeat_1123.py — BB売りを「同じ株で何回も回す」と儲かるか（2026-11-23）。

本人「デイトレみたいにガチャしたい・5分足で」→「同じ株を何回も回す」。
今のルール（bb_sell_live.py / bb_paper_daily.py と同じ）で、中心線で買い戻した後に同じ株がまた合図を出したら、もう一度売る。
  GU+3%・前日代金1億+・決算除外・5分足 BB40/2.07σ・終値≥上バンド×1.005・終値≥寄値×1.02・合図の次足始値で売り・
  終値≤中心線→次足始値で買戻し／戻らなければ大引け（大引けで終わったらその日は終わり）。往復0.10%＋不利1tick×2。
  2回目以降の合図＝買い戻した足（その始値で買った足）以降に確定した足で、もう一度①②を満たしたもの。
並べるもの: 合図の時間（前場10:00〜11:25／1日10:00〜14:25）× 保険（なし／+7%逆指値・逆指値の後も再エントリー可）
  → 1回目／2回目／3回目以降 の成績を別々に・1銘柄1日の合計も。
期間: 選定（〜10/22）・確認（10/23〜）。⚠ 2回目以降はどちらの期間でも未測定＝両方とも新しい情報。
実行: cd kabuai && python bb_repeat_1123.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, SPLIT, is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

P, SIG, DEV, OPEN_UP = 40, 2.07, 0.005, 0.02
WINDOWS = {"前場（合図10:00〜11:25）": (600, 685), "1日（合図10:00〜14:25）": (600, 865)}
STOPS = (None, 0.07)


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
                for wname, (lo, hi) in WINDOWS.items():
                    for sl in STOPS:
                        pos, n = st, 0
                        while True:
                            e = next((k for k in range(pos, en - 1) if lo <= tmin[k] <= hi and np.isfinite(up[k])
                                      and c[k] >= up[k] * (1 + DEV) and c[k] >= o[st] * (1 + OPEN_UP)), None)
                            if e is None:
                                break
                            n += 1
                            ep_raw = o[e + 1]; ep = ep_raw - tick_of(ep_raw, is5)
                            x_idx, x_px, eod = en - 1, c[en - 1], True
                            for k in range(e + 1, en):
                                if c[k] <= ma[k]:
                                    if k + 1 < en:
                                        x_idx, x_px, eod = k + 1, o[k + 1], False
                                    break
                            hit = False
                            if sl is not None:
                                lvl = ep_raw * (1 + sl)
                                for k in range(e + 1, x_idx + 1 if not eod else en):
                                    if k > e + 1 and o[k] >= lvl:
                                        x_idx, x_px, eod, hit = k, o[k], False, True; break
                                    if h[k] >= lvl:
                                        x_idx, x_px, eod, hit = k, lvl, False, True; break
                            r = -(x_px + tick_of(x_px, is5) - ep) / ep * 100 - COST_RT
                            out.append({"w": wname, "sl": "なし" if sl is None else f"+{sl*100:.0f}%", "code": code, "date": ds,
                                        "n": n, "ret": r, "yen": r / 100 * ep * 100, "hit": hit, "fwd": ds >= SPLIT,
                                        "hold": (x_idx - e - 1) * 5})
                            if eod:
                                break
                            pos = x_idx + (1 if hit else 0)
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
    T["k"] = np.where(T.n >= 3, "3回目以降", T.n.astype(str) + "回目")
    for wname in WINDOWS:
        for sl in ("なし", "+7%"):
            d0 = T[(T.w == wname) & (T.sl == sl)]
            print(f"\n################ {wname} ・保険{sl} ################")
            for k in ("1回目", "2回目", "3回目以降"):
                d = d0[d0.k == k]
                print(f"  {k:<6} 選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}  保有中央{d.hold.median() if len(d) else 0:.0f}分")
            g = d0.groupby(["code", "date", "fwd"]).ret.sum().reset_index()
            print(f"  1銘柄1日の合計（回すと）  選定: {st(g[~g.fwd].ret)}  ／ 確認: {st(g[g.fwd].ret)}")
            one = d0[d0.n == 1]
            print(f"  1銘柄1日の合計（1回だけ）選定: {st(one[~one.fwd].ret)}  ／ 確認: {st(one[one.fwd].ret)}")
            print(f"  回った回数の分布: {d0.groupby(['code','date']).n.max().value_counts().sort_index().to_dict()}")
            for lab, m in (("選定", ~d0.fwd), ("確認", d0.fwd)):
                dy = d0[m].groupby("date").yen.sum()
                print(f"  100株の円 {lab}: 回す 合計{d0[m].yen.sum():+,.0f}円 ／ 1回だけ {d0[m & (d0.n == 1)].yen.sum():+,.0f}円 ／ 最悪の日(回す) {dy.min() if len(dy) else 0:+,.0f}円")
    print("\n読み方: 2回目・3回目が「選定」「確認」の両方でプラス（PF1.2以上）なら、回す価値がある。"
          "\n        片方だけ・件数30未満なら、1回目だけの今のルールのまま。空売りの在庫・売り禁は見ていない（楽観側）。")


if __name__ == "__main__":
    main()

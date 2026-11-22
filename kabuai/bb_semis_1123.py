# -*- coding: utf-8 -*-
"""bb_semis_1123.py — 今のBB売りのルール（BB40/2.07σ）を半導体の大型8銘柄にそのまま当てる（2026-11-23）。

本人「じゃあ逆に半導体銘柄・デイトレに向いてる イビデン キオクシア 東エレ アドバンテスト ソフトバンク 太陽誘電 村田 レーザーテック」。
ルールは動かさない（ここで合わせ込まない）。5分足（9:00足は捨てる）・BB40/2.07σ（終値・SMA・母標準偏差）・決算(当日と翌営業日)除外・
往復0.10%＋不利1tick×2・1銘柄1日1回・合図の次足始値で建て・中心線回帰で次足始値決済／戻らねば大引け・保険+7%逆指値。
  売り: 終値 ≥ 上バンド×1.005 かつ ≥ 寄値×1.02（＝今のBB売りから GU+3% の条件だけ外した形）
  買い: 終値 ≤ 下バンド×0.995 かつ ≤ 寄値×0.98（売りの鏡写し）
  買い(下に触れただけ): 足の安値 ≤ 下バンド → 次足始値で買い（本人「40BBで下に触れたら入ればいい？RSIじゃなくて」）
  時間: 前場（合図10:00〜11:25）／1日（10:00〜14:25）
  参考: 今のBB売りそのまま（GU+3%の日だけ）・同じ銘柄日の 10:00 始値→大引け の売り/買い（地合いの物差し）
期間: 選定（〜10/22）・確認（10/23〜）。
実行: cd kabuai && python bb_semis_1123.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, SPLIT, is500_of, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

NAMES = {"4062": "イビデン", "285A": "キオクシア", "8035": "東エレ", "6857": "アドバンテスト", "9984": "ソフトバンクG",
         "6976": "太陽誘電", "6981": "村田", "6920": "レーザーテック"}
P, SIG, DEV, OPEN_MV, STOP = 40, 2.07, 0.005, 0.02, 0.07
WINDOWS = {"前場": (600, 685), "1日": (600, 865)}


def trades(code, s, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    o, h, l, c = (s[k].to_numpy(float) for k in ("o", "h", "l", "c"))
    ser = pd.Series(c); is5 = is500_of(c)
    ma = ser.rolling(P).mean().to_numpy(); sd = ser.rolling(P).std(ddof=0).to_numpy()
    up, lo_b = ma + SIG * sd, ma - SIG * sd
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        ds = dd.strftime("%Y-%m-%d")
        if prev is not None and (code, ds) not in earn:
            gu = o[st] / c[prev[1] - 1] - 1
            base = {"code": code, "date": ds, "fwd": ds >= SPLIT, "gu": gu}
            k10 = next((k for k in range(st, en) if tmin[k] >= 600), None)
            if k10 is not None:
                for side, sgn in (("売り", -1), ("買い", 1)):
                    ep = o[k10] + sgn * tick_of(o[k10], is5); x = c[en - 1] - sgn * tick_of(c[en - 1], is5)
                    out.append({**base, "kind": f"物差し 10:00→引け {side}", "w": "1日", "ret": sgn * (x - ep) / ep * 100 - COST_RT})
            for wname, (a, b) in WINDOWS.items():
                for side, sgn in (("売り", -1), ("買い", 1), ("買い(下に触れただけ)", 1)):
                    if side == "買い(下に触れただけ)":
                        cond = lambda k: l[k] <= lo_b[k]
                    elif side == "売り":
                        cond = lambda k: c[k] >= up[k] * (1 + DEV) and c[k] >= o[st] * (1 + OPEN_MV)
                    else:
                        cond = lambda k: c[k] <= lo_b[k] * (1 - DEV) and c[k] <= o[st] * (1 - OPEN_MV)
                    e = next((k for k in range(st, en - 1) if a <= tmin[k] <= b and np.isfinite(up[k]) and cond(k)), None)
                    if e is None:
                        continue
                    ep_raw = o[e + 1]; ep = ep_raw + sgn * tick_of(ep_raw, is5)
                    x_idx, x_px, eod = en - 1, c[en - 1], True
                    for k in range(e + 1, en):
                        if (c[k] - ma[k]) * sgn >= 0:
                            if k + 1 < en:
                                x_idx, x_px, eod = k + 1, o[k + 1], False
                            break
                    lvl = ep_raw * (1 - sgn * STOP)
                    for k in range(e + 1, x_idx + 1 if not eod else en):
                        gap = (o[k] >= lvl) if sgn < 0 else (o[k] <= lvl)
                        touch = (h[k] >= lvl) if sgn < 0 else (l[k] <= lvl)
                        if k > e + 1 and gap:
                            x_px = o[k]; break
                        if touch:
                            x_px = lvl; break
                    x = x_px - sgn * tick_of(x_px, is5)
                    r = sgn * (x - ep) / ep * 100 - COST_RT
                    out.append({**base, "kind": f"BB{side}", "w": wname, "ret": r, "yen": r / 100 * ep * 100})
                    if side == "売り" and gu >= 0.03:
                        out.append({**base, "kind": "今のBB売り(GU+3%の日だけ)", "w": wname, "ret": r, "yen": r / 100 * ep * 100})
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
    earn = jq_fins(token, dl)
    nxt = {dl[i]: dl[i + 1] for i in range(len(dl) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(set(NAMES))
    print(f"取れた銘柄: {[NAMES[c] for c in M]}")
    T = pd.DataFrame([x for code, s in M.items() for x in trades(code, s, earn)])
    for wname in ("前場", "1日"):
        print(f"\n################ 合図の時間: {wname} ################")
        for kind in ("BB売り", "BB買い", "BB買い(下に触れただけ)", "今のBB売り(GU+3%の日だけ)"):
            d = T[(T.kind == kind) & (T.w == wname)]
            print(f"  {kind:<16} 選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
            if kind != "今のBB売り(GU+3%の日だけ)" and len(d):
                per = d.groupby("code").ret.agg(["count", "mean"])
                print("     銘柄別(全期間): " + " ".join(f"{NAMES[c]} {int(r['count'])}回{r['mean']:+.2f}%" for c, r in per.iterrows()))
    print("\n################ 物差し（同じ銘柄日を10:00始値で建てて大引け） ################")
    for side in ("売り", "買い"):
        d = T[T.kind == f"物差し 10:00→引け {side}"]
        print(f"  {side}  選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
    print("\n読み方: BB売り/BB買いが「選定」「確認」の両方で PF1.2 以上・件数30前後あり、物差しより上なら候補。"
          "\n        ルールはGU株で決めたものをそのまま当てている（ここで数字を動かさない）。")


if __name__ == "__main__":
    main()

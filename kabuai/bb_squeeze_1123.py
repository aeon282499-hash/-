# -*- coding: utf-8 -*-
"""bb_squeeze_1123.py — ボリンジャー本人の「スクイーズ」（バンド幅が一番狭い→放れ）を5分足で測る（2026-11-23）。

本人「スクイーズもはかって」。形は測る前に決めた（ここで数字を動かさない）:
  バンド幅 BW = (上バンド−下バンド) ÷ 中心線
  スクイーズ = BW がその足までの直近125本で一番小さい（本人の「半年で一番狭い」を125本に・TradingView の公開版の既定値と同じ）
  放れ = スクイーズの足から6本（30分）以内に、終値が上バンドの外 → 買い ／ 下バンドの外 → 売り（次足始値）
  出口 = 終値が中心線の反対側に戻ったら次足始値（本人の「バンドウォークが終わったら降りる」）／ 残りは大引け
  時間: 合図 9:30〜14:00・1銘柄1日1回（最初の放れ）・決算(当日と翌営業日)除外
  BB: 20本/2.0σ（本人の標準）と 40本/2.07σ
  参考: 本人が注意する「ヘッドフェイク（だまし）」を逆に取る＝同じ放れに逆向きで入る（出口は同じ考え方の鏡写し）
母集団: 半導体8銘柄 ／ 流動株（60日の売買代金の中央値 50億円以上）
物差し: 合図が出た銘柄日に 10:00始値で同じ向きに建てて大引け
往復0.10%＋不利1tick×2。期間: 選定（〜10/22）・確認（10/23〜）＝初めて測るので両方とも新しい情報。
実行: cd kabuai && python bb_squeeze_1123.py
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

SEMIS = {"4062": "イビデン", "285A": "キオクシア", "8035": "東エレ", "6857": "アドバンテスト", "9984": "ソフトバンクG",
         "6976": "太陽誘電", "6981": "村田", "6920": "レーザーテック"}
BBS = {"BB20/2.0σ": (20, 2.0), "BB40/2.07σ": (40, 2.07)}
LOOK, WITHIN = 125, 6
T_LO, T_HI = 9 * 60 + 30, 14 * 60


def trades(code, s, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    o, c = s.o.to_numpy(float), s.c.to_numpy(float)
    ser = pd.Series(c); is5 = is500_of(c)
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out = []
    for bname, (p, sg) in BBS.items():
        ma = ser.rolling(p).mean().to_numpy(); sd = ser.rolling(p).std(ddof=0).to_numpy()
        up, lo = ma + sg * sd, ma - sg * sd
        bw = pd.Series((up - lo) / ma)
        sq = (bw <= bw.rolling(LOOK).min() + 1e-12).to_numpy()
        last_sq = pd.Series(np.where(sq, np.arange(len(c)), np.nan)).ffill().to_numpy()
        for dd in day.unique():
            ds = dd.strftime("%Y-%m-%d")
            if (code, ds) in earn:
                continue
            ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
            e, d = None, 0
            for k in range(st, en - 1):
                if not (T_LO <= tmin[k] <= T_HI) or not np.isfinite(last_sq[k]) or k - last_sq[k] > WITHIN:
                    continue
                if c[k] > up[k]:
                    e, d = k, 1; break
                if c[k] < lo[k]:
                    e, d = k, -1; break
            if e is None:
                continue
            k10 = next((k for k in range(st, en) if tmin[k] >= 600), None)
            for mode, sgn in (("放れに乗る", d), ("だましを取る(逆)", -d)):
                ep_raw = o[e + 1]; ep = ep_raw + sgn * tick_of(ep_raw, is5)
                x_px = c[en - 1]
                for k in range(e + 1, en):
                    if (c[k] - ma[k]) * sgn < 0 if mode == "放れに乗る" else (c[k] - ma[k]) * d < 0:
                        x_px = o[k + 1] if k + 1 < en else c[en - 1]; break
                x = x_px - sgn * tick_of(x_px, is5)
                r = sgn * (x - ep) / ep * 100 - COST_RT
                base = None
                if k10 is not None:
                    b_ep = o[k10] + sgn * tick_of(o[k10], is5); b_x = c[en - 1] - sgn * tick_of(c[en - 1], is5)
                    base = sgn * (b_x - b_ep) / b_ep * 100 - COST_RT
                out.append({"code": code, "date": ds, "fwd": ds >= SPLIT, "bb": bname, "mode": mode,
                            "dir": "上放れ" if d > 0 else "下放れ", "ret": r, "base": base, "time": s.index[e].strftime("%H:%M")})
    return out


def st(r: pd.Series) -> str:
    r = r.dropna()
    if len(r) == 0:
        return "n=   0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return f"n={len(r):>4} 勝率{(r>0).mean()*100:>3.0f}% PF{(gp/gl if gl else np.inf):>5.2f} 1件{r.mean():>+5.2f}%"


def main() -> None:
    token = _jquants_key()
    dl = _recent_trading_days(62)
    D = jq_daily(token, dl)
    va = D.groupby("code").Va.median()
    liquid = set(va[va >= 5e9].index)
    print(f"流動株（売買代金の中央値50億↑）{len(liquid)}銘柄", flush=True)
    earn = jq_fins(token, dl)
    nxt = {dl[i]: dl[i + 1] for i in range(len(dl) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(liquid | set(SEMIS))
    print(f"半導体で取れた銘柄: {[SEMIS[c] for c in SEMIS if c in M]}")
    T = pd.DataFrame([x for code, s in M.items() for x in trades(code, s, earn)])
    for uname, codes in (("半導体8銘柄", set(SEMIS)), ("流動株（50億↑）", liquid)):
        print(f"\n################ {uname} ################")
        for bname in BBS:
            for mode in ("放れに乗る", "だましを取る(逆)"):
                for dr in ("上放れ", "下放れ"):
                    d = T[T.code.isin(codes) & (T.bb == bname) & (T["mode"] == mode) & (T.dir == dr)]
                    print(f"  {bname} {mode:<9} {dr}  選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
                    print(f"       （物差し: 同じ日に10:00で同じ向き→引け）選定: {st(d[~d.fwd].base)}  ／ 確認: {st(d[d.fwd].base)}")
    print("\n読み方: 「選定」「確認」の両方で PF1.2 以上・件数30以上で、物差しより上なら候補。物差しと同じなら地合いをなぞっただけ。")


if __name__ == "__main__":
    main()

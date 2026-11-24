# -*- coding: utf-8 -*-
"""bb_wbottom_1123.py — ボリンジャー本人の「W底」（%b を使う買い）を5分足で測る（2026-11-23）。

本人「W底もはかって」。形は測る前に決めた（ここで数字を動かさない）:
  ① 1回目の安値: 足の終値が下バンドの外（%b < 0）。そこからの下げの最安値を L1 とする
  ② 戻り: その後、終値が中心線以上（%b ≥ 0.5）まで戻る。戻りの最高値を H とする
  ③ 2回目の安値: その後、足の安値が L1×1.005 以下まで下げる（同じか下）のに、その足の終値の %b > 0（バンドの内側に残る）
  ④ 入り（2通り）: a=2回目の安値の後に陽線（終値>始値）で確定した最初の足 ／ b=終値が H を超えた最初の足 → 次の足の始値で買い
  出口: 終値が上バンド以上 → 次足始値（本人の目標）／ 損切り=2回目の安値 −1tick を安値が割ったら（窓は始値）／ 残りは大引け
  時間: 入りの合図 9:30〜14:00・形はその日の中だけ（前日にまたがらない）・1銘柄1日1回・決算(当日と翌営業日)除外
  BB: 20本/2.0σ（本人の標準）と 40本/2.07σ（今のBB売りと同じ）
母集団: 半導体8銘柄 ／ 流動株（60日の売買代金の中央値 50億円以上）
物差し: 同じ銘柄日の 合図が出た日に「10:00始値で買い→大引け」
往復0.10%＋不利1tick×2。期間: 選定（〜10/22）・確認（10/23〜）＝W底は初めて測るので両方とも新しい情報。
実行: cd kabuai && python bb_wbottom_1123.py
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
T_LO, T_HI = 9 * 60 + 30, 14 * 60


def trades(code, s, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    o, h, l, c = (s[k].to_numpy(float) for k in ("o", "h", "l", "c"))
    ser = pd.Series(c); is5 = is500_of(c)
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out = []
    for bname, (p, sg) in BBS.items():
        ma = ser.rolling(p).mean().to_numpy(); sd = ser.rolling(p).std(ddof=0).to_numpy()
        up, lo = ma + sg * sd, ma - sg * sd
        pb = (c - lo) / np.where(up - lo > 0, up - lo, np.nan)
        for dd in day.unique():
            ds = dd.strftime("%Y-%m-%d")
            if (code, ds) in earn:
                continue
            ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
            if not np.isfinite(pb[st]):
                continue
            k10 = next((k for k in range(st, en) if tmin[k] >= 600), None)
            base = None
            if k10 is not None:
                ep = o[k10] + tick_of(o[k10], is5); x = c[en - 1] - tick_of(c[en - 1], is5)
                base = (x - ep) / ep * 100 - COST_RT
            for trig in ("a陽線", "b山越え"):
                stage, L1, H, L2, e = 0, None, None, None, None
                for k in range(st, en - 1):
                    if stage == 0:
                        if pb[k] < 0:
                            stage, L1 = 1, l[k]
                    elif stage == 1:
                        L1 = min(L1, l[k])
                        if pb[k] >= 0.5:
                            stage, H = 2, h[k]
                    elif stage == 2:
                        if l[k] <= L1 * 1.005:
                            if pb[k] > 0:
                                stage, L2 = 3, l[k]
                                if trig == "a陽線" and c[k] > o[k] and T_LO <= tmin[k] <= T_HI:
                                    e = k; break
                            else:                      # 2回目もバンドの外＝W にならない→ここを新しい1回目にする
                                stage, L1, H = 1, l[k], None
                        else:
                            H = max(H, h[k])
                    elif stage == 3:
                        if l[k] < L2:                  # 2回目の安値を割った＝形が崩れた
                            if pb[k] < 0:
                                stage, L1, H, L2 = 1, l[k], None, None
                            else:
                                L2 = l[k]
                            continue
                        ok = (c[k] > o[k]) if trig == "a陽線" else (c[k] > H)
                        if ok and T_LO <= tmin[k] <= T_HI:
                            e = k; break
                if e is None:
                    continue
                ep_raw = o[e + 1]; ep = ep_raw + tick_of(ep_raw, is5)
                stop = L2 - tick_of(L2, is5)
                x_px, why = c[en - 1], "大引け"
                for k in range(e + 1, en):
                    if o[k] <= stop and k > e + 1:
                        x_px, why = o[k], "損切り"; break
                    if l[k] <= stop:
                        x_px, why = min(stop, o[k]), "損切り"; break
                    if c[k] >= up[k]:
                        x_px, why = (o[k + 1], "上バンド") if k + 1 < en else (c[en - 1], "大引け"); break
                x = x_px - tick_of(x_px, is5)
                r = (x - ep) / ep * 100 - COST_RT
                eod = (c[en - 1] - tick_of(c[en - 1], is5) - ep) / ep * 100 - COST_RT
                out.append({"code": code, "date": ds, "fwd": ds >= SPLIT, "bb": bname, "trig": trig, "ret": r, "ret_eod": eod,
                            "why": why, "base": base, "time": s.index[e].strftime("%H:%M")})
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
            for trig in ("a陽線", "b山越え"):
                d = T[T.code.isin(codes) & (T.bb == bname) & (T.trig == trig)]
                print(f"  {bname} 入り{trig}  選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
                print(f"       （同じ玉を大引けまで）  選定: {st(d[~d.fwd].ret_eod)}  ／ 確認: {st(d[d.fwd].ret_eod)}")
                print(f"       （物差し: その日10:00買い→引け）選定: {st(d[~d.fwd].base)}  ／ 確認: {st(d[d.fwd].base)}")
                if len(d):
                    print(f"       出口の内訳: {d.why.value_counts().to_dict()}・合図の時刻中央 {sorted(d.time)[len(d)//2]}")
    print("\n読み方: W底が「選定」「確認」の両方で PF1.2 以上・件数30以上で、物差し（同じ日に10:00で買うだけ）より上なら候補。"
          "\n        物差しと同じくらいなら、W底は地合いをなぞっているだけ。")


if __name__ == "__main__":
    main()

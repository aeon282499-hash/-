# -*- coding: utf-8 -*-
"""bb_sigma207_check_1123.py — BB売りの幅を 2σ → 2.07σ に変えても成績が崩れないかの確認（2026-11-23）。

本人「ジョン・ボリンジャー氏と同じインジケーターにしたい」→「B（40本のまま幅を本人のルールに合わせる）」。
  ボリンジャー本人の目安: 10本=1.9σ・20本=2.0σ・50本=2.1σ → 40本は 2.0+(40-20)/(50-20)*0.1 ≒ 2.07σ
  標準偏差は母集団（ddof=0）・終値・単純移動平均＝本人の定義どおり（ここは元から同じ）。
ルールは bb_sell_live.py と同じ（幅だけ 2.0 / 2.07 を並べる）:
  GU+3%・前日代金1億+・決算除外・BB40・終値≥上バンド×1.005・終値≥寄値×1.02・合図の次足始値で売り・
  中心線(40本MA)回帰で次足始値買戻し／大引け。保険の逆指値 なし／+7%。往復0.10%＋不利1tick×2。
期間: 選定（〜10/22）・確認（10/23〜）。⚠ 確認期間はすでに期間20/40・前場・寄値+2%を選ぶのに使ったので、
      ここでは「幅を変えても崩れないか」だけを見る（良くなったかどうかで決めない）。
実行: cd kabuai && python bb_sigma207_check_1123.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, DEV, SPLIT, W_HI, W_LO, is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

P = 40
SIGMAS = (2.0, 2.07)
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
    ma = ser.rolling(P).mean().to_numpy(); sd = ser.rolling(P).std(ddof=0).to_numpy()
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum()); ds = dd.strftime("%Y-%m-%d")
            if (100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and o[st] / pc - 1 >= 0.03 \
                    and (code, ds) in cand and (code, ds) not in earn:
                for sig in SIGMAS:
                    up = ma + sig * sd
                    e = next((k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI and np.isfinite(up[k])
                              and c[k] >= up[k] * (1 + DEV) and c[k] >= o[st] * 1.02), None)
                    if e is None:
                        continue
                    ep_raw = o[e + 1]; ep = ep_raw - tick_of(ep_raw, is5)
                    x_idx, x_px = en - 1, c[en - 1]
                    for k in range(e + 1, en):
                        if c[k] <= ma[k]:
                            x_idx, x_px = (k + 1, o[k + 1]) if k + 1 < en else (en - 1, c[en - 1]); break
                    for sl in STOPS:
                        px = x_px
                        if sl is not None:
                            lvl = ep_raw * (1 + sl)
                            for k in range(e + 1, x_idx + 1 if x_idx < en else en):
                                if k > e + 1 and o[k] >= lvl:
                                    px = o[k]; break
                                if h[k] >= lvl:
                                    px = lvl; break
                        r = -(px + tick_of(px, is5) - ep) / ep * 100 - COST_RT
                        out.append({"sig": sig, "sl": "なし" if sl is None else f"+{sl*100:.0f}%", "code": code, "date": ds,
                                    "ret": r, "fwd": ds >= SPLIT, "am": tmin[e] <= 11 * 60 + 25})
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
    for scope, m in (("前場だけ（合図 10:00〜11:25）＝今のルール", T.am.to_numpy()), ("参考: 全時間帯（10:00〜14:25）", np.ones(len(T), bool))):
        print(f"\n################ {scope} ################")
        for sl in ("なし", "+7%"):
            for sig in SIGMAS:
                d = T[m & (T.sig == sig) & (T.sl == sl)]
                print(f"  {sig:.2f}σ 保険{sl:<3}  選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
        a = T[m & (T.sig == 2.0) & (T.sl == "なし")]; b = T[m & (T.sig == 2.07) & (T.sl == "なし")]
        ka, kb = set(zip(a.code, a.date)), set(zip(b.code, b.date))
        print(f"  合図の重なり: 2.0σだけ {len(ka-kb)}件 ／ 両方 {len(ka&kb)}件 ／ 2.07σだけ {len(kb-ka)}件")
    print("\n読み方: 2.07σ は合図が少し減り、少し高い所で売る。成績が大きく崩れなければ本人の定義どおり 2.07σ で紙に進む。")


if __name__ == "__main__":
    main()

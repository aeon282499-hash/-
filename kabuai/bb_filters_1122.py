# -*- coding: utf-8 -*-
"""bb_filters_1122.py — GU株のBB上抜け売りに「もう1つ条件を足す」と良くなるかのバックテスト（2026-11-22）。

本人「他のインジケータつかう？」「バックテストするとどうなん？」。
土台は bb_forward_1122.py と同じ規約（BB40/2σ・上バンドの0.5%以上外・10:00〜14:25の最初の点灯・次足始値で売り・
40本MA回帰で買い戻し・GU+3%・決算除外・往復0.10%＋不利1tick×2）。合図の足で次の3つを記録して、条件を足した場合を比べる:
  VWAP乖離   = 合図の足の終値 ÷ その日のVWAP − 1
  出来高倍率 = 合図の足の出来高 ÷ その日の合図までの足の平均出来高
  寄値乖離   = 合図の足の終値 ÷ 寄値 − 1
判定の手順（後付けを避ける）:
  ① 選定期間（8/25〜10/22）で、各条件の「しきい値ごとの成績」を見る
  ② 選定期間で一番良かったしきい値を、確認期間（10/23〜11/20）に当てる ← ここが答え
  ③ 前場（10:00〜11:25の点灯）だけの成績も同じ手順で出す
実行: cd kabuai && python bb_filters_1122.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, DEV, P, SIG, SPLIT, W_HI, W_LO, is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

FILTERS = {
    "VWAP乖離%": (2, 3, 4, 5, 6),
    "出来高倍率": (1.0, 1.5, 2.0, 3.0),
    "寄値乖離%": (2, 3, 4, 5),
}


def signals_for(code, s, cand, earn):
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 300:
        return []
    full = float(pd.Series(s.index.normalize()).value_counts().median())
    c, o, v = s.c.to_numpy(float), s.o.to_numpy(float), s.v.to_numpy(float)
    ser = pd.Series(c)
    ma = ser.rolling(P).mean().to_numpy(); up = ma + SIG * ser.rolling(P).std(ddof=0).to_numpy()
    is5 = is500_of(c)
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, prev = [], None
    for dd in day.unique():
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum())
            ds = dd.strftime("%Y-%m-%d"); gu = o[st] / pc - 1
            if (100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and gu >= 0.03 \
                    and (code, ds) in cand and (code, ds) not in earn:
                e = next((k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI and np.isfinite(up[k])
                          and c[k] > up[k] and (c[k] - up[k]) / c[k] >= DEV), None)
                if e is not None:
                    ep = o[e + 1] - tick_of(o[e + 1], is5)
                    state = np.sign(c[e] - ma[e]); px = c[en - 1]
                    for k in range(e + 1, en):
                        if (c[k] - ma[k]) * state <= 0:
                            px = o[k + 1] if k + 1 < en else c[en - 1]; break
                    r = -(px + tick_of(px, is5) - ep) / ep * 100 - COST_RT
                    vw = (c[st:e + 1] * v[st:e + 1]).sum() / max(v[st:e + 1].sum(), 1)
                    vavg = v[st:e].mean() if e > st else np.nan
                    out.append({"date": ds, "code": code, "t": tmin[e], "ret": r, "fwd": ds >= SPLIT,
                                "am": tmin[e] <= 11 * 60 + 25,
                                "VWAP乖離%": (c[e] / vw - 1) * 100, "出来高倍率": v[e] / vavg if vavg and vavg > 0 else np.nan,
                                "寄値乖離%": (c[e] / o[st] - 1) * 100})
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
    T = pd.DataFrame([x for code, s in M.items() for x in signals_for(code, s, cand, earn)])
    print(f"\n[filters] BB売りの合図 {len(T)}件（選定 {(~T.fwd).sum()} / 確認 {T.fwd.sum()}）")

    for scope, m0 in (("全時間帯（10:00〜14:25）", np.ones(len(T), bool)), ("前場だけ（10:00〜11:25）", T.am.to_numpy())):
        S = T[m0]
        print(f"\n################ {scope} ################")
        print(f"  条件なし（BBだけ）  選定: {st(S[~S.fwd].ret)}  ／ 確認: {st(S[S.fwd].ret)}")
        for f, ths in FILTERS.items():
            print(f"\n  ◆ {f} が しきい値以上のときだけ売る")
            best, best_v = None, -np.inf
            for th in ths:
                a = S[(S[f] >= th) & ~S.fwd].ret; b = S[(S[f] >= th) & S.fwd].ret
                print(f"    ≥{th:<4}  選定: {st(a)}  ／ 確認: {st(b)}")
                if len(a) >= 30 and a.mean() > best_v:
                    best, best_v = th, a.mean()
            if best is not None:
                b = S[(S[f] >= best) & S.fwd].ret; base = S[S.fwd].ret
                print(f"    → 選定期間で一番良かった ≥{best} を確認期間に当てると {st(b)}"
                      f"（条件なしの確認期間 {st(base)}）")
            else:
                print("    → 選定期間で30件以上残るしきい値なし")
        # 参考: 各指標の高低で半分に分けた成績（確認期間）
        print("\n  参考（確認期間）: 各指標の中央値より上/下で分けた成績")
        for f in FILTERS:
            q = S[f].median()
            print(f"    {f}（中央値 {q:.2f}）  上: {st(S[(S[f] > q) & S.fwd].ret)}  ／ 下: {st(S[(S[f] <= q) & S.fwd].ret)}")
    print("\n読み方: 「確認期間」で条件なしより良く、かつ件数が十分（30件前後以上）残るものだけが、足す価値のある条件。"
          "\n        選定期間だけ良いものは過去への合わせ込み。前場だけの確認期間は件数が少ないので参考程度。")


if __name__ == "__main__":
    main()

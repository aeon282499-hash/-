# -*- coding: utf-8 -*-
"""bb_touch_reversal_1123.py — 本人ルール「BB40・2σタッチ → 反対の足を確認 → エントリー・中心線 or 3%利確」の検証（2026-11-23）。

ルール（本人の言葉を機械化。解釈は下の通り）
  BB: 5分足・期間40・2σ（単純移動平均・母集団σ・前日から連続）
  タッチ: 売り＝高値 ≥ +2σ ／ 買い＝安値 ≤ −2σ（足 i）
  確認: 足 i〜i+3 のうち最初の「反対の色の足」（売り＝陰線 終値<始値・買い＝陽線）で、終値がバンドの内側（売り: 終値<+2σ）
  エントリー: 確認した足の次の足の始値（不利1tick）
  出口: 中心線(40本MA)回帰（終値で判定→次足始値）か 3%利確（指値・到達で約定）の早い方／どちらも無ければ大引け
  比較: 出口 = 中心線のみ／3%利確のみ（無ければ大引け） × 損切り なし／7%（逆指値・窓は始値）
  時間帯: 確認の足が 9:05〜14:25（全）と 10:00〜11:25（前場）
  対象: GU+3%（売り）・GD−3%（買い）・全銘柄（前日代金3億+）の売り/買い。決算の当日と翌営業日は除外
  参考: 今のルール（終値≥+2σ×1.005・寄値+2%以上・GU+3%・中心線回帰）も同じデータで出す
コスト: 往復0.10%＋入口/出口で不利1tick。期間: 選定（8/25〜10/22）・確認（10/23〜11/20）
実行: cd kabuai && python bb_touch_reversal_1123.py
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

P, SIG = 40, 2.0
TP = 0.03
STOPS = (None, 0.07)
CONFIRM_WITHIN = 3


def hm(h, m):
    return h * 60 + m


def run_exit(side, j, en, o, h, l, c, ma, exit_kind, stop, is5):
    """j=建てる足（始値で約定）。戻り: コスト込みリターン%。"""
    ep_raw = o[j]
    ep = ep_raw + side * tick_of(ep_raw, is5)
    tgt = ep_raw * (1 + side * TP)
    sl = ep_raw * (1 - side * stop) if stop else None
    px = c[en - 1]
    for k in range(j, en):
        if sl is not None:
            if k > j and (o[k] - sl) * side <= 0:
                px = o[k]; break
            if (h[k] >= sl) if side < 0 else (l[k] <= sl):
                px = sl; break
        if exit_kind in ("both", "tp"):
            if k > j and (o[k] - tgt) * side >= 0:
                px = o[k]; break
            if (l[k] <= tgt) if side < 0 else (h[k] >= tgt):
                px = tgt; break
        if exit_kind in ("both", "ma") and np.isfinite(ma[k]) and (c[k] - ma[k]) * side >= 0:
            px = o[k + 1] if k + 1 < en else c[en - 1]; break
    xp = px - side * tick_of(px, is5)
    return side * (xp - ep) / ep * 100 - COST_RT


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
        ix = np.flatnonzero(day == dd); st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum()); ds = dd.strftime("%Y-%m-%d")
            if (100 <= pc <= 100000) and (pen - pst) >= 0.9 * full and (code, ds) in cand and (code, ds) not in earn:
                gu = o[st] / pc - 1
                unis = []
                if ptv >= 3e8:
                    unis.append("全銘柄")
                if ptv >= 1e8 and gu >= 0.03:
                    unis.append("GU+3%")
                if ptv >= 1e8 and gu <= -0.03:
                    unis.append("GD−3%")
                if unis:
                    for side in (-1, 1):
                        for win_name, (wlo, whi) in (("全", (hm(9, 5), hm(14, 25))), ("前場", (hm(10, 0), hm(11, 25)))):
                            # 最初のタッチ→確認（時間帯は確認の足で判定）
                            e = None
                            for i in range(st, en - 1):
                                if not np.isfinite(up[i]) or tmin[i] > whi:
                                    continue
                                touch = (h[i] >= up[i]) if side < 0 else (l[i] <= lo[i])
                                if not touch:
                                    continue
                                for k in range(i, min(i + CONFIRM_WITHIN + 1, en - 1)):
                                    opp = (c[k] < o[k] and c[k] < up[k]) if side < 0 else (c[k] > o[k] and c[k] > lo[k])
                                    if opp:
                                        if wlo <= tmin[k] <= whi:      # 時間帯の中で最初の確認だけを使う
                                            e = k
                                        break
                                if e is not None:
                                    break
                            if e is None:
                                continue
                            for uni in unis:
                                if uni == "GU+3%" and side > 0 or uni == "GD−3%" and side < 0:
                                    continue
                                for ex in ("both", "ma", "tp"):
                                    for sl in STOPS:
                                        r = run_exit(side, e + 1, en, o, h, l, c, ma, ex, sl, is5)
                                        out.append({"rule": "タッチ→反対足", "uni": uni, "side": "売り" if side < 0 else "買い",
                                                    "win": win_name, "exit": ex, "sl": sl or 0, "date": ds, "ret": r, "fwd": ds >= SPLIT})
                # 参考: 今のルール（GU+3%の売り）
                if ptv >= 1e8 and gu >= 0.03:
                    for win_name, (wlo, whi) in (("全", (hm(10, 0), hm(14, 25))), ("前場", (hm(10, 0), hm(11, 25)))):
                        e = next((k for k in range(st, en - 1) if wlo <= tmin[k] <= whi and np.isfinite(up[k])
                                  and c[k] >= up[k] * 1.005 and c[k] >= o[st] * 1.02), None)
                        if e is not None:
                            for ex in ("both", "ma"):
                                for sl in STOPS:
                                    r = run_exit(-1, e + 1, en, o, h, l, c, ma, ex, sl, is5)
                                    out.append({"rule": "今のルール", "uni": "GU+3%", "side": "売り", "win": win_name, "exit": ex,
                                                "sl": sl or 0, "date": ds, "ret": r, "fwd": ds >= SPLIT})
        prev = (st, en)
    return out


def st(r: pd.Series) -> str:
    if len(r) == 0:
        return "n=   0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return f"n={len(r):>4} 勝率{(r>0).mean()*100:>3.0f}% PF{(gp/gl if gl else np.inf):>5.2f} 1件{r.mean():>+5.2f}%"


def main() -> None:
    token = _jquants_key()
    dl = _recent_trading_days(62)
    D = jq_daily(token, dl).sort_values(["code", "date"])
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1); D["gu"] = D.AdjO / D.pc - 1
    C = D[(D.pc >= 100) & (D.pc <= 100000) & ((D.pva >= 3e8) | ((D.pva >= 1e8) & (D.gu.abs() >= 0.025)))]
    cand = set(zip(C.code, C.date.astype(str).str[:10]))
    earn = jq_fins(token, dl)
    dd = sorted(set(D.date.astype(str).str[:10])); nxt = {dd[i]: dd[i + 1] for i in range(len(dd) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(set(C.code))
    T = pd.DataFrame([x for code, s in M.items() for x in trades(code, s, cand, earn)])
    EXN = {"both": "中心線 or 3%利確", "ma": "中心線のみ", "tp": "3%利確のみ"}
    for (rule, uni, side), g in T.groupby(["rule", "uni", "side"], sort=False):
        print(f"\n################ {rule} ｜ {uni} の{side} ################")
        for win in ("前場", "全"):
            for ex in ("both", "ma", "tp"):
                for sl in (0, 0.07):
                    d = g[(g.win == win) & (g.exit == ex) & (g.sl == sl)]
                    if not len(d):
                        continue
                    print(f"  [{win:<2}] {EXN[ex]:<10} 損切り{'なし' if not sl else '7%':<3}  "
                          f"選定: {st(d[~d.fwd].ret)}  ／ 確認: {st(d[d.fwd].ret)}")
    print("\n読み方: 判断は「確認」（10/23〜）。前場の確認期間は件数が少ない。売りは空売りできるかを見ていない（楽観側）。")


if __name__ == "__main__":
    main()

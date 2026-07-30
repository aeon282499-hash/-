# -*- coding: utf-8 -*-
"""_bt_sell_straddle.py — 空売りで「決算を跨ぐ」玉は事故っているのか（2026-07-30・本番無変更）。

前回(_bt_sell_earnings.py)は BUY と同じ「シグナル日±N日」で測って不採用にしたが、
売りで本当に怖いのは *保有期間中に決算が出ること*（好決算で寄りから踏み上げられる）。
「シグナル日の近辺かどうか」と「保有期間を跨ぐかどうか」は別物なので測り直す。

  跨ぎ = エントリー日〜最大保有(MAXH=3営業日)の間に実開示日がある
  ※ これは事前に判定できる（JPX公式予定表が1〜2ヶ月先まで持っている）＝実装可能

エンジンは _bt_sell_improve.py と同一。選定ロジックも同一だが、
エントリー日/決済日を記録するため run() を写経して日付を持たせている
（BASEの再現一致をアサートして写経ミスを検出する）。

実行: python -X utf8 _bt_sell_straddle.py
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import _bt_sell_improve as S
from _bt_sell_earnings import CAL          # 実開示日（2016-2021 + 2022-2026 マージ済）

D = S.D
BASE_MASK = S.base_mask(D)

TD = json.load(open("_trading_days_10y.json", encoding="utf-8"))   # 2,448営業日
TDI = {d: i for i, d in enumerate(TD)}


def hold_window(sig: str, maxh: int = 3) -> list[str]:
    """シグナル日 → [エントリー日 .. 最大保有最終日] の営業日リスト（事前に確定できる）。"""
    i = TDI.get(sig)
    if i is None:
        return []
    return TD[i + 1: i + 1 + maxh]


# ── 各候補が「決算を跨ぐ」か（エントリー前に判定可能）──────────────
DISC = {tk: set(map(str, ds)) for tk, ds in CAL.items()}


def straddle_mask(maxh: int = 3) -> pd.Series:
    out = []
    for tk, sig in zip(D["ticker"], D["sig"]):
        w = hold_window(sig, maxh)
        ds = DISC.get(tk, ())
        out.append(any(d in ds for d in w))
    return pd.Series(out, index=D.index)


def run_dated(sub: pd.DataFrame, stop=3.0, tp=5.0, maxh=3, size=S.SIZE, slots=S.SLOTS):
    """S.run と同一の選定・同一の replay。加えて1件ずつの明細を返す。"""
    if not len(sub):
        return None, pd.DataFrame()
    A = sub.sort_values(["sig", "score"], ascending=[True, False])
    days = sorted(A["sig"].unique())
    di = {d: i for i, d in enumerate(days)}
    live, held, rows = [], {}, []
    for d, g in A.groupby("sig", sort=True):
        i = di[d]
        live = [x for x in live if x >= i]
        held = {t: x for t, x in held.items() if x >= i}
        used, nn = {}, 0
        for r in g.to_dict("records"):
            if nn >= S.MAX_PER_DAY or len(live) >= slots:
                break
            if r["ticker"] in held:
                continue
            if r["sector"] and used.get(r["sector"], 0) >= S.SECTOR_CAP:
                continue
            p, k = S.replay(r, stop, tp, maxh)
            sh = int(size / r["entry"] / 100) * 100
            if sh <= 0:
                continue
            if r["sector"]:
                used[r["sector"]] = used.get(r["sector"], 0) + 1
            live.append(i + k)
            held[r["ticker"]] = i + k
            nn += 1
            w = hold_window(r["sig"], maxh)
            ds = DISC.get(r["ticker"], ())
            rows.append({
                "y": r["year"], "sig": r["sig"], "ticker": r["ticker"], "name": r["name"],
                "entry_d": w[0] if w else "", "exit_d": w[k] if k < len(w) else (w[-1] if w else ""),
                "pnl": p, "yen": p / 100 * sh * r["entry"], "held_k": k,
                "straddle": any(x in ds for x in w),
                "straddle_real": any(x in ds for x in w[:k + 1]),   # 実際に持っていた日だけ
            })
    if not rows:
        return None, pd.DataFrame()
    B = pd.DataFrame(rows)
    yr = B.groupby("y").yen.sum().reindex(S.YEARS, fill_value=0)
    loss = -B.pnl[B.pnl < 0].sum()
    out = dict(n=len(B), tot=B.yen.sum(), win=int((yr > 0).sum()),
               pf=(B.pnl[B.pnl > 0].sum() / loss) if loss > 0 else np.inf, yr=yr)
    for lab, y0, y1 in S.ERAS:
        out[lab] = float(yr[(yr.index >= y0) & (yr.index <= y1)].sum())
    return out, B


BASE, BDF = run_dated(D[BASE_MASK])
REF = S.run(D[BASE_MASK])
assert abs(BASE["tot"] - REF["tot"]) < 1, f"写経ズレ {BASE['tot']} vs {REF['tot']}"
print(f"[検証] run_dated は S.run と一致（{BASE['n']}件 {BASE['tot']:+,.0f}円）\n")

print("=" * 104)
print("① 実際に決算を跨いだ玉 vs 跨がなかった玉（現行ロジックが実際に取った153件の内訳）")
print("=" * 104)
grp = BDF.groupby("straddle_real")
print(f"  {'':<12}{'件数':>6}{'勝率':>8}{'平均%':>9}{'合計円':>13}{'最悪1件':>13}{'最良1件':>13}")
for k, g in grp:
    lab = "決算跨ぎ" if k else "跨がない"
    print(f"  {lab:<12}{len(g):>6}{(g.pnl > 0).mean() * 100:>7.1f}%{g.pnl.mean():>9.2f}"
          f"{g.yen.sum():>13,.0f}{g.yen.min():>13,.0f}{g.yen.max():>13,.0f}")

print("\n[テール] 損失ワースト8件（踏み上げ事故が決算に偏っているか）")
w = BDF.nsmallest(8, "yen")[["sig", "ticker", "name", "pnl", "yen", "straddle_real"]]
for r in w.to_dict("records"):
    mark = "★決算跨ぎ" if r["straddle_real"] else ""
    print(f"   {r['sig']} {r['ticker']} {r['name'][:12]:<12} {r['pnl']:>7.2f}% {r['yen']:>10,.0f}円 {mark}")
nb = BDF[BDF.pnl < 0]
print(f"   ※ 負け玉{len(nb)}件のうち決算跨ぎ {int(nb.straddle_real.sum())}件 "
      f"({nb.straddle_real.mean() * 100:.0f}%) / 全体の跨ぎ率 {BDF.straddle_real.mean() * 100:.0f}%")

print("\n" + "=" * 104)
print("② 「跨ぎそうな玉」を最初から撃たない場合（JPX予定表があれば事前判定できる）")
print("=" * 104)
print(f"  {'設定':<24}{'件数':>6}{'PF':>7}{'10年計':>14}{'勝ち年':>9}{'前半17-21':>14}{'後半22-26':>14}{'現行差':>13}")


def line(tag, r, base=None):
    if r is None:
        print(f"  {tag:<24}  —")
        return
    d = f"{r['tot'] - base['tot']:+,.0f}" if base else ""
    print(f"  {tag:<24}{r['n']:>6}{r['pf']:>7.2f}{r['tot']:>14,.0f}{r['win']:>8}/10"
          f"{r['2017-21']:>14,.0f}{r['2022-26']:>14,.0f}{d:>13}")


line("現行（何もしない）", BASE)
sm = straddle_mask(3)
for slots in (3, 5, 8):
    b, _ = run_dated(D[BASE_MASK], slots=slots)
    e, _ = run_dated(D[BASE_MASK & ~sm], slots=slots)
    line(f"跨ぎ除外・枠{slots}", e, b)

print("\n[判定] 枠を振って符号が反転するならノイズ（記憶のルール）。")

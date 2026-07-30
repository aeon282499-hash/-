# -*- coding: utf-8 -*-
"""_bt_sell_earnings.py — SELLにも決算±N日除外を入れるべきか（2026-07-30・本番無変更）。

背景: 決算±3日除外はBUYだけに入っている（2026-05-21〜）。SELLに入れていないのは
「SELLは元々日経25MA以下のときしか出ない」という理由で、測ってはいなかった。
一方でSELLの入口は「前日比+3%以上の急騰 × RSI≥60 × 乖離+4%以上」＝
決算で跳ねた銘柄が構造的に混ざりやすい。だから測る。

エンジンは _bt_sell_improve.py と同一（_sell_wide2.pkl・本番同一の選定：
スコア降順・業種cap2・1日5件・3枠・実株数・TP-5%/STOP+3%/MAXH3/RSI50出口）。
決算日は**実開示日**を使う（2016-2021と2022-2026の2本のカレンダーをマージ）。

採用条件（記憶のバー）: 両期間(2017-21/2022-26)で改善・窓の近傍が高原・機構の説明がつく。

実行: python -X utf8 _bt_sell_earnings.py
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import _bt_sell_improve as S   # collect/run/base_mask/D をそのまま使う

D = S.D
BASE_MASK = S.base_mask(D)


# ── 実開示日カレンダー（2本マージ）──────────────────────────────
def load_actual_calendar() -> dict[str, set[str]]:
    cal: dict[str, set[str]] = {}
    for path in ("earnings_calendar_2016_2021.json", "earnings_calendar.json"):
        try:
            blob = json.load(open(path, encoding="utf-8"))
        except Exception as e:
            print(f"[warn] {path} 読めず: {e}")
            continue
        for tk, dates in blob.items():
            cal.setdefault(tk, set()).update(str(d) for d in dates)
    return cal


CAL = load_actual_calendar()


def near_mask(days: int) -> pd.Series:
    """各候補の sig 日が、その銘柄の実開示日±days 内かどうか。"""
    win: dict[str, set[str]] = {}
    for tk, dates in CAL.items():
        s: set[str] = set()
        for d_str in dates:
            try:
                d = datetime.strptime(d_str, "%Y-%m-%d").date()
            except Exception:
                continue
            for o in range(-days, days + 1):
                s.add((d + timedelta(days=o)).strftime("%Y-%m-%d"))
        win[tk] = s
    return pd.Series(
        [row_sig in win.get(row_tk, ()) for row_tk, row_sig in zip(D["ticker"], D["sig"])],
        index=D.index,
    )


def line(tag, r, base=None):
    if r is None:
        print(f"  {tag:<26}  —")
        return
    d10 = f"{r['tot'] - base['tot']:+,.0f}" if base else ""
    print(f"  {tag:<26}{r['n']:>6}{r['pf']:>7.2f}{r['tot']:>14,.0f}{r['win']:>7}/10"
          f"{r['2017-21']:>14,.0f}{r['2022-26']:>14,.0f}{d10:>14}")


HDR = (f"  {'設定':<26}{'件数':>6}{'PF':>7}{'10年計':>14}{'勝ち年':>10}"
       f"{'前半17-21':>14}{'後半22-26':>14}{'現行差':>14}")

print("\n" + "=" * 118)
print("SELLに決算±N日除外を入れると? （実開示日ベース・本番同一の選定/枠3/業種cap2）")
print("=" * 118)

BASE = S.run(D[BASE_MASK])
print(HDR)
line("現行（除外なし）", BASE)

results = {}
for days in (1, 2, 3, 4, 5):
    nm = near_mask(days)
    r = S.run(D[BASE_MASK & ~nm])
    results[days] = r
    line(f"決算±{days}日を除外", r, BASE)

# ── どれくらいの候補が決算にぶつかっているか ───────────────────
nm3 = near_mask(3)
cand = D[BASE_MASK]
hit = int(nm3[BASE_MASK].sum())
print(f"\n[内訳] 本番条件のSELL候補 {len(cand):,}件 中 決算±3日に重なるもの "
      f"{hit:,}件 ({hit / max(len(cand),1) * 100:.1f}%)")
by_year = cand.assign(near=nm3[BASE_MASK].to_numpy()).groupby("year")["near"].agg(["sum", "count"])
by_year["pct"] = by_year["sum"] / by_year["count"] * 100
print("[年別] 候補数 / うち決算±3日 / 割合  ※カレンダー欠落があると旧年だけ割合が落ちる")
for y, r in by_year.iterrows():
    print(f"   {y}: {int(r['count']):>5}件 / {int(r['sum']):>4}件 / {r['pct']:>5.1f}%")

# ── 除外される玉そのものの成績（機構の確認）─────────────────────
print("\n[機構] 「決算±3日の玉」だけを取り出すと、それは勝っているのか負けているのか")
only_near = S.run(D[BASE_MASK & nm3])
print(HDR)
line("決算±3日の玉だけ", only_near)
line("決算から離れた玉だけ", results[3], BASE)

print("\n注: 3枠のスロットsimなので「除外した分だけ別の玉が入る」効果込み。")
print("    件数が減っても10年計が増えるなら、決算玉が枠を食っていたということ。")

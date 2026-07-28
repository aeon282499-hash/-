# -*- coding: utf-8 -*-
"""_bt_portfolio.py — 信用600万を極み(スイング買い)と決算持ち越しでどう分けると最大か（2026-07-28）。

今までは各システムを単独でしか測っていなかった。実際は同じ信用枠を食い合うので、
同じ時間軸で1本のサイムに乗せて「合計いくら」を測る。

  極み  : entry日の寄りで建て exoff+1営業日で決済（_bt10y_candidates_margin.csv）
  決算  : d0大引けで建て翌寄りで決済（gap>+8%はPEADで5営業日）（_earnings_events_rich2.csv）
  資金  : 共有の budget。埋まっていたら見送り。優先順位は引数で切り替え。

実行: python -X utf8 _bt_portfolio.py
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

BUDGET = 6_000_000
SINCE = "2022-01-01"

# ── 極み（スイング買い）候補 ──
SW = pd.read_csv("_bt10y_candidates_margin.csv", parse_dates=["entry"])
SW = SW[~(SW["days_cover"] > 0.8)]
SEC = json.load(open("sector33_map.json", encoding="utf-8"))
SW["sector"] = SW["ticker"].map(SEC).fillna("")
SW["day"] = SW["entry"].dt.strftime("%Y-%m-%d")
SW = SW[SW["day"] >= SINCE].sort_values(["day", "score"], ascending=[True, False])

# ── デイトレ売り(フェード)候補 ── 当日決済なので夜またぎゼロ。日中だけ枠を食う。
FD = pd.read_pickle("_fade_deep.pkl")
FD = FD[(FD.gain >= 6) & (FD.vr < 6) & (FD.dev < 80)].copy()
for _k in ("dev", "atr"):
    FD["r_" + _k] = FD.groupby("sig")[_k].rank(ascending=False, pct=True)
FD["mix"] = (FD["r_dev"] + FD["r_atr"]) / 2
FD = FD.sort_values(["sig", "mix"], ascending=[True, True]).groupby("sig").head(1)
FD = FD[FD["sig"] >= SINCE]
FD["entry_day"] = FD["sig"]     # sig日の翌営業日に建てるが、資金拘束は「その1日」だけ

# ── 決算持ち越し候補 ──
EA = pd.read_csv("_earnings_events_rich2.csv")
EA = EA[(EA.rsi <= 45) & (EA.runup5 < -3) & (EA.tov20 >= 7.5e8) & (EA.d0 >= SINCE)]
EA = EA.sort_values(["d0", "rsi"])

DAYS = sorted(set(SW["day"]) | set(EA["d0"]))
DI = {d: i for i, d in enumerate(DAYS)}


def shares_amt(price: float, size: int) -> tuple[int, float]:
    sh = int(size / price / 100) * 100
    return sh, sh * price


def simulate(sw_size: int, sw_slots: int, ea_size: int, ea_slots: int,
             priority: str = "earnings", budget: int = BUDGET):
    """priority='earnings' なら決算を先に、'swing' なら極みを先に資金を割り当てる。"""
    free = budget
    live: list[tuple[int, float]] = []          # (解放される日index, 金額)
    sw_open: dict[str, int] = {}
    sw_n = ea_n = 0
    sw_yen = ea_yen = 0.0
    sw_skip = ea_skip = 0
    peak = 0.0
    sw_by_day = {d: g for d, g in SW.groupby("day")}
    ea_by_day = {d: g for d, g in EA.groupby("d0")}

    def take_swing(i, g):
        nonlocal free, sw_n, sw_yen, sw_skip
        used_sec, n = {}, 0
        for r in g.itertuples():
            if n >= sw_slots:
                break
            if r.ticker in sw_open or not np.isfinite(r.pnl):
                continue
            if r.price * 100 > sw_size:
                continue
            if r.sector and used_sec.get(r.sector, 0) >= 3:
                continue
            sh, amt = shares_amt(r.price, sw_size)
            if sh <= 0:
                continue
            if amt > free:
                sw_skip += 1
                continue
            if r.sector:
                used_sec[r.sector] = used_sec.get(r.sector, 0) + 1
            span = int(r.exoff) + 1
            free -= amt
            live.append((i + span, amt))
            sw_open[r.ticker] = i + span
            sw_yen += r.pnl / 100 * amt
            sw_n += 1
            n += 1

    def take_earn(i, g):
        nonlocal free, ea_n, ea_yen, ea_skip
        n = 0
        for r in g.itertuples():
            if n >= ea_slots:
                break
            if not np.isfinite(r.gap) or r.price * 100 > ea_size:
                continue
            sh, amt = shares_amt(r.price, ea_size)
            if sh <= 0:
                continue
            if amt > free:
                ea_skip += 1
                continue
            if r.gap > 8.0 and np.isfinite(r.r5):
                pnl, span = r.r5, 5
            else:
                pnl, span = r.gap, 1
            free -= amt
            live.append((i + span, amt))
            ea_yen += pnl / 100 * amt
            ea_n += 1
            n += 1

    for d in DAYS:
        i = DI[d]
        for k in [x for x in live if x[0] <= i]:
            free += k[1]
        live = [x for x in live if x[0] > i]
        sw_open = {t: x for t, x in sw_open.items() if x > i}
        peak = max(peak, budget - free)
        order = (take_earn, take_swing) if priority == "earnings" else (take_swing, take_earn)
        srcs = ((ea_by_day.get(d), take_earn), (sw_by_day.get(d), take_swing))
        if priority == "swing":
            srcs = srcs[::-1]
        for g, fn in srcs:
            if g is not None:
                fn(i, g)
    return dict(sw_n=sw_n, sw_yen=sw_yen, sw_skip=sw_skip,
                ea_n=ea_n, ea_yen=ea_yen, ea_skip=ea_skip,
                total=sw_yen + ea_yen, peak=peak)


if __name__ == "__main__":
    yrs = 4.6
    print(f"■ 信用{BUDGET // 10000}万を共有した実測（{SINCE}〜2026-07・約{yrs:.1f}年）")
    print(f"  {'構成':<38}{'極み':>18}{'決算':>18}{'合計':>12}{'年平均':>10}")
    cases = [
        ("極み3枠100万 + 決算8枠100万（現案）", 1_000_000, 3, 1_000_000, 8, "earnings"),
        ("極み3枠100万 + 決算8枠150万", 1_000_000, 3, 1_500_000, 8, "earnings"),
        ("極み3枠150万 + 決算8枠100万", 1_500_000, 3, 1_000_000, 8, "earnings"),
        ("極み2枠100万 + 決算8枠150万", 1_000_000, 2, 1_500_000, 8, "earnings"),
        ("極みなし + 決算8枠150万", 1_000_000, 0, 1_500_000, 8, "earnings"),
        ("極み3枠100万 + 決算なし", 1_000_000, 3, 1_000_000, 0, "earnings"),
        ("極み優先(3枠100万) + 決算8枠100万", 1_000_000, 3, 1_000_000, 8, "swing"),
    ]
    for lab, ss, sl, es, el, pri in cases:
        r = simulate(ss, sl, es, el, pri)
        print(f"  {lab:<38}{r['sw_n']:>5}件{r['sw_yen'] / 10000:>+9.0f}万"
              f"{r['ea_n']:>7}件{r['ea_yen'] / 10000:>+9.0f}万"
              f"{r['total'] / 10000:>+11.0f}万{r['total'] / 10000 / yrs:>+9.0f}万")
    print(f"\n  ※資金不足で見送った件数も内部で数えている（下記）")
    for lab, ss, sl, es, el, pri in cases[:3]:
        r = simulate(ss, sl, es, el, pri)
        print(f"  {lab:<38} 見送り 極み{r['sw_skip']}件 / 決算{r['ea_skip']}件 / ピーク{r['peak'] / 10000:.0f}万")

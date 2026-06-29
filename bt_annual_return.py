# -*- coding: utf-8 -*-
"""
bt_annual_return.py — 「正しい年利」を出す（枠制約つき固定資本ポートフォリオ）
================================================================================
背景:
  backtest_range.py / notifier の年間レポートは、全約定の pnl% を等加重で合算し
  capital=size×5 で割って年利を出していた。だが本番は MAX_SIGNALS=5/日・MAX_HOLD=3
  なので同時保有が最大15銘柄まで膨らむ（実測でも最大15・5枠超の日が57〜65%）。
  → 「最大15保有ぶんの利益」を「5枠ぶんの資本」で割るため年利が過大表示になる。

このツール:
  固定 N スロット・1件 BUDGET 円の実ポートフォリオを回す。
    各営業日: entry日より前に exit したポジを解放 → 空き枠を当日シグナルで埋める
    （満枠なら以降は見送り。同一銘柄の重複は元CSV側で既に排除済み）
  年利 = その年に exit したトレードの実現損益(円) / 投下資本(N×BUDGET)。
  ※score は期待値と無相関（検証済み bt_score_buckets）なので、枠が埋まった時に
    どの銘柄を落とすかは利益に中立 → CSV順processでも年利は不偏。

使い方:
  python bt_annual_return.py <trades.csv> [--budget 1000000] [--slots 5] [--side BUY|SELL|ALL]
  引数省略時は現行ロジックの最新CSVと4年通しCSVを既定設定(100万/5枠/BUY)で両方出力。
"""
from __future__ import annotations
import argparse, csv, sys
from datetime import date
from collections import defaultdict, Counter

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _d(s: str) -> date:
    y, m, dd = map(int, s.split("-"))
    return date(y, m, dd)


def load(path: str, side: str = "BUY") -> list[dict]:
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if side != "ALL" and r.get("direction", "BUY") != side:
                continue
            rows.append({"entry": _d(r["entry_date"]),
                         "exit":  _d(r["exit_date"]),
                         "pnl":   float(r["pnl_pct"])})
    rows.sort(key=lambda x: (x["entry"], x["exit"]))
    return rows


def slot_sim(rows: list[dict], nslots: int):
    """満枠は見送り。取れたトレード一覧と見送り件数を返す。"""
    open_exits: list[date] = []
    taken, skipped = [], 0
    for x in rows:
        open_exits = [e for e in open_exits if e >= x["entry"]]   # entry前にexit→解放
        if len(open_exits) < nslots:
            open_exits.append(x["exit"]); taken.append(x)
        else:
            skipped += 1
    return taken, skipped


def concurrency(rows: list[dict]) -> Counter:
    days = sorted({x["entry"] for x in rows} | {x["exit"] for x in rows})
    return Counter(sum(1 for x in rows if x["entry"] <= day <= x["exit"]) for day in days)


def report(path: str, label: str, budget: int, slots: int, side: str):
    rows = load(path, side)
    print("\n" + "=" * 72)
    print(f"  {label}")
    print(f"  CSV: {path.split(chr(92))[-1]}  /  side={side}  /  {budget//10000}万円per件")
    print("=" * 72)
    if not rows:
        print("  対象トレードなし"); return

    # 同時保有の膨らみ
    dist = concurrency(rows); ndays = sum(dist.values()); maxc = max(dist)
    over = sum(v for k, v in dist.items() if k > slots)
    print(f"  トレード {len(rows)}件 / 最大同時保有 {maxc}銘柄 / "
          f"{slots}枠超の日 {over}日({over/ndays*100:.0f}%)")

    # メイン: 指定スロットの正しい年利
    cap = slots * budget
    taken, skip = slot_sim(rows, slots)
    yr_yen = defaultdict(float); yr_n = defaultdict(int); yr_w = defaultdict(int)
    for x in taken:
        yr_yen[x["exit"].year] += x["pnl"] / 100 * budget
        yr_n[x["exit"].year]  += 1
        yr_w[x["exit"].year]  += 1 if x["pnl"] > 0 else 0
    print(f"\n  ★正しい年利（{slots}枠・投下資本{cap//10000}万円・満枠見送り）")
    print(f"   採用{len(taken)}件 / 見送り{skip}件（資金不足で取れず） / 採用率{len(taken)/len(rows)*100:.0f}%")
    print(f"   {'年':>6} {'件数':>5} {'勝率':>6} {'年間損益':>11} {'年利':>8}")
    tot_yen = 0.0
    for y in sorted(yr_yen):
        tot_yen += yr_yen[y]
        print(f"   {y:>6} {yr_n[y]:>5} {yr_w[y]/yr_n[y]*100:>5.0f}% "
              f"{yr_yen[y]/10000:>+9.1f}万 {yr_yen[y]/cap*100:>+7.1f}%")
    n_years = len(yr_yen)
    print(f"   {'─'*44}")
    print(f"   合計実現損益 {tot_yen/10000:+.1f}万円 / 平均年利 {tot_yen/cap*100/n_years:+.1f}%/年"
          f"（資本{cap//10000}万に対し)")

    # 比較: 旧表示（全約定Σpnl%÷slots＝過大）
    sum_all = sum(x["pnl"] for x in rows)
    old_yen = sum_all / 100 * budget
    print(f"\n   旧レポート式（全{len(rows)}約定Σpnl%={sum_all:+.0f}% ÷{slots}枠 相当）"
          f"= {old_yen/10000:+.1f}万 → 平均{old_yen/cap*100/n_years:+.1f}%/年 ★過大")

    # スロット感度
    print(f"\n  スロット感度（資本＝枠×{budget//10000}万）")
    print(f"   {'枠':>3} {'資本':>7} {'採用':>5} {'見送り':>6} {'平均年利':>8}")
    for n in sorted({3, 5, 10, 15, 99} | {slots}):
        tk, sk = slot_sim(rows, n)
        c = n * budget
        yen = sum(x["pnl"] / 100 * budget for x in tk)
        lbl = "∞" if n == 99 else str(n)
        capl = "無制限" if n == 99 else f"{c//10000}万"
        print(f"   {lbl:>3} {capl:>7} {len(tk):>5} {sk:>6} {yen/c*100/n_years:>+7.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?")
    ap.add_argument("--budget", type=int, default=1_000_000)
    ap.add_argument("--slots", type=int, default=5)
    ap.add_argument("--side", default="BUY", choices=["BUY", "SELL", "ALL"])
    a = ap.parse_args()
    if a.csv:
        report(a.csv, "指定CSV", a.budget, a.slots, a.side)
    else:
        report("backtest_2025-01-01_2026-06-26_main.csv",
               "現行ロジック(TP5/寄指/HOLD3) 2025-01〜2026-06", a.budget, a.slots, a.side)
        report("backtest_2022-01-01_2026-05-02_MAX5.csv",
               "4年強通し(MAX5) 2022-01〜2026-05", a.budget, a.slots, a.side)


if __name__ == "__main__":
    main()

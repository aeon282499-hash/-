# -*- coding: utf-8 -*-
"""manual_daytrade_log.py — 裁量デイトレの台帳と採点（2026-08-24・本人「今日から実弾デイトレーダー」）。

校則: 授業料枠30万(現金バッファと別勘定)・1日-1万で当日終了・月-5万で当月終了・
      卒業試験=3ヶ月でPF1.3以上×n>=30(達成で本人エッジとしてBT化・未達で紙に戻る)。
型:   A=ギャップフェード(GU+3〜8%→12:30成行空売り→引成・50万/枚)
      B=VWAP+3%乖離ショート(前日-4%×代金10億×1,000〜5,000円×貸借○・最小単元)
      X=型外(校則違反の記録用・ゼロが理想)

使い方:
  記録:   python -X utf8 manual_daytrade_log.py add 6526 -8500 --type A --note "レーザーテック"
          (yen=確定損益円・プラスは +12000 のように)
  採点:   python -X utf8 manual_daytrade_log.py report
台帳: manual_daytrade_log.csv (date,ticker,type,yen,note)
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import date

PATH = "manual_daytrade_log.csv"
FIELDS = ["date", "ticker", "type", "yen", "note"]
TUITION = 300_000          # 授業料枠
DAY_STOP = -10_000         # 1日ストップ
MONTH_STOP = -50_000       # 月ストップ
GRAD_PF, GRAD_N = 1.3, 30  # 卒業ライン（直近3ヶ月）


def load() -> list[dict]:
    if not os.path.exists(PATH):
        return []
    with open(PATH, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f)]


def add(args) -> None:
    rows = load()
    new = not rows
    with open(PATH, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow({"date": args.date, "ticker": args.ticker, "type": args.type,
                    "yen": int(args.yen), "note": args.note or ""})
    rows = load()
    today = [int(r["yen"]) for r in rows if r["date"] == args.date]
    d_sum = sum(today)
    ym = args.date[:7]
    m_sum = sum(int(r["yen"]) for r in rows if r["date"].startswith(ym))
    total = sum(int(r["yen"]) for r in rows)
    print(f"記帳: {args.date} {args.ticker} 型{args.type} {int(args.yen):+,}円")
    print(f"本日 {d_sum:+,}円 / 今月 {m_sum:+,}円 / 通算 {total:+,}円（授業料残り {TUITION + total:,}円）")
    if d_sum <= DAY_STOP:
        print(f"🛑 校則: 本日{DAY_STOP:+,}円到達 → **今日はここで終了**")
    if m_sum <= MONTH_STOP:
        print(f"🛑 校則: 今月{MONTH_STOP:+,}円到達 → **今月の実弾は終了・紙に戻る**")
    if TUITION + total <= 0:
        print("🛑 授業料枠30万が尽きました → 実弾終了。紙とBTに戻って出直し")


def report(_args) -> None:
    rows = load()
    if not rows:
        print("台帳が空です。add で記帳してください")
        return
    for r in rows:
        r["yen"] = int(r["yen"])

    def pf(xs: list[int]) -> float:
        neg = -sum(x for x in xs if x <= 0)
        pos = sum(x for x in xs if x > 0)
        return pos / neg if neg else float("inf")

    print("=== 月別 ===")
    months = sorted({r["date"][:7] for r in rows})
    for ym in months:
        g = [r for r in rows if r["date"].startswith(ym)]
        xs = [r["yen"] for r in g]
        w = sum(1 for x in xs if x > 0)
        print(f"  {ym}: {len(xs)}回 勝率{w}/{len(xs)} PF{pf(xs):.2f} {sum(xs):+,}円")
    print("\n=== 型別（通算） ===")
    for t in sorted({r["type"] for r in rows}):
        xs = [r["yen"] for r in rows if r["type"] == t]
        w = sum(1 for x in xs if x > 0)
        label = {"A": "ギャップフェード", "B": "VWAP乖離S", "S": "スキャル買い実験", "X": "型外(校則違反)"}.get(t, t)
        print(f"  型{t} {label}: {len(xs)}回 勝率{w}/{len(xs)} PF{pf(xs):.2f} {sum(xs):+,}円")
    total = sum(r["yen"] for r in rows)
    print(f"\n通算 {total:+,}円 / 授業料残り {TUITION + total:,}円")

    # 卒業判定（直近3ヶ月ローリング）
    last3 = months[-3:]
    g3 = [r["yen"] for r in rows if r["date"][:7] in last3]
    n3, pf3 = len(g3), pf([*g3]) if g3 else 0.0
    ok = pf3 >= GRAD_PF and n3 >= GRAD_N
    print(f"卒業試験（直近3ヶ月 {'/'.join(last3)}）: n={n3}(必要{GRAD_N}) PF{pf3:.2f}(必要{GRAD_PF}) "
          f"→ {'✅ 合格圏＝BT化してシステム昇格を検討' if ok else '審査中'}")
    x = [r for r in rows if r["type"] == "X"]
    if x:
        print(f"⚠️ 型外トレード {len(x)}回 {sum(r['yen'] for r in x):+,}円 ＝校則違反。ゼロが目標")


def main() -> None:
    ap = argparse.ArgumentParser(description="裁量デイトレ台帳")
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add", help="1トレード記帳")
    a.add_argument("ticker")
    a.add_argument("yen", help="確定損益円（例 +12000 / -8500）")
    a.add_argument("--type", default="X", choices=["A", "B", "S", "X"],
                   help="A=ギャップフェード B=VWAP乖離S S=スキャル買い実験(最小単元) X=型外")
    a.add_argument("--date", default=date.today().strftime("%Y-%m-%d"))
    a.add_argument("--note", default="")
    a.set_defaults(func=add)
    r = sub.add_parser("report", help="月別・型別の採点と卒業判定")
    r.set_defaults(func=report)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

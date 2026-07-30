# -*- coding: utf-8 -*-
"""_bt_ytd_3systems.py — 3システムを「今の現行モデル」で2026年に流したらどうだったか（2026-07-31）。

本人「売買シグナル/売りフェード/決算シグナルで一番もうかるのはどれ? 今年 現行モデルで
運用してたらどうなってた? 昨日まで」。

⚠️ 各システムのBT用データの終端が違う（後段で明示）。決済まで完了した玉しか採点できないので、
   直近数日は自動的に落ちる（例: 極みは最大3営業日ホールドなので7/23以降の建玉は未確定）。

現行モデル（本番コードから写した値・検証当時の値ではない）:
  フェード  main=daytrade_paper.py  前日+6%以上 × 貸借○ × 張り付き除外 × 出来高6倍未満
            × 25MA乖離80%未満 × **ATR5%以上 × 25MA乖離12%以上**（2026-07-31追加）
            → 乖離+ATR順で上位2本 → 寄付成行で空売り → 引成買戻し ／ 1玉50万
  決算      main_earnings_hold.py   RSI≤55 × 直近5日騰落<-3% × 20日代金7.5億以上
            × 株価1万円以下 → RSI昇順で8枠 → 決算当日大引け買い → 翌寄り売り
            （翌寄りが+8%超なら5営業日目の大引けまで延長＝PEAD） ／ 1玉100万 ／ 大資金1階層のみ
  極み      main.py + backtest_range.py  スコア順・業種cap3・買残回転0.8日以下
            → 3枠 → 損切り-3% / 利確+5% / 最大3営業日 ／ 1玉100万

実行: python -X utf8 _bt_ytd_3systems.py
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

YEARS = list(range(2017, 2027))


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


# ────────────────────────────────────────────────────────── フェード
def fade_trades(size=500_000, npick=2):
    F = pd.read_pickle("_fade_deep.pkl").copy()
    F = F[(F.gain >= 6) & (F.vr < 6) & (F.dev < 80)]
    F = F[(F.atr >= 5.0) & (F.dev >= 12.0)]            # 2026-07-31 の新下限
    r = None
    for k in ("dev", "atr"):
        x = F.groupby("sig")[k].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    F["mix"] = r / 2
    F = F.sort_values(["sig", "mix"])
    F["rk"] = F.groupby("sig").cumcount() + 1
    F = F[F.rk <= npick].copy()
    F["sh"] = (size / F.o1 // 100 * 100).astype(int)
    F = F[(F.sh > 0) & (F.px * 100 <= size)].copy()
    F["yen"] = (F.o1 - F.c1) * F.sh                   # 空売り: 寄り-引け
    F["pnl_pct"] = (F.o1 - F.c1) / F.o1 * 100
    F["date"] = F.sig                                  # シグナル日（建てるのは翌営業日）
    F["used"] = F.sh * F.o1
    return F[["date", "y", "ticker", "pnl_pct", "yen", "used"]]


# ────────────────────────────────────────────────────────── 決算
def earn_trades(size=1_000_000, slots=8):
    E = pd.read_csv("_earnings_events_rich2.csv")
    A = E[(E.rsi <= 55.0) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8)
          & (E.price <= size / 100)].sort_values(["d0", "rsi"])
    days = sorted(A["d0"].unique()); di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            sh = int(size / r.price / 100) * 100
            if sh <= 0:
                continue
            # PEAD延長: 翌寄りが+8%超なら5営業日目の大引けまで持つ
            pnl, span = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            busy.append(i + span); held[r.ticker] = i + span
            out.append({"date": r.d0, "y": r.year, "ticker": r.ticker, "pnl_pct": pnl,
                        "yen": pnl / 100 * sh * r.price, "used": sh * r.price})
    return pd.DataFrame(out)


# ────────────────────────────────────────────────────────── 極み（スイング買い）
def swing_trades(size=1_000_000, slots=3, sector_cap=3):
    C = pd.read_csv("_bt10y_candidates_margin.csv", parse_dates=["entry"])
    C = C[~(C["days_cover"] > 0.8)]                   # 買残回転フィルタ
    SEC = json.load(open("sector33_map.json", encoding="utf-8"))
    C["sector"] = C["ticker"].map(SEC).fillna("")
    C["day"] = C["entry"].dt.strftime("%Y-%m-%d")
    C = C.sort_values(["day", "score"], ascending=[True, False])
    days = sorted(C["day"].unique()); di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in C.groupby("day", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        used_sec, n = {}, 0
        for r in g.itertuples():
            if n >= slots or len(busy) >= slots:
                break
            if r.ticker in held or not np.isfinite(r.pnl) or r.price * 100 > size:
                continue
            if r.sector and used_sec.get(r.sector, 0) >= sector_cap:
                continue
            if r.sector:
                used_sec[r.sector] = used_sec.get(r.sector, 0) + 1
            sh = int(size / r.price / 100) * 100
            span = int(r.exoff) + 1
            busy.append(i + span); held[r.ticker] = i + span; n += 1
            out.append({"date": d, "y": r.year, "ticker": r.ticker, "pnl_pct": r.pnl,
                        "yen": r.pnl / 100 * sh * r.price, "used": sh * r.price})
    return pd.DataFrame(out)


SYS = {"売りフェード": (fade_trades(), 1_000_000, "日中のみ(2枚×50万)"),
       "決算シグナル": (earn_trades(), 8_000_000, "夜のみ(8枠×100万)"),
       "極み(売買シグナル)": (swing_trades(), 3_000_000, "終日 最大3営業日(3枠×100万)")}

print("=" * 122)
print("① データの終端（決済まで終わった玉しか採点できない）")
print("=" * 122)
for k, (T, cap, note) in SYS.items():
    t26 = T[T.date >= "2026-01-01"]
    print(f"  {k:<20} 全期間 {T.date.min()} 〜 {T.date.max()}   2026分 {len(t26):>4}件"
          f"（最終 {t26.date.max()}）")
print("  ※本人の言う「昨日(7/30)まで」には数日届かない。J-Quantsキャッシュが7/28までで、")
print("    さらに極みは最大3営業日ホールドのため直近の建玉は決済が未確定＝採点できない。")

print("\n" + "=" * 122)
print("② 2026年（1/1〜データ終端）を現行モデルで運用していたら")
print("=" * 122)
print(f"  {'システム':<20}{'件数':>6}{'勝率':>8}{'PF':>7}{'平均%':>9}{'損益':>14}"
      f"{'必要資金':>11}{'資金対比':>9}  {'資金の使い方'}")
tot = 0
for k, (T, cap, note) in SYS.items():
    t = T[T.date >= "2026-01-01"]
    tot += t.yen.sum()
    print(f"  {k:<20}{len(t):>6}{(t.pnl_pct>0).mean()*100:>7.1f}%{pf(t.pnl_pct):>7.2f}"
          f"{t.pnl_pct.mean():>+8.2f}%{t.yen.sum():>+13,.0f}円{cap/1e4:>9,.0f}万"
          f"{t.yen.sum()/cap*100:>+8.1f}%  {note}")
print(f"  {'合計':<20}{'':>6}{'':>8}{'':>7}{'':>9}{tot:>+13,.0f}円")

print("\n" + "=" * 122)
print("③ 2026年の月別（円）")
print("=" * 122)
mons = sorted({d[:7] for T, _, _ in SYS.values() for d in T[T.date >= "2026-01-01"].date})
print(f"  {'月':>9}" + "".join(f"{k:>20}" for k in SYS) + f"{'合計':>16}")
for m in mons:
    line = f"  {m:>9}"; s = 0
    for k, (T, _, _) in SYS.items():
        v = T[T.date.str.startswith(m)].yen.sum(); s += v
        line += f"{v:>19,.0f}円"
    print(line + f"{s:>15,.0f}円")
line = f"  {'計':>9}"; s = 0
for k, (T, _, _) in SYS.items():
    v = T[T.date >= "2026-01-01"].yen.sum(); s += v
    line += f"{v:>19,.0f}円"
print(line + f"{s:>15,.0f}円")

print("\n" + "=" * 122)
print("④ 年別（円）— 2026だけ7ヶ月分なので他の年より小さく出る")
print("=" * 122)
print(f"  {'年':>6}" + "".join(f"{k:>20}" for k in SYS) + f"{'合計':>16}")
for y in YEARS:
    line = f"  {y:>6}"; s = 0
    for k, (T, _, _) in SYS.items():
        v = T[T.y == y].yen.sum(); s += v
        line += f"{v:>19,.0f}円"
    print(line + f"{s:>15,.0f}円")
print(f"\n  {'年平均(2017-25の9年)':<22}" + "".join(
    f"{SYS[k][0].query('2017<=y<=2025').yen.sum()/9:>18,.0f}円" for k in SYS))
print(f"  {'勝ち年(2017-25)':<22}" + "".join(
    f"{int((SYS[k][0].query('2017<=y<=2025').groupby('y').yen.sum()>0).sum()):>17}/9年" for k in SYS))

# -*- coding: utf-8 -*-
"""_bt_entry_grid10y.py — 買い入口閾値の10年グリッド（極み構成・2026-08-02）。

_bt10y_pool_wide.csv（RSI≤52/乖離≤-0.5/(rr1.2|vr1.6)/代金8億/ATR≤4.0の広域プール・
決算除外/寄指NOFILL/出口は本番仕様固定）の上で、現行入口の各閾値を単変量で振る。
選定＝参照エンジン（select 1日5件・業種cap3 → sim 3枠×100万・実株数）＝dc1.2採用時と同一。
採用バー: 両期間改善×高原×上位20玉除去×機構。単変量で合格した軸だけ組み合わせを確認する。

実行: python -X utf8 _bt_entry_grid10y.py
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

MAXH, MAX_SIG, SECTOR_CAP = 3, 5, 3
YEARS = list(range(2017, 2027))
SIZE, SLOTS = 1_000_000, 3

C = pd.read_csv("_bt10y_pool_wide.csv", parse_dates=["entry"])
C = C.sort_values(["entry", "score"], ascending=[True, False]).reset_index(drop=True)
SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))
ALLDAYS = sorted(C["entry"].unique())
GDI = {d: i for i, d in enumerate(ALLDAYS)}
n = len(C)
by_day: dict = {}
for i, d in enumerate(C["entry"]):
    by_day.setdefault(GDI[d], []).append(i)
TICK = C["ticker"].to_numpy(); YEAR = C["year"].to_numpy()
SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
PRICE = C["price"].to_numpy(); DC = C["days_cover"].to_numpy()
RSI = C["rsi"].to_numpy(); DEV = C["dev"].to_numpy()
RR = C["rr"].to_numpy(); VR = C["vr"].to_numpy()
TOV = C["tov"].to_numpy(); ATRP = C["atr_pct"].to_numpy()
PNL = C["pnl"].to_numpy(); EXO = C["exoff"].to_numpy()
# エントリー価格: プールには保存していないので price(前日終値)で株数・金額を代用
# （dc1.2採用時のエンジンは entry open を使ったが、全アーム同一基準なら比較は不変）
print(f"[prep] 広域プール {n:,}件 / 営業日{len(ALLDAYS)}日", flush=True)


def run(mask: np.ndarray) -> pd.DataFrame:
    ok = np.isfinite(PNL) & mask
    ou: dict = {}; os_: dict = {}
    picks = []
    for d in range(len(ALLDAYS)):
        for tk in [t for t, u in ou.items() if u < d]:
            del ou[tk]; del os_[tk]
        sc: dict = {}
        for s in os_.values():
            sc[s] = sc.get(s, 0) + 1
        cnt = 0
        for i in by_day.get(d, []):
            if cnt >= MAX_SIG:
                break
            if not ok[i] or TICK[i] in ou:
                continue
            s = SEC[i]
            if sc.get(s, 0) >= SECTOR_CAP:
                continue
            cnt += 1
            ex = min(d + int(EXO[i]), len(ALLDAYS) - 1)
            ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1
            picks.append((d, ex, int(YEAR[i]), float(PNL[i]), float(PRICE[i]), TICK[i]))
    live: list = []
    rows = []
    for d, ex, y, p, e, tk in picks:
        live = [x for x in live if x >= d]
        if len(live) >= SLOTS:
            continue
        sh = int(SIZE / e / 100) * 100
        if sh <= 0:
            continue
        live.append(ex)
        rows.append({"y": y, "ticker": tk, "pnl": p, "yen": p / 100 * sh * e})
    return pd.DataFrame(rows)


def s(R: pd.DataFrame, lab: str):
    yy = R.groupby("y").yen.sum().reindex(YEARS, fill_value=0.0)
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    print(f"  {lab:<26}{len(R):>6}{gp/gl if gl else float('inf'):>6.2f}{R.yen.sum():>+13,.0f}"
          f"{yy[yy.index <= 2021].sum():>+12,.0f}{yy[yy.index >= 2022].sum():>+12,.0f}"
          f"{int((yy > 0).sum()):>4}/10{yy.min():>+11,.0f}"
          f"{R.yen.sum() - R.nlargest(20, 'yen').yen.sum():>+13,.0f}")


def m_base(rsi=45.0, dev=-1.5, rrv=(1.5, 2.0), tov=2e9, atrc=3.0):
    return ((RSI <= rsi) & (DEV <= dev) & ((RR >= rrv[0]) | (VR >= rrv[1]))
            & (TOV >= tov) & (ATRP <= atrc) & (PRICE <= 10000) & ~(DC > 1.2))


HDR = (f"  {'構成':<26}{'件数':>6}{'PF':>6}{'10年計':>13}{'前半17-21':>12}"
       f"{'後半22-26':>12}{'勝年':>5}{'最悪年':>11}{'上位20除去':>13}")

print("\n" + "=" * 120)
print("基準線（現行入口×dc1.2×3枠）— _bt_kiwami_axes2 の+311万を近似再現できているか")
print("=" * 120)
print(HDR)
s(run(m_base()), "基準線")

print("\n" + "=" * 120)
print("① RSI上限（現行45）")
print("=" * 120)
print(HDR)
for v in (40, 42, 45, 48, 50, 52):
    s(run(m_base(rsi=v)), f"RSI≤{v}" + ("（現行）" if v == 45 else ""))

print("\n" + "=" * 120)
print("② 乖離上限（現行-1.5）")
print("=" * 120)
print(HDR)
for v in (-0.5, -1.0, -1.5, -2.0, -2.5):
    s(run(m_base(dev=v)), f"乖離≤{v}" + ("（現行）" if v == -1.5 else ""))

print("\n" + "=" * 120)
print("③ ATR%上限（現行3.0）")
print("=" * 120)
print(HDR)
for v in (2.5, 3.0, 3.5, 4.0):
    s(run(m_base(atrc=v)), f"ATR≤{v}" + ("（現行）" if v == 3.0 else ""))

print("\n" + "=" * 120)
print("④ 売買代金下限（現行20億）")
print("=" * 120)
print(HDR)
for v, lab in ((8e8, "8億"), (1.2e9, "12億"), (2e9, "20億（現行）"), (3e9, "30億"), (4e9, "40億")):
    s(run(m_base(tov=v)), f"代金≥{lab}")

print("\n" + "=" * 120)
print("⑤ 値幅比/出来高比（現行 rr1.5|vr2.0）")
print("=" * 120)
print(HDR)
for rrv, lab in (((1.2, 1.6), "rr1.2|vr1.6"), ((1.5, 2.0), "rr1.5|vr2.0（現行）"),
                 ((1.8, 2.5), "rr1.8|vr2.5"), ((99.0, 2.0), "vr2.0のみ"), ((1.5, 99.0), "rr1.5のみ")):
    s(run(m_base(rrv=rrv)), lab)

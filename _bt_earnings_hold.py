# -*- coding: utf-8 -*-
"""_bt_earnings_hold.py — 決算持ち越しシグナル Phase1: 基礎統計（2026-07-10）。

問い: 決算発表をまたいで保有すると何が起きるか。無条件の期待値と、
      条件(トレンド/RSI/直前ランアップ/流動性/前回決算反応)別の勝ち筋を探す。

データ: earnings_calendar.json(過去の実開示日) × jquants_cache.pkl(2022-2026/7)
前提: 開示は大引け後(15時以降)が支配的 → 「またぎ」= d0終値で保有→d0+1寄りにギャップ。
      d0 = 開示日(非営業日なら直前営業日)。
測定窓: gap(d0終値→d0+1寄り) / ret1(→d0+1終値) / ret3(→d0+3終値) / ret5(→d0+5終値)
条件軸: 25MA上下 / RSI帯 / 直前5日ランアップ / 売買代金帯 / 前回決算のgap符号 / 年別

実行: python -X utf8 _bt_earnings_hold.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
from screener import is_etf_ticker

SINCE = "2022-01-01"
END = "2026-07-28"
# 「翌営業日」とみなせる暦日の上限（年末年始/GWでも実測6日以内）。
# 価格キャッシュの穴をまたいだ偽ギャップを弾くためのガード（2026-07-29）。
GAP_MAX_DAYS = 10


def stats(x: pd.Series) -> str:
    x = x.dropna()
    if len(x) == 0:
        return f"{'—':>7}"
    win = (x > 0).mean() * 100
    pos = x[x > 0].sum()
    neg = abs(x[x <= 0].sum())
    pf = pos / neg if neg > 0 else float("inf")
    return f"{len(x):>7,}{x.mean():>+8.3f}%{x.median():>+8.3f}%{win:>7.1f}%{pf:>6.2f}"


HDR = f"{'件数':>7}{'平均':>9}{'中央値':>9}{'勝率':>8}{'PF':>6}"


def main():
    c = pickle.load(open("jquants_cache.pkl", "rb"))
    data, name_map = c["all_data"], c["name_map"]
    cal = json.load(open("earnings_calendar.json", encoding="utf-8"))
    print(f"[data] pkl {c['start']}〜{c['end']} / calendar {len(cal)}銘柄")

    rows = []
    for tk, dates in cal.items():
        df = data.get(tk)
        if df is None or len(df) < 40:
            continue
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):
            continue
        df = df.sort_index()
        o = df["Open"].astype(float).to_numpy()
        cl = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        cn = cl.to_numpy()
        idx = df.index
        n = len(cn)
        # 指標
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).to_numpy()
        ma25 = cl.rolling(25).mean().to_numpy()
        tov20 = (cl * v).rolling(20).median().to_numpy()

        ev_dates = sorted({d for d in dates if SINCE <= d <= END})
        prev_gap = {}  # event順の前回gap
        last_gap = None
        for ds in ev_dates:
            ts = pd.Timestamp(ds)
            p = idx.searchsorted(ts, side="right") - 1  # d0 = 開示日以前の直近営業日
            if p < 26 or p + 1 >= n:
                continue
            e = cn[p]
            if not (e > 0) or np.isnan(e):
                continue
            # 【2026-07-29 バグ修正】idx[p+k] は「次の行」であって「k営業日後」ではない。
            # 価格キャッシュに穴がある銘柄では数ヶ月〜数年をまたぎ、その値上がりを
            # 一晩のギャップとして計上してしまう（_bt_earnings_rich.py 側で実害を確認）。
            # 暦日で連続性を確認する。GAP_MAX_DAYS=10 なら年末年始/GWは通る。
            def _cont(k: int) -> bool:
                return p + k < n and (idx[p + k] - idx[p]).days <= GAP_MAX_DAYS * max(1, k // 2)

            if not _cont(1):
                continue
            gap = (o[p + 1] - e) / e * 100 if o[p + 1] > 0 else np.nan
            ret1 = (cn[p + 1] - e) / e * 100
            ret3 = (cn[p + 3] - e) / e * 100 if _cont(3) else np.nan
            ret5 = (cn[p + 5] - e) / e * 100 if _cont(5) else np.nan
            runup = (cn[p] - cn[p - 5]) / cn[p - 5] * 100 if cn[p - 5] > 0 else np.nan
            rows.append({
                "ticker": tk, "d0": idx[p].strftime("%Y-%m-%d"),
                "year": idx[p].year, "price": e,
                "gap": gap, "ret1": ret1, "ret3": ret3, "ret5": ret5,
                "runup5": runup, "rsi": rsi[p],
                "above25": bool(e > ma25[p]) if not np.isnan(ma25[p]) else None,
                "tov20": tov20[p], "prev_gap": last_gap,
            })
            last_gap = gap
    D = pd.DataFrame(rows)
    print(f"[collect] 決算イベント {len(D):,}件（{SINCE}〜{END}・上場/非ETF・株価データ突合済）\n")

    W = ["gap", "ret1", "ret3", "ret5"]

    print("=" * 100)
    print("  ① 無条件: 決算またぎの素の期待値（d0終値=100として）")
    print("=" * 100)
    print(f"  {'窓':<12}{HDR}")
    for w in W:
        print(f"  {w:<12}{stats(D[w])}")

    print("\n  ── 年別 gap（d0終値→翌寄り・持ち越しの核心） ──")
    print(f"  {'年':<12}{HDR}")
    for y in sorted(D['year'].unique()):
        print(f"  {y:<12}{stats(D[D['year'] == y]['gap'])}")

    print("\n" + "=" * 100)
    print("  ② トレンド条件: 25MA上（上昇基調で迎える決算） vs 25MA下")
    print("=" * 100)
    for lab, m in [("25MA上", D["above25"] == True), ("25MA下", D["above25"] == False)]:  # noqa: E712
        print(f"  【{lab}】")
        print(f"  {'窓':<12}{HDR}")
        for w in W:
            print(f"  {w:<12}{stats(D[m][w])}")

    print("\n" + "=" * 100)
    print("  ③ RSI帯別 gap")
    print("=" * 100)
    print(f"  {'RSI帯':<12}{HDR}")
    for lo, hi in [(0, 30), (30, 45), (45, 55), (55, 70), (70, 101)]:
        m = (D["rsi"] >= lo) & (D["rsi"] < hi)
        print(f"  {f'{lo}-{hi}':<12}{stats(D[m]['gap'])}")

    print("\n" + "=" * 100)
    print("  ④ 直前5日ランアップ別 gap（先回り買いの是非）")
    print("=" * 100)
    print(f"  {'runup5':<12}{HDR}")
    for lo, hi, lab in [(-99, -5, "<-5%"), (-5, -2, "-5〜-2"), (-2, 2, "-2〜+2"),
                        (2, 5, "+2〜+5"), (5, 999, ">+5%")]:
        m = (D["runup5"] >= lo) & (D["runup5"] < hi)
        print(f"  {lab:<12}{stats(D[m]['gap'])}")

    print("\n" + "=" * 100)
    print("  ⑤ 流動性帯別 gap（20日中央値売買代金）")
    print("=" * 100)
    print(f"  {'売買代金':<12}{HDR}")
    for lo, hi, lab in [(0, 1e8, "<1億"), (1e8, 1e9, "1-10億"),
                        (1e9, 5e9, "10-50億"), (5e9, 1e12, ">50億")]:
        m = (D["tov20"] >= lo) & (D["tov20"] < hi)
        print(f"  {lab:<12}{stats(D[m]['gap'])}")

    print("\n" + "=" * 100)
    print("  ⑥ 前回決算の反応別 gap（好決算の持続性）")
    print("=" * 100)
    print(f"  {'前回gap':<12}{HDR}")
    for lo, hi, lab in [(-99, -3, "<-3%"), (-3, 0, "-3〜0"), (0, 3, "0〜+3"), (3, 999, ">+3%")]:
        m = (D["prev_gap"] >= lo) & (D["prev_gap"] < hi)
        print(f"  {lab:<12}{stats(D[m]['gap'])}")

    # 複合の有望ゾーン素描: 25MA上×RSI55-70×前回gap>0×流動性10億+
    print("\n" + "=" * 100)
    print("  ⑦ 複合スケッチ（順張り×好決算持続×流動性）")
    print("=" * 100)
    combos = {
        "25MA上×前回gap>0": (D["above25"] == True) & (D["prev_gap"] > 0),  # noqa: E712
        "同上×代金10億+": (D["above25"] == True) & (D["prev_gap"] > 0) & (D["tov20"] >= 1e9),  # noqa: E712
        "同上×RSI55-70": (D["above25"] == True) & (D["prev_gap"] > 0) & (D["tov20"] >= 1e9)  # noqa: E712
                        & (D["rsi"] >= 55) & (D["rsi"] < 70),
    }
    for lab, m in combos.items():
        print(f"  【{lab}】")
        print(f"  {'窓':<12}{HDR}")
        for w in W:
            print(f"  {w:<12}{stats(D[m][w])}")
        g = D[m]
        yr = " ".join(f"{y}:{stats(g[g['year'] == y]['gap']).split()[0].strip()}件"
                      for y in sorted(g["year"].unique()))
        print(f"  年別件数: {yr}")

    D.to_csv("earnings_hold_events.csv", index=False)
    print(f"\n[save] earnings_hold_events.csv ({len(D):,}件)")


if __name__ == "__main__":
    main()

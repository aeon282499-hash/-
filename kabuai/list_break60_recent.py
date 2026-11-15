# -*- coding: utf-8 -*-
"""list_break60_recent.py — 直近N営業日の🚩初動ブレイク（買い）と、その鏡像（売り）を一覧で出す（2026-11-15）。

本人「直近3日の買いの初動ブレイクと売りの初動ブレイクだして」。
買い = アプリ(explorer_signals.break60_recent・v7.6.2)と同一判定:
  終値が60日高値を30営業日ぶりに初更新 × 陽線 × 出来高20日平均1.5倍以上 × 20日平均代金1億以上
  除外: 当日+20%以上 / 出来高10倍以上 / 60日高値を10%以上抜き。🆕=250日高値も更新（上位の型）
売り = 上の鏡像（★未検証・アプリに無い）:
  終値が60日安値を30営業日ぶりに初更新 × 陰線 × 出来高1.5倍以上 × 代金1億以上
  除外: 当日-20%以下 / 出来高10倍以上 / 60日安値を10%以上割れ。🆕=250日安値も更新
  ※ 貸借・売り禁はここでは判定しない（空売りできるかは証券会社で確認）
データ: J-Quants V2 直取得（build_data.load_jquants_api と同じ）。
実行: cd kabuai && KABUAI_API_LOOKBACK=300 python list_break60_recent.py [日数=3]
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

os.environ.setdefault("KABUAI_DATA_SOURCE", "jquants_api")
from build_data import load_jquants_api  # noqa: E402

N_DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
HB, FRESH, VOLX, TOV = 60, 30, 1.5, 1e8
MAX_CHG, MAX_VOLX, MAX_OVER = 20.0, 10.0, 10.0


def fresh_mask(ev: np.ndarray) -> np.ndarray:
    out = np.zeros(len(ev), bool)
    last = -10**9
    for i, e in enumerate(ev):
        if e:
            if i - last > FRESH:
                out[i] = True
            last = i
    return out


def scan(data: dict, names: dict, days: list) -> tuple[list, list]:
    buys, sells = [], []
    for tk, df in data.items():
        if len(df) < HB + FRESH + 25:
            continue
        o, c, v = (df[k].astype(float).to_numpy() for k in ("Open", "Close", "Volume"))
        cs = pd.Series(c)
        hi = cs.rolling(HB).max().shift(1).to_numpy()
        lo = cs.rolling(HB).min().shift(1).to_numpy()
        hi250 = cs.rolling(250, min_periods=200).max().shift(1).to_numpy()
        lo250 = cs.rolling(250, min_periods=200).min().shift(1).to_numpy()
        volx = v / (pd.Series(v).rolling(20).mean().shift(1).to_numpy() + 1e-9)
        tov = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        chg = np.r_[np.nan, c[1:] / c[:-1] - 1] * 100
        ok = (tov >= TOV) & (volx >= VOLX) & (volx < MAX_VOLX)
        b = fresh_mask(np.nan_to_num(c > hi, nan=False).astype(bool)) & (c > o) & ok \
            & (chg < MAX_CHG) & ((c / hi - 1) * 100 < MAX_OVER)
        s = fresh_mask(np.nan_to_num(c < lo, nan=False).astype(bool)) & (c < o) & ok \
            & (chg > -MAX_CHG) & ((1 - c / lo) * 100 < MAX_OVER)
        idx = df.index.strftime("%Y-%m-%d")
        code = tk.replace(".T", "")
        nm = names.get(code, code)
        for i in range(len(df)):
            if idx[i] not in days:
                continue
            row = {"date": idx[i], "code": code, "name": nm, "close": c[i], "chg": chg[i],
                   "volx": volx[i], "tov_oku": tov[i] / 1e8}
            if b[i]:
                buys.append({**row, "new": bool(np.isfinite(hi250[i]) and c[i] > hi250[i]),
                             "over": (c[i] / hi[i] - 1) * 100})
            if s[i]:
                sells.append({**row, "new": bool(np.isfinite(lo250[i]) and c[i] < lo250[i]),
                              "over": (1 - c[i] / lo[i]) * 100})
    return buys, sells


def show(title: str, rows: list, lab_over: str) -> None:
    print(f"\n{'=' * 8} {title} {len(rows)}件 {'=' * 8}")
    for d in sorted({r["date"] for r in rows}, reverse=True):
        g = sorted([r for r in rows if r["date"] == d], key=lambda r: (not r["new"], -r["volx"]))
        print(f"\n--- {d}（{len(g)}件）---")
        print("| | コード | 銘柄 | 終値 | 前日比 | 出来高倍率 | 代金20日平均 | " + lab_over + " |")
        print("|---|---|---|---|---|---|---|---|")
        for r in g:
            print(f"| {'🆕' if r['new'] else ''} | {r['code']} | {r['name']} | {r['close']:,.0f} | "
                  f"{r['chg']:+.1f}% | ×{r['volx']:.1f} | {r['tov_oku']:.1f}億 | {r['over']:.1f}% |")


def main() -> None:
    data, names, data_date, _ = load_jquants_api()
    all_days = sorted({d.strftime("%Y-%m-%d") for df in data.values() for d in df.index})
    days = set(all_days[-N_DAYS:])
    print(f"[list] データ最新日 {data_date} / 対象 {sorted(days)}")
    buys, sells = scan(data, names, days)
    show("🚩買いの初動ブレイク（アプリと同一判定）", buys, "60日高値からの抜け幅")
    show("🔻売りの初動ブレイク（鏡像・未検証）", sells, "60日安値からの割れ幅")


if __name__ == "__main__":
    main()

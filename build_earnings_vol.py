# -*- coding: utf-8 -*-
"""build_earnings_vol.py — 銘柄ごとの「決算ボラ」を作って earnings_vol.json に保存する（2026-07-31）。

決算ボラ = その銘柄の過去の決算翌寄りギャップ（開示日終値 → 翌営業日始値）の
           絶対値の中央値。「その銘柄が決算でどれだけ動く体質か」を表す。

なぜ事前生成なのか:
  main_earnings_hold.py の価格取得窓は HIST_DAYS=60（暦日）しかなく、実行時に
  過去12回分の決算ギャップは計算できない。14:55起動で大引けまでに配信する必要も
  あるので実行時の重い取得は不可。earnings_calendar.json と同じく事前生成＋同梱にする。

point-in-time について:
  本ファイルは「過去の開示」だけから作る。将来の決算は使わない。
  実行時は最新版を読むだけなので、その時点から見て未来の情報は入らない。
  （BT側 _bt_earnings_vol_axis.py は expanding median で同じものを再現している）

データ源:
  ローカルに jquants_cache*.pkl があればそれを使う（速い）。
  無ければ J-Quants から必要期間を取得する（CI用・遅い）。

実行: python -X utf8 build_earnings_vol.py [--years 5] [--out earnings_vol.json]
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

CALENDARS = ("earnings_calendar_2016_2021.json", "earnings_calendar.json")
LOCAL_CACHES = ("jquants_cache_2016_2021.pkl", "jquants_cache.pkl")
MIN_EVENTS = 3          # これ未満なら「不明」＝本番ではフェイルオープン（買う）
MAX_ABS_GAP = 30.0      # 株式分割などの異常値を弾く（BTと同一のガード）
GAP_MAX_DAYS = 10       # 「翌営業日」とみなせる暦日の上限（価格の穴をまたがない）


def load_disclosure_dates(until: str) -> dict[str, list[str]]:
    """{ticker: [過去の開示日...]}。until より後の日付（予測含む）は捨てる。"""
    out: dict[str, set[str]] = {}
    for path in CALENDARS:
        if not os.path.exists(path):
            print(f"[vol] {path} が無い → スキップ")
            continue
        with open(path, encoding="utf-8") as f:
            cal = json.load(f)
        for tk, dates in cal.items():
            for d in dates:
                d = str(d)
                if d <= until:
                    out.setdefault(tk, set()).add(d)
    return {k: sorted(v) for k, v in out.items()}


def load_prices_local() -> dict[str, pd.DataFrame] | None:
    frames: dict[str, list[pd.DataFrame]] = {}
    found = False
    for path in LOCAL_CACHES:
        if not os.path.exists(path):
            continue
        found = True
        print(f"[vol] ローカルキャッシュ {path} を読み込み中…")
        blob = pickle.load(open(path, "rb"))
        for tk, df in blob["all_data"].items():
            if df is not None and len(df):
                frames.setdefault(tk, []).append(df)
        del blob
    if not found:
        return None
    out = {}
    for tk, fs in frames.items():
        d = pd.concat(fs).sort_index()
        out[tk] = d[~d.index.duplicated(keep="last")]
    print(f"[vol] ローカル {len(out):,}銘柄")
    return out


def load_prices_jquants(start: str, end: str) -> dict[str, pd.DataFrame]:
    from screener import _jquants_id_token, batch_download_jquants
    print(f"[vol] J-Quantsから取得 {start}〜{end}（CI用・時間がかかる）")
    return batch_download_jquants(_jquants_id_token(), start=start, end=end)


def earnings_vol(df: pd.DataFrame, disc_dates: list[str]) -> tuple[float | None, int]:
    """開示日終値 → 翌営業日始値 のギャップ絶対値の中央値と、使えた回数。"""
    if df is None or len(df) < 5:
        return None, 0
    idx = [str(i)[:10] for i in df.index]
    pos = {d: i for i, d in enumerate(idx)}
    closes = df["Close"].astype(float).to_numpy()
    opens = df["Open"].astype(float).to_numpy()
    gaps: list[float] = []
    for d in disc_dates:
        i = pos.get(d)
        if i is None:                       # 開示日が非営業日なら直前営業日に寄せる
            prior = [k for k, x in enumerate(idx) if x < d]
            if not prior:
                continue
            i = prior[-1]
        j = i + 1
        if j >= len(idx):
            continue
        # 価格データの穴をまたいで «翌営業日» を誤認しないためのガード
        d0 = datetime.strptime(idx[i], "%Y-%m-%d").date()
        d1 = datetime.strptime(idx[j], "%Y-%m-%d").date()
        if (d1 - d0).days > GAP_MAX_DAYS:
            continue
        c, o = closes[i], opens[j]
        if not (c > 0 and o > 0) or np.isnan(c) or np.isnan(o):
            continue
        g = (o - c) / c * 100
        if abs(g) > MAX_ABS_GAP:            # 分割・併合などの異常値
            continue
        gaps.append(abs(g))
    if len(gaps) < MIN_EVENTS:
        return None, len(gaps)
    return float(np.median(gaps)), len(gaps)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5, help="何年ぶんの決算を使うか")
    ap.add_argument("--out", default="earnings_vol.json")
    args = ap.parse_args()

    today = date.today()
    until = today.strftime("%Y-%m-%d")
    since = (today - timedelta(days=int(365.25 * args.years))).strftime("%Y-%m-%d")

    disc = load_disclosure_dates(until)
    disc = {tk: [d for d in ds if d >= since] for tk, ds in disc.items()}
    disc = {tk: ds for tk, ds in disc.items() if ds}
    print(f"[vol] 開示履歴: {len(disc):,}銘柄 / {since}〜{until}")

    prices = load_prices_local()
    if prices is None:
        prices = load_prices_jquants(since, until)

    out: dict[str, dict] = {}
    skipped = 0
    for tk, ds in disc.items():
        v, n = earnings_vol(prices.get(tk), ds)
        if v is None:
            skipped += 1
            continue
        out[tk] = {"vol": round(v, 2), "n": n}

    blob = {"built": until, "since": since, "min_events": MIN_EVENTS, "vol": out}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(blob, f, ensure_ascii=False, indent=1)

    vals = np.array([x["vol"] for x in out.values()])
    print(f"[vol] {args.out} を出力: {len(out):,}銘柄"
          f"（実績不足でスキップ {skipped:,}銘柄＝本番ではフェイルオープン）")
    print(f"[vol] 中央値{np.median(vals):.2f}% / 2.0%以上が{(vals >= 2.0).mean()*100:.0f}%"
          f" / 分位 25%={np.percentile(vals,25):.2f} 75%={np.percentile(vals,75):.2f}")


if __name__ == "__main__":
    main()

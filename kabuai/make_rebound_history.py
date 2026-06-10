"""make_rebound_history.py — 全面リバウンド日の実績ヒストリーを事前計算して JSON 化。

手元の親 jquants_cache.pkl（CIには無い長期履歴）から、営業日ごとの
「強反転点灯数（流動性1億円フィルタ後）」を集計し、広がり日（しきい値10件以上）の
一覧と、その日の強反転バスケットの実現リターン（翌朝寄りentry・出口8日/-12%・利確なし）
を rebound_history.json に書き出す。build_data.py がビルド時に存在すれば
latest.json の rebound.history に注入する（無ければ単に出ない・非致命）。

トラックレコード（make_track_longterm.py）と同じ運用: 緩変動の統計なので
月1回の手動再実行→コミットで十分。
実行: python make_rebound_history.py
"""
from __future__ import annotations

import json
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from signal_track import _rolling
from momentum import MIN_HISTORY

EXIT_T = 8
EXIT_SL = 0.12
TURNOVER_MIN = 1e8          # アプリのユニバースと同じ流動性
THRESHOLD = 10              # 広がり日のしきい値（build_data.REBOUND_B と揃える）
SINCE = "2023-01-01"        # track_longterm と同じ近年窓
RECENT_DAYS = 14            # JSON に詳細を残す直近の広がり日数
OUT = Path(__file__).resolve().parent / "rebound_history.json"


def main():
    pkl = Path(__file__).resolve().parent.parent / "jquants_cache.pkl"
    c = pickle.load(open(pkl, "rb"))
    data = c["all_data"]
    since_ts = pd.Timestamp(SINCE)

    frames = []
    nn = 0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + 2:
            continue
        df = df.sort_index()
        R = _rolling(df)
        close = R["close"]
        low = df["Low"].astype(float)
        openp = df["Open"].astype(float)
        vol = df["Volume"].astype(float)
        r1 = R["r1"]; r5 = R["r5"]; r20 = R["r20"]; rsi = R["rsi"]

        sr = (r20 < -10) & (r5 >= 4.0) & (r1 > 0) & (rsi < 55)
        tov = (close * vol).rolling(20).mean()
        pos = np.arange(len(close))
        m = sr & (tov >= TURNOVER_MIN) & pd.Series(pos >= MIN_HISTORY - 1, index=close.index)
        m &= close.index >= since_ts
        if not m.any():
            continue
        nn += 1

        # 出口リターン（翌朝寄りentry）。窓が足りない直近分は NaN のまま残す（pending扱い）
        o1 = openp.shift(-1)
        winmin = low.rolling(EXIT_T).min().shift(-EXIT_T)
        c8 = close.shift(-EXIT_T)
        stop = o1 * (1.0 - EXIT_SL)
        ret = np.where(winmin <= stop, -EXIT_SL, c8 / o1 - 1.0)
        ret = np.where(o1.notna() & (o1 > 0) & winmin.notna() & c8.notna(), ret, np.nan)

        frames.append(pd.DataFrame({"date": close.index[m.to_numpy()],
                                    "ret": pd.Series(ret, index=close.index)[m].to_numpy()}))

    D = pd.concat(frames, ignore_index=True)
    g = D.groupby("date")["ret"]
    days = pd.DataFrame({"n": g.size(), "n_ret": g.count(), "avg": g.mean(), "win": g.apply(lambda x: (x.dropna() > 0).mean())})
    broad = days[days["n"] >= THRESHOLD].sort_index()
    print(f"集約: {nn}銘柄  {SINCE}以降の営業日 {len(days)}  広がり日({THRESHOLD}件以上) {len(broad)}")

    # サマリ（出口が確定している広がり日のみ）
    settled = broad[broad["n_ret"] >= broad["n"] * 0.5]
    all_rets = D[D["date"].isin(settled.index)]["ret"].dropna()
    day_avgs = settled["avg"].dropna()
    summary = {
        "days": int(len(broad)),
        "days_settled": int(len(settled)),
        "trades": int(all_rets.size),
        "win": round(float((all_rets > 0).mean() * 100), 1),
        "avg": round(float(all_rets.mean() * 100), 2),
        "day_pos_pct": round(float((day_avgs > 0).mean() * 100), 1),  # 日単位でバスケットがプラスだった割合
    }

    recent = []
    for d, row in broad.tail(RECENT_DAYS).iterrows():
        pending = row["n_ret"] < row["n"] * 0.5
        recent.append({
            "date": d.strftime("%Y-%m-%d"),
            "count": int(row["n"]),
            "avg": None if pending or pd.isna(row["avg"]) else round(float(row["avg"] * 100), 2),
            "win": None if pending or pd.isna(row["win"]) else round(float(row["win"] * 100), 1),
        })

    out = {
        "generated": datetime.now().strftime("%Y-%m-%d"),
        "since": SINCE,
        "threshold": THRESHOLD,
        "exit": {"t": EXIT_T, "sl": int(EXIT_SL * 100)},
        "note": ("広がり日=強反転が当日10件以上点灯した営業日。実績はその日の強反転全銘柄を翌朝寄りで買い、"
                 "保有8日・損切り-12%・利確なしで出した場合の理論値（手数料/スリッページ未考慮）。月1回更新。"),
        "summary": summary,
        "recent": recent,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"書き出し: {OUT.name}  summary={summary}")
    for r in recent[-6:]:
        print(f"  {r['date']} 点灯{r['count']:>3}件  平均{('%+.2f%%' % r['avg']) if r['avg'] is not None else '(集計中)'}"
              f"  勝率{('%.1f%%' % r['win']) if r['win'] is not None else '-'}")


if __name__ == "__main__":
    main()

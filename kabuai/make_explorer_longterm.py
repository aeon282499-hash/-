"""
make_explorer_longterm.py — 上昇ランキング用の長期初動イベントを手元pklから事前計算

CI は直近約200営業日しか取得できず、初動判定には135本の履歴が要るため、
CI 単独では「直近約65日」のイベントしか検出できない。1年分のランキングは
手元の親 jquants_cache.pkl から事前計算した explorer_longterm.json をコミットして
build 時にマージする（track_longterm.json / rebound_history.json と同じ月1運用）。

実行: 親pklを cache_jquants_update.py で最新化 → python make_explorer_longterm.py → コミット
"""
from __future__ import annotations

import json
import pickle
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import explorer_signals as ex

HERE = Path(__file__).resolve().parent
LOOKBACK_DAYS = 400          # 1年ランキング(366日)+余裕

def main():
    t0 = time.time()
    cfg = ex.load_config()
    with open(HERE.parent / "jquants_cache.pkl", "rb") as f:
        c = pickle.load(f)
    data = c["all_data"]
    name_map = c.get("name_map", {}) or {}
    end = str(c.get("end", ""))
    since = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    def name_of(code):
        for k in (code + ".T", code):
            if k in name_map:
                return str(name_map[k])
        return code

    events = []
    n_scan = 0
    for ticker, df in data.items():
        if df is None or len(df) < 140:
            continue
        code = ticker.replace(".T", "")
        try:
            if not ex._turnover_ok(df, float(cfg["shodo"]["min_turnover"])):
                continue
            n_scan += 1
            for e in ex.shodo_events(df, cfg):
                if e["date"] < since:
                    continue
                seg = df[df.index >= pd.Timestamp(e["date"])]
                events.append({"code": code, "name": name_of(code),
                               "date": e["date"], "price": e["price"],
                               "max_high": round(float(seg["High"].max()), 1)})
        except Exception:
            continue

    events.sort(key=lambda x: x["date"])
    out = {"generated": end, "since": since, "n_scanned": n_scan, "events": events}
    with open(HERE / "explorer_longterm.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    from collections import Counter
    bym = Counter(e["date"][:7] for e in events)
    print(f"[longterm] {since}〜{end}: 初動イベント {len(events)}件 / 走査{n_scan}銘柄 / {time.time()-t0:.0f}s")
    print(f"[longterm] 月別: {dict(sorted(bym.items()))}")
    print(f"[longterm] 出力: explorer_longterm.json")

if __name__ == "__main__":
    main()

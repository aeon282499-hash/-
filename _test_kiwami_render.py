# -*- coding: utf-8 -*-
import json, os
from datetime import date
from dotenv import load_dotenv
load_dotenv()
import shadow_exit as SE

captured = []
SE._shadow_post = lambda embeds, env=None: (captured.extend(embeds), True)[1]

today = date(2026, 8, 12)
json.dump({"date": "2026-08-12", "signals": [
    {"ticker": "9999.T", "name": "テスト共通A", "direction": "BUY", "prev_close": 1000.0, "limit_price": 1010,
     "rsi": 38.2, "deviation": -2.4, "vol_ratio": 2.1, "range_ratio": 1.6, "turnover": 4.5e9, "days_cover": 0.5},
    {"ticker": "8888.T", "name": "テスト共通B", "direction": "BUY", "prev_close": 2000.0, "limit_price": 2020,
     "rsi": 41.0, "deviation": -3.0, "vol_ratio": 1.2, "range_ratio": 1.8, "turnover": 2.2e9, "days_cover": 0.7},
    {"ticker": "7777.T", "name": "テスト帯オンリー", "direction": "BUY", "prev_close": 500.0, "limit_price": 505,
     "rsi": 36.0, "deviation": -2.0, "vol_ratio": 2.5, "range_ratio": 1.4, "turnover": 1.5e9, "days_cover": 1.1},
]}, open("_test_kiwami_sig.json", "w", encoding="utf-8"), ensure_ascii=False)
SE.KIWAMI_SIG_FILE = "_test_kiwami_sig.json"

SE.load_ledger = lambda k: [
    {"ticker": "9999.T", "signal_date": "2026-08-12", "status": "pending"},
    {"ticker": "7777.T", "signal_date": "2026-08-12", "status": "pending"},
    {"ticker": "5555.T", "signal_date": "2026-08-05", "status": "open"},
] if k == "main" else []
SE._live_closed = lambda k: {}
SE.load_sell_ledger = lambda: []
SE.TIER_FILES["main"] = ("_no_such_file.json",) + SE.TIER_FILES["main"][1:]

SE.send_discord(today)
e = captured[0]
print(e["title"]); print(); print(e["description"]); print(); print("footer:", e.get("footer", {}).get("text", "(なし)"))
os.remove("_test_kiwami_sig.json")

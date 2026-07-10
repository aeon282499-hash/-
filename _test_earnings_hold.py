# -*- coding: utf-8 -*-
"""_test_earnings_hold.py — 決算持ち越しシグナルのユニットテスト。
実行: python -X utf8 _test_earnings_hold.py
"""
from __future__ import annotations

import json
import os
from datetime import date

import numpy as np
import pandas as pd

import main_earnings_hold as m

PASS = 0
FAIL = 0


def check(label, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f"  NG  {label}")


print("── rule_pass 境界 ──")
check("基準ケース通過", m.rule_pass(45.0, -3.1, 1e9, 5000))
check("RSI45.1で除外", not m.rule_pass(45.1, -3.1, 1e9, 5000))
check("runup-3.0ちょうどは除外(<厳密)", not m.rule_pass(40, -3.0, 1e9, 4000))
check("runup-2.9で除外", not m.rule_pass(40, -2.9, 1e9, 4000))
check("代金9.9億で除外", not m.rule_pass(40, -5, 0.99e9, 4000))
check("株価5001円で除外", not m.rule_pass(40, -5, 1e9, 5001))
check("NaNはFalse", not m.rule_pass(float("nan"), -5, 1e9, 4000))
check("NoneはFalse", not m.rule_pass(None, -5, 1e9, 4000))

print("── calc_shares ──")
check("2340円→200株", m.calc_shares(2340) == 200)
check("4999円→100株(最低単元)", m.calc_shares(4999) == 100)
check("500円→1000株", m.calc_shares(500) == 1000)
check("100株未満に切り下がらない", m.calc_shares(4999) >= 100)

print("── next_trading_day ──")
check("金曜→月曜", m.next_trading_day(date(2026, 7, 10)) == date(2026, 7, 13))
check("祝前日→祝明け(海の日7/20月)", m.next_trading_day(date(2026, 7, 17)) == date(2026, 7, 21))
check("大晦日スキップ", m.next_trading_day(date(2026, 12, 30)) == date(2027, 1, 4))

print("── settle_pendings ──")
idx = pd.to_datetime(["2026-07-09", "2026-07-10"])
fake_df = pd.DataFrame({"Open": [1000.0, 1050.0], "Close": [1020.0, 1040.0],
                        "Volume": [1e6, 1e6]}, index=idx)
store = {"last_signal_date": "2026-07-09", "positions": [
    {"ticker": "9999.T", "name": "テスト", "date": "2026-07-09",
     "shares": 400, "status": "pending"},
]}
settled = m.settle_pendings(store, date(2026, 7, 10), {"9999.T": fake_df})
p = store["positions"][0]
check("closed化", p["status"] == "closed")
check("entry=シグナル日終値1020", p["entry"] == 1020.0)
check("exit=翌営業日寄り1050", p["exit"] == 1050.0)
check("pnl_pct=+2.94", abs(p["pnl_pct"] - 2.94) < 0.01)
check("pnl_yen=+12000(400株)", p["pnl_yen"] == 12000)
check("settled明細1件", len(settled) == 1)

# 決済日未到来はpending維持
store2 = {"positions": [{"ticker": "9999.T", "name": "t", "date": "2026-07-10",
                         "shares": 100, "status": "pending"}]}
settled2 = m.settle_pendings(store2, date(2026, 7, 10), {"9999.T": fake_df})
check("決済日未到来はpending維持", store2["positions"][0]["status"] == "pending" and not settled2)

# closedは再処理しない
store3 = {"positions": [{"ticker": "9999.T", "name": "t", "date": "2026-07-09",
                         "shares": 100, "status": "closed", "pnl_yen": 1}]}
settled3 = m.settle_pendings(store3, date(2026, 7, 10), {"9999.T": fake_df})
check("closedは再処理しない", not settled3)

print("── embeds ──")
e = m.embed_signals([], 42, date(2026, 7, 10))
check("対象なしembed", "対象なし" in e["title"] and "42件" in e["description"])
picks = [{"ticker": "1234.T", "code": "1234", "name": "サンプル", "type": "本決算",
          "price": 2340.0, "rsi": 32.5, "runup5": -6.2, "tov20": 2e9}]
e2 = m.embed_signals(picks, 42, date(2026, 7, 10))
check("買いリストembedに株数", "200株" in e2["description"])
check("footerに注意書き", "STOP無効" in e2["footer"]["text"])
er = m.embed_results([{"ticker": "1234.T", "name": "サンプル", "entry": 1020.0,
                       "exit": 1050.0, "pnl_pct": 2.94, "pnl_yen": 12000}])
check("結果embedに合計", "+12,000円" in er["description"])
check("勝敗カウント", "1勝0敗" in er["title"])

print(f"\n{'=' * 40}\nPASS {PASS} / FAIL {FAIL}")
if FAIL:
    raise SystemExit(1)

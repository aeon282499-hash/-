# -*- coding: utf-8 -*-
"""_test_earnings_hold.py — 決算持ち越しシグナル（3階層版）のユニットテスト。
実行: python -X utf8 _test_earnings_hold.py
"""
from __future__ import annotations

from datetime import date

import pandas as pd

import main_earnings_hold as m

PASS = 0
FAIL = 0

TIER_L = m.TIERS[0]  # 大100万
TIER_M = m.TIERS[1]  # 中50万
TIER_S = m.TIERS[2]  # 小30万


def check(label, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f"  NG  {label}")


print("── TIERS構成 ──")
check("3階層", len(m.TIERS) == 3)
check("大=100万/1万円", TIER_L["size"] == 1_000_000 and TIER_L["price_cap"] == 10000)
check("中=50万/5千円", TIER_M["size"] == 500_000 and TIER_M["price_cap"] == 5000)
check("小=30万/3千円", TIER_S["size"] == 300_000 and TIER_S["price_cap"] == 3000)
check("webhook環境変数が階層別", len({t["webhook_env"] for t in m.TIERS}) == 3)
check("positionsファイルが階層別", len({t["positions_file"] for t in m.TIERS}) == 3)

print("── rule_pass 境界 ──")
check("基準ケース通過(中)", m.rule_pass(45.0, -3.1, 1e9, 5000, 5000))
check("RSI45.1で除外", not m.rule_pass(45.1, -3.1, 1e9, 5000, 5000))
check("runup-3.0ちょうどは除外(<厳密)", not m.rule_pass(40, -3.0, 1e9, 4000, 5000))
check("代金9.9億で除外", not m.rule_pass(40, -5, 0.99e9, 4000, 5000))
check("中: 株価5001円で除外", not m.rule_pass(40, -5, 1e9, 5001, 5000))
check("大: 株価9999円は通過", m.rule_pass(40, -5, 1e9, 9999, 10000))
check("大: 株価10001円で除外", not m.rule_pass(40, -5, 1e9, 10001, 10000))
check("小: 株価3001円で除外", not m.rule_pass(40, -5, 1e9, 3001, 3000))
check("NaNはFalse", not m.rule_pass(float("nan"), -5, 1e9, 4000, 5000))
check("NoneはFalse", not m.rule_pass(None, -5, 1e9, 4000, 5000))

print("── calc_shares（階層別） ──")
check("中: 2340円→200株", m.calc_shares(2340, 500_000) == 200)
check("大: 2340円→400株", m.calc_shares(2340, 1_000_000) == 400)
check("小: 2340円→100株", m.calc_shares(2340, 300_000) == 100)
check("中: 4999円→100株(最低単元)", m.calc_shares(4999, 500_000) == 100)
check("大: 9999円→100株(最低単元)", m.calc_shares(9999, 1_000_000) == 100)

print("── next_trading_day ──")
check("金曜→月曜", m.next_trading_day(date(2026, 7, 10)) == date(2026, 7, 13))
check("祝前日→祝明け(海の日7/20月)", m.next_trading_day(date(2026, 7, 17)) == date(2026, 7, 21))
check("大晦日スキップ", m.next_trading_day(date(2026, 12, 30)) == date(2027, 1, 4))

print("── time_bucket / last_disc_time ──")
check("15:30は引け後", m.time_bucket("15:30:00") == "引け後")
check("11:30は場中", m.time_bucket("11:30:00") == "場中")
check("08:30は寄り前", m.time_bucket("08:30:00") == "寄り前")
check("Noneは履歴なし", m.time_bucket(None) == "履歴なし")
tms = {"1234.T": {"2026-04-10": "11:30:00", "2026-07-10": "15:00:00"}}
check("直近の過去時刻を返す", m.last_disc_time(tms, "1234.T", "2026-07-10") == "11:30:00")
check("履歴なしはNone", m.last_disc_time(tms, "9999.T", "2026-07-10") is None)

print("── settle_pendings（通常決済） ──")
idx = pd.to_datetime(["2026-07-09", "2026-07-10"])
fake_df = pd.DataFrame({"Open": [1000.0, 1050.0], "Close": [1020.0, 1040.0],
                        "Volume": [1e6, 1e6]}, index=idx)
store = {"last_signal_date": "2026-07-09", "positions": [
    {"ticker": "9999.T", "name": "テスト", "date": "2026-07-09",
     "shares": 400, "status": "pending"},
]}
settled, ext = m.settle_pendings(store, date(2026, 7, 10), {"9999.T": fake_df}, 500_000)
p = store["positions"][0]
check("closed化", p["status"] == "closed")
check("entry=シグナル日終値1020", p["entry"] == 1020.0)
check("exit=翌営業日寄り1050", p["exit"] == 1050.0)
check("pnl_pct=+2.94", abs(p["pnl_pct"] - 2.94) < 0.01)
check("pnl_yen=+12000(400株)", p["pnl_yen"] == 12000)
check("settled明細1件・延長0件", len(settled) == 1 and not ext)
check("exit_kind=翌寄り", p["exit_kind"] == "翌寄り")
check("gap+2.9%は延長しない(閾値8%)", p["status"] == "closed")

store2 = {"positions": [{"ticker": "9999.T", "name": "t", "date": "2026-07-10",
                         "shares": 100, "status": "pending"}]}
settled2, _ = m.settle_pendings(store2, date(2026, 7, 10), {"9999.T": fake_df}, 500_000)
check("決済日未到来はpending維持", store2["positions"][0]["status"] == "pending" and not settled2)

store3 = {"positions": [{"ticker": "9999.T", "name": "t", "date": "2026-07-09",
                         "shares": 100, "status": "closed", "pnl_yen": 1}]}
settled3, _ = m.settle_pendings(store3, date(2026, 7, 10), {"9999.T": fake_df}, 500_000)
check("closedは再処理しない", not settled3)

print("── PEAD延長 ──")
check("nth_trading_day: 金7/10+5営業日=7/17", m.nth_trading_day(date(2026, 7, 10), 5) == date(2026, 7, 17))
# 寄り+10% > 閾値8% → extended昇格
fake_big = pd.DataFrame({"Open": [1000.0, 1122.0], "Close": [1020.0, 1100.0],
                         "Volume": [1e6, 1e6]}, index=idx)
store4 = {"positions": [{"ticker": "8888.T", "name": "爆勝ち", "date": "2026-07-09",
                         "shares": 400, "status": "pending"}]}
settled4, ext4 = m.settle_pendings(store4, date(2026, 7, 10), {"8888.T": fake_big}, 500_000)
p4 = store4["positions"][0]
check("gap+10%はextendedに昇格", p4["status"] == "extended" and len(ext4) == 1 and not settled4)
check("gap_pct=+10.0", abs(p4["gap_pct"] - 10.0) < 0.01)
check("ext_exit_date=7/9+5営業日=7/16", p4["ext_exit_date"] == "2026-07-16")

# 延長分の決済: 売却日(7/16)の翌営業日(7/17)に終値1200で記帳
idx2 = pd.to_datetime(["2026-07-16", "2026-07-17"])
fake_ext = pd.DataFrame({"Open": [1150.0, 1190.0], "Close": [1200.0, 1210.0],
                         "Volume": [1e6, 1e6]}, index=idx2)
settled5, _ = m.settle_pendings(store4, date(2026, 7, 16), {"8888.T": fake_ext}, 500_000)
check("売却日当日はまだ決済しない(終値未確定)", p4["status"] == "extended" and not settled5)
settled6, _ = m.settle_pendings(store4, date(2026, 7, 17), {"8888.T": fake_ext}, 500_000)
check("翌日に売却日終値1200で決済", p4["status"] == "closed" and p4["exit"] == 1200.0)
check("exit_kind=PEAD延長", p4["exit_kind"] == "PEAD延長")
check("延長pnl=+72000円(1020→1200×400株)", p4["pnl_yen"] == 72000)

ee = m.embed_extended([{"ticker": "8888.T", "name": "爆勝ち", "gap_pct": 10.0,
                        "ext_exit_date": "2026-07-16"}], TIER_M)
check("延長embedに売却日", "2026-07-16 大引けで売却" in ee["description"])
ex = m.embed_ext_exit_today([{"ticker": "8888.T", "name": "爆勝ち", "shares": 400,
                              "date": "2026-07-09"}], TIER_M)
check("売却日embedに大引け成行指示", "大引け成行" in ex["description"])
err = m.embed_reminder([{"ticker": "8888.T", "name": "爆勝ち", "shares": 400,
                         "date": "2026-07-09", "prev_close": 1000.0}], TIER_M)
check("リマインダーにホールド判定ライン1,080円", "1,080円" in err["description"])

print("── embeds（階層ラベル） ──")
e = m.embed_signals([], 42, date(2026, 7, 10), TIER_M)
check("対象なしembed", "対象なし" in e["title"] and "42件" in e["description"])
check("階層ラベル【中資金】", "【中資金】" in e["title"])
picks = [{"ticker": "1234.T", "code": "1234", "name": "サンプル", "type": "本決算",
          "price": 2340.0, "rsi": 32.5, "runup5": -6.2, "tov20": 2e9,
          "last_time": "15:30", "last_bucket": "引け後"}]
e2 = m.embed_signals(picks, 42, date(2026, 7, 10), TIER_M)
check("中: 200株表示", "200株" in e2["description"])
e2L = m.embed_signals(picks, 42, date(2026, 7, 10), TIER_L)
check("大: 400株表示", "400株" in e2L["description"])
check("大: footerに8枠×100万", "8枠×100万" in e2L["footer"]["text"])
check("footerに注意書き", "STOP無効" in e2["footer"]["text"])
check("前回発表時刻の表示", "前回発表 15:30" in e2["description"])
picks[0].update({"last_time": "11:30", "last_bucket": "場中"})
e3 = m.embed_signals(picks, 42, date(2026, 7, 10), TIER_M)
check("場中型は⚠️つき", "⚠️ 前回発表 11:30" in e3["description"])
er = m.embed_results([{"ticker": "1234.T", "name": "サンプル", "entry": 1020.0,
                       "exit": 1050.0, "pnl_pct": 2.94, "pnl_yen": 12000}], TIER_S)
check("結果embedに合計", "+12,000円" in er["description"])
check("結果embedに階層ラベル", "【小資金】" in er["title"])
check("勝敗カウント", "1勝0敗" in er["title"])

print("── リマインダー ──")
er2 = m.embed_reminder(
    [{"ticker": "1234.T", "name": "サンプル", "shares": 200, "date": "2026-07-09"}],
    TIER_M)
check("リマインダーtitleに件数と階層", "1件" in er2["title"] and "【中資金】" in er2["title"])
check("リマインダーに株数と買い日", "200株" in er2["description"] and "2026-07-09買い" in er2["description"])
check("寄り成行の指示", "寄り成行" in er2["description"])

print("── 週次 ──")
check("金曜7/10は週最終営業日", m.is_week_last_trading_day(date(2026, 7, 10)))
check("水曜7/8は週最終でない", not m.is_week_last_trading_day(date(2026, 7, 8)))
check("祝前日の木曜7/16は週最終(金曜が海の日でない→7/17金は平日)",
      not m.is_week_last_trading_day(date(2026, 7, 16)))
check("金曜7/17は週最終(翌営業日=7/21火は別週)", m.is_week_last_trading_day(date(2026, 7, 17)))
wstore = {"positions": [
    {"ticker": "1.T", "name": "A", "status": "closed", "exit_date": "2026-07-08",
     "pnl_pct": 2.0, "pnl_yen": 10000},
    {"ticker": "2.T", "name": "B", "status": "closed", "exit_date": "2026-07-10",
     "pnl_pct": -1.0, "pnl_yen": -5000},
    {"ticker": "3.T", "name": "C", "status": "closed", "exit_date": "2026-07-03",
     "pnl_pct": 3.0, "pnl_yen": 15000},  # 先週分=今週集計外・通算には入る
    {"ticker": "4.T", "name": "D", "status": "pending", "date": "2026-07-10"},
]}
ew = m.embed_weekly(wstore, date(2026, 7, 10), TIER_M)
check("週次: 今週2件のみ集計", "決済 2件" in ew["description"])
check("週次: 週間+5,000円", "+5,000円" in ew["description"])
check("週次: 通算3件+20,000円", "3件" in ew["description"] and "+20,000円" in ew["description"])
check("週次: 0件週も生成", "0件" in m.embed_weekly({"positions": []}, date(2026, 7, 10), TIER_S)["description"])

print(f"\n{'=' * 40}\nPASS {PASS} / FAIL {FAIL}")
if FAIL:
    raise SystemExit(1)

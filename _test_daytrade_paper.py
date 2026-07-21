# -*- coding: utf-8 -*-
"""_test_daytrade_paper.py — 紙トレ台帳の純ロジック検証（外部I/Oなし）。"""
from datetime import date
import pandas as pd

import daytrade_paper as dp

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"  NG  {name}")


def mkdf(rows):
    """rows = [(datestr, open, high, low, close, vol), ...] → DataFrame(index=Date)."""
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame({
        "Open":  [r[1] for r in rows],
        "High":  [r[2] for r in rows],
        "Low":   [r[3] for r in rows],
        "Close": [r[4] for r in rows],
        "Volume":[r[5] for r in rows],
    }, index=idx)


def base_book(pos):
    return {"positions": list(pos), "expired": [], "last_report_date": None}


# ---------------------------------------------------------------- shortability
def test_shortability():
    iss = {"1234": "2", "5678": "1"}
    check("貸借○ (IssType=2)", dp.shortability("1234.T", iss)["mark"] == "○")
    check("信用× (IssType=1)", dp.shortability("5678.T", iss)["mark"] == "×")
    check("不明? (無し)",       dp.shortability("9999.T", iss)["mark"] == "?")
    check("英字コード4桁化",     dp.shortability("464A.T", {"464A": "2"})["mark"] == "○")


# ---------------------------------------------------------------- settle BUY
def test_settle_buy_win():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-13", "basis_date": "2026-07-13",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    closed = dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("BUY勝ち: CLOSE", p["exit_type"] == "CLOSE")
    check("BUY勝ち: pnl=(1070-1050)/1050", p["pnl_pct"] == round((1070 - 1050) / 1050 * 100, 3))
    check("BUY勝ち: win=True", p["win"] is True)
    check("BUY勝ち: entry_session=07-14", p["entry_session"] == "2026-07-14")
    check("BUY勝ち: just_closed 1件", len(closed) == 1)
    check("BUY勝ち: pnl_yen>0", p["pnl_yen"] > 0)


def test_settle_buy_skip():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-13", "basis_date": "2026-07-13",
            "limit_price": 1040, "status": "pending"}]
    book = base_book(pos)
    # 寄り1050 > MAX指値1040 → 見送り
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("BUY見送り: SKIP", p["exit_type"] == "SKIP")
    check("BUY見送り: pnl=0", p["pnl_pct"] == 0.0)
    check("BUY見送り: pnl_yen=0", p["pnl_yen"] == 0)


# ---------------------------------------------------------------- settle SELL
def test_settle_sell_win():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
            "signal_date": "2026-07-13", "basis_date": "2026-07-13",
            "limit_price": 1000, "status": "pending"}]
    book = base_book(pos)
    # 寄り1050 >= MIN指値1000 → 執行, 引け1000 < 寄り → 空売り利益
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 990, 1000, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("SELL勝ち: CLOSE", p["exit_type"] == "CLOSE")
    check("SELL勝ち: pnl=(1050-1000)/1050", p["pnl_pct"] == round((1050 - 1000) / 1050 * 100, 3))
    check("SELL勝ち: win=True", p["win"] is True)


def test_settle_sell_skip():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
            "signal_date": "2026-07-13", "basis_date": "2026-07-13",
            "limit_price": 1000, "status": "pending"}]
    book = base_book(pos)
    # 寄り980 < MIN指値1000（ギャップダウン）→ 見送り
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 980, 1000, 950, 970, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("SELL見送り: SKIP", p["exit_type"] == "SKIP")


# ---------------------------------------------------------------- pending / expired
def test_settle_pending_kept():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-14", "basis_date": "2026-07-14",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    # basis後の足がまだ無い（当日足も無い）→ pending維持
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    closed = dp.settle(book, data, date(2026, 7, 15))
    check("エントリー足未到来→pending維持", book["positions"][0]["status"] == "pending")
    check("pending維持: just_closed 0件", len(closed) == 0)


def test_settle_today_not_closed():
    """エントリーセッションが当日(=まだ引けてない)なら決済しない。"""
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-14", "basis_date": "2026-07-14",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    # 当日=07-15 の足がデータに存在しても（寄り前実行では通常無いが）確定扱いしない
    data = {"1301.T": mkdf([("2026-07-14", 1050, 1080, 1040, 1070, 1e6),
                            ("2026-07-15", 1080, 1090, 1070, 1085, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    check("当日足は未確定→pending維持", book["positions"][0]["status"] == "pending")


def test_settle_expired():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-06-01", "basis_date": "2026-06-01",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    data = {"1301.T": mkdf([("2026-05-30", 1000, 1010, 990, 1000, 1e6)])}  # basis後の足なし
    dp.settle(book, data, date(2026, 7, 15))
    check("14日超で足取れず→expired", len(book["expired"]) == 1)
    check("expired: activeから除外", all(p.get("status") != "pending" for p in book["positions"]))


# ---------------------------------------------------------------- record
def test_record_and_dedup():
    book = base_book([])
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    sigs = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
             "prev_close": 1070, "daily_gain": 27.5, "min_entry_price": 1070}]
    iss = {"1301": "2"}
    added = dp.record(book, sigs, data, iss, date(2026, 7, 15))
    check("記帳1件", len(added) == 1)
    check("basis_date=07-14(当日前の最終足)", book["positions"][0]["basis_date"] == "2026-07-14")
    check("limit_price=min指値", book["positions"][0]["limit_price"] == 1070)
    check("SELLにshort付与", book["positions"][0]["short"]["mark"] == "○")
    # 同じ(ticker,signal_date)は重複記帳しない
    added2 = dp.record(book, sigs, data, iss, date(2026, 7, 15))
    check("重複記帳しない", len(added2) == 0 and len(book["positions"]) == 1)


# ---------------------------------------------------------------- stats
def test_cumulative_stats():
    book = base_book([
        {"ticker": "A.T", "direction": "BUY", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": 2.0, "pnl_yen": 80000, "win": True},
        {"ticker": "B.T", "direction": "BUY", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": -1.0, "pnl_yen": -40000, "win": False},
        {"ticker": "C.T", "direction": "SELL", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": 3.0, "pnl_yen": 120000, "win": True},
        {"ticker": "D.T", "direction": "BUY", "status": "closed", "exit_type": "SKIP",
         "pnl_pct": 0.0, "pnl_yen": 0, "win": False},
        {"ticker": "E.T", "direction": "SELL", "status": "pending"},
    ])
    st = dp.cumulative_stats(book)
    check("執行n=3(SKIP/pending除外)", st["all"]["n"] == 3)
    check("勝率=2/3", abs(st["all"]["win"] - 2 / 3 * 100) < 1e-6)
    check("PF=(2+3)/1=5.0", abs(st["all"]["pf"] - 5.0) < 1e-6)
    check("損益円=160000", st["all"]["yen"] == 160000)
    check("BUY n=2", st["buy"]["n"] == 2)
    check("SELL n=1", st["sell"]["n"] == 1)
    check("見送り=1", st["skipped"] == 1)
    check("保有中=1", st["pending"] == 1)


def _flat_then(last_gain_pct, base=1000):
    """30日フラット→最終日に指定%急騰(出来高6倍)のOHLCV rowsを作る。"""
    last = round(base * (1 + last_gain_pct / 100))
    rows = [(f"2026-06-{d:02d}", base, base + 2, base - 2, base, 1_000_000) for d in range(1, 29)]
    rows += [("2026-07-13", base, base + 2, base - 2, base, 1_000_000),
             ("2026-07-14", base, last, base, last, 6_000_000)]
    return mkdf(rows)


def test_daily_top_fade():
    """毎日1番=前日上昇率トップを選び、+15%以上×貸借○ならGO・それ以外はNOGO。閾値は必ず復元。"""
    import screener
    screener.fetch_tse_universe = lambda *a, **k: []   # 名前補完の実ネットを止める
    import screener_sell_day as ssd
    before = ssd.DAILY_GAIN_MIN

    data = {"9999.T": _flat_then(21), "8888.T": _flat_then(8)}   # +21% と +8%
    today = date(2026, 7, 15)

    # 貸借○ → GO・トップは9999(+21%)
    pick = dp.daily_top_fade(data, today, {"9999": "2"})
    check("1番=最大上昇株9999", pick and pick["ticker"] == "9999.T")
    check("+21%×貸借○ → GO", pick["verdict"] == "GO")
    check("min指値=前日終値", pick["min_entry_price"] == pick["prev_close"])
    check("閾値を必ず復元", ssd.DAILY_GAIN_MIN == before)

    # 貸借不明(?) → 空売り不可でNOGO
    pick2 = dp.daily_top_fade(data, today, {})
    check("+21%でも貸借?ならNOGO", pick2["verdict"] == "NOGO" and "貸借" in pick2["nogo_reason"])

    # トップが+8%しかない → 薄いのでNOGO
    pick3 = dp.daily_top_fade({"8888.T": _flat_then(8)}, today, {"8888": "2"})
    check("+8%(貸借○)でも薄いのでNOGO", pick3["verdict"] == "NOGO" and "薄い" in pick3["nogo_reason"])

    # 候補ゼロ(急騰なし) → None
    check("急騰ゼロ→候補なしNone", dp.daily_top_fade({"7777.T": _flat_then(1)}, today, {}) is None)


def run_all():
    for fn in [test_shortability, test_settle_buy_win, test_settle_buy_skip,
               test_settle_sell_win, test_settle_sell_skip, test_settle_pending_kept,
               test_settle_today_not_closed, test_settle_expired,
               test_record_and_dedup, test_cumulative_stats,
               test_daily_top_fade]:
        print(f"\n▶ {fn.__name__}")
        fn()
    print(f"\n==== {PASS} PASS / {FAIL} FAIL ====")
    return FAIL == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_all() else 1)

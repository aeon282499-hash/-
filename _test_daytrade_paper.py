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


def _flat_then(last_gain_pct, base=1000, sticky=False):
    """30日フラット→最終日に指定%急騰(出来高6倍)のOHLCV rowsを作る。
    sticky=True で最終日レンジを極小(張り付きS高)にする。"""
    last = round(base * (1 + last_gain_pct / 100))
    if sticky:                          # 張り付き: 高安が終値にほぼ張り付く
        hi, lo = round(last * 1.002), round(last * 0.998)
    else:
        hi, lo = last, base             # レンジ大（安値=前日水準まで振れた）
    rows = [(f"2026-06-{d:02d}", base, base + 2, base - 2, base, 1_000_000) for d in range(1, 29)]
    rows += [("2026-07-13", base, base + 2, base - 2, base, 1_000_000),
             ("2026-07-14", base, hi, lo, last, 6_000_000)]
    return mkdf(rows)


def test_daily_top_fades():
    """選定=貸借○×前日+5%以上×張り付き除外。上位3を降順で返す・各GO/NOGO判定。"""
    import screener
    screener.fetch_tse_universe = lambda *a, **k: []   # 名前補完の実ネットを止める

    # 4銘柄: +21%,+18%,+16%,+8%（全部貸借○・レンジ大）
    data = {"9999.T": _flat_then(21), "6666.T": _flat_then(18),
            "7777.T": _flat_then(16), "8888.T": _flat_then(8)}
    today = date(2026, 7, 15)
    iss = {"9999": "2", "6666": "2", "7777": "2", "8888": "2"}

    picks = dp.daily_top_fades(data, today, iss)
    check("上位3を返す", len(picks) == 3)
    check("降順(1番=9999+21%)", picks[0]["ticker"] == "9999.T" and picks[0]["rank"] == 1)
    check("2番=6666(+18%)", picks[1]["ticker"] == "6666.T")
    check("3番=7777(+16%)", picks[2]["ticker"] == "7777.T")
    check("上位3は全部+15%以上→全GO", all(p["verdict"] == "GO" for p in picks))
    check("min指値=前日終値", picks[0]["min_entry_price"] == picks[0]["prev_close"])
    check("range_pct記録(>5%)", picks[0].get("range_pct", 0) > 5)

    # 貸借○が1つも無ければ[]（売れない玉は選ばない）
    check("貸借○ゼロ→空リスト", dp.daily_top_fades(data, today, {}) == [])

    # +8%(貸借○)だけ → 1件・NOGO薄い
    p8 = dp.daily_top_fades({"8888.T": _flat_then(8)}, today, {"8888": "2"})
    check("+8%→1件NOGO薄い", len(p8) == 1 and p8[0]["verdict"] == "NOGO" and "薄い" in p8[0]["nogo_reason"])

    # 張り付きS高(+20%貸借○)は除外 → 空リスト（踏み上げ回避の核心）
    check("張り付きS高は除外→空",
          dp.daily_top_fades({"5555.T": _flat_then(20, sticky=True)}, today, {"5555": "2"}) == [])

    # 張り付き#1と非張り付き#2 → 非張り付きだけ残る
    mix = {"5555.T": _flat_then(30, sticky=True), "6666.T": _flat_then(18)}
    pm = dp.daily_top_fades(mix, today, {"5555": "2", "6666": "2"})
    check("張り付き#1を飛ばし6666だけ", len(pm) == 1 and pm[0]["ticker"] == "6666.T")

    # 値がさ株(1単元>予算100万=株価>1万)は除外。base=20000で+20%→last=24000>1万→除外
    check("値がさ株(>1万)は除外",
          dp.daily_top_fades({"4444.T": _flat_then(20, base=20000)}, today, {"4444": "2"}) == [])


def run_all():
    for fn in [test_shortability, test_settle_buy_win, test_settle_buy_skip,
               test_settle_sell_win, test_settle_sell_skip, test_settle_pending_kept,
               test_settle_today_not_closed, test_settle_expired,
               test_record_and_dedup, test_cumulative_stats,
               test_daily_top_fades]:
        print(f"\n▶ {fn.__name__}")
        fn()
    print(f"\n==== {PASS} PASS / {FAIL} FAIL ====")
    return FAIL == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_all() else 1)

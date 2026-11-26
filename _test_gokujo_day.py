"""極上デイの単体テスト: python _test_gokujo_day.py"""
import json, os, tempfile
from datetime import date
import pandas as pd
import gokujo_day as g


def _df(o, h, l, c, d="2026-12-01"):
    return pd.DataFrame({"Open": [o], "High": [h], "Low": [l], "Close": [c]}, index=pd.to_datetime([d]))


def _row(lp=1010):
    return {"signal_date": "2026-12-01", "entry_date": "2026-12-01", "ticker": "1111.T", "name": "T",
            "size": 1_000_000, "limit_price": lp, "status": "pending"}


def test_rules():
    cases = [  # (o,h,l,c,lp) -> (status, exit, pnl)
        ((1000, 1020, 990, 1015, 1010), ("closed", "CLOSE", 1.5)),
        ((1000, 1060, 960, 1050, 1010), ("closed", "STOP", -3.0)),   # 損切りが利確より優先
        ((1000, 1060, 990, 1040, 1010), ("closed", "TP", 5.0)),
        ((1020, 1030, 1000, 1025, 1010), ("expired", "NOFILL", None)),
    ]
    for (o, h, l, c, lp), (st, ex, pnl) in cases:
        rows = [_row(lp)]
        n = g.advance(rows, date(2026, 12, 2), {"1111.T": _df(o, h, l, c)})
        r = rows[0]
        assert n == 1 and r["status"] == st and r["exit_type"] == ex and r.get("pnl_pct") == pnl, r


def test_same_day_not_settled():
    rows = [_row()]
    assert g.advance(rows, date(2026, 12, 1), {"1111.T": _df(1000, 1010, 995, 1005)}) == 0
    assert rows[0]["status"] == "pending"


def test_record_one_per_day():
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as td:
        os.chdir(td)
        try:
            json.dump({"date": "2026-12-01", "signals": [
                {"ticker": "A.T", "name": "A", "direction": "BUY", "prev_close": 1000, "limit_price": 1010},
                {"ticker": "B.T", "name": "B", "direction": "BUY", "prev_close": 1000, "limit_price": 1010},
            ]}, open(g.SIG_FILE, "w"))
            rows = []
            assert g.record(rows, date(2026, 12, 1)) == 1 and rows[0]["ticker"] == "A.T"
            assert g.record(rows, date(2026, 12, 1)) == 0          # 再実行で増えない
            assert g.record(rows, date(2026, 12, 2)) == 0          # 日付違いのファイルは読まない
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    test_rules(); test_same_day_not_settled(); test_record_one_per_day()
    print("OK")

# -*- coding: utf-8 -*-
"""10月ルール（fibo_oct.py）の判定テスト。値動きは手で作った1分足（実データではない）。
実行: python -X utf8 tests/test_fibo_oct.py   （pytest でも動く）"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import os  # noqa: E402
import fibo_daytrade as FD  # noqa: E402
from fibo_oct import OctEngine, DayRisk, calc_shares, round_tick  # noqa: E402

DAY = "2026-09-30"


def mode(entry, bars):
    """テストごとに入り方と足を固定（設定ファイルの値に左右されないように）。エンジン作成時に読まれる"""
    os.environ["FIBO_ENTRY_MODE"] = entry
    os.environ["FIBO_BARS"] = bars


def feed(eng, bars):
    """bars: [(足の終わり"HH:MM", o, h, l, c, v)]"""
    ev = []
    for hmm, o, h, l, c, v in bars:
        t = datetime.strptime(f"{DAY} {hmm}", "%Y-%m-%d %H:%M")
        ev += eng.feed(t, o, h, l, c, v)
    return ev


# オーディオS（621A）9/30 をボスの数字で再現: 寄り1,576→安値1,525→高値1,690（9:00〜9:05の足）→9:06に1,627まで押す→9:10に1,683
# 9:05以降の4本は Yahoo の1分足そのまま。9:00〜9:04 は Yahoo に無いのでボスの数字から作った仮の足。
AUDIO = [("09:01", 1576, 1580, 1570, 1575, 50000), ("09:02", 1575, 1576, 1525, 1540, 60000), ("09:03", 1540, 1640, 1538, 1630, 80000),
         ("09:04", 1630, 1690, 1628, 1680, 90000), ("09:05", 1680, 1688, 1660, 1665, 60000),
         ("09:06", 1658, 1672, 1650, 1668, 40000), ("09:07", 1667, 1667, 1627, 1636, 73600), ("09:08", 1636, 1645, 1629, 1634, 37900),
         ("09:09", 1633, 1640, 1618, 1639, 34200), ("09:10", 1640, 1683, 1637, 1675, 86200)]


def test_audio_boss_numbers():
    mode("limit", "5")
    eng = OctEngine("621A", "オーディオS", DAY, prev_close=1536, prev_bars=[], prev_day_vol=3_000_000, day_open=1576,
                    risk=DayRisk(2.0), ignore_price_band=True)
    ev = feed(eng, AUDIO)
    p = eng.trade.plan
    assert (p.origin, p.high) == (1525, 1690), (p.origin, p.high)
    assert p.entry == 1627 and p.stop == 1578 and p.shares == 400, (p.entry, p.stop, p.shares)
    assert p.stop_kind == "−3%"
    assert eng.trade.books[2.0].exit_type == "利確" and eng.trade.books[2.0].exit_price == 1660
    assert [e["kind"] for e in ev if e["kind"] in ("approach", "zone")] == ["zone"]


def test_audio_price_band():
    """株価2,000円未満なので、ルール通りなら見送り（約定しない）"""
    mode("limit", "5")
    eng = OctEngine("621A", "オーディオS", DAY, prev_close=1536, prev_bars=[], prev_day_vol=3_000_000, day_open=1576, risk=DayRisk(2.0))
    feed(eng, AUDIO)
    assert eng.trade is None and eng.status() == "対象外"


REBOUND = [("09:01", 2000, 2010, 1995, 2005, 5000), ("09:02", 2005, 2060, 2003, 2055, 5000), ("09:03", 2055, 2100, 2050, 2095, 5000),
           ("09:04", 2095, 2098, 2070, 2075, 3000), ("09:05", 2075, 2080, 2058, 2062, 3000), ("09:06", 2062, 2070, 2060, 2068, 3000),
           ("09:07", 2068, 2075, 2066, 2074, 3000), ("09:08", 2074, 2080, 2070, 2078, 3000), ("09:09", 2078, 2085, 2076, 2083, 3000),
           ("09:10", 2083, 2104, 2082, 2100, 3000)]


def test_rebound_3min():
    """3分足: 9:00〜9:03の足で高値2,100 → 38.2%=2,060にタッチ（9:05）→ 9:03足は陰線で待つ → 9:06足が陽線で2,083に引け → 買い"""
    mode("rebound", "3/15")
    eng = OctEngine("X", "X", DAY, prev_close=1990, prev_bars=[], prev_day_vol=1_000_000, day_open=2000, risk=DayRisk(1.0))
    ev = feed(eng, REBOUND)
    p = eng.trade.plan
    assert (p.origin, p.high) == (1995, 2100) and round(p.level) == 2060
    assert p.entry == 2083 and p.stop == 2021 and p.shares == 300, (p.entry, p.stop, p.shares)
    assert eng.trade.fill_t.strftime("%H:%M") == "09:09"
    assert eng.trade.books[1.0].exit_type == "利確" and eng.trade.books[1.0].exit_price == 2104
    assert [e["kind"] for e in ev if e["kind"] in ("approach", "zone")] == ["approach", "zone"]   # 9:04に2,075＝あと0.7%で🟡


def test_rebound_cancel_below_618():
    """タッチ後、陽線が出る前に61.8%(2,035)を割ったら買わない"""
    mode("rebound", "3/15")
    eng = OctEngine("X", "X", DAY, prev_close=1990, prev_bars=[], prev_day_vol=1_000_000, day_open=2000, risk=DayRisk(1.0))
    bars = REBOUND[:5] + [("09:06", 2062, 2062, 2030, 2032, 3000), ("09:07", 2032, 2040, 2031, 2039, 3000),
                          ("09:08", 2039, 2045, 2038, 2044, 3000), ("09:09", 2044, 2050, 2043, 2049, 3000), ("09:10", 2049, 2052, 2048, 2050, 3000)]
    feed(eng, bars)
    assert eng.trade is None


def test_gap_6pct_out():
    eng = OctEngine("X", "X", DAY, prev_close=2000, prev_bars=[], day_open=2130)
    feed(eng, [("09:01", 2130, 2140, 2120, 2135, 1000)])
    assert eng.out and "6%" in eng.out


def test_first_minute_big_bearish():
    eng = OctEngine("X", "X", DAY, prev_close=3000, prev_bars=[], prev_day_vol=300_000, day_open=3050)
    feed(eng, [("09:01", 3050, 3055, 2990, 2995, 20_000)])     # −1.8%・前日1分平均(1,000株)の20倍
    assert eng.out == "最初の1分足が大陰線＋大出来高"


def test_shares_and_rounding():
    assert calc_shares(2480, 2406) == 200            # 2万円÷74円=270 → 200株
    assert calc_shares(1627, 1578) == 400
    assert calc_shares(9000, 8990) == 100            # 建玉130万円まで（2万円÷10円=2,000株 → 100株）
    assert round_tick(2479.61) == 2480 and round_tick(4012) == 4010


def test_day_stop_two_losses():
    r = DayRisk(1.0)
    r.on_fill(); r.on_close(-5000); r.on_fill(); r.on_close(-3000)
    ok, why = r.can_enter()
    assert not ok and "2連敗" in why


if __name__ == "__main__":
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f(); print("OK", k)

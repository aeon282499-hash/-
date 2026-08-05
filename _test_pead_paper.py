# -*- coding: utf-8 -*-
"""_test_pead_paper.py — PEAD紙運用の単体テスト。実行: python -X utf8 _test_pead_paper.py"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date

import pandas as pd

import main_pead_paper as mp

N = [0]


def ok(cond, label):
    assert cond, f"FAIL: {label}"
    N[0] += 1
    print(f"  ok {label}")


def mkdf(rows):
    """rows = [(ds, o, c, v), ...]"""
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame({"Open": [r[1] for r in rows], "Close": [r[2] for r in rows],
                         "High": [r[2] for r in rows], "Low": [r[1] for r in rows],
                         "Volume": [r[3] for r in rows]}, index=idx)


# ── 値幅・張り付き判定 ─────────────────────────────
ok(mp.lim_up(999) == 999 + 150, "lim_up 1000円未満=+150")
ok(mp.lim_up(1000) == 1000 + 300, "lim_up 1000円=+300帯")
ok(mp.is_stop_high(1150, 999.99), "S高ちょうどはTrue")
ok(not mp.is_stop_high(1149, 1000), "S高未満はFalse")

# ── 営業日 ─────────────────────────────
ok(mp.prev_trading_day(date(2026, 8, 3)) == date(2026, 7, 31), "prev_trading_day 月→金")

# ── 20日代金中央値 ─────────────────────────────
rows = [(f"2026-06-{d:02d}", 100, 100, 100_000) for d in range(1, 27) if date(2026, 6, d).weekday() < 5]
ok(mp.tov20_median({"9999.T": mkdf(rows)}, "9999.T", "2026-06-26") == 100 * 100_000,
   "tov20中央値=1,000万")
ok(mp.tov20_median({"9999.T": mkdf(rows[:5])}, "9999.T", "2026-06-26") is None, "20日未満はNone")

with tempfile.TemporaryDirectory() as td:
    cwd = os.getcwd()
    os.chdir(td)
    try:
        d0, d1 = "2026-08-03", "2026-08-04"
        sched = {"schedule": {d0: [
            {"code": "1111", "name": "通常GO", "type": "1Q"},
            {"code": "2222", "name": "張り付き", "type": "1Q"},
            {"code": "3333", "name": "ギャップ不足", "type": "1Q"},
            {"code": "4444", "name": "本体重複", "type": "1Q"},
        ]}}
        json.dump(sched, open(mp.SCHEDULE_PATH, "w", encoding="utf-8"), ensure_ascii=False)
        json.dump({"positions": [{"ticker": "4444.T", "date": d0, "status": "pending"}]},
                  open("positions_earnings.json", "w", encoding="utf-8"), ensure_ascii=False)

        base = [(f"2026-07-{d:02d}", 1000, 1000, 1_000_000) for d in range(1, 32)
                if date(2026, 7, d).weekday() < 5]
        # 1111: gap+15%・引け+18%(S高1300未満)=D1大引けエントリー
        d_1111 = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1150, 1180, 1_000_000)])
        # 2222: gap+15%・引け1300=S高張り付き → D2追撃待ち
        d_2222 = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1150, 1300, 1_000_000)])
        # 3333: gap+5%=不足
        d_3333 = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1050, 1100, 1_000_000)])
        # 4444: gap+20%だが本体重複で除外
        d_4444 = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1200, 1250, 1_000_000)])
        all_data = {"1111.T": d_1111, "2222.T": d_2222, "3333.T": d_3333, "4444.T": d_4444}

        book = {"last_run_date": None, "positions": []}
        new_e = mp.build_entries(book, all_data, d0, date(2026, 8, 4))
        ok(len(new_e) == 2, "エントリー2件（不足/重複は落ちる）")
        by = {p["ticker"]: p for p in new_e}
        ok(by["1111.T"]["status"] == "held" and by["1111.T"]["entry"] == 1180,
           "通常GOはD1大引け1180でheld")
        ok(by["2222.T"]["status"] == "pending_d2" and by["2222.T"]["chase_date"] == "2026-08-05",
           "張り付きはD2追撃待ち")
        ok(by["1111.T"]["exit_date"] == "2026-08-10", "売り=D1+4営業日(8/10月)")

        # D2追撃: 2222が8/5寄り1400(S高1600未満)で約定・1111はまだ決済日前
        d_2222b = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1150, 1300, 1_000_000),
                               ("2026-08-05", 1400, 1450, 1_000_000)])
        all_data["2222.T"] = d_2222b
        closed, chased, dropped = mp.settle_and_fill(book, all_data, "2026-08-05")
        ok(len(chased) == 1 and chased[0]["entry"] == 1400, "D2寄り1400で追撃約定")
        ok(not closed and not dropped, "決済/見送りなし")

        # 決済: 8/10終値1062 → 1111 pnl=-10%
        d_1111b = mkdf(base + [(d0, 1000, 1000, 1_000_000), (d1, 1150, 1180, 1_000_000),
                               ("2026-08-10", 1100, 1062, 1_000_000)])
        all_data["1111.T"] = d_1111b
        closed, _, _ = mp.settle_and_fill(book, all_data, "2026-08-10")
        ok(len(closed) == 1 and closed[0]["pnl_pct"] == -10.0, "8/10大引け決済-10.0%")
        ok(closed[0]["pnl_yen"] == -100_000, "名目100万で-10万円")

        # 2日連続張り付き見送り
        book2 = {"positions": [{"ticker": "5555.T", "name": "x", "status": "pending_d2",
                                "chase_date": "2026-08-05", "d1_close": 1300}]}
        d_5555 = mkdf([("2026-08-05", 1600, 1600, 1)])
        _, _, dr = mp.settle_and_fill(book2, {"5555.T": d_5555}, "2026-08-05")
        ok(len(dr) == 1 and book2["positions"][0]["status"] == "dropped", "2連続張り付き=見送り")

        # 統計
        st = mp.stats(book)
        ok(st["n"] == 1 and st["yen"] == -100_000, "通算=1件-10万")

        # embed が組めて必須の注意書きを含む
        emb = mp.build_embed(new_e, closed, [], [], book, date(2026, 8, 5))
        ok("紙運用＝実弾禁止" in emb["footer"]["text"], "footerに実弾禁止")
    finally:
        os.chdir(cwd)

print(f"\nALL {N[0]}/{N[0]} PASS")

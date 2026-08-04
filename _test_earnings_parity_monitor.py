# -*- coding: utf-8 -*-
"""_test_earnings_parity_monitor.py — 月次パリティ監視の純ロジック検証（外部I/Oなし）。
実行: python -X utf8 _test_earnings_parity_monitor.py
"""
from datetime import date

import earnings_parity_monitor as pm

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


def _closed(exit_date, pnl_pct, pnl_yen=0, kind="翌寄り", entry_date="2026-07-01"):
    return {"status": "closed", "date": entry_date, "exit_date": exit_date,
            "exit_kind": kind, "pnl_pct": pnl_pct, "pnl_yen": pnl_yen}


def test_prev_month():
    check("8/1の前月は7月", pm.prev_month_ym(date(2026, 8, 1)) == "2026-07")
    check("1/2の前月は前年12月", pm.prev_month_ym(date(2026, 1, 2)) == "2025-12")


def test_month_filter():
    ps = [
        _closed("2026-07-31", +2.0, 10000),
        _closed("2026-08-01", -1.0, -5000),          # 月境界: exitが8/1→8月分
        _closed("2026-07-15", -3.0, -15000, kind="PEAD延長"),
        {"status": "expired", "expired_date": "2026-07-20", "date": "2026-07-05"},
        {"status": "pending", "date": "2026-07-31", "name": "テスト", "ticker": "9999.T"},
        {"status": "closed", "exit_date": "2026-07-10", "pnl_pct": None},  # 壊れた行は除外
    ]
    m = pm.summarize_month(ps, "2026-07")
    check("7月の決済は2件（8/1決済と壊れ行を除く）", m["n"] == 2)
    check("平均は(+2-3)/2=-0.5%", abs(m["mean_pct"] - (-0.5)) < 1e-9)
    check("PEAD延長1件", m["pead_n"] == 1)
    check("失効1件", m["expired_n"] == 1)
    check("未決済1件", m["open_n"] == 1)
    check("損益合計-5000円", m["total_yen"] == 10000 - 15000)
    m8 = pm.summarize_month(ps, "2026-08")
    check("8月分は1件", m8["n"] == 1)


def test_judge_z():
    # 平均=BT平均ぴったり → z=0・想定内
    j = pm.judge(10, pm.BT_REF["mean_pct"], month=7)
    check("z=0", abs(j["z"]) < 1e-9)
    check("想定内判定", "想定内" in j["verdict"])
    # 大きく下振れ: n=25で平均-4% → z=(-4-0.97)/(9.37/5)=-2.65
    j2 = pm.judge(25, -4.0, month=8)
    check("z≈-2.65", abs(j2["z"] - (-2.65)) < 0.02)
    check("-2σ警戒判定", "-2σ" in j2["verdict"])
    check("単月で停止しない文言", "単月では停止しない" in j2["verdict"])
    # n<5はサンプル不足
    j3 = pm.judge(3, -8.0, month=6)
    check("n=3はサンプル不足", "サンプル不足" in j3["verdict"])
    # 上振れ
    j4 = pm.judge(25, +5.0, month=8)
    check("+2σ上振れ判定", "上振れ" in j4["verdict"])


def test_judge_count_warn():
    # 8月(BT想定26.1件)に5件しか決済がない → 4割未満警告
    j = pm.judge(5, 0.97, month=8)
    check("件数警告あり", any("4割未満" in w for w in j["warns"]))
    # 6月(BT想定4.0件)は閑散月なので0件でも警告なし
    j2 = pm.judge(0, 0.0, month=6)
    check("閑散月の0件は警告なし", not j2["warns"])
    check("決済なし判定", "決済なし" in j2["verdict"])
    # 2月(BT想定28.6件)の0件は要確認警告
    j3 = pm.judge(0, 0.0, month=2)
    check("繁忙月の0件は要確認", any("要確認" in w for w in j3["warns"]))


def test_summarize_all():
    ps = [_closed("2026-07-31", +2.0, 10000, entry_date="2026-07-14"),
          _closed("2026-08-01", -1.0, -5000, entry_date="2026-07-31")]
    c = pm.summarize_all(ps)
    check("通算2件", c["n"] == 2)
    check("初回日付", c["since"] == "2026-07-14")
    check("通算+5000円", c["total_yen"] == 5000)


def test_embed():
    ps = [_closed("2026-07-31", +2.0, 10000)]
    m = pm.summarize_month(ps, "2026-07")
    j = pm.judge(m["n"], m["mean_pct"], 7)
    e = pm.build_embed(m, j, pm.summarize_all(ps))
    check("タイトルに対象月", "2026-07" in e["title"])
    check("帳簿≠本人約定の注記", "本人の実約定とは別物" in e["footer"]["text"])
    check("判定文が本文にある", "判定:" in e["description"])
    # 赤色分岐（z≤-2）
    j2 = pm.judge(25, -4.0, month=8)
    m2 = dict(m, n=25, mean_pct=-4.0)
    e2 = pm.build_embed(m2, j2, pm.summarize_all(ps))
    check("下振れは赤色", e2["color"] == 0xE74C3C)


def run_all():
    for fn in [test_prev_month, test_month_filter, test_judge_z,
               test_judge_count_warn, test_summarize_all, test_embed]:
        print(f"\n▶ {fn.__name__}")
        fn()
    print(f"\n==== {PASS} PASS / {FAIL} FAIL ====")
    return FAIL == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_all() else 1)

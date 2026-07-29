# -*- coding: utf-8 -*-
"""_test_kiwami_sell.py — 極みの売り台帳（損切り+2.5%・3枠）のテスト（2026-07-29）。

検証点:
  ①記帳: SELLだけ拾う / 3枠で打ち止め / 同一銘柄の重複を建てない / 日付違いは無視
  ②出口: 踏み上げ+2.5%で切れる（通常版の3.0%では切れない値でも切れる）
         利確-5% / RSI≤50 / 期限3日 / 判定順（STOP優先）
  ③非破壊: 通常版の positions_sell.json を読まない・書かない
  ④15時チェック: 新台帳を読む / 空なら通常版へフォールバック
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date

import pandas as pd

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import shadow_exit as SE

PASS = FAIL = 0


def ok(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {msg}")
    else:
        FAIL += 1
        print(f"  ❌ {msg}")


def mkdf(rows):
    """rows = [(date, open, high, low, close), ...]"""
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame({"Open": [r[1] for r in rows], "High": [r[2] for r in rows],
                         "Low": [r[3] for r in rows], "Close": [r[4] for r in rows]}, index=idx)


def with_tmp(fn):
    """カレントを一時ディレクトリにして実行（本物の台帳を汚さない）。"""
    def wrapper():
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as td:
            os.chdir(td)
            try:
                fn()
            finally:
                os.chdir(cwd)
    return wrapper


# ────────────────────────── ① 記帳 ──────────────────────────
@with_tmp
def test_record():
    print("\n■ ① 記帳")
    today = date(2026, 7, 29)
    sigs = {"date": "2026-07-29", "signals": [
        {"ticker": "1111.T", "name": "A", "direction": "SELL", "prev_close": 1000},
        {"ticker": "2222.T", "name": "B", "direction": "SELL", "prev_close": 2000},
        {"ticker": "3333.T", "name": "C", "direction": "SELL", "prev_close": 3000},
        {"ticker": "4444.T", "name": "D", "direction": "SELL", "prev_close": 4000},
        {"ticker": "9999.T", "name": "買い", "direction": "BUY", "prev_close": 500},
    ]}
    json.dump(sigs, open(SE.SELL_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)

    n = SE.record_sell_signals(today)
    rows = SE.load_sell_ledger()
    ok(n == 3, f"3枠で打ち止め（記帳{n}件）")
    ok(len(rows) == 3, f"台帳は3件（{len(rows)}）")
    ok(all(r["direction"] == "SELL" for r in rows), "BUYは混入しない")
    ok(all(r["stop_pct"] == 2.5 for r in rows), "stop_pct=2.5 が入る")
    ok([r["ticker"] for r in rows] == ["1111.T", "2222.T", "3333.T"], "上から3件を採用")
    ok(os.path.exists("_shadow_skipped_sell.json"), "見送りが記録される")
    ok(json.load(open("_shadow_skipped_sell.json", encoding="utf-8"))["names"] == ["D"],
       "見送り銘柄名が正しい")

    n2 = SE.record_sell_signals(today)
    ok(n2 == 0, "同日の再実行で二重記帳しない")

    # 日付が違うファイルは無視
    json.dump({"date": "2026-07-28", "signals": [
        {"ticker": "5555.T", "name": "E", "direction": "SELL", "prev_close": 500}]},
        open(SE.SELL_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    ok(SE.record_sell_signals(today) == 0, "今日以外のシグナルファイルは無視")

    # 枠が空けば入る
    rows = SE.load_sell_ledger()
    rows[0]["status"] = "closed"
    SE.save_sell_ledger(rows)
    json.dump({"date": "2026-07-29", "signals": [
        {"ticker": "6666.T", "name": "F", "direction": "SELL", "prev_close": 600}]},
        open(SE.SELL_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    ok(SE.record_sell_signals(today) == 1, "枠が空いたら新規が入る")

    # 保有中の同一銘柄は建てない
    json.dump({"date": "2026-07-29", "signals": [
        {"ticker": "2222.T", "name": "B", "direction": "SELL", "prev_close": 2000}]},
        open(SE.SELL_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    before = len(SE.load_sell_ledger())
    SE.record_sell_signals(today)
    ok(len(SE.load_sell_ledger()) == before, "保有中の同一銘柄は重複して建てない")


# ────────────────────────── ② 出口 ──────────────────────────
@with_tmp
def test_exit():
    print("\n■ ② 出口（踏み上げ+2.5% / 利確-5% / RSI / 期限）")

    def pos(tk="1111.T"):
        return [{"signal_date": "2026-07-01", "entry_date": "2026-07-01", "ticker": tk,
                 "name": "T", "direction": "SELL", "prev_close": 1000, "stop_pct": 2.5,
                 "live_stop": 3.0, "entry_open": None, "status": "pending", "hold_days": 0,
                 "pnl_pct": None, "exit_type": None, "exit_date": None}]

    # 高値が+2.6%止まり（極みの2.5%＝1025は超えるが、通常版の3.0%＝1030には届かない）
    df = mkdf([("2026-07-01", 1000, 1026, 995, 1020),
               ("2026-07-02", 1020, 1028, 1010, 1025),
               ("2026-07-03", 1025, 1028, 1010, 1025)])
    p = pos()
    SE.advance_sell(p, date(2026, 7, 6), {"1111.T": df})
    ok(p[0]["exit_type"] == "STOP" and p[0]["pnl_pct"] == -2.5,
       f"高値+2.6%で踏み上げ損切り（{p[0]['exit_type']} {p[0]['pnl_pct']}）")

    # 同じ板を通常版の3.0%で判定したら切れない（＝この差がまさに今回の変更点）
    p2 = pos(); p2[0]["stop_pct"] = 3.0
    SE.advance_sell(p2, date(2026, 7, 6), {"1111.T": df})
    ok(p2[0]["exit_type"] != "STOP", f"同じ板でも3.0%なら損切りされない（{p2[0]['exit_type']}）")

    # 利確 -5%
    df2 = mkdf([("2026-07-01", 1000, 1005, 940, 950),
                ("2026-07-02", 950, 960, 940, 950),
                ("2026-07-03", 950, 960, 940, 950)])
    p = pos()
    SE.advance_sell(p, date(2026, 7, 6), {"1111.T": df2})
    ok(p[0]["exit_type"] == "TP" and p[0]["pnl_pct"] == 5.0, "安値-5%で利確")

    # STOP優先（同じ日に両方タッチ）
    df3 = mkdf([("2026-07-01", 1000, 1030, 940, 1000),
                ("2026-07-02", 1000, 1010, 990, 1000),
                ("2026-07-03", 1000, 1010, 990, 1000)])
    p = pos()
    SE.advance_sell(p, date(2026, 7, 6), {"1111.T": df3})
    ok(p[0]["exit_type"] == "STOP", "同日に両方タッチしたらSTOP優先（本番と同順）")

    # 期限3日
    df4 = mkdf([("2026-07-01", 1000, 1005, 995, 1000),
                ("2026-07-02", 1000, 1005, 995, 1000),
                ("2026-07-03", 1000, 1005, 995, 998),
                ("2026-07-06", 1000, 1005, 995, 1000)])
    p = pos()
    SE.advance_sell(p, date(2026, 7, 7), {"1111.T": df4})
    ok(p[0]["exit_type"] in ("MAXHOLD", "RSI"), f"3日で手仕舞い（{p[0]['exit_type']}）")
    ok(p[0]["hold_days"] == 3, f"保有日数3（{p[0]['hold_days']}）")

    # エントリー日が未到来なら pending のまま
    p = pos()
    SE.advance_sell(p, date(2026, 6, 30), {"1111.T": df})
    ok(p[0]["status"] == "pending", "エントリー日前は pending のまま")

    # SELLはNOFILL判定をしない（BUYのみ）
    p = pos()
    p[0]["limit_price"] = 900              # あっても無視されるべき
    SE.advance_sell(p, date(2026, 7, 6), {"1111.T": df})
    ok(p[0]["exit_type"] != "NOFILL", "SELLは寄指NOFILLの対象外（本番と同じ）")

    # 決済済みは再処理しない
    p = pos(); p[0].update(status="closed", exit_type="TP", pnl_pct=5.0)
    SE.advance_sell(p, date(2026, 7, 6), {"1111.T": df})
    ok(p[0]["exit_type"] == "TP" and p[0]["pnl_pct"] == 5.0, "決済済みは再処理しない")

    # データ無しでも落ちない
    p = pos()
    ok(SE.advance_sell(p, date(2026, 7, 6), {}) == 0, "価格データ無しでも例外にならない")


# ────────────────────────── ③ 非破壊 ──────────────────────────
@with_tmp
def test_isolation():
    print("\n■ ③ 通常版への非破壊")
    live = [{"ticker": "7777.T", "direction": "SELL", "status": "open", "entry_date": "2026-07-01",
             "entry_open": 1000, "hold_days": 1}]
    json.dump(live, open("positions_sell.json", "w", encoding="utf-8"), ensure_ascii=False)
    snap = open("positions_sell.json", encoding="utf-8").read()

    json.dump({"date": "2026-07-29", "signals": [
        {"ticker": "1111.T", "name": "A", "direction": "SELL", "prev_close": 1000}]},
        open(SE.SELL_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    SE.record_sell_signals(date(2026, 7, 29))
    df = mkdf([("2026-07-29", 1000, 1030, 990, 1000),
               ("2026-07-30", 1000, 1010, 990, 1000)])
    SE.update_sell_ledger(date(2026, 7, 31), {"1111.T": df})

    ok(open("positions_sell.json", encoding="utf-8").read() == snap,
       "通常版 positions_sell.json は1バイトも変わらない")
    ok(os.path.exists(SE.KIWAMI_SELL_LEDGER), "極み専用台帳が作られる")
    ok("7777.T" not in open(SE.KIWAMI_SELL_LEDGER, encoding="utf-8").read(),
       "通常版の玉が極み台帳に混入しない")


# ────────────────────────── ④ 15時チェック ──────────────────────────
@with_tmp
def test_close_reader():
    print("\n■ ④ 15時チェックの台帳参照")
    import importlib
    import kiwami_close as KC
    importlib.reload(KC)

    ok(KC.SELL_LEDGER == "kiwami_sell.json", "既定は極み専用台帳を見る")
    ok(KC.load_open_sell() == [], "台帳が無ければ空")

    json.dump([{"ticker": "8888.T", "direction": "SELL", "status": "open", "stop_pct": 3.0}],
              open("positions_sell.json", "w", encoding="utf-8"), ensure_ascii=False)
    fb = KC.load_open_sell()
    ok(len(fb) == 1 and fb[0]["ticker"] == "8888.T",
       "移行期（極み台帳が空）は通常版へフォールバック")

    json.dump([{"ticker": "1111.T", "direction": "SELL", "status": "open", "stop_pct": 2.5},
               {"ticker": "2222.T", "direction": "SELL", "status": "closed", "stop_pct": 2.5}],
              open("kiwami_sell.json", "w", encoding="utf-8"), ensure_ascii=False)
    rows = KC.load_open_sell()
    ok(len(rows) == 1 and rows[0]["ticker"] == "1111.T",
       "極み台帳があればそちらを使い、保有中だけ返す")
    ok(rows[0]["stop_pct"] == 2.5, "stop_pct=2.5 が15時チェックへ渡る")


# ────────────────────────── ⑤ 定数 ──────────────────────────
def test_consts():
    print("\n■ ⑤ 定数")
    ok(SE.SELL_STOP_PCT == 2.5, "SELL_STOP_PCT = 2.5")
    ok(SE.SELL_MAX_SLOTS == 3, "SELL_MAX_SLOTS = 3")
    ok(SE.LIVE_STOP == 3.0, "通常版の LIVE_STOP は 3.0 のまま（触っていない）")
    ok(SE.TAKE_PROFIT == 5.0, "利確は5.0のまま")
    ok(SE.MAX_HOLD == 3, "最大保有3日のまま")
    import tracker
    ok(tracker.STOP_LOSS == 3.0, "通常版 tracker.STOP_LOSS は 3.0 のまま（友達用は無変更）")


if __name__ == "__main__":
    print("=" * 70)
    print("極みの売り台帳（損切り+2.5%・3枠）テスト")
    print("=" * 70)
    test_consts()
    test_record()
    test_exit()
    test_isolation()
    test_close_reader()
    print("\n" + "=" * 70)
    print(f"結果: {PASS}/{PASS + FAIL} 合格" + ("" if FAIL == 0 else f"  ❌ {FAIL}件 失敗"))
    print("=" * 70)
    sys.exit(1 if FAIL else 0)

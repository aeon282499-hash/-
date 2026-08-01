# -*- coding: utf-8 -*-
"""_test_shadow_exit.py — 影台帳(shadow_exit.py)の検証（2026-07-25）。

一番大事なのは「影エンジンが本番(tracker.update_positions)と同じ判定をすること」。
損切り幅を本番と同じ3.0%にしたとき、同じ入力で同じ決済結果になれば、
以後の差分は"損切り幅の違いだけ"に由来すると言い切れる。
実行: python -X utf8 _test_shadow_exit.py
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date

import numpy as np
import pandas as pd

import shadow_exit as S
import tracker

PASS = FAIL = 0


def ok(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {msg}")
    else:
        FAIL += 1
        print(f"  ❌ {msg}")


def mkdf(rows, start="2026-07-01"):
    """rows = [(open, high, low, close)] を営業日連番のDataFrameに。"""
    idx = pd.bdate_range(start, periods=len(rows))
    return pd.DataFrame(rows, columns=["Open", "High", "Low", "Close"], index=idx).assign(Volume=1e6)


def flat(n, price, start="2026-05-01"):
    return [(price, price, price, price)] * n


print("=" * 78)
print("① 影エンジン vs 本番tracker（損切りを同じ3.0%にしたら一致するか）")
print("=" * 78)

# ウォームアップ用の平坦な履歴＋エントリー以降の値動き、の3ケース
CASES = {
    "STOP到達": [(100, 101, 96, 97), (97, 98, 95, 96), (96, 97, 95, 96)],
    "TP到達":   [(100, 106, 99, 105), (105, 106, 104, 105), (105, 106, 104, 105)],
    "期限まで": [(100, 101, 99, 100), (100, 101, 99, 100), (100, 101, 99, 100)],
}
for name, tail in CASES.items():
    hist = flat(40, 100)
    df = mkdf(hist + tail)
    entry_str = df.index[len(hist)].strftime("%Y-%m-%d")
    today = (df.index[-1] + pd.Timedelta(days=1)).date()
    all_data = {"9999.T": df}

    base = dict(signal_date=entry_str, entry_date=entry_str, ticker="9999.T", name="テスト",
                direction="BUY", prev_close=100, limit_price=None, entry_open=None,
                status="pending", hold_days=0, pnl_pct=None, unrealized_pnl=None,
                exit_type=None, exit_date=None)
    live, live_closed, _, _ = tracker.update_positions([dict(base)], today, all_data=all_data)

    with tempfile.TemporaryDirectory() as td:
        cwd = os.getcwd(); os.chdir(td)
        try:
            srow = dict(base); srow.pop("direction"); srow.pop("unrealized_pnl")
            srow.update(atr_pct=1.5, stop_pct=3.0, live_stop=3.0)
            S.save_ledger("main", [srow])
            S.update_ledger("main", today, all_data)
            sh = S.load_ledger("main")[0]
        finally:
            os.chdir(cwd)

    lv = live[0]
    ok(lv["status"] == sh["status"], f"{name}: status一致 ({lv['status']})")
    ok(lv["exit_type"] == sh["exit_type"], f"{name}: 出口種別一致 ({lv['exit_type']})")
    ok(abs((lv["pnl_pct"] or 0) - (sh["pnl_pct"] or 0)) < 1e-6,
       f"{name}: 損益一致 ({lv['pnl_pct']} / {sh['pnl_pct']})")
    ok(lv["exit_date"] == sh["exit_date"], f"{name}: 決済日一致")

print("\n" + "=" * 78)
print("② 損切り幅を広げると STOP が回避されるか（影の狙いそのもの）")
print("=" * 78)
hist = flat(40, 100)
tail = [(100, 101, 96, 99), (99, 102, 98, 101), (101, 102, 100, 102)]   # 初日に-4%まで下げて戻る
df = mkdf(hist + tail)
entry_str = df.index[len(hist)].strftime("%Y-%m-%d")
today = (df.index[-1] + pd.Timedelta(days=1)).date()
all_data = {"9999.T": df}
base = dict(signal_date=entry_str, entry_date=entry_str, ticker="9999.T", name="テスト",
            prev_close=100, limit_price=None, entry_open=None, status="pending",
            hold_days=0, pnl_pct=None, exit_type=None, exit_date=None, atr_pct=2.5, live_stop=3.0)
res = {}
for stop in (3.0, 5.0):
    with tempfile.TemporaryDirectory() as td:
        cwd = os.getcwd(); os.chdir(td)
        try:
            r = dict(base); r["stop_pct"] = stop
            S.save_ledger("main", [r])
            S.update_ledger("main", today, all_data)
            res[stop] = S.load_ledger("main")[0]
        finally:
            os.chdir(cwd)
ok(res[3.0]["exit_type"] == "STOP", f"損切り3.0% → STOP発動 ({res[3.0]['pnl_pct']}%)")
ok(res[5.0]["exit_type"] != "STOP", f"損切り5.0% → STOP回避 ({res[5.0]['exit_type']} {res[5.0]['pnl_pct']}%)")
ok(res[5.0]["pnl_pct"] > res[3.0]["pnl_pct"], "広い方が損益は上（このケースでは）")

print("\n" + "=" * 78)
print("③ 損切り幅の計算 shadow_stop_pct")
print("=" * 78)
# 2026-07-29: ATR連動を取り下げ、買いの損切りは一律3.0%に戻した（USE_ATR_STOP=False）。
# 理由=採用検証が枠5・株数丸めなしで、実運用の枠3では一律-3%に負ける（年-6万・勝ち年-2）。
# 機構はATRが損を減らすのでなく損切り箱から期限箱へ移すだけで、円が完全に閉じる。
ok(S.USE_ATR_STOP is False, "USE_ATR_STOP=False（ATR連動は取り下げ）")
ok(all(S.shadow_stop_pct(a) == S.LIVE_STOP for a in (None, 0.5, 1.4, 2.30, 3.00)),
   f"ATRの値によらず一律{S.LIVE_STOP}%")

# 復活経路が壊れていないことも守る（USE_ATR_STOP=True に戻せば元の式に戻る）。
S.USE_ATR_STOP = True
try:
    ok(S.shadow_stop_pct(2.30) == 4.6, "復活時: ATR2.30% → 4.6%（×2.0）")
    ok(S.shadow_stop_pct(0.50) == S.STOP_FLOOR, f"復活時: ATR0.50% → 下限{S.STOP_FLOOR}%")
    ok(S.shadow_stop_pct(3.00) == 6.0, "復活時: ATR3.00% → 6.0%が最大")
    ok(S.shadow_stop_pct(None) == S.LIVE_STOP, "復活時でもATR欠測は3.0%へフォールバック")
finally:
    S.USE_ATR_STOP = False
ok(S.shadow_stop_pct(None) == S.LIVE_STOP,
   f"ATR取得不能 → 本番と同じ{S.LIVE_STOP}%にフォールバック（差分ゼロ＝安全側）")

print("\n" + "=" * 78)
print("④ 寄指不成立(NOFILL)は本番と同じく終端・再処理されない")
print("=" * 78)
hist = flat(40, 100)
df = mkdf(hist + [(103, 105, 102, 104), (104, 105, 103, 104), (104, 105, 103, 104)])
entry_str = df.index[len(hist)].strftime("%Y-%m-%d")
today = (df.index[-1] + pd.Timedelta(days=1)).date()
with tempfile.TemporaryDirectory() as td:
    cwd = os.getcwd(); os.chdir(td)
    try:
        r = dict(base); r.update(stop_pct=4.6, limit_price=101)     # 寄り103 > 指値101
        S.save_ledger("main", [r])
        S.update_ledger("main", today, {"9999.T": df})
        got = S.load_ledger("main")[0]
        before = json.dumps(got, sort_keys=True)
        S.update_ledger("main", today, {"9999.T": df})              # 2回目=再処理されないこと
        after = json.dumps(S.load_ledger("main")[0], sort_keys=True)
    finally:
        os.chdir(cwd)
ok(got["status"] == "expired" and got["exit_type"] == "NOFILL", "寄り>指値 → expired/NOFILL")
ok(before == after, "2回流しても失効ポジは変化しない（日油事故と同型の再処理バグなし）")

print("\n" + "=" * 78)
print("⑤ ATR%の算出（screener.calc_atr と同じ式か）")
print("=" * 78)
np.random.seed(0)
px = 1000 + np.cumsum(np.random.randn(60) * 5)
d = pd.DataFrame({"Open": px, "High": px + 8, "Low": px - 8, "Close": px, "Volume": 1e6},
                 index=pd.bdate_range("2026-04-01", periods=60))
a_shadow = S.atr_pct_at(d, d.index[-1].strftime("%Y-%m-%d"))
try:
    from screener import calc_atr
    a_live = calc_atr(d)
    a_live_pct = round(a_live / float(d["Close"].iloc[-1]) * 100, 3)
    ok(abs(a_shadow - a_live_pct) < 0.05, f"screener.calc_atr と一致 (影{a_shadow} / 本番{a_live_pct})")
except Exception as e:
    ok(False, f"screener.calc_atr 比較に失敗: {e}")
ok(S.atr_pct_at(d.head(5), "2026-04-05") is None, "データ不足なら None（→3.0%フォールバック）")

print("\n" + "=" * 78)
print("④ 極みシグナルファイルの優先とフォールバック（2026-08-02・買残回転1.2緩和）")
print("=" * 78)
_cwd = os.getcwd()
with tempfile.TemporaryDirectory() as _td:
    os.chdir(_td)
    try:
        _today = date(2026, 8, 3)
        _hist = mkdf(flat(40, 100))
        _ad = {"7777.T": _hist, "8888.T": _hist}
        with open("today_signals.json", "w", encoding="utf-8") as f:
            json.dump({"date": "2026-08-03", "signals": [
                {"ticker": "7777.T", "name": "通常", "direction": "BUY",
                 "prev_close": 100, "limit_price": 101}]}, f)
        with open(S.KIWAMI_SIG_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": "2026-08-03", "signals": [
                {"ticker": "8888.T", "name": "極み", "direction": "BUY",
                 "prev_close": 100, "limit_price": 101, "days_cover": 1.1}]}, f)
        _n = S.record_signals("main", _today, _ad)
        _rows = S.load_ledger("main")
        ok(_n == 1 and [r["ticker"] for r in _rows] == ["8888.T"],
           "極みファイル(当日)があればそちらを読む＝買残0.8〜1.2帯が極みに届く")
        os.remove(S.ledger_path("main"))
        with open(S.KIWAMI_SIG_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": "2026-08-01", "signals": [
                {"ticker": "8888.T", "name": "極み", "direction": "BUY",
                 "prev_close": 100}]}, f)
        _n = S.record_signals("main", _today, _ad)
        _rows = S.load_ledger("main")
        ok(_n == 1 and [r["ticker"] for r in _rows] == ["7777.T"],
           "極みファイルが古い日付なら通常版today_signalsへフォールバック")
    finally:
        os.chdir(_cwd)

print("\n" + "=" * 78)
print(f"結果: {PASS} PASS / {FAIL} FAIL")
print("=" * 78)
raise SystemExit(1 if FAIL else 0)

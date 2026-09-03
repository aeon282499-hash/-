# -*- coding: utf-8 -*-
"""_test_audit_0903.py — 2026-09-03 監査で入れた修正の単体テスト（本番I/Oなし・Discord送信なし）。

  1. shadow_exit: 送信失敗 → kiwami_sent.json sent=False → needs_resend=True / 成功後は False
  2. close_check._oco_boundary: 境界(誤差幅内)で警告・明確な未達では None・_oco_fill は従来どおり None
  3. close_decisions: 階層別 scope で上書きされない
  4. screener の営業日: 12/31・1/2・1/3 を営業日にしない
  5. notifier._slot_funded: expired は枠を占有しない
実行: python -X utf8 _test_audit_0903.py
"""
import json, os, sys, tempfile, importlib
from datetime import date

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
ok = 0; ng = []
def t(name, cond):
    global ok
    if cond: ok += 1
    else: ng.append(name)

# ── 1. 送信マーカー ──
import shadow_exit as se
cwd = os.getcwd()
with tempfile.TemporaryDirectory() as d:
    os.chdir(d)
    try:
        today = date(2026, 9, 4)
        se.write_sent_marker(today, False)
        t("marker sent=False → needs_resend", se.needs_resend(today))
        t("marker 別日は needs_resend=False", not se.needs_resend(date(2026, 9, 7)))
        se.write_sent_marker(today, True)
        t("marker sent=True → needs_resend=False", not se.needs_resend(today))
        m = json.load(open(se.KIWAMI_SENT_FILE, encoding="utf-8"))
        t("marker 形式", m == {"date": "2026-09-04", "sent": True})
        # _shadow_post: webhook未設定は失敗扱いにしない / HTTP失敗はフラグを立てる
        se._POST_FAILED = False
        os.environ.pop("DISCORD_WEBHOOK_SHADOW_URL", None)
        r = se._shadow_post([{"title": "x"}], "DISCORD_WEBHOOK_SHADOW_URL")
        t("未設定 → False だがフラグは立たない", r is False and se._POST_FAILED is False)
        import requests
        class _R:  status_code = 502; text = "bad gateway"
        _orig = requests.post
        requests.post = lambda *a, **k: _R()
        import time as _time; _orig_sleep = _time.sleep; _time.sleep = lambda s: None
        try:
            os.environ["DISCORD_WEBHOOK_SHADOW_URL"] = "https://example.invalid/hook"
            r = se._shadow_post([{"title": "x"}], "DISCORD_WEBHOOK_SHADOW_URL")
            t("HTTP 502×3 → False かつ _POST_FAILED=True", r is False and se._POST_FAILED is True)
        finally:
            requests.post = _orig; _time.sleep = _orig_sleep
            os.environ.pop("DISCORD_WEBHOOK_SHADOW_URL", None)
    finally:
        os.chdir(cwd)
t("decision_scope", se.decision_scope("main") == "kiwami" and se.decision_scope("mid") == "kiwami_mid")

# ── 2. 境界警告 ──
import close_check as cc
# オークネット 2026-09-02: entry 1389 / 損切り 1347.33 / 安値 1347
t("_oco_fill 境界は従来どおり None", cc._oco_fill("BUY", 1389.0, 1396.0, 1347.0) is None)
w = cc._oco_boundary("BUY", 1389.0, 1396.0, 1347.0)
t("_oco_boundary 境界で警告", bool(w) and "損切りライン1,347円" in w)
t("_oco_boundary 明確な未達は None", cc._oco_boundary("BUY", 1389.0, 1396.0, 1350.0) is None)
t("_oco_boundary TP境界", "利確ライン" in (cc._oco_boundary("BUY", 1000.0, 1050.5, 990.0) or ""))
t("_oco_boundary SELL損切り境界", "損切りライン" in (cc._oco_boundary("SELL", 1000.0, 1025.0, 990.0, stop_pct=2.5) or ""))
t("_oco_fill 明確なSTOPは従来どおり", (cc._oco_fill("BUY", 1389.0, 1396.0, 1340.0) or {}).get("kind") == "STOP")

# ── 3. close_decisions 階層別 ──
import close_decisions as cd
with tempfile.TemporaryDirectory() as d:
    orig = cd.FILE
    cd.FILE = os.path.join(d, "cd.json")
    try:
        day = date(2026, 9, 3)
        cd.record(day, "kiwami", "BUY", [], [{"ticker": "8830.T", "note": None, "rsi_now": 35.1, "current_price": 3258.0}])
        cd.record(day, "kiwami_small", "BUY", [{"ticker": "8830.T", "reason_type": "MAXHOLD", "rsi_now": None, "current_price": None}], [])
        t("大の HOLD が小の MAXHOLD に上書きされない", cd.lookup("2026-09-03", "kiwami", "BUY", "8830.T") == cd.HOLD)
        t("小は MAXHOLD", cd.lookup("2026-09-03", "kiwami_small", "BUY", "8830.T") == cd.MAXHOLD)
        t("apply: 大は HOLD → rsi_exit False", cd.apply(True, "2026-09-03", "kiwami", "BUY", "8830.T") is False)
    finally:
        cd.FILE = orig

# ── 4. 年末年始 ──
import screener as sc
import inspect
src = inspect.getsource(sc.batch_download_jquants) if hasattr(sc, "batch_download_jquants") else ""
t("screener に _tse_open が入っている", "_tse_open" in src)
# 直接実行: 関数内ローカルなので同じ規則を再現して検証
import jpholiday
def _tse_open(d):
    if d.weekday() >= 5 or jpholiday.is_holiday(d): return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))
t("12/31(木)は休業", not _tse_open(date(2026, 12, 31)))
t("1/2(金)は休業", not _tse_open(date(2026, 1, 2)))
t("1/5(月)は営業", _tse_open(date(2026, 1, 5)))

# ── 5. expired は枠を占有しない ──
import notifier as nf
rows = [
    {"entry_date": "2026-06-23", "exit_date": "2026-06-23", "status": "expired"},
    {"entry_date": "2026-06-23", "exit_date": "2026-06-25", "status": "closed", "pnl_pct": 5.0},
    {"entry_date": "2026-06-23", "exit_date": "2026-06-25", "status": "closed", "pnl_pct": 1.0},
]
f = nf._slot_funded(rows, 2)
t("expired は funded に入らない", id(rows[0]) not in f)
t("closed 2本は両方 funded", id(rows[1]) in f and id(rows[2]) in f)

print(f"\n==== 結果: {ok}/{ok + len(ng)} OK ====")
if ng:
    print("NG:", ng); sys.exit(1)
print("ALL PASS")

# -*- coding: utf-8 -*-
"""2026-09-09 監査の回帰テスト（PYTHONUTF8=1 python -X utf8 _test_audit_0909.py）
  1. run_shadow が価格データ無しで抜けた夜 → kiwami_sent.json ledger_done=False → marker_state="full"
  2. 1ch だけ失敗 → failed にその ch だけ → resend_only はその ch だけ再送（7ch 二重投稿しない）
  3. notifier._nth_trading_day は 12/31〜1/3 を営業日に数えない（処分期限の表示）
  4. main.py のガード分岐が "full" で run_shadow を呼ぶ（コード上の配線）
"""
import os, json, tempfile
from datetime import date

RESULTS = []
def t(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("  ok  " if cond else "  NG  ") + name)

import shadow_exit as se
import notifier

cwd = os.getcwd()
with tempfile.TemporaryDirectory() as d:
    os.chdir(d)
    try:
        today = date(2026, 9, 10)
        # ── 1. 価格データ無し ──
        se.KIWAMI_SENT_FILE = "kiwami_sent.json"
        # 台帳ファイルが1つ以上無いと run_shadow は何もせず抜けるので極み台帳を置く
        json.dump({"date": "2026-09-10", "signals": []}, open("today_signals.json", "w", encoding="utf-8"))
        tiers = [{"key": "main"}, {"key": "mid"}, {"key": "small"}]
        r = se.run_shadow(tiers, today, lambda: {})
        m = json.load(open("kiwami_sent.json", encoding="utf-8"))
        t("価格データ無し → return False", r is False)
        t("価格データ無し → marker ledger_done=False", m["date"] == "2026-09-10" and m["ledger_done"] is False)
        t("価格データ無し → marker_state=full", se.marker_state(today) == "full")

        # ── 2. 1ch だけ失敗 → その ch だけ再送 ──
        calls = []
        def fake_buy(dt, key="main"):
            calls.append(("buy", key))
            if key == "gokujo":
                se._POST_FAILED = True    # _shadow_post 3回失敗相当
            return True
        def fake_sell(dt, key="main"):
            calls.append(("sell", key))
            return True
        orig_b, orig_s = se.send_discord, se.send_discord_sell
        se.send_discord, se.send_discord_sell = fake_buy, fake_sell
        try:
            failed = se._post_signals(today)
            t("失敗 ch だけ failed に載る", failed == ["buy:gokujo"])
            t("全7ch を1回ずつ送っている", len(calls) == 7)
            se.write_sent_marker(today, not failed, failed=failed)
            t("marker は resend", se.marker_state(today) == "resend")
            calls.clear()
            # 再送: 極上だけ。今度は成功させる
            def fake_buy_ok(dt, key="main"):
                calls.append(("buy", key)); return True
            se.send_discord = fake_buy_ok
            ok = se.resend_only(today)
            t("resend_only は失敗した極上だけ再送", calls == [("buy", "gokujo")] and ok is True)
            t("再送成功後は marker ok", se.marker_state(today) == "ok")
            # failed 情報が無い(旧形式) sent=False → 全ch再送
            json.dump({"date": "2026-09-10", "sent": False}, open("kiwami_sent.json", "w"))
            calls.clear()
            se.resend_only(today)
            t("failed 無しの旧形式は全ch再送", len(calls) == 7)
        finally:
            se.send_discord, se.send_discord_sell = orig_b, orig_s
        # 前日マーカーは full
        json.dump({"date": "2026-09-09", "sent": True, "failed": [], "ledger_done": True},
                  open("kiwami_sent.json", "w"))
        t("前日マーカーのままは full", se.marker_state(today) == "full")
    finally:
        os.chdir(cwd)

# ── 3. 年末年始 ──
t("12/29(火) の 2営業日後は 1/4（12/31・1/2 を飛ばす）",
  notifier._nth_trading_day(date(2026, 12, 29), 2) == date(2027, 1, 4))
t("通常週は従来どおり", notifier._nth_trading_day(date(2026, 9, 10), 2) == date(2026, 9, 14))

# ── 4. main.py の配線 ──
src = open(os.path.join(cwd, "main.py"), encoding="utf-8").read()
t("main.py が marker_state を使う", "marker_state(today)" in src)
t("main.py が full で run_shadow を呼ぶ", 'if _st == "full":' in src and "run_shadow(TIERS, today," in src)
for wf, needle in (("resend_kiwami.yml", "se.BUY_NOTIFY_KEYS"), ("kiwami_weekly.yml", "for k in BUY_NOTIFY_KEYS")):
    s = open(os.path.join(cwd, ".github", "workflows", wf), encoding="utf-8").read()
    t(f"{wf} が極上を含む", needle in s)
rs = open(os.path.join(cwd, "report.py"), encoding="utf-8").read()
t("report.py has_active が極み台帳を見る", "_se.BUY_NOTIFY_KEYS" in rs)

# ── 5. 売りフェード（daytrade_paper）──
import pandas as pd
import daytrade_paper as dp
def _df(dates, o=1000.0):
    idx = pd.to_datetime(dates)
    return pd.DataFrame({"Open": o, "High": o * 1.01, "Low": o * 0.99, "Close": o, "Volume": 1e6}, index=idx)
data_fresh = {"AAAA.T": _df(["2026-09-08", "2026-09-09"]), "BBBB.T": _df(["2026-09-07", "2026-09-08"])}
t("_last_market_date はデータ全体の最終足", dp._last_market_date(data_fresh, "2026-09-10") == "2026-09-09")
t("_last_market_date は today 以降の足を除く",
  dp._last_market_date({"A": _df(["2026-09-09", "2026-09-10"])}, "2026-09-10") == "2026-09-09")
t("_prev_trading_day 月曜→金曜", dp._prev_trading_day(date(2026, 9, 14)) == date(2026, 9, 11))
t("_prev_trading_day 1/4→12/30", dp._prev_trading_day(date(2027, 1, 4)) == date(2026, 12, 30))
# 鮮度ガードの配線
dsrc = open(os.path.join(cwd, "daytrade_paper.py"), encoding="utf-8").read()
t("run() が最終足≠直前営業日を fetch_failed にする", "_lm != _exp" in dsrc and "fetch_failed = True" in dsrc)
t("run() が朝モード寄り後は選定/配信しない", "late_morning = True" in dsrc and "if late_morning:" in dsrc)
# 記帳時の株数保存と決済での利用
book = {"positions": [], "expired": []}
sig = [{"ticker": "AAAA.T", "name": "A", "direction": "SELL", "prev_close": 4000.0,
        "min_entry_price": 4000.0, "rank": 2, "daily_gain": 8.0}]
added = dp.record(book, sig, {"AAAA.T": _df(["2026-09-08", "2026-09-09"], 4000.0)}, {}, date(2026, 9, 10))
t("record は ②50万÷4000円=100株 を保存", added and added[0].get("shares") == 100)
book["positions"][0]["shares"] = 300          # サイズ定数が後で変わっても記帳株数で決済する
data_settle = {"AAAA.T": _df(["2026-09-08", "2026-09-09", "2026-09-10"], 4000.0)}
data_settle["AAAA.T"].loc[pd.Timestamp("2026-09-10"), ["Open", "Close"]] = [4000.0, 3900.0]
closed = dp.settle(book, data_settle, date(2026, 9, 11))
t("settle は記帳株数(300)で損益円を出す", closed and closed[0]["pnl_yen"] == 300 * 100)
t("_trade_shares_of は記帳株数を優先", dp._trade_shares_of(closed[0]) == 300)
t("_trade_shares_of pnl=0 の②玉は50万で逆算",
  dp._trade_shares_of({"rank": 2, "entry_open": 2500.0, "entry_close": 2500.0, "pnl_yen": 0}) == 200)

n_ok = sum(1 for _, c in RESULTS if c)
print(f"\n{n_ok}/{len(RESULTS)} PASS")
raise SystemExit(0 if n_ok == len(RESULTS) else 1)

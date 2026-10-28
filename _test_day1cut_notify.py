# -*- coding: utf-8 -*-
"""_test_day1cut_notify.py — 極上の1日目-1%割れ「明朝 寄成で売り」が15:00通知と夜の配信に出る（2026-10-28 ソフトバンク10/27で出なかった件）"""
import os, sys, types, tempfile
from datetime import date
import pandas as pd
sys.stdout.reconfigure(encoding="utf-8")

class _FakeTicker:
    plan = {}
    def __init__(self, t): self.t = t
    def history(self, period=None, interval=None, **kw): return _FakeTicker.plan.get(self.t, pd.DataFrame())
fy = types.ModuleType("yfinance"); fy.Ticker = _FakeTicker; sys.modules["yfinance"] = fy

import close_check as CC, kiwami_close as KC, shadow_exit as SE
ok = fail = 0
def check(lab, cond):
    global ok, fail
    print(("  ok  " if cond else "  NG  ") + lab); ok += bool(cond); fail += (not cond)

# ソフトバンク: 10/27 寄り235.7で建て(寄指239)・15:00に231円(-2.0%)・下落基調でRSI<50
idx = pd.bdate_range("2026-08-03", "2026-10-26")
cl = [260 - i * 0.4 for i in range(len(idx))]
hist = pd.DataFrame({"Open": cl, "High": [c + 1 for c in cl], "Low": [c - 1 for c in cl], "Close": cl}, index=idx)
_FakeTicker.plan["9434.T"] = pd.DataFrame({"Open": [235.7, 233.0], "High": [235.9, 233.5], "Low": [232.0, 230.2], "Close": [233.0, 231.0]})
pos = {"ticker": "9434.T", "name": "ソフトバンク", "direction": "BUY", "signal_date": "2026-10-27", "entry_date": "2026-10-27",
       "prev_close": 237.4, "limit_price": 239, "entry_open": None, "status": "pending", "stop_pct": 3.0}
today = date(2026, 10, 27)
tg, ck = CC.collect_targets([pos], "BUY", today, {"9434.T": hist})
check(f"15:00判定は保有継続(targetsなし)・checkedに建値235.7が載る: {ck and ck[0].get('entry_open')}",
      tg == [] and len(ck) == 1 and ck[0].get("entry_open") == 235.7 and ck[0].get("today_hold") == 1)
emb = KC.build_embeds(tg, ck, today, [pos], brand="極上")
txt = "\n".join(e.get("description", "") for e in emb)
check("15:00通知に『⛔ … 明朝 寄り成行で処分』が出る", "⛔" in txt and "明朝 寄り成行で処分" in txt)

# -1%割れでない1日目は出ない
_FakeTicker.plan["9434.T"] = pd.DataFrame({"Open": [235.7, 235.0], "High": [236.5, 236.0], "Low": [234.0, 234.5], "Close": [235.0, 235.2]})
tg, ck = CC.collect_targets([dict(pos)], "BUY", today, {"9434.T": hist})
txt = "\n".join(e.get("description", "") for e in KC.build_embeds(tg, ck, today, [pos], brand="極上"))
check("-0.2%なら⛔は出ない", "⛔" not in txt)

# 夜の極上配信: 台帳に day1_cut_pending の保有玉 → 「明朝 寄成で売り」・買いシグナルなしでも出る
rows = [{"signal_date": "2026-10-27", "entry_date": "2026-10-27", "ticker": "9434.T", "name": "ソフトバンク", "size": 1500000,
         "prev_close": 237.4, "limit_price": 239, "stop_pct": 3.0, "entry_open": 235.7, "status": "open", "hold_days": 1,
         "pnl_pct": None, "exit_type": None, "exit_date": None, "day1_cut_pending": True}]
sent = []
SE.load_ledger = lambda k: rows
SE._shadow_post = lambda embeds, env=None: sent.append(embeds) or True
SE._mirror_post = lambda embeds: 0
cwd = os.getcwd(); os.chdir(tempfile.mkdtemp())       # シグナルファイル無し＝買いなしの夜
try:
    SE.send_discord(date(2026, 10, 28), key=SE.GOKUJO_KEY)
finally:
    os.chdir(cwd)
allt = "\n".join((e.get("title", "") + "\n" + e.get("description", "")) for e in (sent[0] if sent else []))
check("夜の配信に『明朝 寄成で売り』が出る", "明朝 寄成で売り" in allt and "ソフトバンク" in allt)
check("買いシグナルなしの表示も残る", "シグナルなし" in allt)
rows[0]["day1_cut_pending"] = False; sent.clear(); os.chdir(tempfile.mkdtemp())
try:
    SE.send_discord(date(2026, 10, 28), key=SE.GOKUJO_KEY)
finally:
    os.chdir(cwd)
allt = "\n".join((e.get("title", "") + "\n" + e.get("description", "")) for e in (sent[0] if sent else []))
check("該当なしなら売りの行は出ない", "明朝 寄成で売り" not in allt)
print(f"\n{ok} PASS / {fail} FAIL"); sys.exit(1 if fail else 0)

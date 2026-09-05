# -*- coding: utf-8 -*-
"""_test_gokujo.py — 「スイング極上」(1枠×300万・5日出来高トレンド≤1.09) のテスト（2026-09-05）。

①screener.calc_vol_trend5 がBT(_bt_kiwami_gokujo_0905.py)と同じ式 ②main.py の極上ファイル生成ロジック(vt5フィルタ→score順)
③shadow_exit.record_signals(gokujo)=専用ファイルのみ・1枠・300万・値がさ1万円 ④配信文面(👑/1銘柄/300万) ⑤週次は買いだけ
⑥kiwami_close が極上台帳を読む ⑦極み/通常版の挙動が不変（既存キーのslots/size/px_cap）
実行: python -X utf8 _test_gokujo.py
"""
from __future__ import annotations

import json, os, sys, tempfile
from datetime import date

import numpy as np
import pandas as pd

os.environ.setdefault("DISCORD_WEBHOOK_GOKUJO_URL", "http://example.invalid/hook")
import shadow_exit as SE
import kiwami_close as KC
from screener import calc_vol_trend5, yose_limit_price

PASS = 0; FAIL = 0
def check(name, cond):
    global PASS, FAIL
    PASS += cond; FAIL += (not cond); print(("  ok  " if cond else "  NG  ") + name)

tmp = tempfile.mkdtemp(); os.chdir(tmp)
TODAY = date(2026, 9, 7)

# ── ① vt5 の式 ──
v = np.array([100.0] * 20 + [50.0] * 5)                      # 前20日=100・直近5日=50 → 0.5
df = pd.DataFrame({"Volume": v, "Close": np.linspace(100, 110, 25)})
check("vt5 = 直近5日平均÷前20日平均 (0.5)", calc_vol_trend5(df) == 0.5)
check("vt5 25本未満はNone", calc_vol_trend5(df.iloc[:24]) is None)
v2 = np.random.default_rng(0).integers(1000, 5000, 60).astype(float)
bt = v2[-5:].mean() / v2[-25:-5].mean()                        # BTの式 v[t-4..t] / v[t-24..t-5]
check("vt5 BT式と一致（60本）", abs(calc_vol_trend5(pd.DataFrame({"Volume": v2, "Close": v2})) - round(bt, 3)) < 1e-9)

# ── ② main.py の極上プール抽出（同じ式を再現） ──
all_buy = [
    {"ticker": "1111.T", "name": "高出来高", "prev_close": 1000, "vt5": 1.8, "days_cover": 0.5, "rsi": 38, "deviation": -3, "turnover": 5e9},
    {"ticker": "2222.T", "name": "枯れA",   "prev_close": 2000, "vt5": 0.9, "days_cover": 0.5, "rsi": 40, "deviation": -2, "turnover": 3e9},
    {"ticker": "3333.T", "name": "枯れB",   "prev_close": 3000, "vt5": 1.09, "days_cover": 1.1, "rsi": 36, "deviation": -4, "turnover": 4e9},
    {"ticker": "4444.T", "name": "vt5欠損", "prev_close": 1500, "vt5": None, "days_cover": 0.5, "rsi": 39, "deviation": -3, "turnover": 6e9},
    {"ticker": "5555.T", "name": "値がさ",   "prev_close": 12000, "vt5": 0.8, "days_cover": 0.5, "rsi": 38, "deviation": -3, "turnover": 9e9},
    {"ticker": "6666.T", "name": "買残超",  "prev_close": 800, "vt5": 0.7, "days_cover": 1.5, "rsi": 38, "deviation": -3, "turnover": 2e9},
]
pool = [c for c in all_buy if c.get("vt5") is not None and c["vt5"] <= SE.GOKUJO_VT5_MAX and (c.get("prev_close") or 0) <= SE.GOKUJO_PX_CAP]
check("vt5≤1.09・有限・1万円以下だけ残る", [c["ticker"] for c in pool] == ["2222.T", "3333.T", "6666.T"])
import main as M
sig, _ = M._select_tier_signals(pool, [], {"key": "gokujo", "label": "極上", "size": SE.GOKUJO_SIZE}, [], [], 5, dc_max=1.2)
check("買残1.2超は選定で落ちる（score順は維持）", [c["ticker"] for c in sig] == ["2222.T", "3333.T"])

# ── ③ record_signals(gokujo) ──
json.dump({"date": TODAY.isoformat(), "signals": [
    {"ticker": "2222.T", "name": "枯れA", "direction": "BUY", "prev_close": 2000.0, "limit_price": yose_limit_price(2000.0)},
    {"ticker": "3333.T", "name": "枯れB", "direction": "BUY", "prev_close": 3000.0, "limit_price": yose_limit_price(3000.0)},
    {"ticker": "5555.T", "name": "値がさ", "direction": "BUY", "prev_close": 12000.0, "limit_price": yose_limit_price(12000.0)},
]}, open(SE.GOKUJO_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
json.dump({"date": TODAY.isoformat(), "signals": [
    {"ticker": "9999.T", "name": "極みだけ", "direction": "BUY", "prev_close": 1000.0, "limit_price": 1010},
]}, open(SE.KIWAMI_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
added = SE.record_signals("gokujo", TODAY, {})
rows = SE.load_ledger("gokujo")
check("極上は1件だけ記帳（1枠）", added == 1 and len(rows) == 1)
check("記帳した玉はscore順先頭（2222）・size=300万・stop=3.0", rows[0]["ticker"] == "2222.T" and rows[0]["size"] == 3_000_000 and rows[0]["stop_pct"] == 3.0)
check("極みファイル(9999)へはフォールバックしない", all(r["ticker"] != "9999.T" for r in rows))
check("見送り記録に枯れB（枠満杯）", json.load(open("_shadow_skipped_gokujo.json", encoding="utf-8"))["names"] == ["枯れB"])
# 翌日: 保有中なら新規は入らない
json.dump({"date": "2026-09-08", "signals": [{"ticker": "3333.T", "name": "枯れB", "direction": "BUY", "prev_close": 3000.0, "limit_price": 3030}]},
          open(SE.GOKUJO_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
check("保有中(pending)は枠満杯で新規0", SE.record_signals("gokujo", date(2026, 9, 8), {}) == 0)
# 極み(main)は従来どおり3枠・100万
json.dump({"date": TODAY.isoformat(), "signals": [
    {"ticker": f"{i}000.T", "name": f"K{i}", "direction": "BUY", "prev_close": 1000.0, "limit_price": 1010} for i in range(1, 6)
]}, open(SE.KIWAMI_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
check("極み(main)は3枠×100万のまま", SE.record_signals("main", TODAY, {}) == 3 and all(r["size"] == 1_000_000 for r in SE.load_ledger("main")))
check("値がさカット 極上=1万円・極み大=1万円・中=5千円", (SE.kiwami_px_cap("gokujo"), SE.kiwami_px_cap("main"), SE.kiwami_px_cap("mid")) == (10_000, 10_000, 5_000))
check("枠数 極上1・極み3", (SE.max_slots("gokujo"), SE.max_slots("main")) == (1, 3))

# ── ④ 配信文面 ──
captured = []
SE._shadow_post = lambda embeds, env=None: (captured.append((env, embeds)), True)[1]
json.dump({"date": TODAY.isoformat(), "signals": [
    {"ticker": "2222.T", "name": "枯れA", "direction": "BUY", "prev_close": 2000.0, "limit_price": yose_limit_price(2000.0)},
]}, open(SE.GOKUJO_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
SE.send_discord(TODAY, "gokujo")
env, emb = captured[-1]
d = emb[0]["description"]
check("極上の配信先はGOKUJO webhook", env == "DISCORD_WEBHOOK_GOKUJO_URL")
check("タイトルは👑スイング極上", emb[0]["title"].startswith("👑【スイング極上】"))
check("1件300万円・#1を買う（1銘柄・最大保有1）", "1件300万円" in d and "最大保有1" in d)
check("株数は300万基準（前日終値2000円→1,500株・極みと同じ前日終値基準）", "1,500株" in d)
SE.send_discord(TODAY, "main")
env_m, emb_m = captured[-1]
check("極み(main)は従来のタイトル/webhook", emb_m[0]["title"].startswith("⚡【スイング極み】") and env_m == "DISCORD_WEBHOOK_SHADOW_URL")
# シグナルなしの日
json.dump({"date": "2026-09-09", "signals": []}, open(SE.GOKUJO_SIG_FILE, "w", encoding="utf-8"))
SE.load_ledger = (lambda _orig: (lambda k: [] if k == "gokujo" else _orig(k)))(SE.load_ledger)
SE.send_discord(date(2026, 9, 9), "gokujo")
check("極上0件の日は「シグナルなし」を出す", "シグナルなし" in captured[-1][1][0]["title"] and "月3〜4件" in captured[-1][1][0]["description"])

# ── ⑤ 週次は買いだけ（売りchへ二重に出さない） ──
captured.clear()
SE.weekly_report(TODAY, None, [{"status": "closed", "pnl_pct": 2.0, "direction": "SELL", "exit_date": TODAY.isoformat(), "ticker": "7777.T", "name": "売り玉", "entry_open": 100}], key="gokujo")
check("極上の週次は1通（買い）だけ", len(captured) == 1 and captured[0][0] == "DISCORD_WEBHOOK_GOKUJO_URL" and "極上" in captured[0][1][0]["title"])
check("週次footerは1枠", "1枠" in captured[0][1][0]["footer"]["text"])
captured.clear()
SE.weekly_report(TODAY, None, None, key="main")
check("極み(main)の週次は買い＋売りの2通のまま", len(captured) == 2)

# ── ⑥ kiwami_close ──
json.dump([{"ticker": "2222.T", "name": "枯れA", "signal_date": TODAY.isoformat(), "entry_date": TODAY.isoformat(),
            "prev_close": 2000.0, "limit_price": 2020, "entry_open": 2010.0, "status": "open", "stop_pct": 3.0, "atr_pct": 2.0, "size": 3_000_000}],
          open("shadow_exit_gokujo.json", "w", encoding="utf-8"))
op = KC.load_open("gokujo")
check("kiwami_close が極上台帳を読む", len(op) == 1 and op[0]["ticker"] == "2222.T" and KC.TIER_LEDGERS["gokujo"][2] == "DISCORD_WEBHOOK_GOKUJO_URL")
e = KC.build_embeds([{"ticker": "2222.T", "name": "枯れA", "reason_type": "RSI", "rsi_now": 55.0, "current_price": 2050.0, "entry_open": 2010.0, "today_hold": 2}], [], TODAY, op, brand="極上")
check("処分指示のタイトルは👑極上", e[0]["title"].startswith("👑【極上"))
e2 = KC.build_embeds([{"ticker": "2222.T", "name": "枯れA", "reason_type": "RSI", "rsi_now": 55.0, "current_price": 2050.0, "entry_open": 2010.0, "today_hold": 2}], [], TODAY, op)
check("既定(極み)のタイトルは従来どおり⚡極み", e2[0]["title"].startswith("⚡【極み"))

# ── ⑦ run_shadow が極上キーを含む（送信はモック） ──
sent = []
SE.send_discord = lambda today, key="main": sent.append(("buy", key)) or True
SE.send_discord_sell = lambda today, key="main": sent.append(("sell", key)) or True
SE.monthly_report = lambda today: True
SE.record_signals = lambda key, today, all_data: 0
SE.update_ledger = lambda key, today, all_data: (0, 0)
SE.update_sell_ledger = lambda today, all_data: 0
SE.record_sell_signals = lambda today: 0
tiers = [{"key": "main"}, {"key": "mid"}, {"key": "small"}]
SE.run_shadow(tiers, TODAY, lambda: {"x": pd.DataFrame({"Close": [1.0]})})
check("買い配信は main/mid/small/gokujo・売り配信は3階層だけ",
      [k for t, k in sent if t == "buy"] == ["main", "mid", "small", "gokujo"] and [k for t, k in sent if t == "sell"] == ["main", "mid", "small"])

print(f"\n{PASS} PASS / {FAIL} FAIL")
sys.exit(1 if FAIL else 0)

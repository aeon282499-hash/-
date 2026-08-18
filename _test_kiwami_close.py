# -*- coding: utf-8 -*-
"""_test_kiwami_close.py — 「売買シグナル極み」の出口管理テスト（2026-07-26）。

実弾で回す経路なので、①通常版の挙動が不変 ②銘柄別損切りが正しく効く
③極みの帳簿から保有玉を正しく取り出す ④重複エントリーを防ぐ を確認する。

実行: python -X utf8 _test_kiwami_close.py
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date

import close_check as CC
import kiwami_close as KC
import shadow_exit as SE

ok = ng = 0


def check(cond: bool, label: str) -> None:
    global ok, ng
    if cond:
        ok += 1
        print(f"  OK  {label}")
    else:
        ng += 1
        print(f"  NG  {label}")


print("■ ① 通常版(_oco_fill既定)の挙動が不変")
check(CC._oco_fill("BUY", 1000, 1010, 960) == {"kind": "STOP", "pnl_pct": -3.0,
                                               "level": 970.0, "hit": 960},
      "BUY -3%割れでSTOP・pnl-3.0")
check(CC._oco_fill("BUY", 1000, 1060, 990)["kind"] == "TP", "BUY +5%超でTP")
check(CC._oco_fill("BUY", 1000, 1010, 971) is None, "境界内は未約定（安全側）")
check(CC._oco_fill("SELL", 1000, 1035, 990)["pnl_pct"] == -3.0, "SELL踏み上げSTOP -3.0")
check(CC._oco_fill("SELL", 1000, 1010, 940)["kind"] == "TP", "SELL利確TP")
check(CC._oco_fill("BUY", None, 1010, 960) is None, "entry欠落はNone")

print("■ ② 極みの銘柄別損切り")
r = CC._oco_fill("BUY", 1000, 1010, 940, stop_pct=5.6)
check(r == {"kind": "STOP", "pnl_pct": -5.6, "level": 944.0, "hit": 940},
      "stop5.6%→944で約定・pnl-5.6")
check(CC._oco_fill("BUY", 1000, 1010, 950, stop_pct=5.6) is None,
      "stop5.6%なら-5%到達でも未約定（通常版なら約定していた＝取り違え防止）")
check(CC._oco_fill("BUY", 1000, 1010, 975, stop_pct=2.0)["pnl_pct"] == -2.0,
      "下限2.0%の玉も正しく判定")
check(CC._oco_fill("BUY", 1000, 1060, 940, stop_pct=5.6)["kind"] == "STOP",
      "STOP優先（TPと同日ならSTOP・BT同順）")

print("■ ③ 極み帳簿からの保有玉抽出")
tmp = tempfile.mkdtemp()
led = os.path.join(tmp, "ledger.json")
rows = [
    {"ticker": "1721.T", "name": "コムシス", "signal_date": "2026-07-22",
     "entry_date": "2026-07-22", "status": "open", "stop_pct": 5.64,
     "atr_pct": 2.82, "entry_open": 5200.0, "prev_close": 5188.0, "limit_price": 5240},
    {"ticker": "9999.T", "name": "決済済み", "signal_date": "2026-07-01",
     "entry_date": "2026-07-01", "status": "closed", "stop_pct": 4.0},
    {"ticker": "8888.T", "name": "失効", "signal_date": "2026-07-02",
     "entry_date": "2026-07-02", "status": "expired", "stop_pct": 4.0},
    {"ticker": "7777.T", "name": "寄指待ち", "signal_date": "2026-07-24",
     "entry_date": "2026-07-24", "status": "pending", "stop_pct": 3.0, "limit_price": 1010},
]
json.dump(rows, open(led, "w", encoding="utf-8"), ensure_ascii=False)
_orig = KC.LEDGER
KC.LEDGER = led
got = KC.load_open()
KC.LEDGER = _orig
check([p["ticker"] for p in got] == ["1721.T", "7777.T"],
      "open/pendingだけ拾い closed/expired は除外")
check(got[0]["stop_pct"] == 5.64, "銘柄別損切りが引き継がれる")
check(got[0]["direction"] == "BUY", "directionはBUY固定（売りは極み対象外）")
check(all("stop_pct" in p for p in got), "全玉にstop_pctがある")

print("■ ④ 重複エントリー防止（極みが保有中の銘柄は再エントリーしない）")
d = tempfile.mkdtemp()
cwd = os.getcwd()
os.chdir(d)
try:
    json.dump({"date": "2026-07-27", "signals": [
        {"ticker": "1721.T", "name": "コムシス", "direction": "BUY",
         "prev_close": 5000, "limit_price": 5050},
        {"ticker": "6501.T", "name": "日立", "direction": "BUY",
         "prev_close": 4000, "limit_price": 4040},
        {"ticker": "8306.T", "name": "三菱UFJ", "direction": "SELL", "prev_close": 2000},
    ]}, open("today_signals.json", "w", encoding="utf-8"), ensure_ascii=False)
    json.dump([{"ticker": "1721.T", "name": "コムシス", "signal_date": "2026-07-22",
                "entry_date": "2026-07-22", "status": "open", "stop_pct": 5.64}],
              open("shadow_exit_main.json", "w", encoding="utf-8"), ensure_ascii=False)
    added = SE.record_signals("main", date(2026, 7, 27), {})
    after = json.load(open("shadow_exit_main.json", encoding="utf-8"))
    tks = [r["ticker"] for r in after]
    check(added == 1, f"追加は1件のみ（実際{added}件）")
    check(tks.count("1721.T") == 1, "保有中のコムシスは二重に建てない")
    check("6501.T" in tks, "新規の日立は取り込む")
    check("8306.T" not in tks, "SELLは対象外")
finally:
    os.chdir(cwd)

print("■ ⑤ 通知の組み立て")
pos = [{"ticker": "1721.T", "name": "コムシス", "stop_pct": 5.64, "atr_pct": 2.82},
       {"ticker": "6501.T", "name": "日立", "stop_pct": 2.0, "atr_pct": 0.9}]
emb = KC.build_embeds(
    targets=[{"ticker": "1721.T", "name": "コムシス", "reason": "RSI回復",
              "current_price": 5300.0, "unrealized_pnl": 1.9}],
    checked=[{"ticker": "6501.T", "name": "日立", "rsi_now": 44.0,
              "current_price": 3950.0, "today_hold": 2, "note": None}],
    today=date(2026, 7, 27), positions=pos)
blob = json.dumps(emb, ensure_ascii=False)
# 2026-08-09改装（本人指示「売買シグナルと一緒の書体にして」・戻さない）に合わせて
# 2026-08-18にピンを更新: ⚡処分指示embed＋🔍大引けチェックembedの2枚構成。
# 「保有継続」は本文へ・旧「取り違え注意」行は廃止・OCO約定済みは✅行でチェック側に載る。
check(any("処分指示" in e["title"] for e in emb), "処分対象の見出しが出る")
check(any("保有継続" in e.get("description", "") for e in emb), "保有継続が本文に出る")
check("-2.0%" in blob, "保有継続に銘柄別の損切り幅が載る")
check(any("大引けチェック" in e["title"] for e in emb), "チェック側は通常版書体のタイトル")

emb2 = KC.build_embeds(targets=[], checked=[
    {"ticker": "1721.T", "name": "コムシス", "note": "本日OCO **-5.6%損切**で決済済み",
     "rsi_now": None, "current_price": None, "today_hold": 2}],
    today=date(2026, 7, 27), positions=pos)
blob2 = json.dumps(emb2, ensure_ascii=False)
check("✅" in blob2 and "決済済み" in blob2, "OCO約定済みが✅行で載る")
check(not any(e["title"].startswith("⚡") for e in emb2), "約定済みだけの日は処分指示embedを出さない")
check(any("処分対象なし" in e["title"] for e in emb2), "タイトルに『処分対象なし』が付く")

print(f"\n==== 結果: {ok}/{ok + ng} OK ====")
print("ALL PASS" if ng == 0 else f"{ng} FAILED")
raise SystemExit(1 if ng else 0)

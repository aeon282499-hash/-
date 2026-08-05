# -*- coding: utf-8 -*-
"""_test_earnings_preview.py — 朝の予備リストの単体テスト。実行: python -X utf8 _test_earnings_preview.py"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date

import pandas as pd

import main_earnings_preview as ep

N = [0]


def ok(cond, label):
    assert cond, f"FAIL: {label}"
    N[0] += 1
    print(f"  ok {label}")


def mkdf(closes, vol=1_000_000, start="2026-06-01"):
    idx = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"Close": closes, "Volume": [vol] * len(closes)}, index=idx)


# RSI: 全部上げ→100 / 全部下げ→0近辺 / フラット→50
ok(ep.rsi14([100 + i for i in range(40)]) > 99, "RSI 連騰≈100")
ok(ep.rsi14([100 - i for i in range(40)]) < 1, "RSI 連落≈0")
ok(abs(ep.rsi14([100.0] * 40) - 50.0) < 1e-9, "RSI フラット=50")
ok(ep.rsi14([100, 101]) is None, "RSI 足不足=None")

# scan: 圏内/ボーダー/価格cap/代金floorの切り分け
down = [3000 - 30 * i for i in range(40)]       # 急落トレンド=低RSI・5日<-3%
up = [1000 + 5 * i for i in range(40)]          # 上昇=高RSI
expensive = [8000 - 8 * i for i in range(40)]   # 下落だが5千円超
thin = [2000 - 5 * i for i in range(40)]        # 下落だが代金薄い

all_data = {"1111.T": mkdf(down), "2222.T": mkdf(up),
            "3333.T": mkdf(expensive), "4444.T": mkdf(thin, vol=100)}
codes = [{"code": c, "name": f"銘柄{c}"} for c in ("1111", "2222", "3333", "4444")]

orig = ep.eh.vol_pass
ep.eh.vol_pass = lambda tk: (tk != "2222.T", 3.0)  # 2222はボラゲートで弾く
try:
    upto = mkdf(down).index[-1].strftime("%Y-%m-%d")
    hits, border, n_gate = ep.scan(all_data, codes, upto)
    ok(n_gate == 3, "ゲート通過3件(2222はボラで落ちる)")
    ok([h["ticker"] for h in hits] == ["1111.T"], "圏内=1111のみ(価格cap/代金floorが効く)")
    ok(hits[0]["rsi"] < 20 and hits[0]["runup5"] < -1, "1111は低RSI×5日マイナス")
finally:
    ep.eh.vol_pass = orig

# open_slots: 帳簿から空き枠
with tempfile.TemporaryDirectory() as td:
    cwd = os.getcwd()
    os.chdir(td)
    try:
        json.dump({"positions": [
            {"status": "pending"},                                   # 今朝決済=解放
            {"status": "extended", "ext_exit_date": "2026-08-06"},   # 当日=占有
            {"status": "extended", "ext_exit_date": "2026-08-05"},   # 昨日売却済=解放
            {"status": "closed"}]},
            open("positions_earnings.json", "w", encoding="utf-8"))
        ok(ep.open_slots("2026-08-06") == 7, "空き枠=15:06見込み(extended当日以降のみ占有)")
    finally:
        os.chdir(cwd)

# embed: 注意書きと枠数
emb = ep.build_embed(hits, border, 100, 3, 3, date(2026, 8, 6))
ok("事前発注しない" in emb["description"], "embedに事前発注禁止")
ok("空き枠**3**" in emb["description"], "embedに空き枠")
ok("15:06" in emb["footer"]["text"], "footerに本配信時刻")

print(f"\nALL {N[0]}/{N[0]} PASS")

# -*- coding: utf-8 -*-
"""_test_friends50.py — 友達用フェード50万移設（2026-08-18）の配線テスト。

①定数 ②値がさカットのcapital連動（友達=5,000円/本人=7,000円） ③配信文面（50万・#1のみ・見送り）
④答え合わせの株数が前日終値基準（BTと同一・寄り値基準に戻ると検出）
選定＝BTの玉単位一致は _audit_friends50_parity.py が担当（こちらはプール不要の高速テスト）。
実行: python -X utf8 _test_friends50.py
"""
import contextlib
import io
import json
import os
from datetime import date

import numpy as np
import pandas as pd

import daytrade_paper as DP
import screener

PASS = 0

def ok(name, cond):
    global PASS
    assert cond, f"FAIL: {name}"
    PASS += 1
    print(f"  ok {name}")

# ── ①定数 ──────────────────────────────────────────
ok("FRIENDS_SIZE=50万", DP.FRIENDS_SIZE == 500_000)
ok("FRIENDS_PICKS=1", DP.FRIENDS_PICKS == 1)
ok("FRIENDS_TOV_MIN=7.5億", DP.FRIENDS_TOV_MIN == 7.5e8)

# ── ②capital連動の値がさカット ─────────────────────────
def _mk_df(px_base, days=26):
    idx = pd.bdate_range(end="2026-08-14", periods=days)
    close = np.full(days, float(px_base))
    close[-1] = px_base * 1.15                      # 前日+15%＝GO級の急騰
    o = close * 0.99
    h = close * 1.10
    lo = close * 0.94                               # 日中レンジ>5%＝張り付きでない
    v = np.full(days, 500_000.0)
    return pd.DataFrame({"Open": o, "High": h, "Low": lo, "Close": close, "Volume": v},
                        index=idx)

data = {"2001.T": _mk_df(2000), "6001.T": _mk_df(6000)}
iss = {"2001": "2", "6001": "2"}                    # 両方貸借○
_orig_fetch = screener.fetch_tse_universe
screener.fetch_tse_universe = lambda: (_ for _ in ()).throw(RuntimeError("test"))  # フェイルオープン経路
try:
    today = date(2026, 8, 17)
    own = [p["ticker"] for p in DP.daily_top_fades(data, today, iss)]
    frd = [p["ticker"] for p in DP.daily_top_fades(data, today, iss,
                                                   tov_min=DP.FRIENDS_TOV_MIN,
                                                   capital=DP.FRIENDS_SIZE)]
finally:
    screener.fetch_tse_universe = _orig_fetch
ok("本人(70万)は6,000円もOK", "6001.T" in own and "2001.T" in own)
ok("友達(50万)は6,000円を値がさ除外", "6001.T" not in frd and "2001.T" in frd)

# ── ③④ run_friends の配信文面と答え合わせ株数 ─────────────────
bak = None
if os.path.exists(DP.FRIENDS_FILE):
    bak = DP.FRIENDS_FILE + ".bak_test50"
    os.replace(DP.FRIENDS_FILE, bak)
try:
    # ③ 配信文面: 偽GO 1本で dry 実行
    fake = [{"verdict": "GO", "ticker": "9999.T", "name": "テスト", "prev_close": 2165.0,
             "min_entry_price": 2165.0, "daily_gain": 15.0, "vol_ratio": 3.0,
             "short": {"mark": "○"}}]
    _orig_picks = DP.daily_top_fades
    DP.daily_top_fades = lambda *a, **k: fake
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            DP.run_friends({}, date(2026, 8, 17), {}, None, None, dry=True)
        out = buf.getvalue()
    finally:
        DP.daily_top_fades = _orig_picks
    ok("文面: 1玉50万円", "1玉50万円" in out)
    ok("文面: 建てられない日は見送り（代打なし）", "見送り" in out and "#2" not in out)
    ok("文面: 株数は前日終値基準200株", "200株" in out)

    # ④ 答え合わせ: prev_close=2,400(→200株) / 寄り2,600(→旧ロジックだと100株)
    json.dump({"last_date": "2026-08-14", "last_weekly": None,
               "picks": [{"ticker": "9998.T", "name": "答え合わせ",
                          "date": "2026-08-14", "prev_close": 2400.0}],
               "history": []},
              open(DP.FRIENDS_FILE, "w", encoding="utf-8"))
    idx = pd.DatetimeIndex([pd.Timestamp("2026-08-14")])
    df14 = pd.DataFrame({"Open": [2600.0], "High": [2650.0], "Low": [2450.0],
                         "Close": [2500.0], "Volume": [1_000_000.0]}, index=idx)
    DP.daily_top_fades = lambda *a, **k: []
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            DP.run_friends({"9998.T": df14}, date(2026, 8, 17), {}, None, None, dry=True)
        out = buf.getvalue()
    finally:
        DP.daily_top_fades = _orig_picks
    # 空売り: 200株×(寄2,600-引2,500)=+20,000円（寄り値基準の旧ロジックなら+10,000円）
    ok("答え合わせ: 前日終値基準の株数で+20,000円", "+20,000円" in out)
    ok("答え合わせ: 寄り値基準(+10,000円)に退行していない", "+10,000円" not in out)
finally:
    if os.path.exists(DP.FRIENDS_FILE):
        os.remove(DP.FRIENDS_FILE)
    if bak:
        os.replace(bak, DP.FRIENDS_FILE)

print(f"friends50: {PASS}/{PASS} PASS")

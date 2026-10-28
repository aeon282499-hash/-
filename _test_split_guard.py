# -*- coding: utf-8 -*-
"""_test_split_guard.py — 保有中の分割/併合で誤STOPしない（2026-10-28 日本ハム 2282 の再現）"""
import sys
from datetime import date
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding="utf-8")
import split_guard as SG, shadow_exit as SE, close_check as CC

ok = fail = 0
def check(lab, cond):
    global ok, fail
    print(("  ok  " if cond else "  NG  ") + lab); ok += bool(cond); fail += (not cond)

# 日本ハム: 9/25 寄り6022(生)・9/29に1→3分割。日足は調整済み(9/26以前は÷3)
days = pd.bdate_range("2026-08-01", "2026-09-30")
px = np.linspace(6400, 6060, len(days))
df = pd.DataFrame({"Open": px, "High": px * 1.005, "Low": px * 0.995, "Close": px, "Volume": 1e5}, index=days)
df.loc["2026-09-25", ["Open", "High", "Low", "Close"]] = [6022, 6080, 6012, 6050]
df.loc["2026-09-26", ["Open", "High", "Low", "Close"]] = [6050, 6090, 6010, 6058]
df.loc["2026-09-29", ["Open", "High", "Low", "Close"]] = [2010 * 3, 2015 * 3, 1941 * 3, 1960 * 3]
df.loc["2026-09-30", ["Open", "High", "Low", "Close"]] = [1965 * 3, 1990 * 3, 1950 * 3, 1980 * 3]
adj = df.copy(); adj[["Open", "High", "Low", "Close"]] = adj[["Open", "High", "Low", "Close"]] / 3   # 分割後の調整済み

check("split_factor=1/3", abs(SG.split_factor(adj, "2026-09-25", 6022) - 1 / 3) < 1e-9)
check("分割なしは1.0", SG.split_factor(df, "2026-09-25", 6022) == 1.0)
check("寄りが建値と数%ずれても1.0（分割扱いしない）", SG.split_factor(df, "2026-09-25", 6022 * 1.05) == 1.0)
check("分割比率でない不整合(1.27倍)は1.0", SG.split_factor(df, "2026-09-25", 6022 * 1.27) == 1.0)
check("1:2分割(0.5倍)は採用", abs(SG.split_factor(df, "2026-09-25", 6022 * 2) - 0.5) < 1e-9)
check("suspect_split: 建値6022に2010は疑い", SG.suspect_split(6022, 2010))
check("suspect_split: -3%は疑いでない", not SG.suspect_split(6022, 5841, 6100))

def pos():
    return dict(signal_date="2026-09-24", entry_date="2026-09-25", ticker="2282.T", name="日本ハム", size=3_000_000,
                prev_close=6039.0, limit_price=6090, stop_pct=3.0, live_stop=3.0, entry_open=6022.0, status="open", hold_days=0)
scope = f"kiwami_{SE.GOKUJO_KEY}"
r = [pos()]; SE.advance(r, date(2026, 10, 1), {"2282.T": adj}, scope=scope)
check(f"調整済み日足でも 9/29 STOP（誤って9/25にしない）: {r[0].get('exit_type')} {r[0].get('exit_date')}",
      r[0].get("exit_type") == "STOP" and r[0].get("exit_date") == "2026-09-29")
check("建値(生の値段)は台帳に残る", r[0]["entry_open"] == 6022.0)
r2 = [pos()]; SE.advance(r2, date(2026, 10, 1), {"2282.T": df}, scope=scope)
check("未調整日足でも同じ結果", r2[0].get("exit_type") == "STOP" and r2[0].get("exit_date") == "2026-09-29")

# 引け後の当日足OCO確定: 分割当日に建値(生)と当日足(分割後)の基準がずれていても STOP と断定しない
f = CC._final_fills([dict(ticker="2282.T", name="日本ハム", entry_date="2026-09-25", entry_open=6022.0)], "BUY",
                    {"2282": {"o": 2010, "h": 2015, "l": 1941, "c": 1960}}, "2026-09-29")
check("分割当日の当日足では約定を断定しない", f == [])
f = CC._final_fills([dict(ticker="2282.T", name="日本ハム", entry_date="2026-09-25", entry_open=6022.0)], "BUY",
                    {"2282": {"o": 5900, "h": 5950, "l": 5800, "c": 5850}}, "2026-09-29")
check("普通の-3%割れは従来どおりSTOP", len(f) == 1 and f[0]["exit_type"] == "STOP")
print(f"\n{ok} PASS / {fail} FAIL"); sys.exit(1 if fail else 0)

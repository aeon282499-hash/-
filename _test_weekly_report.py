# -*- coding: utf-8 -*-
"""週次レポート修正のローカル検証（ネット不要・Discord送信なし）

検証対象（2026-07-04 修正・発端=2026-07-03 週次レポの日本化薬誤表示）:
  1. tracker.update_positions — all_data 受け渡しで取得スキップ（ドライラン用）
  2. 当日MAXHOLD処分が「決済」に入り「保有中（持ち越し）」に出ないこと
  3. 当日エントリー(pending)が約定→保有中に出る／寄指不成立→どこにも出ないこと
  4. 週次明細 — 全決済の 買付額→売却額・損益円 と合計行（BUY/空売り両方）
  5. 先週決済分が今週明細に混入しないこと
"""
import sys
import os
import types
from datetime import date

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ---------- スタブ: yfinance / jpholiday / dotenv（_test_yose_fixes.py と同じ） ----------
fake_yf = types.ModuleType("yfinance")
fake_yf.Ticker = lambda t: None
sys.modules.setdefault("yfinance", fake_yf)
try:
    import jpholiday  # noqa: F401
except ImportError:
    m = types.ModuleType("jpholiday")
    m.is_holiday = lambda d: False
    sys.modules["jpholiday"] = m
try:
    import dotenv  # noqa: F401
except ImportError:
    m = types.ModuleType("dotenv")
    m.load_dotenv = lambda *a, **k: None
    sys.modules["dotenv"] = m

results = []


def check(label, cond):
    results.append((label, bool(cond)))
    print(("OK " if cond else "NG ") + label)


TODAY   = date(2026, 7, 3)   # 金曜（週次発火日）
VIRTUAL = date(2026, 7, 4)   # 引け後ドライラン基準（当日バーまで処理させる）

# ================= 合成バー（3本だけ→calc_rsiはNone＝RSI出口なし） =================
idx3 = pd.to_datetime(["2026-07-01", "2026-07-02", "2026-07-03"])
idx1 = pd.to_datetime(["2026-07-03"])
ALL_DATA = {
    # 7/1建て・金曜が3日目MAXHOLD（日本化薬シナリオ）。OCOには触れない値幅。
    "NKY.T": pd.DataFrame({"Open": [2006.0, 2012.0, 2015.0],
                           "High": [2050.0, 2040.0, 2060.0],
                           "Low":  [1990.0, 1995.0, 1998.0],
                           "Close": [2010.0, 2010.0, 2036.0]}, index=idx3),
    # 金曜建てpending→寄り5350≦指値5380で約定→持ち越し
    "OSK.T": pd.DataFrame({"Open": [5350.0], "High": [5420.0],
                           "Low": [5300.0], "Close": [5400.0]}, index=idx1),
    # 金曜建てpending→寄り5200＞指値5170で寄指不成立(NOFILL)
    "NOF.T": pd.DataFrame({"Open": [5200.0], "High": [5250.0],
                           "Low": [5180.0], "Close": [5210.0]}, index=idx1),
}


def _pos(ticker, name, entry, status, *, entry_open=None, prev_close=0,
         limit_price=None, direction="BUY", pnl=None, exit_type=None,
         exit_date=None, hold_days=0):
    return {"signal_date": entry, "entry_date": entry, "ticker": ticker,
            "name": name, "direction": direction, "prev_close": prev_close,
            "limit_price": limit_price, "entry_open": entry_open,
            "status": status, "hold_days": hold_days, "pnl_pct": pnl,
            "unrealized_pnl": None, "exit_type": exit_type, "exit_date": exit_date}


buy_positions = [
    _pos("OLD.T", "テスト先週", "2026-06-24", "closed", entry_open=1000.0,
         prev_close=1000.0, pnl=5.0, exit_type="TP", exit_date="2026-06-26"),
    _pos("TPW.T", "テスト利確", "2026-06-29", "closed", entry_open=2500.0,
         prev_close=2500.0, pnl=5.0, exit_type="TP", exit_date="2026-07-01"),
    _pos("NKY.T", "テスト化薬", "2026-07-01", "open", entry_open=2006.0,
         prev_close=1993.0, limit_price=2012, hold_days=2),
    _pos("OSK.T", "テスト瓦斯", "2026-07-03", "pending",
         prev_close=5334.0, limit_price=5380),
    _pos("NOF.T", "テスト不成立", "2026-07-03", "pending",
         prev_close=5121.0, limit_price=5170),
]
sell_positions = [
    _pos("SLX.T", "テスト空売り", "2026-06-30", "closed", entry_open=3000.0,
         prev_close=3000.0, direction="SELL", pnl=5.0, exit_type="TP",
         exit_date="2026-07-02"),
]

# ================= 1) tracker: all_data渡しドライラン =================
import tracker  # noqa: E402

tracker.batch_download_jquants = lambda *a, **k: (_ for _ in ()).throw(
    AssertionError("all_data渡しなのにJ-Quants取得が呼ばれた"))
tracker._jquants_id_token = lambda: (_ for _ in ()).throw(
    AssertionError("all_data渡しなのにトークン取得が呼ばれた"))

import copy  # noqa: E402
sim_buy = tracker.update_positions(copy.deepcopy(buy_positions), VIRTUAL,
                                   all_data=ALL_DATA)[0]
sim_sell = tracker.update_positions(copy.deepcopy(sell_positions), VIRTUAL,
                                    all_data=ALL_DATA)[0]

by = {p["ticker"]: p for p in sim_buy}
check("1-1 all_data渡しでJ-Quants取得なしに完走", True)
check("2-1 当日MAXHOLD: NKYがclosed", by["NKY.T"]["status"] == "closed")
check("2-2 当日MAXHOLD: exit_date=当日", by["NKY.T"]["exit_date"] == "2026-07-03")
check("2-3 当日MAXHOLD: exit_type=MAXHOLD", by["NKY.T"]["exit_type"] == "MAXHOLD")
check("2-4 当日MAXHOLD: pnl=+1.496%", by["NKY.T"]["pnl_pct"] == 1.496)
check("3-1 当日pending約定: OSKがopen", by["OSK.T"]["status"] == "open")
check("3-2 当日pending約定: entry_open=寄り値", by["OSK.T"]["entry_open"] == 5350.0)
check("3-3 当日NOFILL: NOFがexpired", by["NOF.T"]["status"] == "expired")
check("3-4 先週/今週の既決済は不変", by["OLD.T"]["status"] == "closed"
      and by["TPW.T"]["pnl_pct"] == 5.0)

# ================= 2) notifier: 週次embed =================
import notifier  # noqa: E402

captured = []
notifier._dispatch = lambda payload, **kw: captured.append(payload)

TIER = {"key": "main", "label": "大資金", "emoji": "", "size": 1_000_000,
        "buy_webhook": "dummy", "sell_webhook": "dummy", "public_mirror": False}

notifier.send_weekly_report(sim_buy, sim_sell, TODAY, tier=TIER)

check("4-0 embed送信1通", len(captured) == 1)
desc = captured[0]["embeds"][0]["description"]
print("\n----- 生成された週次レポート -----\n" + desc + "\n-----------------------------\n")

# 当日MAXHOLDが決済明細に入る（保有中に出ない）
check("2-5 化薬が決済明細に出る（07/01→07/03 期限）",
      "テスト化薬 07/01→07/03" in desc and "期限" in desc)
hold_line = [l for l in desc.splitlines() if l.startswith("💼")][0]
check("2-6 化薬が保有中に出ない", "テスト化薬" not in hold_line)
# 当日エントリーの扱い
check("3-5 瓦斯（当日約定）が保有中に出る", "テスト瓦斯" in hold_line)
check("3-6 不成立(NOFILL)はどこにも出ない", "テスト不成立" not in desc)
# 金額明細（株数=朝シグナルと同じ式・100株単位）
check("4-1 利確明細: 400株 買1,000,000→売1,050,000 +50,000円",
      "テスト利確 06/29→07/01 400株｜買 1,000,000円 → 売 1,050,000円｜**+50,000円**" in desc)
check("4-2 化薬明細: 500株 買1,003,000→売1,018,005 +15,005円",
      "テスト化薬 07/01→07/03 500株｜買 1,003,000円 → 売 1,018,005円｜**+15,005円**" in desc)
check("4-3 BUY合計行: 買付2,003,000→売却2,068,005 ＝+65,005円",
      "💰 買付合計 2,003,000円 → 売却合計 2,068,005円 ＝ **+65,005円**" in desc)
check("4-4 空売り明細: 売建900,000→買戻855,000 +45,000円",
      "テスト空売り 06/30→07/02 300株｜売建 900,000円 → 買戻 855,000円｜**+45,000円**" in desc)
check("4-5 空売り合計行: 売建900,000→買戻855,000 ＝+45,000円",
      "💰 売建合計 900,000円 → 買戻合計 855,000円 ＝ **+45,000円**" in desc)
check("4-6 勝率・PF表示あり", "勝率2/2" in desc and "PF" in desc)
check("4-7 週次色=緑（週間プラス）",
      captured[0]["embeds"][0]["color"] == notifier.COLOR_WIN)
check("5-1 先週決済は明細に混入しない", "テスト先週" not in desc)

# ================= 結果 =================
ok = sum(1 for _, c in results if c)
print(f"\n{ok}/{len(results)} PASS")
sys.exit(0 if ok == len(results) else 1)

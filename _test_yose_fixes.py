# -*- coding: utf-8 -*-
"""寄指まわり修正＋大引け「対象なし」確認通知のローカル検証（ネット不要・Discord送信なし）

検証対象:
  1. tracker.update_positions — 4要素タプル化・NOFILLがexpired_todayに入る
  2. close_check.collect_targets — (targets, checked)タプル・寄指約定確認・失敗のnote化
  3. notifier._build_results_embed — 寄指不成立セクション表示
  4. notifier.send_close_no_targets — 保有継続確認embed・警告色・早期return
  5. report._process_signals — limit_price保存値で不成立スナップショット除外
"""
import sys
import os
import types
from datetime import date

import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ---------- スタブ: yfinance（このPCはAV傍受で実ネット不可のため必須） ----------
class _FakeTicker:
    plan = {}  # ticker -> intraday DataFrame

    def __init__(self, t):
        self.t = t

    def history(self, period=None, interval=None, **kw):
        return _FakeTicker.plan.get(self.t, pd.DataFrame())


fake_yf = types.ModuleType("yfinance")
fake_yf.Ticker = _FakeTicker
sys.modules["yfinance"] = fake_yf

# ---------- スタブ: jpholiday / dotenv（未インストール環境向け） ----------
try:
    import jpholiday  # noqa: F401
except ImportError:
    m = types.ModuleType("jpholiday")
    m.is_holiday = lambda d: False
    m.is_holiday_name = lambda d: None
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


TODAY = date(2026, 6, 15)  # 月曜想定

# ================= 1) tracker =================
import tracker  # noqa: E402


def fake_batch(token=None, start=None, end=None, tickers=None):
    idx = pd.to_datetime(["2026-06-12"])
    dfA = pd.DataFrame({"Open": [1370.0], "High": [1375.0], "Low": [1340.0],
                        "Close": [1352.0]}, index=idx)  # 高寄り→NOFILL
    dfB = pd.DataFrame({"Open": [1348.0], "High": [1360.0], "Low": [1339.0],
                        "Close": [1350.0]}, index=idx)  # 指値以下→約定
    return {"AAAA.T": dfA, "BBBB.T": dfB}


tracker.batch_download_jquants = fake_batch
tracker._jquants_id_token = lambda: "dummy"


def _mk_pending(t, lp, entry="2026-06-12"):
    return {"signal_date": entry, "entry_date": entry, "ticker": t, "name": t,
            "direction": "BUY", "prev_close": 1353.0, "limit_price": lp,
            "entry_open": None, "status": "pending", "hold_days": 0,
            "pnl_pct": None, "unrealized_pnl": None,
            "exit_type": None, "exit_date": None}


poss = [_mk_pending("AAAA.T", 1366), _mk_pending("BBBB.T", 1366)]
updated, closed, expired, still = tracker.update_positions(poss, TODAY)
check("tracker: 戻り値が4要素タプル", isinstance(expired, list))
check("tracker: 高寄りNOFILLがexpired_todayに入る",
      len(expired) == 1 and expired[0]["ticker"] == "AAAA.T"
      and expired[0]["exit_type"] == "NOFILL" and expired[0]["status"] == "expired")
check("tracker: NOFILLはclosed/still_openに混入しない",
      all(p["ticker"] != "AAAA.T" for p in closed + still))
check("tracker: 指値以下は約定→open・entry_open=寄り値",
      len(still) == 1 and still[0]["ticker"] == "BBBB.T"
      and still[0]["status"] == "open" and still[0]["entry_open"] == 1348.0)

# 既に失効(expired/NOFILL)したポジは終端状態＝再処理しない。
# 旧コードは status=="closed" しか弾かず、expiredに含み損益/保有日数を付けて
# still_openに混入させ「保有中・本日処分」と誤通知していた（2026-06-25 日油の事故）。
_expired = {"signal_date": "2026-06-12", "entry_date": "2026-06-12",
            "ticker": "AAAA.T", "name": "AAAA.T", "direction": "BUY",
            "prev_close": 1353.0, "limit_price": 1366, "entry_open": 1370.0,
            "status": "expired", "hold_days": 0, "pnl_pct": None,
            "unrealized_pnl": None, "exit_type": "NOFILL", "exit_date": "2026-06-12"}
poss2 = [_expired, _mk_pending("BBBB.T", 1366)]  # activeを1件混ぜて早期returnを回避
u2, c2, e2, s2 = tracker.update_positions(poss2, TODAY)
_ax = next(p for p in u2 if p["ticker"] == "AAAA.T")
check("tracker: 既存expiredは再処理されず still/closed/expired に出ない",
      all(p["ticker"] != "AAAA.T" for p in c2 + s2 + e2)
      and _ax["status"] == "expired" and _ax["hold_days"] == 0
      and _ax["unrealized_pnl"] is None)

# ================= 2) close_check =================
import close_check as cc  # noqa: E402

# 履歴データ（RSI計算用に上昇トレンド20日・6/12まで）→ RSI高
hist_idx = pd.bdate_range("2026-05-14", "2026-06-12")
n = len(hist_idx)
closes_up = [1300.0 + i * 3 for i in range(n)]
hist_up = pd.DataFrame({
    "Open":  closes_up, "High": [c + 5 for c in closes_up],
    "Low":   [c - 5 for c in closes_up], "Close": closes_up,
}, index=hist_idx)
# 下落トレンド → RSI低（保有継続側）
closes_dn = [1500.0 - i * 3 for i in range(n)]
hist_dn = pd.DataFrame({
    "Open":  closes_dn, "High": [c + 5 for c in closes_dn],
    "Low":   [c - 5 for c in closes_dn], "Close": closes_dn,
}, index=hist_idx)
# 6/10の寄りを高寄り(1380)に書き換え → ゾンビpendingの不成立判定用
hist_zombie = hist_up.copy()
hist_zombie.loc[hist_zombie.index == pd.Timestamp("2026-06-10"), "Open"] = 1380.0

# --- A: ゾンビpending（entry=6/10・4営業日経過→旧コードならMAXHOLD誤通知） ---
zombie = _mk_pending("ZZZZ.T", 1366, entry="2026-06-10")
targets, checked = cc.collect_targets([zombie], "BUY", TODAY, {"ZZZZ.T": hist_zombie})
check("close_check: 不成立ゾンビpendingはMAXHOLD通知されない", targets == [])
check("close_check: 不成立はcheckedにnote付きで載る",
      len(checked) == 1 and "寄指不成立" in (checked[0]["note"] or ""))

# --- B: 当日pending・寄指約定→RSI回復で処分対象＆entry_openが寄り値に補正 ---
intraday_fill = pd.DataFrame({"Open": [1350.0, 1351.0], "Close": [1351.0, 1352.0]})
_FakeTicker.plan["CCCC.T"] = intraday_fill
pend_fill = _mk_pending("CCCC.T", 1366, entry=TODAY.strftime("%Y-%m-%d"))
targets, checked = cc.collect_targets([pend_fill], "BUY", TODAY, {"CCCC.T": hist_up})
check("close_check: 約定済み当日pendingはRSI処分対象になる",
      len(targets) == 1 and targets[0]["reason_type"] == "RSI" and checked == [])
check("close_check: 含み損益基準が実寄り値(1350)に補正される",
      len(targets) == 1 and targets[0]["entry_open"] == 1350.0)

# --- C: 当日pending・高寄り不成立→スキップ＆checkedにnote ---
intraday_nofill = pd.DataFrame({"Open": [1380.0, 1381.0], "Close": [1381.0, 1382.0]})
_FakeTicker.plan["DDDD.T"] = intraday_nofill
pend_nofill = _mk_pending("DDDD.T", 1366, entry=TODAY.strftime("%Y-%m-%d"))
targets, checked = cc.collect_targets([pend_nofill], "BUY", TODAY, {"DDDD.T": hist_up})
check("close_check: 不成立の当日pendingは処分通知されない",
      targets == [] and len(checked) == 1 and "寄指不成立" in checked[0]["note"])

# --- C2: 当日pending・寄りが指値±誤差幅内（境界）→ 約定を断定せずスキップ ---
# 2026-07-15 東海カーボン(5301)回帰: 公式寄り1,653円 > 指値1,652円のNOFILLを、
# yfinance 5分足の寄り(≤1,652円)が「約定」と誤判定し中/小で保有継続と誤表示した。
intraday_edge_over = pd.DataFrame({"Open": [1367.0, 1368.0], "Close": [1368.0, 1369.0]})
_FakeTicker.plan["IIII.T"] = intraday_edge_over
pend_edge_over = _mk_pending("IIII.T", 1366, entry=TODAY.strftime("%Y-%m-%d"))
targets, checked = cc.collect_targets([pend_edge_over], "BUY", TODAY, {"IIII.T": hist_up})
check("close_check: 寄り=指値+1円の境界は保有扱いにも処分対象にもしない",
      targets == [] and len(checked) == 1 and "境界" in checked[0]["note"])

intraday_edge_under = pd.DataFrame({"Open": [1365.0, 1366.0], "Close": [1366.0, 1367.0]})
_FakeTicker.plan["JJJJ.T"] = intraday_edge_under
pend_edge_under = _mk_pending("JJJJ.T", 1366, entry=TODAY.strftime("%Y-%m-%d"))
targets, checked = cc.collect_targets([pend_edge_under], "BUY", TODAY, {"JJJJ.T": hist_up})
check("close_check: 寄り=指値-1円も境界扱い（yfinance精度では約定断定不可）",
      targets == [] and len(checked) == 1 and "境界" in checked[0]["note"])

# 過去日はJ-Quants公式寄り＝正確なので1円差でも緩衝なしで確定NOFILL
hist_edge = hist_up.copy()
hist_edge.loc[hist_edge.index == pd.Timestamp("2026-06-12"), "Open"] = 1367.0
pend_past_edge = _mk_pending("KKKK.T", 1366, entry="2026-06-12")
targets, checked = cc.collect_targets([pend_past_edge], "BUY", TODAY, {"KKKK.T": hist_edge})
check("close_check: 過去日は公式寄りで1円差でも確定NOFILL（境界緩衝なし）",
      targets == [] and len(checked) == 1 and "寄指不成立" in checked[0]["note"])

# --- D: 当日pending・yfinance空（約定不明）→保守的にスキップ＆note ---
pend_unknown = _mk_pending("EEEE.T", 1366, entry=TODAY.strftime("%Y-%m-%d"))
targets, checked = cc.collect_targets([pend_unknown], "BUY", TODAY, {"EEEE.T": hist_up})
check("close_check: 約定不明はスキップ＆checkedに要手動確認",
      targets == [] and len(checked) == 1 and "約定確認不可" in checked[0]["note"])

# --- E: 旧形式（limit_priceなし・open状態）は従来どおり判定される ---
old_open = _mk_pending("CCCC.T", None, entry="2026-06-12")
old_open.update(status="open", entry_open=1348.0)
targets, checked = cc.collect_targets([old_open], "BUY", TODAY, {"CCCC.T": hist_up})
check("close_check: 既存openポジションは従来どおりRSI判定",
      len(targets) == 1 and targets[0]["reason_type"] == "RSI")

# --- F: RSI<50の保有継続 → targetsなし・checkedに正常エントリ ---
intraday_low = pd.DataFrame({"Open": [1200.0, 1199.0], "Close": [1199.0, 1198.0]})
_FakeTicker.plan["GGGG.T"] = intraday_low
hold_pos = _mk_pending("GGGG.T", None, entry="2026-06-12")
hold_pos.update(status="open", entry_open=1210.0)
targets, checked = cc.collect_targets([hold_pos], "BUY", TODAY, {"GGGG.T": hist_dn})
check("close_check: RSI<50は対象なし＆checkedに正常記録",
      targets == [] and len(checked) == 1 and checked[0]["note"] is None
      and checked[0]["rsi_now"] is not None and checked[0]["rsi_now"] < 50)

# --- G: yfinance失敗(open状態)もcheckedに要手動確認で載る ---
fail_pos = _mk_pending("HHHH.T", None, entry="2026-06-12")
fail_pos.update(status="open", entry_open=1000.0)
targets, checked = cc.collect_targets([fail_pos], "BUY", TODAY, {"HHHH.T": hist_dn})
check("close_check: 現在値取得失敗がnoteとして可視化される",
      targets == [] and len(checked) == 1 and "取得失敗" in checked[0]["note"])

# --- H: _oco_fill 境界緩衝（yfinance高安の1円ズレでOCO約定を断定しない） ---
# 約定と誤断定するとMAXHOLD/RSIの処分指示がスキップされ実保有と帳簿がズレる。
# 水準±max(2円,0.1%)の境界は未約定扱い＝処分判定に回す（安全側・2026-07-16）。
_e = 1650.0  # BUY: stop=1600.5/tp=1732.5・SELL: stop=1699.5/tp=1567.5・eps=2円
check("oco: STOP水準ちょうどは未約定扱い", cc._oco_fill("BUY", _e, 1700.0, 1600.5) is None)
check("oco: STOP-eps超えで約定断定",
      (cc._oco_fill("BUY", _e, 1700.0, 1598.0) or {}).get("kind") == "STOP")
check("oco: TP+1円は未約定扱い", cc._oco_fill("BUY", _e, 1733.5, 1650.0) is None)
check("oco: TP+eps超えで約定断定",
      (cc._oco_fill("BUY", _e, 1735.0, 1650.0) or {}).get("kind") == "TP")
check("oco: SELLもSTOP境界は未約定", cc._oco_fill("SELL", _e, 1699.5, 1640.0) is None)
check("oco: SELLのSTOP+eps超えで約定断定",
      (cc._oco_fill("SELL", _e, 1702.0, 1640.0) or {}).get("kind") == "STOP")

# 統合: MAXHOLD日にyf高値が水準+1円(境界)→ ✅決済済みでなくMAXHOLD処分指示が出る
# （旧コードは1733≥1732.5で「決済済み・保有なし」と誤断定し強制処分が抜けた）
intraday_edge_tp = pd.DataFrame({"Open": [1720.0, 1728.0], "High": [1730.0, 1733.0],
                                 "Low": [1700.0, 1725.0], "Close": [1728.0, 1731.0]})
_FakeTicker.plan["MMMM.T"] = intraday_edge_tp
pos_maxhold = _mk_pending("MMMM.T", None, entry="2026-06-11")
pos_maxhold.update(status="open", entry_open=1650.0)
targets, checked = cc.collect_targets([pos_maxhold], "BUY", TODAY, {"MMMM.T": hist_up})
check("oco: TP境界(高値1,733 vs 水準1,732.5)はMAXHOLD処分指示を出す",
      len(targets) == 1 and targets[0]["reason_type"] == "MAXHOLD")

# ================= 3) notifier =================
import notifier  # noqa: E402

embed = notifier._build_results_embed([], [], TODAY, notifier._tier(None),
                                      expired=[expired[0]])
desc = embed["description"]
check("notifier: 寄指不成立セクションが出る", "寄指不成立" in desc)
check("notifier: 寄り/指値の数字が出る", "1,370" in desc and "1,366" in desc)

# send_results の早期return（全部空なら送らない）
called = []
notifier._dispatch = lambda *a, **k: called.append(k.get("side") or a)
notifier.send_results([], [], TODAY, tier=None, expired=[])
check("notifier: 全部空なら送信しない", called == [])
notifier.send_results([], [], TODAY, tier=None, expired=[expired[0]])
check("notifier: expiredのみでも送信する", len(called) == 1)

# --- 大引け「対象なし」確認通知 ---
ok_checked = [{"ticker": "9301.T", "name": "三菱倉庫", "today_hold": 2,
               "rsi_now": 42.6, "current_price": 1429.0, "note": None}]
embed = notifier._build_close_no_targets_embed(ok_checked, TODAY,
                                               notifier._tier(None), sell=False)
d = embed["description"]
check("notifier: 対象なしembedに保有継続・RSI・期限が出る",
      "保有継続" in d and "42.6" in d and "三菱倉庫" in d and "明日が処分期限" in d)
check("notifier: 正常時はグレー色", embed["color"] == notifier.COLOR_NONE)

warn_checked = ok_checked + [{"ticker": "9023.T", "name": "東京地下鉄", "today_hold": 1,
                              "rsi_now": None, "current_price": None,
                              "note": "現在値の取得失敗（要手動確認）"}]
embed = notifier._build_close_no_targets_embed(warn_checked, TODAY,
                                               notifier._tier(None), sell=False)
check("notifier: 取得失敗があると⚠️行＋警告色",
      "⚠️" in embed["description"] and embed["color"] == notifier.COLOR_ERROR)

called.clear()
notifier.send_close_no_targets([], TODAY, tier=None)
check("notifier: checked空なら対象なし通知は送らない", called == [])
notifier.send_close_no_targets(ok_checked, TODAY, tier=None)
check("notifier: 対象なし通知が送信される", len(called) == 1)

# ================= 4) report =================
import report  # noqa: E402

TEST_HIST = "_test_history_tmp.json"
if os.path.exists(TEST_HIST):
    os.remove(TEST_HIST)

sigs_new = [{"ticker": "FFFF.T", "name": "テスト", "direction": "BUY",
             "prev_close": 1353.0, "limit_price": 1366}]
ohlc_nofill = {"FFFF.T": {"open": 1380.0, "close": 1390.0}}
res, _ = report._process_signals(sigs_new, ohlc_nofill, "2026-06-15", TEST_HIST)
check("report: 寄指不成立はスナップショット除外（limit_price保存値）", res == [])

ohlc_fill = {"FFFF.T": {"open": 1360.0, "close": 1390.0}}
res, _ = report._process_signals(sigs_new, ohlc_fill, "2026-06-15", TEST_HIST)
check("report: 約定日は従来どおり記録",
      len(res) == 1 and abs(res[0]["pnl"] - 2.21) < 0.05)

sigs_old = [{"ticker": "FFFF.T", "name": "テスト", "direction": "BUY",
             "prev_close": 1353.0}]
res, _ = report._process_signals(sigs_old, ohlc_nofill, "2026-06-16", TEST_HIST)
check("report: 旧形式(prev_closeのみ)でも除外が機能", res == [])

if os.path.exists(TEST_HIST):
    os.remove(TEST_HIST)

# ================= まとめ =================
ng = [l for l, ok in results if not ok]
print("\n==== 結果: {}/{} OK ====".format(len(results) - len(ng), len(results)))
if ng:
    print("NG:", ng)
    sys.exit(1)
print("ALL PASS")

# -*- coding: utf-8 -*-
"""_test_daytrade_paper.py — 紙トレ台帳の純ロジック検証（外部I/Oなし）。"""
from datetime import date
import pandas as pd

import daytrade_paper as dp

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"  NG  {name}")


def mkdf(rows):
    """rows = [(datestr, open, high, low, close, vol), ...] → DataFrame(index=Date)."""
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame({
        "Open":  [r[1] for r in rows],
        "High":  [r[2] for r in rows],
        "Low":   [r[3] for r in rows],
        "Close": [r[4] for r in rows],
        "Volume":[r[5] for r in rows],
    }, index=idx)


def base_book(pos):
    return {"positions": list(pos), "expired": [], "last_report_date": None}


# ---------------------------------------------------------------- shortability
def test_shortability():
    iss = {"1234": "2", "5678": "1"}
    check("貸借○ (IssType=2)", dp.shortability("1234.T", iss)["mark"] == "○")
    check("信用× (IssType=1)", dp.shortability("5678.T", iss)["mark"] == "×")
    check("不明? (無し)",       dp.shortability("9999.T", iss)["mark"] == "?")
    check("英字コード4桁化",     dp.shortability("464A.T", {"464A": "2"})["mark"] == "○")


# ---------------------------------------------------------------- settle BUY
def test_settle_buy_win():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-14", "basis_date": "2026-07-13",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    closed = dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("BUY勝ち: CLOSE", p["exit_type"] == "CLOSE")
    check("BUY勝ち: pnl=(1070-1050)/1050", p["pnl_pct"] == round((1070 - 1050) / 1050 * 100, 3))
    check("BUY勝ち: win=True", p["win"] is True)
    check("BUY勝ち: entry_session=07-14", p["entry_session"] == "2026-07-14")
    check("BUY勝ち: just_closed 1件", len(closed) == 1)
    check("BUY勝ち: pnl_yen>0", p["pnl_yen"] > 0)


def test_settle_buy_skip():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-14", "basis_date": "2026-07-13",
            "limit_price": 1040, "status": "pending"}]
    book = base_book(pos)
    # 寄り1050 > MAX指値1040 → 見送り
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("BUY見送り: SKIP", p["exit_type"] == "SKIP")
    check("BUY見送り: pnl=0", p["pnl_pct"] == 0.0)
    check("BUY見送り: pnl_yen=0", p["pnl_yen"] == 0)


# ---------------------------------------------------------------- settle SELL
def test_settle_sell_win():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
            "signal_date": "2026-07-14", "basis_date": "2026-07-13",
            "limit_price": 1000, "status": "pending"}]
    book = base_book(pos)
    # 寄り1050 >= MIN指値1000 → 執行, 引け1000 < 寄り → 空売り利益
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 990, 1000, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("SELL勝ち: CLOSE", p["exit_type"] == "CLOSE")
    check("SELL勝ち: pnl=(1050-1000)/1050", p["pnl_pct"] == round((1050 - 1000) / 1050 * 100, 3))
    check("SELL勝ち: win=True", p["win"] is True)


def test_settle_sell_skip():
    """2026-07-28: 寄指→成売りに変更。下寄りでも建てるので SKIP しなくなった。
    下寄りの玉も10年両期間でPF1超（前1.08/後1.21）で、成行の方が年+4.4万・
    最悪年-2.0万→+28.2万・勝ち10/11→11/11年。旧挙動は FADE_ENTRY_MARKET=False で復活。"""
    pos = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
            "signal_date": "2026-07-14", "basis_date": "2026-07-13",
            "limit_price": 1000, "status": "pending"}]
    book = base_book(pos)
    # 寄り980（前日終値1000より下寄り）→ 成売りなので約定し、引け970まで下げて利益
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 980, 1000, 950, 970, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    p = book["positions"][0]
    check("下寄りでも約定(成売り)", p["exit_type"] == "CLOSE")
    check("下寄りの損益は寄→引", abs(p["pnl_pct"] - (980 - 970) / 980 * 100) < 0.01)
    check("FADE_ENTRY_MARKETはTrue", dp.FADE_ENTRY_MARKET is True)

    # 旧挙動（寄指）に戻せることも確認
    dp.FADE_ENTRY_MARKET = False
    try:
        book2 = base_book([{"ticker": "1301.T", "name": "A", "direction": "SELL",
                            "signal_date": "2026-07-14", "basis_date": "2026-07-13",
                            "limit_price": 1000, "status": "pending"}])
        dp.settle(book2, data, date(2026, 7, 15))
        check("FADE_ENTRY_MARKET=Falseなら従来通りSKIP",
              book2["positions"][0]["exit_type"] == "SKIP")
    finally:
        dp.FADE_ENTRY_MARKET = True


# ---------------------------------------------------------------- pending / expired
def test_settle_pending_kept():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-15", "basis_date": "2026-07-14",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    # basis後の足がまだ無い（当日足も無い）→ pending維持
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    closed = dp.settle(book, data, date(2026, 7, 15))
    check("エントリー足未到来→pending維持", book["positions"][0]["status"] == "pending")
    check("pending維持: just_closed 0件", len(closed) == 0)


def test_settle_today_not_closed():
    """エントリーセッションが当日(=まだ引けてない)なら決済しない。"""
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-07-15", "basis_date": "2026-07-14",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    # 当日=07-15 の足がデータに存在しても（寄り前実行では通常無いが）確定扱いしない
    data = {"1301.T": mkdf([("2026-07-14", 1050, 1080, 1040, 1070, 1e6),
                            ("2026-07-15", 1080, 1090, 1070, 1085, 1e6)])}
    dp.settle(book, data, date(2026, 7, 15))
    check("当日足は未確定→pending維持", book["positions"][0]["status"] == "pending")


def test_settle_halt_skip():
    """シグナル当日に売買停止(足なし)→翌日の足で約定扱いにせずSKIP（実弾は寄指不成立のため）。"""
    pos = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
            "signal_date": "2026-07-15", "basis_date": "2026-07-14",
            "limit_price": 1000, "status": "pending"}]
    book = base_book(pos)
    # 7/15の足が無い(停止)。7/16に再開して大幅高でも紙は約定させない
    data = {"1301.T": mkdf([("2026-07-14", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-16", 1300, 1350, 1250, 1280, 1e6)])}
    dp.settle(book, data, date(2026, 7, 17))
    p = book["positions"][0]
    check("停止: SKIP", p["exit_type"] == "SKIP")
    check("停止: 理由=当日約定なし", "当日約定なし" in p.get("skip_reason", ""))
    check("停止: pnl=0", p["pnl_pct"] == 0.0)


def test_settle_expired():
    pos = [{"ticker": "1301.T", "name": "A", "direction": "BUY",
            "signal_date": "2026-06-01", "basis_date": "2026-06-01",
            "limit_price": 1100, "status": "pending"}]
    book = base_book(pos)
    data = {"1301.T": mkdf([("2026-05-30", 1000, 1010, 990, 1000, 1e6)])}  # basis後の足なし
    dp.settle(book, data, date(2026, 7, 15))
    check("14日超で足取れず→expired", len(book["expired"]) == 1)
    check("expired: activeから除外", all(p.get("status") != "pending" for p in book["positions"]))


# ---------------------------------------------------------------- record
def test_record_and_dedup():
    book = base_book([])
    data = {"1301.T": mkdf([("2026-07-13", 1000, 1010, 990, 1000, 1e6),
                            ("2026-07-14", 1050, 1080, 1040, 1070, 1e6)])}
    sigs = [{"ticker": "1301.T", "name": "A", "direction": "SELL",
             "prev_close": 1070, "daily_gain": 27.5, "min_entry_price": 1070}]
    iss = {"1301": "2"}
    added = dp.record(book, sigs, data, iss, date(2026, 7, 15))
    check("記帳1件", len(added) == 1)
    check("basis_date=07-14(当日前の最終足)", book["positions"][0]["basis_date"] == "2026-07-14")
    check("limit_price=min指値", book["positions"][0]["limit_price"] == 1070)
    check("SELLにshort付与", book["positions"][0]["short"]["mark"] == "○")
    # 同じ(ticker,signal_date)は重複記帳しない
    added2 = dp.record(book, sigs, data, iss, date(2026, 7, 15))
    check("重複記帳しない", len(added2) == 0 and len(book["positions"]) == 1)


# ---------------------------------------------------------------- stats
def test_cumulative_stats():
    book = base_book([
        {"ticker": "A.T", "direction": "BUY", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": 2.0, "pnl_yen": 80000, "win": True},
        {"ticker": "B.T", "direction": "BUY", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": -1.0, "pnl_yen": -40000, "win": False},
        {"ticker": "C.T", "direction": "SELL", "status": "closed", "exit_type": "CLOSE",
         "pnl_pct": 3.0, "pnl_yen": 120000, "win": True},
        {"ticker": "D.T", "direction": "BUY", "status": "closed", "exit_type": "SKIP",
         "pnl_pct": 0.0, "pnl_yen": 0, "win": False},
        {"ticker": "E.T", "direction": "SELL", "status": "pending"},
    ])
    st = dp.cumulative_stats(book)
    check("執行n=3(SKIP/pending除外)", st["all"]["n"] == 3)
    check("勝率=2/3", abs(st["all"]["win"] - 2 / 3 * 100) < 1e-6)
    check("PF=(2+3)/1=5.0", abs(st["all"]["pf"] - 5.0) < 1e-6)
    check("損益円=160000", st["all"]["yen"] == 160000)
    check("BUY n=2", st["buy"]["n"] == 2)
    check("SELL n=1", st["sell"]["n"] == 1)
    check("見送り=1", st["skipped"] == 1)
    check("保有中=1", st["pending"] == 1)


def _flat_then(last_gain_pct, base=1000, sticky=False, vol_x=3.0,
               rng_pct=4.0, predrift_pct=20.0, vol_base=1_000_000):
    """29日じわ上げ→最終日に指定%急騰のOHLCV rowsを作る。前日終値は必ず base に揃う。
    sticky=True で最終日レンジを極小(張り付きS高)にする。
    vol_x=出来高倍率。2026-07-28にFADE_VOL_RATIO_MAX=6を入れたため既定を6→3に下げた
    （6倍だと候補から除外されてテストが成立しない）。

    2026-07-31に FADE_ATR_MIN=5 / FADE_DEV25_MIN=12 を入れたので、旧「完全フラット→急騰」
    （ATR約1.5% / 乖離≒上昇率）では現実に存在しない玉になり全テストが候補外に落ちた。
    そこで平常日にも値幅と上昇トレンドを持たせる:
      rng_pct       … 平常日の高安の振れ（片側%）。ATR%を作る。既定4%→ATR約7%
      predrift_pct  … 急騰前に既に何%上げてきているか。25MA乖離を上昇率と切り離す。
    どちらも0にすれば旧来の「動かない玉が突然跳ねた」状態を再現できる（＝NO-GO側のテスト用）。
    """
    last = round(base * (1 + last_gain_pct / 100))
    if sticky:                          # 張り付き: 高安が終値にほぼ張り付く
        hi, lo = round(last * 1.002), round(last * 0.998)
    else:
        hi, lo = last, base             # レンジ大（安値=前日水準まで振れた）
    lo0 = base * (1 - predrift_pct / 100)
    rows = []
    dates = [f"2026-06-{d:02d}" for d in range(1, 29)] + ["2026-07-13"]
    for i, ds in enumerate(dates):      # lo0 → base へ線形に上げる（最終要素がちょうど base）
        c = lo0 + (base - lo0) * i / (len(dates) - 1)
        rows.append((ds, round(c), round(c * (1 + rng_pct / 100)),
                     round(c * (1 - rng_pct / 100)), round(c), vol_base))
    rows.append(("2026-07-14", base, hi, lo, last, int(vol_base * vol_x)))
    return mkdf(rows)


def test_alert_map_exclusion():
    """売り禁(jsf_stop)は**除外せず**🚫バッジ+jsf_stopフラグで表示（2026-07-23本人指示=ハイカラで売れる）。
    GO判定は不変。注意喚起(jsf_warn)/増担保(tse_reg)は⚠️reg_note注記のみ。alert_map無し=従来通り。"""
    import screener
    screener.fetch_tse_universe = lambda *a, **k: []
    data = {"9999.T": _flat_then(21), "6666.T": _flat_then(18),
            "7777.T": _flat_then(16), "8888.T": _flat_then(15)}
    today = date(2026, 7, 15)
    iss = {"9999": "2", "6666": "2", "7777": "2", "8888": "2"}

    # 1位の9999が売り禁 → 除外されず1番のまま・🚫バッジ+jsf_stop=True・GO維持
    am = {"9999": {"jsf_stop": True}}
    banned = []
    picks = dp.daily_top_fades(data, today, iss, alert_map=am, excluded_out=banned)
    check("売り禁でも1番のまま表示", picks[0]["ticker"] == "9999.T" and picks[0]["rank"] == 1)
    check("🚫売り禁バッジ", "🚫売り禁" in picks[0]["reg_note"] and "ハイカラ" in picks[0]["reg_note"])
    check("jsf_stopフラグ付与", picks[0]["jsf_stop"] is True)
    check("GO判定は不変(+21%はGO)", picks[0]["verdict"] == "GO")
    check("非売り禁はフラグFalse・バッジなし", picks[1]["jsf_stop"] is False and "🚫" not in picks[1]["reg_note"])
    check("excluded_outは常に空(旧互換)", banned == [])

    # 紙記帳にjsf_stopが乗る（在庫依存分の分離分析用）
    book = {"positions": [], "expired": []}
    dp.record(book, [picks[0]], data, iss, today)
    check("紙記帳にjsf_stop記録", book["positions"][0].get("jsf_stop") is True)

    # 注意喚起/増担保は⚠️注記のみ
    am2 = {"9999": {"jsf_warn": True}, "6666": {"tse_reg": True}}
    picks2 = dp.daily_top_fades(data, today, iss, alert_map=am2)
    check("注意喚起の⚠️注記", "注意喚起" in picks2[0]["reg_note"])
    check("増担保の⚠️注記", "増担保" in picks2[1]["reg_note"])
    # 上限2なので3番目は返らない。規制なしのreg_note空はn=4指定で確認する
    picks2b = dp.daily_top_fades(data, today, iss, n=4, alert_map=am2)
    check("規制なしはreg_note空", picks2b[2]["reg_note"] == "")

    # alert_map None → 従来と同一（上限2）
    picks3 = dp.daily_top_fades(data, today, iss)
    check("alert_map無し=従来通り", picks3[0]["ticker"] == "9999.T" and len(picks3) == 2)


def test_daily_top_fades():
    """選定=貸借○×前日+5%以上×張り付き除外。上位3を降順で返す・各GO/NOGO判定。"""
    import screener
    screener.fetch_tse_universe = lambda *a, **k: []   # 名前補完の実ネットを止める

    # 4銘柄: +21%,+18%,+16%,+8%（全部貸借○・レンジ大）
    data = {"9999.T": _flat_then(21), "6666.T": _flat_then(18),
            "7777.T": _flat_then(16), "8888.T": _flat_then(8)}
    today = date(2026, 7, 15)
    iss = {"9999": "2", "6666": "2", "7777": "2", "8888": "2"}

    picks = dp.daily_top_fades(data, today, iss)
    # 2026-07-28: 上限8→2。エッジは1番に集中し3番以降はPF0.99＝撃つと損のため表示ごと落とした
    check("上限2に絞る", len(picks) == 2)
    check("1番にrankが付く", picks[0]["rank"] == 1)
    check("2番にrankが付く", picks[1]["rank"] == 2)
    check("PAPER_MAX_PICKSは2", dp.PAPER_MAX_PICKS == 2)
    # 2026-07-28: 出来高が20日平均の6倍以上=本物の材料で翌日も買われる（帯別PF0.81）→候補外
    check("出来高6倍以上は候補から除外",
          dp.daily_top_fades({"9999.T": _flat_then(21, vol_x=8.0)}, today, {"9999": "2"}) == [])
    check("出来高5倍は候補に残る",
          len(dp.daily_top_fades({"9999.T": _flat_then(21, vol_x=5.0)}, today, {"9999": "2"})) == 1)
    check("FADE_VOL_RATIO_MAXは6.0", dp.FADE_VOL_RATIO_MAX == 6.0)
    # 2026-07-31: 乖離80%上限を撤廃。旧上限は「株価300円下限あり」の土台でJDI対策として
    # 入れたものだが、低位株を入れて再検証すると乖離80%超は10年89件で計+50.5万のプラスで、
    # 上限は「一番よく落ちる玉」を捨てる側に回っていた（年+66.6→+69.1万・最悪月-35.2→-19.9万）。
    _hi = _flat_then(20, base=1000)
    _hi.iloc[-1, _hi.columns.get_loc("Close")] = 2400      # 25MA比+約140%
    _hi.iloc[-1, _hi.columns.get_loc("High")] = 2500
    _hi.iloc[-1, _hi.columns.get_loc("Low")] = 2200
    check("FADE_DEV25_MAXはNone（上限撤廃）", dp.FADE_DEV25_MAX is None)
    check("乖離80%超でも候補に残る",
          len(dp.daily_top_fades({"9999.T": _hi}, today, {"9999": "2"})) == 1)
    dp.FADE_DEV25_MAX = 80.0                               # 復活経路が生きていること
    try:
        check("FADE_DEV25_MAX=80に戻せば除外される",
              dp.daily_top_fades({"9999.T": _hi}, today, {"9999": "2"}) == [])
    finally:
        dp.FADE_DEV25_MAX = None
    # 2026-07-31: ATR%下限5・25MA乖離下限12（勝率55.4%→59.6%・PF1.22→1.45・年+59.1万→+67.4万）。
    # 候補プールからは落とさず「必ず順位の後ろ＋NO-GO理由」にする＝0件の日も理由が見える。
    _dull = dp.daily_top_fades({"9999.T": _flat_then(21, rng_pct=0.5)}, today, {"9999": "2"})
    check("ATR低い玉はNOGO(除外はしない)",
          len(_dull) == 1 and _dull[0]["verdict"] == "NOGO" and "ATR" in _dull[0]["nogo_reason"])
    _near = dp.daily_top_fades({"9999.T": _flat_then(8, predrift_pct=0.0)}, today, {"9999": "2"})
    check("25MA乖離が小さい玉はNOGO",
          len(_near) == 1 and _near[0]["verdict"] == "NOGO" and "乖離" in _near[0]["nogo_reason"])
    check("FADE_ATR_MINは5.0", dp.FADE_ATR_MIN == 5.0)
    check("FADE_DEV25_MINは12.0", dp.FADE_DEV25_MIN == 12.0)
    # 2026-07-31: 株価下限を撤廃（旧300円）。10年で年+53.3万→+66.7万・両期間改善・最悪月も改善。
    # 低位株でも①貸借○は元から条件②板は出来高の0.03%③寄成/引成は板寄せで呼値を払わない。
    _low = _flat_then(21, base=200, vol_base=3_000_000)      # 株価242円・代金6億
    check("FADE_PX_MINは0（下限撤廃）", dp.FADE_PX_MIN == 0)
    check("300円未満でも候補に残る",
          len(dp.daily_top_fades({"9999.T": _low}, today, {"9999": "2"})) == 1)
    dp.FADE_PX_MIN = 300                                      # 復活経路が生きていること
    try:
        check("FADE_PX_MIN=300に戻せば除外される",
              dp.daily_top_fades({"9999.T": _low}, today, {"9999": "2"}) == [])
    finally:
        dp.FADE_PX_MIN = 0
    # 条件を満たす玉は必ずNO-GO玉より前に来る（画面の1番＝BTの1番）
    _mix = dp.daily_top_fades({"9999.T": _flat_then(21, rng_pct=0.5),   # ATR不足=撃たない
                               "6666.T": _flat_then(9)}, today, {"9999": "2", "6666": "2"})
    check("撃てる玉がNO-GO玉より前", _mix[0]["ticker"] == "6666.T" and _mix[0]["verdict"] == "GO")
    check("後ろのNO-GO玉も理由付きで見える", _mix[1]["verdict"] == "NOGO" and _mix[1]["nogo_reason"])
    # 2026-07-31: GO閾値 +6% → +7%（低位株を入れて再検証・PF1.43→1.52・両期間改善）
    check("DAILY_PICK_GAIN_MINは7.0", dp.DAILY_PICK_GAIN_MIN == 7.0)
    check("GO閾値+7%: 返る2件とも全部GO",
          all(p["verdict"] == "GO" for p in picks))
    check("n=3指定なら3件(後方互換)", len(dp.daily_top_fades(data, today, iss, n=3)) == 3)
    # 2026-07-28: GU下限は撤回（上位1〜3本の実運用条件では最悪年が2〜5倍悪化した）。
    # 寄指MINは前日終値＝「下寄りだけ見送る」に戻した。
    _mp, _pc = picks[0]["min_entry_price"], picks[0]["prev_close"]
    check("min指値=前日終値（GU下限なし）", abs(_mp - _pc) < 1.0)
    check("FADE_MIN_GAP_UP_PCTは0", dp.FADE_MIN_GAP_UP_PCT == 0.0)
    check("range_pct記録(>5%)", picks[0].get("range_pct", 0) > 5)

    # 貸借○が1つも無ければ[]（売れない玉は選ばない）
    check("貸借○ゼロ→空リスト", dp.daily_top_fades(data, today, {}) == [])

    # 閾値+7%の境界（貸借○だけの単独ケース）
    p8 = dp.daily_top_fades({"8888.T": _flat_then(8)}, today, {"8888": "2"})
    check("+8%→GO（閾値+7%を超える）", len(p8) == 1 and p8[0]["verdict"] == "GO")
    p65 = dp.daily_top_fades({"8888.T": _flat_then(6.5)}, today, {"8888": "2"})
    check("+6.5%→NOGO薄い（+7%未満は撃たない）",
          len(p65) == 1 and p65[0]["verdict"] == "NOGO" and "薄い" in p65[0]["nogo_reason"])
    p5 = dp.daily_top_fades({"8888.T": _flat_then(5.5)}, today, {"8888": "2"})
    check("+5.5%→NOGO薄い（閾値未満）",
          len(p5) == 1 and p5[0]["verdict"] == "NOGO" and "薄い" in p5[0]["nogo_reason"])

    # 張り付きS高(+20%貸借○)は除外 → 空リスト（踏み上げ回避の核心）
    check("張り付きS高は除外→空",
          dp.daily_top_fades({"5555.T": _flat_then(20, sticky=True)}, today, {"5555": "2"}) == [])

    # ── 2026-07-31 監査で見つかった3件の修正 ──────────────────────────────
    # ①ETF/ETN除外。銘柄マスタ（プライム/スタンダード/グロースのみ）に無い銘柄は撃たない。
    #   実害: 2020-03-12と2025-04-09は通常株の候補ゼロで、ベア2倍ETFの空売り指示だけが出ていた。
    # 1,000件超＝正常なマスタ。ETF帯(13xx)は含めない＝ここに無い銘柄が除外対象になる
    big = [(f"{i}.T", f"stock{i}") for i in range(3000, 4200)]
    screener.fetch_tse_universe = lambda *a, **k: big + [("6666.T", "普通株")]
    only_etf = dp.daily_top_fades({"1360.T": _flat_then(21)}, today, {"1360": "2"})
    check("マスタに無い銘柄(ETF)は候補から除外", only_etf == [])
    mixed = dp.daily_top_fades({"1360.T": _flat_then(25), "6666.T": _flat_then(9)},
                               today, {"1360": "2", "6666": "2"})
    check("ETFを飛ばして通常株が1番", len(mixed) == 1 and mixed[0]["ticker"] == "6666.T")
    # フェイルオープン: マスタが少数しか返らない（＝取得失敗のフォールバック）ならガードしない。
    # ここで閉じると毎日「シグナルなし」で黙って死ぬ。
    screener.fetch_tse_universe = lambda *a, **k: [("7203.T", "トヨタ")]
    check("マスタ取得失敗時はガードせず従来通り",
          len(dp.daily_top_fades({"1360.T": _flat_then(21)}, today, {"1360": "2"})) == 1)
    screener.fetch_tse_universe = lambda *a, **k: []

    # ②同点は ticker 昇順で決める（従来は辞書の並び順任せ＝BTと構造的に一致しない）
    a1, a2 = _flat_then(15), _flat_then(15)                        # 完全に同じ形＝必ず同点
    p_fwd = dp.daily_top_fades({"8111.T": a1, "8222.T": a2}, today, {"8111": "2", "8222": "2"}, n=1)
    p_rev = dp.daily_top_fades({"8222.T": a2, "8111.T": a1}, today, {"8111": "2", "8222": "2"}, n=1)
    check("同点の1番は入力順に依存しない",
          p_fwd[0]["ticker"] == p_rev[0]["ticker"] == "8111.T")

    # ③閾値判定は丸める前の値で行う（ATR4.996%が表示丸めで5.00%に化けて通るのを防ぐ）
    check("丸め前の値でATR下限を判定", dp.fade_nogo_reason(20.0, 4.996, 30.0) is not None)
    # 2026-08-01: 1桁丸めで閾値と同値に見える境界玉は3桁表示（「ATR5.0%<5%」の矛盾を避ける）
    check("境界玉は3桁で矛盾表示を避ける", "ATR4.996%" in dp.fade_nogo_reason(20.0, 4.996, 30.0))
    check("非境界は従来の1桁表示", "ATR3.4%" in dp.fade_nogo_reason(20.0, 3.44, 30.0))

    # 張り付き#1と非張り付き#2 → 非張り付きだけ残る
    mix = {"5555.T": _flat_then(30, sticky=True), "6666.T": _flat_then(18)}
    pm = dp.daily_top_fades(mix, today, {"5555": "2", "6666": "2"})
    check("張り付き#1を飛ばし6666だけ", len(pm) == 1 and pm[0]["ticker"] == "6666.T")

    # 値がさ株(1単元>予算100万=株価>1万円)は除外（2026-08-05 50万→100万・本人決定）
    check("値がさ株(>1万円)は除外",
          dp.daily_top_fades({"4444.T": _flat_then(20, base=20000)}, today, {"4444": "2"}) == [])
    check("100万境界: 終値10,800円は除外",
          dp.daily_top_fades({"3333.T": _flat_then(20, base=9000)}, today, {"3333": "2"}) == [])
    check("100万境界: 終値6,600円は対象内(50万時代は除外だった帯)",
          len(dp.daily_top_fades({"3333.T": _flat_then(20, base=5500)}, today, {"3333": "2"})) == 1)
    # 2026-08-18 100万へ復帰（入金で現金余力50万＝8/15の約束ライン40万を超過。
    # 70万は8/15の一時措置だった。友達用FRIENDS_SIZEは50万で独立(2026-08-18移設)）
    check("CAPITAL_PER_TRADEは100万(2026-08-18復帰)", dp.CAPITAL_PER_TRADE == 1_000_000)

    # 借りやすさグレード: ratio_mapを渡すとborrowが付く
    pr = dp.daily_top_fades({"9999.T": _flat_then(21)}, today, {"9999": "2"}, ratio_map={"9999": 45.0})
    check("ratio_map→borrow付与(売残少)", "◎売残少" in pr[0]["borrow"])

    # 鮮度ガード: 売買停止で最終足が古い銘柄(+30%だが7/10止まり)は除外し、直近足の銘柄を選ぶ
    stale = _flat_then(30)
    stale.index = stale.index - pd.Timedelta(days=4)   # 最終足を7/10に後退=停止中を再現
    mix2 = {"5550.T": stale, "6666.T": _flat_then(18)}
    pf2 = dp.daily_top_fades(mix2, today, {"5550": "2", "6666": "2"})
    check("停止中(古い足+30%)は除外→直近足6666", len(pf2) == 1 and pf2[0]["ticker"] == "6666.T")


def test_rank_within_go_and_raw_gain():
    """2026-08-01監査の2バグ: ①GO判定にround(gain,2)を使い raw+6.998%が「+7.00%」に化けて
    GOになっていた（ATR/乖離はda0ae89でraw判定に直したのにgainだけ直し漏れ・10年2件-3.3万）。
    ②順位（乖離+ATRの順位平均）を閾値未達のNO-GO玉込みの全候補で付けていたため、GOが3本以上の
    日に撃つ2本がBT（絞ってから順位）と食い違っていた（10年42日/1,191日・_audit_fade_rankpool.py）。"""
    import screener
    screener.fetch_tse_universe = lambda *a, **k: []
    today = date(2026, 7, 15)

    # ① base1429→1529 = raw+6.9979%。表示は+7.00%に丸まるが判定はraw＝NOGO
    p = dp.daily_top_fades({"7770.T": _flat_then(6.9979, base=1429)}, today, {"7770": "2"})
    check("raw+6.998%はNOGO（丸めでGOに化けない）", p[0]["verdict"] == "NOGO")
    check("境界玉のgainは3桁表示で矛盾を避ける", "6.99" in p[0]["nogo_reason"])

    # ② GO2本: A=9998(乖離30.8/ATR5.2) B=1111(乖離17.8/ATR7.9) ＋ NOGO玉X=5555(gain6%・
    # 乖離21.5がAとBの間)。GO内だけで順位を付けると2本は同点→ticker昇順で1111が1番。
    # 旧実装(全候補で順位)はXが乖離軸だけに割り込んで9998が1番になっていた（順位の入れ替わり）。
    data = {"9998.T": _flat_then(8.0, rng_pct=3.0, predrift_pct=45.0),
            "1111.T": _flat_then(8.0, rng_pct=4.5, predrift_pct=22.0),
            "5555.T": _flat_then(6.0, rng_pct=1.2, predrift_pct=33.0)}
    iss = {"9998": "2", "1111": "2", "5555": "2"}
    picks = dp.daily_top_fades(data, today, iss, n=3)
    check("GO2本+NOGO1本の構図", [q["verdict"] for q in picks] == ["GO", "GO", "NOGO"])
    check("順位はGO玉の中だけで決める（NO-GO玉に影響されない）",
          picks[0]["ticker"] == "1111.T" and picks[1]["ticker"] == "9998.T")


def test_fetch_failed_is_not_miokuri():
    """2026-08-02: データ取得失敗は「見送り」でなく「判定不能」として配信し、日付ガードを
    立てない（8:20保険便が再試行できるように）。従来は障害が正常な見送りに偽装されていた。"""
    import io
    import contextlib
    import json as _json
    stats = dp.cumulative_stats({"positions": [], "expired": []})

    def _desc(fetch_failed):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            dp.send_report([], [], [], stats, date(2026, 8, 3), dry=True,
                           fetch_failed=fetch_failed)
        return _json.loads(buf.getvalue())["embeds"][0]["description"]

    d_fail, d_ok = _desc(True), _desc(False)
    check("fetch失敗は「判定不能」表示", "データ取得に失敗" in d_fail and "判定不能" in d_fail)
    check("fetch失敗は「撃つ銘柄なし」を出さない", "撃つ銘柄なし" not in d_fail)
    # 2026-08-10 スイング書体への刷新で0件文言が変更（「撃つ銘柄なし」→下記）
    check("通常の0件は従来どおり「見送り」", "条件を満たす銘柄がありません" in d_ok)

    # run()レベル: fetch失敗→fetch_failed=Trueで配信・last_report_dateを立てない＝再試行可能
    book = {"positions": [], "expired": [], "last_report_date": None}
    sent = {}
    orig = (dp._fetch_all, dp.send_report, dp.load_book, dp.save_book)
    dp._fetch_all = lambda today: (_ for _ in ()).throw(RuntimeError("boom"))
    dp.send_report = lambda *a, **k: sent.update(k)
    dp.load_book = lambda: book
    dp.save_book = lambda b: None
    try:
        dp.run(today=date(2026, 8, 3), signals=[], dry=False)
    finally:
        dp._fetch_all, dp.send_report, dp.load_book, dp.save_book = orig
    check("run: fetch失敗はfetch_failed=Trueで配信", sent.get("fetch_failed") is True)
    check("run: fetch失敗は日付ガードを立てない（保険便が再試行できる）",
          book.get("last_report_date") is None)


def test_premium_pershare_line():
    """2026-08-15: 売り禁玉だけに出すプレミアム料の円/株判断行（アスタリスク8円/株が発端）。
    SBI画面の円/株とそのまま見比べられること・貸借○の玉には出ないことを確認する。"""
    import io
    import contextlib
    import json as _json
    stats = dp.cumulative_stats({"positions": [], "expired": []})

    def _desc(picks):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            dp.send_report([], [], picks, stats, date(2026, 8, 17), dry=True)
        return _json.loads(buf.getvalue())["embeds"][0]["description"]

    def _pick(rank, ticker, price, jsf):
        return {"verdict": "GO", "rank": rank, "ticker": ticker, "name": ticker[:4],
                "prev_close": float(price), "min_entry_price": float(price),
                "daily_gain": 13.9, "vol_ratio": 3.3, "range_pct": 17.5,
                "dev25": 27.6, "atr_pct": 9.7, "jsf_stop": jsf,
                "short": {"mark": "○", "iss": "2", "note": ""},
                "reg_note": "🚫売り禁" if jsf else ""}

    # アスタリスク実例: 2,165円。期待バンドは定数から動的に計算＝玉サイズ変更(70万⇔100万)に追従
    # （2026-08-15再導出 GAPDN0.45%/MAIN1.46%。例: 100万=400株なら11円/36円・70万=300株なら10円/34円）
    cap = dp.CAPITAL_PER_TRADE
    sh_a = int(cap / 2165 / 100) * 100
    ok = int(dp.FADE_EDGE_PCT_GAPDN / 100 * cap // sh_a)
    lim = int(dp.FADE_EDGE_PCT_MAIN / 100 * cap // sh_a)
    d = _desc([_pick(1, "6522.T", 2165, True), _pick(2, "3156.T", 5510, False)])
    check("売り禁玉に円/株の判断行が出る", "SBIのプレミアム料を見て" in d)
    check(f"成行のままの上限={ok}円/株", f"〜{ok}円/株→成行のまま" in d)
    check(f"寄指切替帯={ok + 1}〜{lim}円", f"{ok + 1}〜{lim}円→寄指¥2,165に変更" in d)
    # 2026-08-21 2本実弾化: #1を見送っても#2はもう建っているので「#2に振り替え」でなく「今日は#2だけ」
    check(f"{lim + 1}円〜は撃たない（今日は#2だけ）",
          f"{lim + 1}円〜→撃たない（今日は#2だけ）" in d)
    check("貸借○の玉には出さない（1回だけ）", d.count("SBIのプレミアム料") == 1)

    # 超低位株（多株数）: 50円→20,000株なら1円/株でもエッジ超え＝原則見送り
    d2 = _desc([_pick(1, "9999.T", 50, True)])
    check("低位株はバンドでなく原則見送り表示", "1円/株でもエッジ超え" in d2)

    # 売り禁が2番のとき: tailは「#2へ」でなく見送り
    d3 = _desc([_pick(1, "3156.T", 5510, False), _pick(2, "6522.T", 2165, True)])
    check("2番が売り禁なら超過帯は「見送り」表示", "撃たない（見送り）" in d3)


def test_common_stock_code_guard():
    """2026-08-02: 優先株式(5桁目≠0)の4桁衝突ガード。94345/94346(ソフトバンク優先株式)が
    94340(本体)を後勝ちで上書きし、伊藤園/インフロニア/ゼンショー/JAL/ANA/ソフトバンクの
    6銘柄が価格データ・貸借区分とも優先株式のものに置換されていた（IssType=1上書きで
    5銘柄が貸借×扱い＝フェード候補から永久除外）。"""
    from screener import is_common_stock_code
    check("普通株式(5桁目=0)は通る", is_common_stock_code("94340") and is_common_stock_code("72030"))
    check("英字入り新コードも通る", is_common_stock_code("130A0"))
    check("優先株式(5桁目≠0)は弾く", not is_common_stock_code("94345")
          and not is_common_stock_code("94346") and not is_common_stock_code("25935"))
    check("4桁以下は通る(旧互換)", is_common_stock_code("9434"))


def test_borrow_grade():
    check("倍率<1→⭐売り長", "⭐売り長" in dp.borrow_grade(0.6))
    check("倍率>=10→◎売残少", "◎売残少" in dp.borrow_grade(30))
    check("倍率1-10→○普通", dp.borrow_grade(2.0) == "○普通")
    check("None→貸株?", dp.borrow_grade(None) == "貸株?")


def _book_for_monthly():
    def row(d, yen, pct, et="CLOSE", tk="1111.T"):
        return {"ticker": tk, "name": "テスト社", "direction": "SELL", "signal_date": d,
                "status": "closed", "exit_type": et, "pnl_yen": yen, "pnl_pct": pct,
                "win": yen > 0}
    return {"positions": [
        row("2026-07-02", +10000, +2.0),
        row("2026-07-02", -30000, -1.0),          # 同じ日にもう1件（日次集計の確認）
        row("2026-07-15", +5000, +1.0),
        row("2026-07-20", 0, 0.0, et="SKIP"),      # 見送り＝成績に入れない
        row("2026-06-10", +99999, +9.9),           # 前月＝混ざってはいけない
        {"ticker": "9999.T", "name": "保有中", "direction": "SELL",
         "signal_date": "2026-07-31", "status": "pending"},   # 未決済＝入れない
    ], "expired": [], "last_report_date": None}


def test_monthly_stats():
    b = _book_for_monthly()
    m = dp.monthly_stats(b, "2026-07")
    check("月次: SKIPと未決済を除く3件", m["n"] == 3)
    check("月次: 見送り件数を別に持つ", m["n_skip"] == 1)
    check("月次: 前月が混ざらない", m["yen"] == 10000 - 30000 + 5000)
    check("月次: 勝率は円ベースの勝ち数", abs(m["win_rate"] - 2 / 3 * 100) < 0.01)
    check("月次: PF(円)とPF(%)は別物", abs(m["pf"] - 15000 / 30000) < 1e-9
          and abs(m["pf_pct"] - 3.0 / 1.0) < 1e-9)
    check("月次: 撃った日は2日", m["day_n"] == 2)
    check("月次: 最悪日は同日合算で判定", m["worst_day"][0] == "2026-07-02"
          and m["worst_day"][1] == -20000)
    check("月次: 最良/最悪トレード", m["best"]["pnl_yen"] == 10000
          and m["worst"]["pnl_yen"] == -30000)
    e = dp.monthly_stats(b, "2026-05")
    check("月次: 該当なしの月はn=0", e["n"] == 0)


def test_monthly_era_capital():
    """2026-08-15: 月利%は各月の玉サイズが分母（〜2026-07=50万/2026-08〜=100万）。
    現行100万で過去月を割ると月利が半分に薄まる＝監査第6弾の指摘の修正を固定する。"""
    import io
    import contextlib
    import json as _json
    b = {"positions": [
        {"status": "closed", "exit_type": "CLOSE", "signal_date": "2026-07-10",
         "direction": "SELL", "ticker": "1111.T", "name": "七月玉",
         "pnl_yen": -15000, "pnl_pct": -3.0, "rank": 1},
        {"status": "closed", "exit_type": "CLOSE", "signal_date": "2026-08-05",
         "direction": "SELL", "ticker": "2222.T", "name": "八月玉",
         "pnl_yen": 30000, "pnl_pct": 3.0, "rank": 1},
    ], "expired": []}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        dp.send_monthly(b, "2026-08", dry=True)
    d = _json.loads(buf.getvalue())["embeds"][0]["description"]
    mr8 = 30000 / dp.CAPITAL_PER_TRADE * 100     # 8月は現行定数が分母（サイズ変更に追従）
    ann = -3.0 + mr8
    check("7月-1.5万は50万分母で月利-3.0%", "`2026-07`" in d and "月利-3.0%" in d)
    check("8月+3.0万は現行定数分母", f"月利+{mr8:.1f}%" in d)
    check("年間%は月利の和", f"合計: {'+' if ann >= 0 else ''}{ann:.1f}%" in d)


def test_monthly_send_guard():
    b = _book_for_monthly()
    sent = []
    orig = dp.send_monthly
    dp.send_monthly = lambda book, ym, dry=False: (sent.append(ym), True)[1]
    try:
        dp.maybe_send_monthly(b, date(2026, 8, 3))
        check("月初に前月分を送る", sent == ["2026-07"])
        check("送信後にマーカーが立つ", b.get("last_monthly_report") == "2026-07")
        dp.maybe_send_monthly(b, date(2026, 8, 4))
        check("同じ月は二度送らない", sent == ["2026-07"])
        dp.maybe_send_monthly(b, date(2026, 9, 1))
        check("翌月になれば8月分を送る", sent == ["2026-07", "2026-08"])
    finally:
        dp.send_monthly = orig
    check("prev_month: 年跨ぎ", dp.prev_month(date(2026, 1, 5)) == "2025-12")
    check("prev_month: 通常", dp.prev_month(date(2026, 8, 1)) == "2026-07")


def test_monthly_no_data_no_send():
    """確定ゼロの月は無送信＝マーカーも立てない（翌月に持ち越さない）。"""
    b = {"positions": [], "expired": [], "last_report_date": None}
    check("確定なしならFalse", dp.send_monthly(b, "2026-07", dry=True) is False)
    dp.maybe_send_monthly(b, date(2026, 8, 3), dry=True)
    check("確定なしならマーカーを立てない", "last_monthly_report" not in b)


def test_weekly_stats():
    b = _book_for_monthly()
    # 2026-07-02(木)と07-15(水)はW27とW29
    check("週キー: 月曜始まり", dp.week_key(date(2026, 7, 2)) == "2026-W27")
    check("週キー: 日曜は同じ週", dp.week_key(date(2026, 7, 5)) == "2026-W27")
    check("週キー: 翌月曜で繰り上がる", dp.week_key(date(2026, 7, 6)) == "2026-W28")
    mon, fri = dp.week_range("2026-W27")
    check("週の範囲", mon == "2026-06-29" and fri == "2026-07-03")
    s = dp.weekly_stats(b, "2026-W27")
    check("週次: その週の2件だけ", s["n"] == 2 and s["yen"] == -20000)
    check("週次: SKIPは別勘定", dp.weekly_stats(b, "2026-W30")["n"] == 0)
    check("週次: 該当なしはn=0", dp.weekly_stats(b, "2026-W01")["n"] == 0)
    check("週次: PF(円)とPF(%)", abs(s["pf"] - 10000 / 30000) < 1e-9
          and abs(s["pf_pct"] - 2.0 / 1.0) < 1e-9)


def test_weekly_send_guard():
    b = _book_for_monthly()
    sent = []
    orig = dp.send_weekly
    dp.send_weekly = lambda book, wk, dry=False: (sent.append(wk), True)[1]
    try:
        # 2026-08-03(月)はW32 → 前週はW31
        dp.maybe_send_weekly(b, date(2026, 8, 3))
        check("週明けに前週分を送る", sent == ["2026-W31"])
        check("送信後にマーカー", b.get("last_weekly_report") == "2026-W31")
        dp.maybe_send_weekly(b, date(2026, 8, 4))
        check("同じ週は二度送らない", sent == ["2026-W31"])
        dp.maybe_send_weekly(b, date(2026, 8, 11))
        check("翌週になれば次を送る", sent == ["2026-W31", "2026-W32"])
    finally:
        dp.send_weekly = orig
    check("prev_week: 年跨ぎ", dp.prev_week(date(2026, 1, 5)) == "2026-W01")


def test_weekly_no_data_no_send():
    b = {"positions": [], "expired": [], "last_report_date": None}
    check("週次: 確定なしならFalse", dp.send_weekly(b, "2026-W31", dry=True) is False)
    dp.maybe_send_weekly(b, date(2026, 8, 3), dry=True)
    check("週次: 確定なしならマーカーを立てない", "last_weekly_report" not in b)


def run_all():
    for fn in [test_shortability, test_settle_buy_win, test_settle_buy_skip,
               test_settle_sell_win, test_settle_sell_skip, test_settle_pending_kept,
               test_settle_today_not_closed, test_settle_halt_skip, test_settle_expired,
               test_record_and_dedup, test_cumulative_stats,
               test_daily_top_fades, test_alert_map_exclusion,
               test_rank_within_go_and_raw_gain, test_fetch_failed_is_not_miokuri,
               test_premium_pershare_line,
               test_common_stock_code_guard, test_borrow_grade,
               test_monthly_stats, test_monthly_era_capital,
               test_monthly_send_guard, test_monthly_no_data_no_send,
               test_weekly_stats, test_weekly_send_guard, test_weekly_no_data_no_send]:
        print(f"\n▶ {fn.__name__}")
        fn()
    print(f"\n==== {PASS} PASS / {FAIL} FAIL ====")
    return FAIL == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_all() else 1)

"""
capitulation.py — 案A「投げ売り日ブースター」A/Bテスト専用ランナー
=================================================================
【目的】本番スイングBUYと"同じシグナル"を別Discordに配信し、
        「投げ売り日(BUY候補が多い日)に増し玉すると本当に儲かるか」を
        フォワードで検証する。本番(screener/main/notifier/tracker/close_check)は
        1行も変更せず import して再利用する＝A/Bの独立性を保証する。

【根拠(BT・memory)】候補21件以上の全面投げ売り日は top5採用分で
        +0.639%/件・PF1.80・勝率60.7%・陽性4/4年(通常日の約4倍)。
        ※「凪の日を切るゲート」は別BT(bt_pick_rank.py)で却下されている
          (2勝3敗・2024の+62.8%を捨てる)。robustなのは"上振れ側=増し玉"だけ。
        よって本ランナーは【毎日は普段どおり出す＋投げ売り日だけ増し玉】。

【口座設定】ユーザーの当面の本命=中50万に合わせ size=50万。BUYのみ
        (投げ売りエッジはBUY候補数の話・SELLは対象外)。出口・帳簿・寄指は
        本番と完全同一(tracker/close_check をそのまま流用)。

【案E同梱(2026-07-07)】top5選定に「1業種は同時保有 最大CAPIT_SECTOR_CAP(=3)枠」の
        分散キャップを追加(bt_sector_cap.py: cap=3は件数ほぼ不変・PF/累積微増・弱い年
        底上げのほぼタダの微益)。投げ売り日ほど業種が固まるので増し玉と相性が良い。
        CAPIT_SECTOR_CAP=0 or sector33_map欠損 なら無効＝現行と完全一致。

【比較軸(report)】positions_test.json の確定トレードを
        ①均等(weight=1・現行と同じ) vs ②投げ売り増し玉(weight=BOOST) で
        実現円/PFを並べ、さらに投げ売り日 vs 通常日のPFを出して n=145 の
        フォワード再現を見る。

使い方:
    python capitulation.py morning     # 朝8:05 相当（配信＋帳簿）
    python capitulation.py close        # 15:00 大引けチェック
    python capitulation.py report       # 週次比較レポート
    python capitulation.py selftest     # API不要のロジック検証
    (--force で時間/営業日ガードを無視)
"""

import os
import sys
import json
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

# ── パラメータ（envで上書き可＝コード変更なしで閾値/倍率を調整できる）──────
# envが空文字（Actionsで未設定のrepo変数を参照した場合など）でも既定に落ちるよう `or` を使う
CAPIT_THRESHOLD  = int(os.getenv("CAPIT_THRESHOLD") or "21")     # 全BUY候補がこれ以上=投げ売り日
CAPIT_BOOST      = float(os.getenv("CAPIT_BOOST") or "2.0")      # 投げ売り日の増し玉倍率
TEST_SIZE        = int(os.getenv("CAPIT_SIZE") or "500000")      # 1件のサイズ（中50万に合わせる）
# 案E同梱: 同時保有の同一sector33を最大N枠に制限（0=無効）。BT(bt_sector_cap.py)で
# cap=3が「件数ほぼ不変・PF/累積微増・弱い年底上げ」のほぼタダの微益＝投げ売り増し玉と相性良。
CAPIT_SECTOR_CAP = int(os.getenv("CAPIT_SECTOR_CAP") or "3")

POS_FILE   = "positions_test.json"
TODAY_SIG  = "today_signals_test.json"
LAST_CLOSE = "last_close_check_test.json"

# notifier/close_check がそのまま食える tier 辞書（key!="main"でサブ口座扱い＝ラベル付与）
TEST_TIER = {
    "key":           "test",
    "label":         "投げ売りブースター",
    "emoji":         "🔥",
    "size":          TEST_SIZE,
    "buy_pos_file":  POS_FILE,
    "sell_pos_file": "positions_sell_test.json",   # BUYのみ運用だが互換で定義
    "buy_webhook":   os.getenv("DISCORD_WEBHOOK_TEST_URL", "").strip(),
    "sell_webhook":  os.getenv("DISCORD_WEBHOOK_TEST_URL", "").strip(),
    "public_mirror": False,
}


# ══════════════════════════════════════════════════════════════════
#  純ロジック（screener/tracker/notifier に依存しない＝selftestで検証可能）
# ══════════════════════════════════════════════════════════════════
def classify_breadth(breadth: int) -> tuple[bool, float]:
    """その日の全BUY候補数から (投げ売り日か, 増し玉倍率) を返す。"""
    is_capit = breadth >= CAPIT_THRESHOLD
    weight = CAPIT_BOOST if is_capit else 1.0
    return is_capit, weight


def select_buy_top5(all_buy: list[dict], size: int,
                    open_positions: list[dict], max_signals: int,
                    secmap: dict | None = None, sector_cap: int | None = None) -> list[dict]:
    """本番 main._select_tier_signals と同一ロジック（BUY側）＋案E「分散キャップ」。
    サイズで買える(prev_close*100<=size)＆保有中でない候補からスコア降順に、
    「同時保有(既存+当日採用)の同一sector33が sector_cap 件に達したら
     その業種は以降スキップ」して最大 max_signals 件。bt_sector_cap.py の select と同一。
    all_buy は screener が既にスコア降順に並べたリストである前提。
    secmap が空(=sector33_map欠損) か sector_cap=0 ならキャップ無効＝現行と完全一致。"""
    from collections import Counter
    secmap = secmap or {}
    if sector_cap is None:
        sector_cap = CAPIT_SECTOR_CAP

    def _sec(tk: str) -> str:
        # 業種未登録は自分自身をキー化＝他と相互キャップしない（データ欠損で過剰カット防止）
        return secmap.get(tk) or f"__unk_{tk}"

    open_tickers = {p["ticker"] for p in open_positions
                    if p.get("status") in ("pending", "open")}
    sec_count: Counter = Counter()
    for p in open_positions:
        if p.get("status") in ("pending", "open"):
            sec_count[_sec(p["ticker"])] += 1

    picked: list[dict] = []
    for c in all_buy:
        if len(picked) >= max_signals:
            break
        if c.get("prev_close", 0) * 100 > size:
            continue
        if c["ticker"] in open_tickers:
            continue
        sec = _sec(c["ticker"])
        if sector_cap and sec_count[sec] >= sector_cap:
            continue
        picked.append(c)
        open_tickers.add(c["ticker"])
        sec_count[sec] += 1
    return picked


def _load_sector_map() -> dict:
    """sector33_map.json（ticker→33業種）を読む。無ければ空dict＝分散キャップ無効。"""
    try:
        with open("sector33_map.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[capit] sector33_map.json 読込失敗 → 分散キャップ無効: {e}")
        return {}


def tag_new_positions(positions: list[dict], before_keys: set,
                      weight: float, breadth: int) -> None:
    """今朝追加された(before_keysに無い)ポジションに weight/entry_breadth を刻む。
    tracker が dict をそのまま持ち回るので追加キーは以後も保持される。"""
    for p in positions:
        key = (p["ticker"], p["signal_date"])
        if key not in before_keys and "weight" not in p:
            p["weight"] = weight
            p["entry_breadth"] = breadth


def report_stats(closed: list[dict], size: int, threshold: int) -> dict:
    """確定トレードから ①均等 vs ②増し玉 の実現円/PF と、
    投げ売り日 vs 通常日 のバケット別成績を計算する。"""
    def _pf(pnls: list[float]) -> float | None:
        gains = sum(p for p in pnls if p > 0)
        losses = -sum(p for p in pnls if p < 0)
        if losses == 0:
            return None if gains == 0 else float("inf")
        return gains / losses

    def _bucket(rows: list[dict]) -> dict:
        pnls = [r["pnl_pct"] for r in rows]
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        yen = sum(size * p / 100 for p in pnls)
        return {
            "n": n,
            "win_pct": (wins / n * 100) if n else 0.0,
            "avg_pct": (sum(pnls) / n) if n else 0.0,
            "pf": _pf(pnls),
            "yen": yen,
        }

    valid = [c for c in closed
             if c.get("status") == "closed" and c.get("pnl_pct") is not None]

    flat = _bucket(valid)  # weight=1（現行運用と同じ）

    # 増し玉版: 各トレードを weight 倍のサイズで建てたときの実現円
    weighted_yen = sum(size * c.get("weight", 1.0) * c["pnl_pct"] / 100 for c in valid)
    # 増し玉版の実効PF（利益/損失を weight で加重）
    w_gain = sum(size * c.get("weight", 1.0) * c["pnl_pct"] / 100
                 for c in valid if c["pnl_pct"] > 0)
    w_loss = -sum(size * c.get("weight", 1.0) * c["pnl_pct"] / 100
                  for c in valid if c["pnl_pct"] < 0)
    weighted_pf = (None if w_loss == 0 and w_gain == 0
                   else float("inf") if w_loss == 0 else w_gain / w_loss)

    capit = _bucket([c for c in valid if c.get("entry_breadth", 0) >= threshold])
    normal = _bucket([c for c in valid if c.get("entry_breadth", 0) < threshold])

    return {
        "flat": flat,
        "weighted_yen": weighted_yen,
        "weighted_pf": weighted_pf,
        "capit_day": capit,
        "normal_day": normal,
    }


# ══════════════════════════════════════════════════════════════════
#  共通ヘルパー
# ══════════════════════════════════════════════════════════════════
def is_trading_day(d) -> bool:
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def _already_sent_today(path: str, today_str: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("date") == today_str
    except Exception:
        return False


def _post_capit_banner(today: date, breadth: int, weight: float, no_pick: bool = False) -> None:
    """投げ売り日に増し玉を促す 🔥 バナーをテストチャンネルへ。"""
    from notifier import _post
    date_str = today.strftime("%Y年%m月%d日")
    if no_pick:
        desc = (f"本日はBUY候補 **{breadth}件** ＝投げ売り日(閾値{CAPIT_THRESHOLD}件↑)。\n"
                f"ただし中50万枠で買える候補が無く、増し玉対象はありません。")
    else:
        desc = (f"本日はBUY候補 **{breadth}件**（閾値{CAPIT_THRESHOLD}件↑）＝**全面投げ売り日**。\n"
                f"📈 過去BT: 投げ売り日のtop5は **+0.64%/件・PF1.80・勝率60.7%（陽性4/4年）**。\n"
                f"→ 本日は **増し玉 {weight:.1f}倍** で発注推奨（信用余力の範囲で）。")
    payload = {"embeds": [{
        "title": f"🔥【投げ売り日】{date_str} — 増し玉推奨",
        "description": desc,
        "color": 0xFB8C00,  # オレンジ
        "footer": {"text": datetime.now(JST).strftime("%H:%M JST")},
    }]}
    _post(TEST_TIER["buy_webhook"], payload, "capit-banner")


# ══════════════════════════════════════════════════════════════════
#  朝: 配信＋帳簿（main.py の test-tier 版）
# ══════════════════════════════════════════════════════════════════
def run_morning(force: bool = False, screener_result: dict | None = None,
                pos_file: str = POS_FILE, post: bool = True) -> dict:
    now = datetime.now(JST)
    today = now.date()
    today_str = today.strftime("%Y-%m-%d")

    if not force:
        if not is_trading_day(today):
            print("[capit] 休場日 → スキップ")
            return {}
        if not (7 <= now.hour < 10 or (now.hour == 10 and now.minute <= 45)):
            print(f"[capit] 配信時間外（{now.strftime('%H:%M')} JST）→ スキップ")
            return {}
        if _already_sent_today(TODAY_SIG, today_str):
            print(f"[capit] 本日分({today_str})送信済み → スキップ")
            return {}

    # ① シグナル取得（本番と同一の run_screener を使う）
    if screener_result is None:
        from screener import run_screener
        _sig, _sell, macro, all_buy, all_sell = run_screener()
    else:
        all_buy = screener_result["all_buy"]
        macro = screener_result.get("macro", {})

    breadth = len(all_buy)
    is_capit, weight = classify_breadth(breadth)
    print(f"[capit] 全BUY候補 {breadth}件 / 投げ売り日={is_capit} / 増し玉×{weight}")

    from screener import MAX_SIGNALS
    from tracker import (load_positions, save_positions,
                         update_positions, add_signals_to_positions)

    # ② 帳簿更新（前日までの決済/寄指不成立を確定）
    positions = load_positions(pos_file)
    if [p for p in positions if p["status"] in ("pending", "open")]:
        positions, _closed, _expired, _still = update_positions(positions, today)

    # ③ top5選定（中50万・保有中除外・案E分散キャップcap3同梱）
    secmap = _load_sector_map()
    top5 = select_buy_top5(all_buy, TEST_SIZE, positions, MAX_SIGNALS, secmap=secmap)
    print(f"[capit] 中{TEST_SIZE//10000}万で買えるtop5: {len(top5)}件"
          f"（分散cap={CAPIT_SECTOR_CAP}・sector33 {len(secmap)}銘柄）")

    # ④ ポジション追加＋増し玉weight刻印
    before = {(p["ticker"], p["signal_date"]) for p in positions}
    positions = add_signals_to_positions(positions, top5, today, today)
    tag_new_positions(positions, before, weight, breadth)
    save_positions(positions, pos_file)

    # ⑤ 配信（本番 send_signals をそのまま流用）＋投げ売りバナー
    if post:
        from notifier import send_signals
        send_signals(top5, today, macro, today, tier=TEST_TIER)
        if is_capit:
            _post_capit_banner(today, breadth, weight, no_pick=(len(top5) == 0))

        from screener import yose_limit_price
        with open(TODAY_SIG, "w", encoding="utf-8") as f:
            json.dump({
                "date": today_str,
                "breadth": breadth,
                "is_capit": is_capit,
                "weight": weight,
                "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "BUY",
                             "prev_close": s.get("prev_close", 0),
                             "limit_price": yose_limit_price(s.get("prev_close", 0) or 0)}
                            for s in top5],
            }, f, ensure_ascii=False, indent=2)

    return {"breadth": breadth, "is_capit": is_capit, "weight": weight, "top5": top5}


# ══════════════════════════════════════════════════════════════════
#  15:00: 大引けチェック（close_check.py の test-tier 版）
# ══════════════════════════════════════════════════════════════════
def run_close(force: bool = False) -> None:
    now = datetime.now(JST)
    today = now.date()
    today_str = today.strftime("%Y-%m-%d")

    if not force:
        if not is_trading_day(today):
            print("[capit-close] 休場日 → スキップ")
            return
        if not (14 <= now.hour <= 17):
            print(f"[capit-close] 時間外（{now.strftime('%H:%M')}）→ スキップ")
            return
        if _already_sent_today(LAST_CLOSE, today_str):
            print(f"[capit-close] 本日分({today_str})送信済み → スキップ")
            return

    from close_check import collect_targets, _load_active
    from screener import batch_download_jquants, _jquants_id_token
    from notifier import send_close_signals, send_close_no_targets

    buy_open = _load_active(POS_FILE, "BUY")
    print(f"[capit-close] BUYオープン {len(buy_open)}件")

    historical_data: dict = {}
    if buy_open:
        tickers = {p["ticker"] for p in buy_open}
        token = _jquants_id_token()
        start = (today - timedelta(days=45)).strftime("%Y-%m-%d")
        end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        all_data = batch_download_jquants(token, start=start, end=end)
        for t in tickers:
            df = all_data.get(t)
            if df is not None and not df.empty:
                historical_data[t] = df

    targets, checked = collect_targets(buy_open, "BUY", today, historical_data)
    if targets:
        send_close_signals(targets, today, tier=TEST_TIER)
    elif checked:
        send_close_no_targets(checked, today, tier=TEST_TIER, sell=False)
    else:
        print("[capit-close] 保有なし → 通知なし")

    with open(LAST_CLOSE, "w", encoding="utf-8") as f:
        json.dump({"date": today_str, "targets": [t["ticker"] for t in targets]},
                  f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════
#  週次比較レポート
# ══════════════════════════════════════════════════════════════════
def _pf_str(pf) -> str:
    if pf is None:
        return "—"
    if pf == float("inf"):
        return "∞"
    return f"{pf:.2f}"


def run_report(force: bool = False, post: bool = True) -> dict:
    from tracker import load_positions
    positions = load_positions(POS_FILE)
    stats = report_stats(positions, TEST_SIZE, CAPIT_THRESHOLD)
    f, c, n = stats["flat"], stats["capit_day"], stats["normal_day"]

    print(f"[capit-report] 確定{f['n']}件 / 均等{f['yen']:+,.0f}円 PF{_pf_str(f['pf'])} "
          f"/ 増し玉{stats['weighted_yen']:+,.0f}円 PF{_pf_str(stats['weighted_pf'])}")
    print(f"  投げ売り日 {c['n']}件 勝率{c['win_pct']:.0f}% avg{c['avg_pct']:+.2f}% PF{_pf_str(c['pf'])}")
    print(f"  通常日   {n['n']}件 勝率{n['win_pct']:.0f}% avg{n['avg_pct']:+.2f}% PF{_pf_str(n['pf'])}")

    if post and TEST_TIER["buy_webhook"]:
        from notifier import _post
        lines = [
            f"確定トレード **{f['n']}件**（中{TEST_SIZE//10000}万枠）",
            "─" * 22,
            f"**A. 現行（均等）**  実現 {f['yen']:+,.0f}円 / PF {_pf_str(f['pf'])} / 勝率 {f['win_pct']:.0f}%",
            f"**B. 投げ売り増し玉×{CAPIT_BOOST:.0f}**  実現 {stats['weighted_yen']:+,.0f}円 / PF {_pf_str(stats['weighted_pf'])}",
            "─" * 22,
            "🔥 **投げ売り日** " + f"{c['n']}件 勝率{c['win_pct']:.0f}% avg{c['avg_pct']:+.2f}% PF{_pf_str(c['pf'])}",
            "😑 **通常日**   " + f"{n['n']}件 勝率{n['win_pct']:.0f}% avg{n['avg_pct']:+.2f}% PF{_pf_str(n['pf'])}",
            "",
            f"（BT前提: 投げ売り日 PF1.80 / 通常日はもっと低い。B>A なら増し玉に効果あり）",
        ]
        payload = {"embeds": [{
            "title": f"🔥【投げ売りブースター】A/B比較レポート {datetime.now(JST):%m/%d}",
            "description": "\n".join(lines),
            "color": 0xFB8C00,
            "footer": {"text": "現行運用は無変更・これは検証用の別口座"},
        }]}
        _post(TEST_TIER["buy_webhook"], payload, "capit-report")
    return stats


# ══════════════════════════════════════════════════════════════════
#  selftest（APIもファイルも触らずロジックだけ検証）
# ══════════════════════════════════════════════════════════════════
def _selftest() -> None:
    ok = 0

    # 1) breadth分類
    assert classify_breadth(CAPIT_THRESHOLD - 1) == (False, 1.0)
    assert classify_breadth(CAPIT_THRESHOLD) == (True, CAPIT_BOOST)
    assert classify_breadth(CAPIT_THRESHOLD + 50) == (True, CAPIT_BOOST)
    ok += 1

    # 2) select_buy_top5: サイズフィルタ＋保有中除外＋スコア順(入力順)維持＋top5
    cands = [
        {"ticker": "1.T", "prev_close": 1000},   # 100株=10万 ≤50万 OK
        {"ticker": "2.T", "prev_close": 6000},   # 100株=60万 >50万 NG
        {"ticker": "3.T", "prev_close": 500},    # OK だが保有中
        {"ticker": "4.T", "prev_close": 2000},   # OK
        {"ticker": "5.T", "prev_close": 100},    # OK
        {"ticker": "6.T", "prev_close": 300},    # OK
        {"ticker": "7.T", "prev_close": 400},    # OK（6件目=top5からあふれる）
    ]
    picked = select_buy_top5(cands, 500000, [{"ticker": "3.T", "status": "open"}], 5, secmap={})
    got = [p["ticker"] for p in picked]
    assert got == ["1.T", "4.T", "5.T", "6.T", "7.T"], got  # 2(高すぎ)/3(保有中)除外・順序維持
    ok += 1

    # 3) tag_new_positions
    positions = [
        {"ticker": "OLD.T", "signal_date": "2026-07-01"},                 # 既存
        {"ticker": "NEW.T", "signal_date": "2026-07-07"},                 # 今朝
    ]
    before = {("OLD.T", "2026-07-01")}
    tag_new_positions(positions, before, weight=2.0, breadth=25)
    assert "weight" not in positions[0]
    assert positions[1]["weight"] == 2.0 and positions[1]["entry_breadth"] == 25
    ok += 1

    # 4) report_stats: 均等 vs 増し玉 と バケット
    size = 500000
    closed = [
        # 投げ売り日(breadth25・weight2): +5%勝ち, -3%負け
        {"status": "closed", "pnl_pct": 5.0, "weight": 2.0, "entry_breadth": 25},
        {"status": "closed", "pnl_pct": -3.0, "weight": 2.0, "entry_breadth": 25},
        # 通常日(breadth10・weight1): +5%勝ち, -3%負け, +5%勝ち
        {"status": "closed", "pnl_pct": 5.0, "weight": 1.0, "entry_breadth": 10},
        {"status": "closed", "pnl_pct": -3.0, "weight": 1.0, "entry_breadth": 10},
        {"status": "closed", "pnl_pct": 5.0, "weight": 1.0, "entry_breadth": 10},
        {"status": "open", "pnl_pct": None, "weight": 1.0, "entry_breadth": 30},  # 未確定→無視
    ]
    st = report_stats(closed, size, CAPIT_THRESHOLD)
    # 均等: pnl合計 = 5-3+5-3+5 = 9% → 円 = 500000*9/100 = 45,000
    assert abs(st["flat"]["yen"] - 45000) < 1e-6, st["flat"]["yen"]
    assert st["flat"]["n"] == 5
    # 増し玉: 投げ売り2件が2倍 → (5-3)*2 + (5-3+5) = 4 + 7 = 11% → 55,000
    assert abs(st["weighted_yen"] - 55000) < 1e-6, st["weighted_yen"]
    # バケット件数
    assert st["capit_day"]["n"] == 2 and st["normal_day"]["n"] == 3
    # 投げ売り日PF = 利得5 / 損失3
    assert abs(st["capit_day"]["pf"] - (5.0 / 3.0)) < 1e-6, st["capit_day"]["pf"]
    ok += 1

    # 5) 案E分散キャップ: 同一sector33が sector_cap 件でその業種スキップ
    scands = [{"ticker": t, "prev_close": 500} for t in ["A.T", "B.T", "C.T", "D.T", "E.T"]]
    smap = {"A.T": "銀行業", "B.T": "銀行業", "C.T": "銀行業", "D.T": "電気機器", "E.T": "電気機器"}
    got = [p["ticker"] for p in select_buy_top5(scands, 500000, [], 5, secmap=smap, sector_cap=2)]
    assert got == ["A.T", "B.T", "D.T", "E.T"], got  # 銀行は2件で打ち止め→C除外
    # 既存保有に銀行1件(Z)があると新規銀行は1件しか採れない（同時保有ベース）
    got2 = [p["ticker"] for p in select_buy_top5(
        scands, 500000, [{"ticker": "Z.T", "status": "open"}], 5,
        secmap={**smap, "Z.T": "銀行業"}, sector_cap=2)]
    assert got2 == ["A.T", "D.T", "E.T"], got2   # 既存Z + A で cap2到達→B,C除外
    # cap=0（無効）なら現行と同じ＝top5全部
    got3 = [p["ticker"] for p in select_buy_top5(scands, 500000, [], 5, secmap=smap, sector_cap=0)]
    assert got3 == ["A.T", "B.T", "C.T", "D.T", "E.T"], got3
    ok += 1

    print(f"[selftest] 全{ok}グループ PASS ✅  "
          f"(THRESHOLD={CAPIT_THRESHOLD}, BOOST={CAPIT_BOOST}, SIZE={TEST_SIZE}, "
          f"SECTOR_CAP={CAPIT_SECTOR_CAP})")


# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    args = sys.argv[1:]
    force = "--force" in args
    cmd = next((a for a in args if not a.startswith("-")), "selftest")

    if cmd == "morning":
        run_morning(force=force)
    elif cmd == "close":
        run_close(force=force)
    elif cmd == "report":
        run_report(force=force)
    elif cmd == "selftest":
        _selftest()
    else:
        print(f"unknown command: {cmd}")
        print("usage: python capitulation.py [morning|close|report|selftest] [--force]")
        sys.exit(1)

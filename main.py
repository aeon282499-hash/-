"""
main.py — エントリーポイント（Phase 2: 3階層独立運用）
==============================
毎営業日 8:00 JST に GitHub Actions から呼ばれる。

3階層構成（大資金/中資金/小資金）。各階層で独立に銘柄選定・positions管理:
- 大資金: 1件100万 / positions.json / positions_sell.json
- 中資金: 1件 50万 / positions_mid.json / positions_sell_mid.json
- 小資金: 1件 30万 / positions_small.json / positions_sell_small.json

実行フロー（各階層を順に処理）:
  1. 今日が営業日か判定（土日・祝日はスキップ）
  2. screener.run_screener() で「スコア降順全候補」を取得
  3. 各階層ごとに:
     a. 前日ポジションの結果チェック → Discord に送信
     b. サイズで買える銘柄から自分のpositions保有中を除外→top5
     c. notifier.send_signals() で Discord 通知
     d. positions に追加して保存
  4. 例外発生時は notifier.send_error() でエラー通知
"""

import os
import sys
import json
from datetime import datetime, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")


# ── 階層定義 ─────────────────────────────────────
# tier=None（または最初の要素）が大資金（既存の DISCORD_WEBHOOK_URL / DISCORD_WEBHOOK_SELL_URL を使う）
TIERS = [
    {
        "key":             "main",
        "label":           "大資金",
        "emoji":           "",
        "size":            1_000_000,
        "buy_pos_file":    "positions.json",
        "sell_pos_file":   "positions_sell.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
        # 2026-05-21: DISCORD_WEBHOOK_URL_PUBLIC が大資金チャンネルを指していて二重投稿に
        # なっていたため停止（6b86ff0はnotifier側の既定tierのみでここが漏れていた）。
        # note専用チャンネルのWebhook用意後に notifier/close_check と合わせて True に戻す。
        "public_mirror":   False,
    },
    {
        "key":             "mid",
        "label":           "中資金",
        "emoji":           "🔵",
        "size":            500_000,
        "buy_pos_file":    "positions_mid.json",
        "sell_pos_file":   "positions_sell_mid.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
        "public_mirror":   False,
    },
    {
        "key":             "small",
        "label":           "小資金",
        "emoji":           "🟢",
        "size":            300_000,
        "buy_pos_file":    "positions_small.json",
        "sell_pos_file":   "positions_sell_small.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
        "public_mirror":   False,
    },
]


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    # 年末年始(12/31〜1/3)は東証休業（jpholidayは1/1のみ・2026-08-02統一）。
    # next/prev_trading_day もこの関数経由なので、年跨ぎの期限計算も同時に直る。
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3):
        return False
    return True


def next_trading_day(d) -> object:
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def prev_trading_day(d) -> object:
    prv = d - timedelta(days=1)
    while not is_trading_day(prv):
        prv -= timedelta(days=1)
    return prv


def is_month_first_trading_day(d) -> bool:
    """今日がその月の最初の営業日か（前営業日が別月なら True）。"""
    return prev_trading_day(d).month != d.month


# 業種分散キャップ（2026-07-09 bt_sector_cap.py・中50万選定シム 2022-2026/7）:
# 投げ売り日にtop5が同一業種へ固まる相関DDを抑える。cap=3で PF1.20→1.22・
# 累積+483.8→+515.0%・MaxDD-105.6→-101.2%・件数-0.9%・弱い年(2022/2026)ほど改善・
# 陽性年5/5維持。cap=2/1は悪化=3が山。BUYのみ適用（SELLはBT未検証・件数僅少）。
SECTOR_CAP = 3

# SELL版・業種分散キャップ（2026-07-22 sell_group_cap_bt.py・大中小3階層選定シム 2022-2026）:
# 相関の高い同業種の空売りが同日に固まる担がれ(踏み上げ)DDを抑える。cap=2で
# 大PF1.40→1.53/中1.40→1.58/小1.24→1.36・前期(2022-23)改善/後期(2024-26)不変/全年悪化なし・
# 除外7件。cap=3は4.5年で一度も発火せず無効・cap=1は切り過ぎで後期/2025が悪化=2が山。
# BUYと同一機構。※今日型の「銀行2＋その他金融2」等の跨ぎクラスタは各2枚でcap内=止めない
# （金融束ねcapはBT無害だが証拠ゼロの後付けのため不採用・2026-07-22判断）。
SECTOR_CAP_SELL = 2
_SECTOR33: dict | None = None


def _sector_of(ticker: str) -> str:
    """ticker('7203.T')→33業種。不明銘柄は自分自身をキー化＝他と相互キャップしない
    （マップ欠損で過剰カットしないためのフェイルセーフ・BTと同一仕様）。"""
    global _SECTOR33
    if _SECTOR33 is None:
        try:
            with open("sector33_map.json", encoding="utf-8") as f:
                _SECTOR33 = json.load(f)
        except Exception:
            _SECTOR33 = {}
    return _SECTOR33.get(ticker) or f"__unk_{ticker}"


def _select_tier_signals(all_buy_candidates: list[dict],
                         all_sell_candidates: list[dict],
                         tier: dict,
                         buy_positions: list[dict],
                         sell_positions: list[dict],
                         max_signals: int,
                         dc_max: float | None = None) -> tuple[list[dict], list[dict]]:
    """階層の口座サイズで買える＆自分の保有中銘柄を除外し、BUY/SELLとも同時保有の
    同一業種を SECTOR_CAP / SECTOR_CAP_SELL 件までに制限（超過は次点繰り上げ）したtop5を返す。

    dc_max: 買残回転日数の上限（2026-08-02〜）。screenerのプール除外は緩い側
    (MARGIN_DC_POOL_MAX=1.2)で行い、通常版（友達用3階層）はここで従来どおり0.8を適用
    ＝通常版のトレードは完全不変。極みだけ1.2で選定する（10年+194万→+311万・PF1.13→1.21・
    勝ち年7→9/10・両期間改善・根拠=_bt_kiwami_axes2.py）。days_cover欠損はフェイルオープン。"""
    size = tier["size"]
    buy_open = {p["ticker"] for p in buy_positions if p.get("status") in ("pending", "open")}
    sell_open = {p["ticker"] for p in sell_positions if p.get("status") in ("pending", "open")}

    # 保有中の業種カウント（pending=寄指の約定待ちも保守的に数える・buy_openと同基準）
    sec_count: dict[str, int] = {}
    for tkr in buy_open:
        sec = _sector_of(tkr)
        sec_count[sec] = sec_count.get(sec, 0) + 1

    buy_pool = []
    capped = 0
    for c in all_buy_candidates:
        if len(buy_pool) >= max_signals:
            break
        if c.get("prev_close", 0) * 100 > size or c["ticker"] in buy_open:
            continue
        _dcv = c.get("days_cover")
        if dc_max is not None and _dcv is not None and _dcv > dc_max:
            continue   # 買残回転がこの階層の上限超（通常版0.8/極み1.2）
        sec = _sector_of(c["ticker"])
        if sec_count.get(sec, 0) >= SECTOR_CAP:
            capped += 1
            continue
        sec_count[sec] = sec_count.get(sec, 0) + 1
        buy_pool.append(c)
    if capped:
        print(f"[main-{tier['label']}] 業種キャップ(cap={SECTOR_CAP})で{capped}件を次点繰り上げ")

    # SELLも同一業種を SECTOR_CAP_SELL 件までに制限（相関ショートの担がれ抑制・2026-07-22）
    sell_sec_count: dict[str, int] = {}
    for tkr in sell_open:
        sec = _sector_of(tkr)
        sell_sec_count[sec] = sell_sec_count.get(sec, 0) + 1

    sell_pool = []
    sell_capped = 0
    for c in all_sell_candidates:
        if len(sell_pool) >= max_signals:
            break
        if c.get("prev_close", 0) * 100 > size or c["ticker"] in sell_open:
            continue
        sec = _sector_of(c["ticker"])
        if sell_sec_count.get(sec, 0) >= SECTOR_CAP_SELL:
            sell_capped += 1
            continue
        sell_sec_count[sec] = sell_sec_count.get(sec, 0) + 1
        sell_pool.append(c)
    if sell_capped:
        print(f"[main-{tier['label']}] SELL業種キャップ(cap={SECTOR_CAP_SELL})で{sell_capped}件を次点繰り上げ")

    return buy_pool, sell_pool


def main() -> None:
    now   = datetime.now(JST)
    today = now.date()
    print(f"[main] 実行日時: {now.strftime('%Y-%m-%d %H:%M JST')}")

    # ── 前夜配信モード（EVENING_RUN=1・2026-08-09本人依頼「翌営業日の銘柄を前日21時に」）──
    # このランを「翌営業日の朝8:05ランの前倒し実行」として動かす。株価四本値は当日16:30頃・
    # 信用週末残高は火曜16:30頃にJ-Quants公開済み（公式仕様）なので、夜21時の時点で翌朝ランと
    # 同一データ＝同一銘柄になる。やることは today を翌営業日へ差し替えるだけで、選定・帳簿・
    # ファイル出力・極み(shadow_exit)まで朝ランと完全一致（screener側の基準日は
    # SIGNAL_EFFECTIVE_DATE 経由で追従）。配信成功時は today_signals.json の日付が翌営業日に
    # なるため、翌朝8:05ランは既存の送信済みガードで自然にスキップ＝朝ランは保険として無傷。
    # 金曜夜のランは「翌営業日=月曜(祝なら火曜)」分を出す。休場日の夜は新しい終値がないので何もしない。
    evening = os.getenv("EVENING_RUN", "").strip() == "1"
    if evening:
        # 17時ガード: 四本値は16:30公開（公式仕様）なので17時以降なら朝ランと同一データ。
        # 2026-08-28に19→17へ緩和（ハイカラ在庫が19時解禁＝18:50着弾に前倒し・本人依頼）。
        if now.hour < 17:
            print(f"[main] 前夜配信モードだが時間外（{now.strftime('%H:%M')} JST）→ スキップします")
            sys.exit(0)
        if not is_trading_day(today):
            print("[main] 前夜配信モード: 本日は休場＝新しい終値が出ない日 → スキップします")
            sys.exit(0)
        today = next_trading_day(today)
        os.environ["SIGNAL_EFFECTIVE_DATE"] = today.strftime("%Y-%m-%d")
        print(f"[main] 前夜配信モード: {now.date()} の夜 → {today} 分のシグナルとして実行")

    # 配信許可窓 7:00〜10:45 JST（朝ランのみ）。Cloudflare本トリガ(8:05)が落ちた日に、
    # GitHubスケジュール保険(cron 8:20)が最大2h遅延(実測10:37着)しても拾えるよう
    # 9:30→10:45 に拡張。本トリガ成功時は today_signals.json の送信済みガードで二重送信を防ぐ。
    if not evening and not (7 <= now.hour < 10 or (now.hour == 10 and now.minute <= 45)):
        print(f"[main] 配信時間外（{now.strftime('%H:%M')} JST）→ スキップします")
        sys.exit(0)

    if not is_trading_day(today):
        reason = (
            "土日のため休場" if today.weekday() >= 5
            else f"{jpholiday.is_holiday_name(today)} のため休場"
        )
        print(f"[main] {reason} → スキップします")
        sys.exit(0)

    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists("today_signals.json"):
        with open("today_signals.json", encoding="utf-8") as _f:
            _saved = json.load(_f)
        if _saved.get("date") == today_str:
            # 2026-09-03 監査: ガードは極み配信より前に書かれるので、Discord側の失敗で極みだけ
            # 未達の日がある。その時は台帳を触らず当日分を再送するだけ（保険ランが自然に拾う）。
            try:
                from shadow_exit import needs_resend, resend_only
                if needs_resend(today):
                    print(f"[main] 本日分({today_str})は送信済みだが極み配信が未達 → 極みだけ再送")
                    ok = resend_only(today)
                    print("[main] 極み再送 " + ("成功" if ok else "失敗（次の保険ランで再試行）"))
                    sys.exit(0)
            except SystemExit:
                raise
            except Exception as _re:
                print(f"[main] 極み再送チェック失敗: {_re}")
            print(f"[main] 本日分({today_str})は送信済みです → スキップ")
            sys.exit(0)

    from screener import run_screener, yose_limit_price, MAX_SIGNALS
    from notifier import (send_signals, send_results, send_error, send_monthly_report,
                          send_sell_signals, send_sell_results, send_sell_monthly_report,
                          send_monthly_bundle)
    from tracker import (load_positions, save_positions, update_positions, add_signals_to_positions,
                         load_sell_positions, save_sell_positions)

    try:
        # ── ① スクリーニング（共通: スコア降順全候補を取得）────────────────
        signals_main, sell_signals_main, macro, all_buy, all_sell = run_screener()
        if evening and not macro.get("data_ok", True):
            # 当日終値がまだ公開されていない（J-Quants 16:30更新の遅延等）。古いデータで
            # 選定・記帳する事故を防ぐため、帳簿もファイルも触らず即終了 →
            # today_signals.json が昨日のままなので翌朝8:05ランがフルフォールバックする。
            print("[main] 前夜配信モード: 当日終値が未公開 → 何もせず終了（朝ランに委ねます）")
            sys.exit(0)
        if not evening and not macro.get("data_ok", True):
            # 2026-09-03 監査: 朝ランはここを見ておらず、J-Quants障害の日に「候補なし」を配信して
            # ガードを立て、8:20の保険ランまで自分で塞いでいた。データ未取得はエラーとして落とす。
            raise RuntimeError("当日データ未取得（J-Quants障害/公開遅延）: 配信せず終了＝保険ランに委ねる")
        print(f"[main] 全BUY候補 {len(all_buy)}件 / 全SELL候補 {len(all_sell)}件")

        # 大資金用 top5 はメインチャンネル用に保存
        entry_date = today  # 当日寄り付きエントリー

        # 月別・年間損益レポートは「その月の最初の営業日」だけ配信する（毎日は不要・
        # ユーザー指示 2026-06-26）。月初に前月が確定した形で月1回届く。同日の二重実行は
        # 上の today_signals.json 日付ガードで弾かれるので重複配信にはならない。
        send_monthly = is_month_first_trading_day(today)
        # 月次chへは階層ごとに送らず、全階層のembedをここに集めてループ後に1通で送る
        # （2026-09-01 本人「月次レポート何回も来てるわかりずらい」＝最大6通→1通）。
        monthly_embeds: list[dict] = []
        if send_monthly:
            print("[main] 月初営業日 → 月別・年間損益レポートを配信")

        # ── 帳簿更新用の価格データ（全階層・BUY/SELLで1回だけ取得して共有）────
        # 旧: update_positions が階層×方向ごとに30日窓を最大6回fetchしていた。
        # RSIウォームアップのため窓を120日に広げるのに合わせ、遅延1回取得に集約
        # （保有ゼロの日はfetchしない）。窓の根拠は screener.RSI_WARMUP_CAL_DAYS 参照。
        from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS
        _ledger_cache: dict = {}

        def _decision_scope(key: str) -> str:
            """close_decisions の scope（階層別・close_check.py と対で 2026-09-03）。"""
            return "main" if key == "main" else f"main_{key}"

        def _ledger_data() -> dict:
            if "data" not in _ledger_cache:
                start = (today - timedelta(days=RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
                _ledger_cache["data"] = batch_download_jquants(
                    _jquants_id_token(), start=start, end=today_str)
            return _ledger_cache["data"]

        # ── ② 各階層を順に処理 ──────────────────────────
        first_tier_signals = []
        first_tier_sell_signals = []
        first_tier_closed = []

        for tier in TIERS:
            label = tier["label"]
            key   = tier["key"]
            if not tier["buy_webhook"] and key != "main":
                # サブ口座のwebhook未設定はスキップ（envなし環境）
                print(f"[main-{label}] buy_webhook未設定 → スキップ")
                continue

            print(f"\n========== [{label} ({tier['size']//10000}万)] ==========")

            # 前日結果チェック
            positions      = load_positions(tier["buy_pos_file"])
            sell_positions = load_sell_positions(tier["sell_pos_file"])

            active = [p for p in positions if p["status"] in ("pending", "open")]
            print(f"[main-{label}] BUYオープン {len(active)}件 / SELLオープン "
                  f"{len([p for p in sell_positions if p['status'] in ('pending','open')])}件")

            closed_today = []
            still_open   = []
            if active:
                # update_positions は約定確認・決済・寄指不成立(NOFILL)確定の帳簿処理なので継続。
                # 日別の決済結果レポート(send_results)はユーザー指示(2026-06-26)で廃止＝成績は
                # 週次/月次のみ。保有/処分期限とNOFILLは15時の大引けチェックで吸収される。
                positions, closed_today, expired_today, still_open = update_positions(
                    positions, today, all_data=_ledger_data(), scope=_decision_scope(key))
            if send_monthly:
                # 月次はポジションゼロの朝でも送る（closed履歴から集計できる）。
                # 旧: if active の内側にあり、月初の朝にフラットな階層は月次が欠落する穴だった
                # （2026-07-19修正・7/1は3階層とも保有があったため実害は未発生）。
                send_monthly_report(positions, today, tier=tier, collect=monthly_embeds)

            sell_closed_today = []
            sell_still_open   = []
            sell_active = [p for p in sell_positions if p["status"] in ("pending", "open")]
            if sell_active:
                # 帳簿処理は継続・日別の決済結果レポート(send_sell_results)は廃止（同上）。
                sell_positions, sell_closed_today, _sell_expired, sell_still_open = update_positions(
                    sell_positions, today, all_data=_ledger_data(), scope=_decision_scope(key))
            if send_monthly and sell_positions:
                # SELLの月次は「取引履歴が1件でもあれば」送る（現状SELLは全期間0件=帳簿が
                # 空なので送らない＝ゼロ行レポートのノイズ回避。初トレード後の月から出る）。
                send_sell_monthly_report(sell_positions, today, tier=tier, collect=monthly_embeds)

            # 階層別シグナル選定（通常版は買残回転0.8を維持＝従来とトレード完全一致）
            from screener import MARGIN_DAYS_COVER_MAX
            tier_signals, tier_sell_signals = _select_tier_signals(
                all_buy, all_sell, tier, positions, sell_positions, MAX_SIGNALS,
                dc_max=MARGIN_DAYS_COVER_MAX,
            )
            print(f"[main-{label}] サイズ{tier['size']//10000}万で買える: "
                  f"BUY {len(tier_signals)}件 / SELL {len(tier_sell_signals)}件")

            # ── 極み用シグナル（買残回転1.2で選定・2026-08-02）─────────────
            # 通常版(0.8)より広い候補で同じ選定を行い、専用ファイルに書く。
            # shadow_exit.record_signals がこれを優先して読む（無ければ通常版へフォールバック）。
            # 【2026-08-04 バグ修正】このブロックは add_signals_to_positions の**前**に
            # 置くこと。旧位置（記帳後）では当日の通常版採用5本が「保有中」扱いで除外され、
            # 極みファイルが「追加帯の差分だけ」になっていた（8/3=0件・8/4=1件の実害）。
            # 正しい極み＝通常版と同じ選定を dc1.2 の広い候補でやり直したスーパーセット。
            if key == "main":
                try:
                    from screener import MARGIN_DC_POOL_MAX
                    kiwami_signals, _ = _select_tier_signals(
                        all_buy, [], tier, positions, sell_positions, MAX_SIGNALS,
                        dc_max=MARGIN_DC_POOL_MAX,
                    )
                    with open("today_signals_kiwami.json", "w", encoding="utf-8") as f:
                        json.dump({
                            "date":    today_str,
                            "signals": [{"ticker": s["ticker"], "name": s["name"],
                                         "direction": "BUY",
                                         "prev_close": s.get("prev_close", 0),
                                         "limit_price": yose_limit_price(s.get("prev_close", 0) or 0),
                                         "days_cover": s.get("days_cover"),
                                         # 配信を通常版と同形式にするための表示用指標（2026-08-04）
                                         "rsi": s.get("rsi"),
                                         "deviation": s.get("deviation"),
                                         "vol_ratio": s.get("vol_ratio"),
                                         "range_ratio": s.get("range_ratio"),
                                         "turnover": s.get("turnover", 0)}
                                        for s in kiwami_signals],
                        }, f, ensure_ascii=False, indent=2)
                    print(f"[main-極み] 買残1.2選定: {len(kiwami_signals)}件 → today_signals_kiwami.json")
                except Exception as _ke:
                    # 極みファイル生成が死んでも友達向け配信(中・小)は止めない。
                    print(f"[main-極み] 生成失敗（通常版にフォールバック）: {_ke}")

                # ── 極上（2026-09-05 本人決定「買いシグナル極み 1玉300万・保有は1銘柄」）────
                # 極みの入口5条件＋買残1.2に「5日出来高トレンド≤GOKUJO_VT5_MAX（出来高が枯れた押し目）」を
                # 足し、同じscore順で選ぶ。枠1×300万は shadow_exit 側（記帳・配信）。値がさは1万円カット。
                # 根拠: _bt_kiwami_gokujo_0905.py 10年 1枠×300万=+536万/勝率57.9%/件+0.45%/DD-51万、
                # 26年 17-21 年+55万/22-26 年+55万。vt5欠損は見送り（BTは有限値のみ）。
                try:
                    from shadow_exit import GOKUJO_VT5_MAX, GOKUJO_SIG_FILE, GOKUJO_SIZE, GOKUJO_PX_CAP
                    from screener import MARGIN_DC_POOL_MAX as _dc_pool
                    _g_pool = [c for c in all_buy
                               if c.get("vt5") is not None and c["vt5"] <= GOKUJO_VT5_MAX
                               and (c.get("prev_close") or 0) <= GOKUJO_PX_CAP]
                    _g_tier = {"key": "gokujo", "label": "極上", "size": GOKUJO_SIZE}
                    gokujo_signals, _ = _select_tier_signals(
                        _g_pool, [], _g_tier, [], [], MAX_SIGNALS, dc_max=_dc_pool,
                    )
                    with open(GOKUJO_SIG_FILE, "w", encoding="utf-8") as f:
                        json.dump({
                            "date":    today_str,
                            "signals": [{"ticker": s["ticker"], "name": s["name"],
                                         "direction": "BUY",
                                         "prev_close": s.get("prev_close", 0),
                                         "limit_price": yose_limit_price(s.get("prev_close", 0) or 0),
                                         "days_cover": s.get("days_cover"),
                                         "vt5": s.get("vt5"),
                                         "rsi": s.get("rsi"),
                                         "deviation": s.get("deviation"),
                                         "vol_ratio": s.get("vol_ratio"),
                                         "range_ratio": s.get("range_ratio"),
                                         "turnover": s.get("turnover", 0)}
                                        for s in gokujo_signals],
                        }, f, ensure_ascii=False, indent=2)
                    print(f"[main-極上] vt5≤{GOKUJO_VT5_MAX} 候補{len(_g_pool)}件 → 選定{len(gokujo_signals)}件 → {GOKUJO_SIG_FILE}")
                except Exception as _ge:
                    print(f"[main-極上] 生成失敗（極上は当日見送り・他の配信に影響なし）: {_ge}")

            # 新規シグナル追加・保存
            positions      = add_signals_to_positions(positions, tier_signals, today, entry_date)
            sell_positions = add_signals_to_positions(sell_positions, tier_sell_signals, today, entry_date)
            save_positions(positions, tier["buy_pos_file"])
            save_sell_positions(sell_positions, tier["sell_pos_file"])

            # 配信
            send_signals(tier_signals, today, macro, entry_date, tier=tier)
            send_sell_signals(tier_sell_signals, today, entry_date, tier=tier)

            # 階層別の当日シグナル記録（夕方report.py用）
            if key == "main":
                today_sig_file      = "today_signals.json"
                today_sell_sig_file = "today_sell_signals.json"
            else:
                today_sig_file      = f"today_signals_{key}.json"
                today_sell_sig_file = f"today_sell_signals_{key}.json"
            # prev_close/limit_price は夕方report.pyの寄指不成立スキップ判定に必要
            with open(today_sig_file, "w", encoding="utf-8") as f:
                json.dump({
                    "date":    today_str,
                    "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "BUY",
                                 "prev_close": s.get("prev_close", 0),
                                 "limit_price": yose_limit_price(s.get("prev_close", 0) or 0)}
                                for s in tier_signals],
                }, f, ensure_ascii=False, indent=2)
            with open(today_sell_sig_file, "w", encoding="utf-8") as f:
                json.dump({
                    "date":    today_str,
                    "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "SELL",
                                 "prev_close": s.get("prev_close", 0)}
                                for s in tier_sell_signals],
                }, f, ensure_ascii=False, indent=2)

            # 大資金分は後段（Twitter等）で使うので保持
            if key == "main":
                first_tier_signals = tier_signals
                first_tier_sell_signals = tier_sell_signals
                first_tier_closed = closed_today

        print(f"[main] today_signals.json ({len(first_tier_signals)}件) / "
              f"today_sell_signals.json ({len(first_tier_sell_signals)}件) 保存")

        # 月次まとめ送信（大買→大売→中買→中売→小買→小売の順で1通・上の collect 参照）
        if monthly_embeds:
            send_monthly_bundle(monthly_embeds)

        # ── ④ Twitter（大資金分のみ・既存挙動）──────────────────
        # TWITTER_PAUSED: 2026-05-21 ユーザー指示で一時停止（PEADフィルタB案BT中）。再開時はコメント解除。
        # from twitter_notifier import post_swing_signals, post_swing_results, post_monthly_summary
        # post_swing_signals(first_tier_signals, today, macro, sell_signals=first_tier_sell_signals)
        # if first_tier_closed:
        #     post_swing_results(first_tier_closed, today)
        # if today.day == 1:
        #     post_monthly_summary(today)
        print("[main] Twitter配信は一時停止中（TWITTER_PAUSED）")

        # ── ⑤ 出口ボラ正規化の紙並走（2026-07-25・本人指示=案2）────────────
        # 本番の損切りは一律-3%のまま一切変えない。その横で「損切りをATR%×2.0に
        # していたら各玉はどうなったか」だけを shadow_exit_*.json に記録し続ける。
        # 数ヶ月ぶん貯めてから実績と突き合わせて採否を決める（根拠BT: _bt_atr_exit_*.py）。
        # 完全非破壊: 影台帳しか書かない・配信/発注/実帳簿には触れない・例外は握り潰す
        # （デイトレ紙台帳 daytrade_paper.run_paper と同じパターン）。
        try:
            from shadow_exit import run_shadow
            _shadow_ok = run_shadow(TIERS, today, _ledger_data)
            if _shadow_ok is False:
                print("[main] ⚠️ 極み配信が失敗（kiwami_sent.json sent=False）→ 次の保険ランが極みだけ再送する")
        except Exception as _shadow_err:
            print(f"[shadow] スキップ（配信には影響なし）: {_shadow_err}")

        print("[main] 正常終了")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"[main] エラー発生:\n{err_msg}", file=sys.stderr)
        try:
            send_error(err_msg, today)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()

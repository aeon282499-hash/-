"""
notifier.py — Discord Webhook 通知モジュール
"""

import os
import requests
from datetime import date, datetime, timedelta
import zoneinfo
import jpholiday

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

COLOR_BUY   = 0xE53935   # 赤
COLOR_NONE  = 0x757575   # グレー
COLOR_ERROR = 0xFDD835   # 黄
COLOR_WIN   = 0x43A047   # 緑
COLOR_SELL  = 0x1E88E5   # 青

# 表示の分岐用しきい値（screener.VOL_MULT と整合）
VOL_MULT_THRESHOLD = 2.0

CAPITAL  = 5_000_000   # 総資金（円）= 100万 × MAX_SIGNALS5並列
WEIGHT   = 1 / 5       # 1トレード投入比率（100万 / 500万）
MAX_HOLD = 3           # 最大保有営業日数（tracker.py と一致）


# 公開（note メンバー向け）Discord Webhook URLs
# 副業運用：株AI Discord サーバーの各チャンネルに並行送信する
PUBLIC_BUY     = os.getenv("DISCORD_WEBHOOK_URL_PUBLIC", "").strip()
PUBLIC_SELL    = os.getenv("DISCORD_WEBHOOK_SELL_URL_PUBLIC", "").strip()
PUBLIC_CLOSE   = os.getenv("DISCORD_WEBHOOK_CLOSE_URL_PUBLIC", "").strip()
PUBLIC_MONTHLY = os.getenv("DISCORD_WEBHOOK_MONTHLY_URL_PUBLIC", "").strip()

# サブアカウント（個人運用・サイズ違い口座）: 既存通知を別Discordにミラー
# 追加する場合は SUB_ACCOUNTS にエントリを追加するだけ。env未設定なら自動スキップ。
SUB_ACCOUNTS = [
    {
        "label":    "中資金",
        "emoji":    "🔵",
        "size":     500_000,    # 1件50万 × 5並列 = 250万
        "buy_url":  os.getenv("DISCORD_WEBHOOK_BUY_MID_URL",  "").strip(),
        "sell_url": os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
    },
    {
        "label":    "小資金",
        "emoji":    "🟢",
        "size":     300_000,    # 1件30万 × 5並列 = 150万
        "buy_url":  os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL",  "").strip(),
        "sell_url": os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
    },
]

_MIRROR_DEFAULT = "__USE_DEFAULT__"  # sentinel: Noneと区別するため


def _mirror_to_public(payload: dict, url: str) -> None:
    """公開Discordへ並行送信。失敗しても本送信を止めない。"""
    if not url:
        return
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier-public] mirror HTTP {resp.status_code}")
    except Exception as e:
        print(f"[notifier-public] mirror failed: {e}")


def _affordable(price: float, size_yen: int) -> bool:
    """100株単元で size_yen 以内で買えるか判定。price=0/未取得は買えない扱い。"""
    if not price or price <= 0:
        return False
    return price * 100 <= size_yen


def _post_to_subaccount(payload: dict, *, side: str, acc: dict) -> None:
    """サブ口座Webhookへ送信。失敗してもメイン送信は影響しない。"""
    url = acc["buy_url"] if side == "BUY" else acc["sell_url"]
    if not url:
        return
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier-{acc['label']}-{side}] HTTP {resp.status_code}")
    except Exception as e:
        print(f"[notifier-{acc['label']}-{side}] failed: {e}")


def _get_webhook_url() -> str:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        raise ValueError("DISCORD_WEBHOOK_URL が未設定です。")
    return url


def _post(payload: dict, *, mirror: str = _MIRROR_DEFAULT) -> None:
    """個人用BUY Webhookに送信し、公開Discord(PUBLIC_BUY by default)にも並行送信する。

    mirror引数で上書き可能：
        - 省略時: PUBLIC_BUY にミラー（buy/results/no_signal用）
        - mirror=PUBLIC_CLOSE / PUBLIC_MONTHLY: 別チャンネルに出したい場合
        - mirror=None: ミラーしない

    個人用送信が失敗しても公開ミラーは試行する（運用上：個人用壊れ時の救済）。
    """
    try:
        url  = _get_webhook_url()
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier] 個人用送信失敗: HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"[notifier] 個人用送信失敗: {e}")
    # 公開用ミラー
    target = PUBLIC_BUY if mirror == _MIRROR_DEFAULT else mirror
    if target:
        _mirror_to_public(payload, target)
    # サブ口座（中資金/小資金等）は各 send_* 関数でサイズ別フィルタ・再構築して送信する


def _macro_description(macro: dict) -> str:
    """マクロ環境の説明文を生成する。"""
    sp   = macro.get("sp500")
    nas  = macro.get("nasdaq")
    bias = macro.get("bias", "neutral")

    sp_str  = f"S&P500(SPY) {sp:+.1f}%"  if sp  is not None else "S&P500 取得不可"
    nas_str = f"ナスダック総合 {nas:+.1f}%" if nas is not None else "ナスダック 取得不可"

    if bias == "bearish":
        env = "⚠️ 米国株安"
    elif bias == "bullish":
        env = "🌕 米国株高"
    else:
        env = "⚖️ 米国市場はほぼ横ばい"

    return f"{sp_str} ／ {nas_str}\n{env}"


def _nth_trading_day(d, n: int):
    """d から n 営業日後を返す。"""
    cur = d
    count = 0
    while count < n:
        cur += timedelta(days=1)
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            count += 1
    return cur


def _calc_today_hold_day(entry_date_str: str, today: date) -> int:
    """エントリー日から today までの実営業日数（両端含む）= 当日の保有日目。
    hold_days に依存しない（tracker.py の更新失敗でズレないよう堅牢化）。"""
    try:
        entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    except Exception:
        return 0
    if entry_dt > today:
        return 0
    if entry_dt == today:
        return 1
    cur, count = entry_dt, 0
    while cur <= today:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            count += 1
        cur += timedelta(days=1)
    return count


def _build_buy_embed(buy_signals: list[dict], size_yen: int, *, date_str: str, time_str: str,
                     exit_date_str: str, label: str | None = None, emoji: str | None = None) -> dict:
    """BUYシグナルからembedを構築。size_yen=1件あたりの投入額。
    label/emoji 指定時はサブ口座用タイトル＆ヘッダ付き。"""
    size_man = size_yen // 10000
    sep = "─" * 24
    lines = [
        f"🎯 **9:00 寄り付き成行**・1件{size_man}万円",
        f"🛑 損切 寄値×0.97 (-3%)  ✅ 利確 寄値×1.05 (+5%)",
        f"📅 最大3営業日・RSI≥50で早期決済・処分期限 **{exit_date_str}**",
        sep,
    ]

    for i, sig in enumerate(buy_signals, 1):
        ticker     = sig["ticker"].replace(".T", "")
        name       = sig["name"]
        prev_close = sig.get("prev_close", 0) or 0
        rsi        = sig.get("rsi")
        deviation  = sig.get("deviation")
        range_r    = sig.get("range_ratio")
        vol_r      = sig.get("vol_ratio")
        turnover   = sig.get("turnover", 0) or 0

        if prev_close > 0:
            shares     = max(100, int(size_yen / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            line1      = f"**#{i} {name}** ({ticker}) 前日{prev_close:,.0f}円 {shares:,}株/約{invest_amt/1e4:.0f}万"
        else:
            line1      = f"**#{i} {name}** ({ticker}) {size_man}万円目安"

        parts = []
        if rsi is not None:
            parts.append(f"RSI={rsi:.1f}")
        if deviation is not None:
            parts.append(f"乖離{deviation:+.1f}%")
        if vol_r is not None and vol_r >= VOL_MULT_THRESHOLD:
            parts.append(f"出来高×{vol_r:.1f}")
        elif range_r is not None:
            parts.append(f"値幅/ATR={range_r:.1f}")
        if turnover > 0:
            parts.append(f"代金{turnover/1e8:.0f}億")
        line2 = "   " + "・".join(parts)

        lines.append(line1)
        lines.append(line2)
        lines.append("")

    if label:
        title = f"{emoji or ''}【{label}】📊スイング {date_str} — 買い{len(buy_signals)}銘柄"
    else:
        title = f"📊【スイング】{date_str} — 買い{len(buy_signals)}銘柄"

    return {
        "title":       title,
        "description": "\n".join(lines).rstrip(),
        "color":       COLOR_BUY,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_signals(signals: list[dict], today: date, macro: dict | None = None, entry_date=None) -> None:
    """買いシグナルを1embedにまとめて送信（共通ルールはヘッダ・銘柄は2行コンパクト）。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    macro = macro or {}

    if entry_date is None:
        entry_date = today
    exit_date = _nth_trading_day(entry_date, 2)
    exit_date_str = exit_date.strftime("%m/%d")

    if not signals:
        _send_no_signal(date_str, time_str, macro)
        return

    # BUYのみ対象（SELLはsend_sell_signalsで別配信）
    buy_signals = [s for s in signals if s.get("direction") == "BUY"]
    if not buy_signals:
        print("[notifier] BUYシグナルなし → send_signalsスキップ")
        return

    # メイン送信（大資金100万）
    embed = _build_buy_embed(
        buy_signals, size_yen=1_000_000,
        date_str=date_str, time_str=time_str, exit_date_str=exit_date_str,
    )
    _post({"embeds": [embed]})
    print(f"[notifier] {len(buy_signals)} 件のシグナルを Discord に送信しました。")

    # サブ口座（中資金/小資金）: 100株単元で買える銘柄だけにフィルタ→個別embed
    for acc in SUB_ACCOUNTS:
        if not acc.get("buy_url"):
            continue
        sub_signals = [s for s in buy_signals if _affordable(s.get("prev_close", 0) or 0, acc["size"])]
        if not sub_signals:
            # サイズで買える銘柄ゼロでも「該当なし」を投げて欠落を避ける
            payload = {"embeds": [{
                "title":       f"{acc['emoji']}【{acc['label']}】📊スイング {date_str} — 該当なし",
                "description": (
                    f"💼 1件{acc['size']//10000}万円枠で買える銘柄は本日ありません。\n"
                    f"（大資金側のシグナル{len(buy_signals)}件すべて株価が枠超過）"
                ),
                "color":       COLOR_NONE,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]}
            _post_to_subaccount(payload, side="BUY", acc=acc)
            continue
        sub_embed = _build_buy_embed(
            sub_signals, size_yen=acc["size"],
            date_str=date_str, time_str=time_str, exit_date_str=exit_date_str,
            label=acc["label"], emoji=acc["emoji"],
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="BUY", acc=acc)
        print(f"[notifier-{acc['label']}-BUY] {len(sub_signals)}/{len(buy_signals)} 件配信")


def _send_no_signal(date_str: str, time_str: str, macro: dict) -> None:
    macro_desc = _macro_description(macro)
    payload = {
        "embeds": [{
            "title":       f"📊【スイング】{date_str} — シグナルなし",
            "description": (
                "本日は極限まで吟味した結果、確実に勝てる優位性を持つ銘柄が存在しません。\n"
                "大切な資金の防衛を優先し、本日のトレードは **0銘柄（見送り）** とします。\n\n"
                f"**【本日の相場環境】**\n{macro_desc}"
            ),
            "color":  COLOR_NONE,
            "footer": {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print("[notifier] シグナル 0 件の通知を送信しました。")
    # サブ口座にも「シグナルなし」を配信（口座サイズ問わず共通）
    for acc in SUB_ACCOUNTS:
        if not acc.get("buy_url"):
            continue
        sub_payload = {"embeds": [{
            "title":       f"{acc['emoji']}【{acc['label']}】📊スイング {date_str} — シグナルなし",
            "description": (
                f"💼 1件{acc['size']//10000}万円枠 — 本日は買いシグナル0件です。"
            ),
            "color":  COLOR_NONE,
            "footer": {"text": f"配信時刻: {time_str}"},
        }]}
        _post_to_subaccount(sub_payload, side="BUY", acc=acc)


def _build_results_embed(closed: list[dict], still_open: list[dict], today: date,
                         *, label: str | None = None, emoji_acc: str | None = None,
                         size_yen: int | None = None) -> dict:
    """決済結果embedを構築。label指定時はサブ口座用ヘッダ付き。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    lines = []

    if label and size_yen:
        lines.append(f"💼 **{label}口座（1件{size_yen//10000}万円枠）**")
        lines.append("")

    if closed:
        lines.append("**── 📋 決済済み（OCO・大引け処分） ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            dir_str = "買い" if p["direction"] == "BUY" else "売り"
            reason = {
                "RSI":     "RSI回復（≥50・大引け）",
                "TP":      "利確（+5%・OCO）",
                "STOP":    "損切り（-3%・OCO）",
                "MAXHOLD": "最大保有日数（大引け）",
            }.get(etype, etype)
            exit_d = p.get("exit_date", "")
            exit_str = f"（{exit_d[5:].replace('-', '/')}）" if exit_d else ""
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）{dir_str} "
                f"→ **{pnl:+.2f}%** ／ {reason}{exit_str}"
            )

    if still_open:
        if lines:
            lines.append("")
        lines.append("**── 保有中（持ち越し） ──**")
        for p in still_open:
            upnl       = p.get("unrealized_pnl", 0) or 0
            today_hold = _calc_today_hold_day(p.get("entry_date", ""), today)
            emoji      = "📈" if upnl >= 0 else "📉"
            dir_str    = "買い" if p["direction"] == "BUY" else "売り"

            try:
                entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                deadline = _nth_trading_day(entry_dt, MAX_HOLD - 1)
                deadline_str = deadline.strftime("%m月%d日")
                remaining = MAX_HOLD - today_hold
                if remaining <= 0:
                    warn = "⚠️ **本日大引けに処分**"
                elif remaining == 1:
                    warn = f"⚠️ **あと1日／{deadline_str} 大引けに処分**"
                else:
                    warn = f"（あと{remaining}日／{deadline_str}までに処分）"
            except Exception:
                warn = ""

            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）{dir_str} "
                f"含み **{upnl:+.2f}%** — {today_hold}日目 {warn}"
            )

    if closed:
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    if label:
        title = (
            f"{emoji_acc or ''}【{label}】📋スイング決済結果 {date_str}"
            if closed else
            f"{emoji_acc or ''}【{label}】📋スイング保有中 {date_str}"
        )
    else:
        title = f"📋【スイング決済結果】{date_str}" if closed else f"📋【スイング保有中】{date_str}"

    color = COLOR_WIN if any((p.get("pnl_pct") or 0) > 0 for p in closed) else COLOR_ERROR
    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       color,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """前日シグナルの損益結果を Discord に送信する。"""
    if not closed and not still_open:
        return

    # メイン送信
    embed = _build_results_embed(closed, still_open, today)
    _post({"embeds": [embed]})
    print(f"[notifier] 結果レポートを Discord に送信しました（決済{len(closed)}件 / 保有中{len(still_open)}件）")

    # サブ口座: entry_open でその口座サイズで買えていた銘柄だけにフィルタ
    for acc in SUB_ACCOUNTS:
        if not acc.get("buy_url"):
            continue
        size = acc["size"]
        sub_closed     = [p for p in closed     if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        sub_still_open = [p for p in still_open if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        if not sub_closed and not sub_still_open:
            continue
        sub_embed = _build_results_embed(
            sub_closed, sub_still_open, today,
            label=acc["label"], emoji_acc=acc["emoji"], size_yen=size,
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="BUY", acc=acc)
        print(f"[notifier-{acc['label']}-RESULT] 決済{len(sub_closed)}/保有中{len(sub_still_open)}")


def _build_sell_embed(signals: list[dict], size_yen: int, *, date_str: str, time_str: str,
                      exit_date_short: str, label: str | None = None, emoji: str | None = None) -> dict:
    """SELLシグナルからembedを構築。"""
    size_man = size_yen // 10000
    sep = "─" * 24
    lines = [
        f"🎯 **9:00 寄り付き成行（信用売り）**・1件{size_man}万円",
        f"🛑 損切 寄値×1.03 (+3%)  ✅ 利確 寄値×0.95 (-5%)",
        f"📅 最大3営業日・RSI≤50で早期買戻し・処分期限 **{exit_date_short}**",
        sep,
    ]

    for i, sig in enumerate(signals, 1):
        ticker     = sig["ticker"].replace(".T", "")
        name       = sig["name"]
        prev_close = sig.get("prev_close", 0) or 0
        rsi        = sig.get("rsi")
        deviation  = sig.get("deviation")
        day_change = sig.get("day_change")
        range_r    = sig.get("range_ratio")
        vol_r      = sig.get("vol_ratio")
        turnover   = sig.get("turnover", 0) or 0

        if prev_close > 0:
            shares     = max(100, int(size_yen / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            line1      = f"**#{i} {name}** ({ticker}) 前日{prev_close:,.0f}円 {shares:,}株/約{invest_amt/1e4:.0f}万"
        else:
            line1      = f"**#{i} {name}** ({ticker}) {size_man}万円目安"

        parts = []
        if day_change is not None:
            parts.append(f"前日比{day_change:+.1f}%")
        if rsi is not None:
            parts.append(f"RSI={rsi:.1f}")
        if deviation is not None:
            parts.append(f"乖離{deviation:+.1f}%")
        if vol_r is not None and vol_r >= VOL_MULT_THRESHOLD:
            parts.append(f"出来高×{vol_r:.1f}")
        elif range_r is not None:
            parts.append(f"値幅/ATR={range_r:.1f}")
        if turnover > 0:
            parts.append(f"代金{turnover/1e8:.0f}億")
        line2 = "   " + "・".join(parts)

        lines.append(line1)
        lines.append(line2)
        lines.append("")

    if label:
        title = f"{emoji or ''}【{label}】📉スイング空売り {date_str} — 売り{len(signals)}銘柄"
    else:
        title = f"📉【スイング空売り】{date_str} — 売り{len(signals)}銘柄"

    return {
        "title":       title,
        "description": "\n".join(lines).rstrip(),
        "color":       COLOR_SELL,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_sell_signals(signals: list[dict], today: date, entry_date=None) -> None:
    """空売りシグナルを SELL専用Webhook + 公開SELL Discordに送信する。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    if entry_date is None:
        entry_date = today
    exit_date       = _nth_trading_day(entry_date, 2)
    exit_date_short = exit_date.strftime("%m/%d")

    if not signals:
        _post_sell({
            "embeds": [{
                "title":       f"📉【スイング空売り】{date_str} — シグナルなし",
                "description": "本日の空売りシグナルは0件です。",
                "color":       COLOR_NONE,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]
        })
        print("[notifier] SELL シグナルなし を送信しました。")
        # サブ口座にも「シグナルなし」を配信
        for acc in SUB_ACCOUNTS:
            if not acc.get("sell_url"):
                continue
            sub_payload = {"embeds": [{
                "title":       f"{acc['emoji']}【{acc['label']}】📉スイング空売り {date_str} — シグナルなし",
                "description": f"💼 1件{acc['size']//10000}万円枠 — 本日は空売りシグナル0件です。",
                "color":       COLOR_NONE,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]}
            _post_to_subaccount(sub_payload, side="SELL", acc=acc)
        return

    # メイン送信（大資金100万）
    embed = _build_sell_embed(
        signals, size_yen=1_000_000,
        date_str=date_str, time_str=time_str, exit_date_short=exit_date_short,
    )
    _post_sell({"embeds": [embed]})
    print(f"[notifier] SELL {len(signals)} 件のシグナルを Discord に送信しました。")

    # サブ口座: 100株単元で建てられる銘柄だけにフィルタ
    for acc in SUB_ACCOUNTS:
        if not acc.get("sell_url"):
            continue
        sub_signals = [s for s in signals if _affordable(s.get("prev_close", 0) or 0, acc["size"])]
        if not sub_signals:
            sub_payload = {"embeds": [{
                "title":       f"{acc['emoji']}【{acc['label']}】📉スイング空売り {date_str} — 該当なし",
                "description": (
                    f"💼 1件{acc['size']//10000}万円枠で建てられる銘柄は本日ありません。\n"
                    f"（大資金側のSELLシグナル{len(signals)}件すべて株価が枠超過）"
                ),
                "color":       COLOR_NONE,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]}
            _post_to_subaccount(sub_payload, side="SELL", acc=acc)
            continue
        sub_embed = _build_sell_embed(
            sub_signals, size_yen=acc["size"],
            date_str=date_str, time_str=time_str, exit_date_short=exit_date_short,
            label=acc["label"], emoji=acc["emoji"],
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="SELL", acc=acc)
        print(f"[notifier-{acc['label']}-SELL] {len(sub_signals)}/{len(signals)} 件配信")


def _build_monthly_embed(closed: list[dict], today: date, *, size_yen: int,
                         label: str | None = None, emoji_acc: str | None = None,
                         sell: bool = False) -> dict | None:
    """月別・年間損益embedを構築。closedはサイズで事前フィルタ済みである前提。"""
    from collections import defaultdict

    monthly = defaultdict(list)
    for p in closed:
        ym = (p.get("exit_date") or "")[:7]
        if ym:
            monthly[ym].append(p["pnl_pct"])

    current_year = str(today.year)
    year_months  = {ym: pnls for ym, pnls in monthly.items() if ym.startswith(current_year)}
    if not year_months:
        return None

    one_size = size_yen
    capital  = size_yen * 5  # MAX5並列
    weight   = 1 / 5

    lines = []
    for ym in sorted(year_months.keys()):
        pnls  = year_months[ym]
        mr    = sum(pnls) * weight
        wins  = sum(1 for p in pnls if p > 0)
        yen   = mr / 100 * capital
        sign  = "+" if mr >= 0 else ""
        lines.append(
            f"`{ym}` {len(pnls)}件 勝率{wins}/{len(pnls)} "
            f"**月利{sign}{mr:.1f}%**（{sign}{yen/10000:.1f}万円）"
        )

    year_pnls  = [p for pnls in year_months.values() for p in pnls]
    annual_pct = sum(year_pnls) * weight
    annual_yen = annual_pct / 100 * capital
    a_sign     = "+" if annual_pct >= 0 else ""

    desc  = "\n".join(lines)
    desc += f"\n\n**{current_year}年合計: {a_sign}{annual_pct:.1f}%（{a_sign}{annual_yen/10000:.1f}万円）**"

    kind = "空売り" if sell else "スイング"
    if label:
        title = f"{emoji_acc or ''}【{label}】📈 {current_year}年 月別・年間損益（{kind}）"
    else:
        title = f"📈 {current_year}年 月別・年間損益（{kind}）" if not sell else \
                f"📉 {current_year}年 月別・年間損益（{kind}）"

    color = COLOR_WIN if annual_pct >= 0 else COLOR_ERROR
    if sell:
        color = 0x43A047 if annual_pct >= 0 else 0xFDD835

    return {
        "title":       title,
        "description": desc,
        "color":       color,
        "footer":      {"text": f"※資金{capital//10000}万・1トレード{one_size//10000}万・MAX5並列基準"},
    }


def send_monthly_report(positions: list[dict], today: date) -> None:
    """月別・年間損益をDiscordに送信する。"""
    closed = [
        p for p in positions
        if p.get("status") == "closed" and p.get("pnl_pct") is not None
    ]
    if not closed:
        return

    # メイン送信（大資金100万）
    embed = _build_monthly_embed(closed, today, size_yen=1_000_000)
    if embed:
        _post({"embeds": [embed]}, mirror=PUBLIC_MONTHLY)
        print(f"[notifier] 月別・年間損益レポートを送信しました")

    # サブ口座: その口座サイズで実際に買えていた銘柄のみで集計
    for acc in SUB_ACCOUNTS:
        if not acc.get("buy_url"):
            continue
        size = acc["size"]
        sub_closed = [p for p in closed if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        sub_embed = _build_monthly_embed(
            sub_closed, today, size_yen=size,
            label=acc["label"], emoji_acc=acc["emoji"],
        )
        if sub_embed:
            _post_to_subaccount({"embeds": [sub_embed]}, side="BUY", acc=acc)
            print(f"[notifier-{acc['label']}-MONTHLY] {len(sub_closed)}件")


def _post_sell(payload: dict, *, mirror: str = _MIRROR_DEFAULT) -> None:
    """個人用SELL Webhookに送信し、公開Discord(PUBLIC_SELL by default)にも並行送信する。"""
    url = os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip()
    if not url:
        print("[notifier] DISCORD_WEBHOOK_SELL_URL が未設定 → SELL通知スキップ")
    else:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier] SELL Discord送信失敗: HTTP {resp.status_code}")
    # 公開用ミラー
    target = PUBLIC_SELL if mirror == _MIRROR_DEFAULT else mirror
    if target:
        _mirror_to_public(payload, target)
    # サブ口座（中資金/小資金等）は各 send_* 関数でサイズ別フィルタ・再構築して送信する


def _build_sell_results_embed(closed: list[dict], still_open: list[dict], today: date,
                              *, label: str | None = None, emoji_acc: str | None = None,
                              size_yen: int | None = None) -> dict:
    """空売り結果embedを構築。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    lines = []

    if label and size_yen:
        lines.append(f"💼 **{label}口座（1件{size_yen//10000}万円枠・信用売り）**")
        lines.append("")

    if closed:
        lines.append("**── 📋 決済済み（買戻し・OCO/大引け） ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            reason = {
                "RSI":     "RSI回復（≤50・大引け）",
                "TP":      "利確（-5%・OCO）",
                "STOP":    "損切り（+3%・OCO）",
                "MAXHOLD": "最大保有日数（大引け）",
            }.get(etype, etype)
            exit_d = p.get("exit_date", "")
            exit_str = f"（{exit_d[5:].replace('-', '/')}）" if exit_d else ""
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）空売り "
                f"→ **{pnl:+.2f}%** ／ {reason}{exit_str}"
            )

    if still_open:
        if lines:
            lines.append("")
        lines.append("**── 保有中（売りポジション持ち越し） ──**")
        for p in still_open:
            upnl       = p.get("unrealized_pnl", 0) or 0
            today_hold = _calc_today_hold_day(p.get("entry_date", ""), today)
            emoji      = "📈" if upnl >= 0 else "📉"
            try:
                entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                deadline = _nth_trading_day(entry_dt, MAX_HOLD - 1)
                deadline_str = deadline.strftime("%m月%d日")
                remaining = MAX_HOLD - today_hold
                if remaining <= 0:
                    warn = "⚠️ **本日大引けに買戻し**"
                elif remaining == 1:
                    warn = f"⚠️ **あと1日／{deadline_str} 大引けに買戻し**"
                else:
                    warn = f"（あと{remaining}日／{deadline_str}までに買戻し）"
            except Exception:
                warn = ""
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）含み **{upnl:+.2f}%** — {today_hold}日目 {warn}"
            )

    if closed:
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    if label:
        title = f"{emoji_acc or ''}【{label}】📋スイング空売り結果 {date_str}"
    else:
        title = f"📋【スイング空売り結果】{date_str}"

    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       0x43A047 if any((p.get("pnl_pct") or 0) > 0 for p in closed) else 0xFDD835,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_sell_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """空売りポジションの損益結果をSELL専用Webhookに送信する。"""
    if not closed and not still_open:
        return

    # メイン送信
    embed = _build_sell_results_embed(closed, still_open, today)
    _post_sell({"embeds": [embed]})
    print(f"[notifier] SELL結果レポートを送信しました（決済{len(closed)}件 / 保有中{len(still_open)}件）")

    # サブ口座: entry_open でフィルタ
    for acc in SUB_ACCOUNTS:
        if not acc.get("sell_url"):
            continue
        size = acc["size"]
        sub_closed     = [p for p in closed     if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        sub_still_open = [p for p in still_open if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        if not sub_closed and not sub_still_open:
            continue
        sub_embed = _build_sell_results_embed(
            sub_closed, sub_still_open, today,
            label=acc["label"], emoji_acc=acc["emoji"], size_yen=size,
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="SELL", acc=acc)
        print(f"[notifier-{acc['label']}-SELL-RESULT] 決済{len(sub_closed)}/保有中{len(sub_still_open)}")


def send_sell_monthly_report(positions: list[dict], today: date) -> None:
    """空売りの月別・年間損益をSELL専用Webhookに送信する。"""
    closed = [
        p for p in positions
        if p.get("status") == "closed" and p.get("pnl_pct") is not None
    ]
    if not closed:
        return

    # メイン送信
    embed = _build_monthly_embed(closed, today, size_yen=1_000_000, sell=True)
    if embed:
        _post_sell({"embeds": [embed]}, mirror=PUBLIC_MONTHLY)
        print(f"[notifier] SELL月別・年間損益レポートを送信しました")

    # サブ口座: フィルタして再集計
    for acc in SUB_ACCOUNTS:
        if not acc.get("sell_url"):
            continue
        size = acc["size"]
        sub_closed = [p for p in closed if _affordable(p.get("entry_open") or p.get("prev_close") or 0, size)]
        sub_embed = _build_monthly_embed(
            sub_closed, today, size_yen=size, sell=True,
            label=acc["label"], emoji_acc=acc["emoji"],
        )
        if sub_embed:
            _post_to_subaccount({"embeds": [sub_embed]}, side="SELL", acc=acc)
            print(f"[notifier-{acc['label']}-SELL-MONTHLY] {len(sub_closed)}件")


def send_error(error_message: str, today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    payload = {
        "embeds": [{
            "title":       f"⚠️ {date_str} — シグナル配信エラー",
            "description": f"```\n{error_message[:1800]}\n```",
            "color":       COLOR_ERROR,
            "footer":      {"text": time_str},
        }]
    }
    _post(payload)


def _build_close_embed(targets: list[dict], today: date, *, sell: bool = False,
                       label: str | None = None, emoji_acc: str | None = None,
                       size_yen: int | None = None) -> dict:
    """大引け処分指示embedを構築。sell=Trueなら空売り買戻し版。"""
    date_str = today.strftime("%m/%d")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    sep = "─" * 22

    if sell:
        header = "🛒 **15:25-15:30 クロージング**で成行買戻し（SBI証券・信用）"
        title_kind = "空売り大引け処分指示"
    else:
        header = "🛒 **15:25-15:30 クロージング**で成行売り（SBI証券）"
        title_kind = "大引け処分指示"

    lines = []
    if label and size_yen:
        lines.append(f"💼 **{label}口座（1件{size_yen//10000}万円枠）**")
    lines += [header, f"対象: **{len(targets)}銘柄**", sep]

    for i, t in enumerate(targets, 1):
        ticker = t["ticker"].replace(".T", "")
        name   = t["name"]
        rtype  = t["reason_type"]
        hold   = t.get("today_hold", "?")
        rsi    = t.get("rsi_now")
        price  = t.get("current_price")
        entry  = t.get("entry_open")

        if rtype == "RSI":
            icon = "🔔"
            tag  = f"RSI回復(RSI={rsi:.1f})" if rsi is not None else "RSI回復"
        else:
            icon = "⏰"
            tag  = f"{hold}日目MAXHOLD"

        line = f"{icon} **#{i} {name}** ({ticker}) — {tag}"
        if price is not None and entry is not None and entry > 0:
            pnl_now = (entry - price) / entry * 100 if sell else (price - entry) / entry * 100
            line += f" / {pnl_now:+.2f}%"
        lines.append(line)

    if sell:
        color = COLOR_WIN if any(
            t.get("current_price") and t.get("entry_open") and t["entry_open"] > t["current_price"]
            for t in targets
        ) else COLOR_ERROR
    else:
        color = COLOR_WIN if any(
            t.get("current_price") and t.get("entry_open") and t["current_price"] > t["entry_open"]
            for t in targets
        ) else COLOR_ERROR

    if label:
        title = f"{emoji_acc or ''}【{label}】⚡{title_kind} {date_str}"
    else:
        title = f"⚡【{title_kind}】{date_str}"

    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       color,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_close_signals(targets: list[dict], today: date) -> None:
    """大引け前にRSI≥50またはMAXHOLDで処分すべきポジションをDiscord通知する。

    targets: close_check.py が生成した sell_targets リスト
      [{"ticker", "name", "reason_type": "RSI"|"MAXHOLD", "reason",
        "today_hold", "rsi_now", "current_price", "entry_open"}]
    """
    if not targets:
        return

    # メイン送信
    embed = _build_close_embed(targets, today)
    _post({"embeds": [embed]}, mirror=PUBLIC_CLOSE)
    print(f"[notifier] 大引け処分指示を Discord に送信しました（{len(targets)}件）")

    # サブ口座: そのサイズで実際に持っていた銘柄だけ
    for acc in SUB_ACCOUNTS:
        if not acc.get("buy_url"):
            continue
        size = acc["size"]
        sub_targets = [t for t in targets if _affordable(t.get("entry_open") or 0, size)]
        if not sub_targets:
            continue
        sub_embed = _build_close_embed(
            sub_targets, today,
            label=acc["label"], emoji_acc=acc["emoji"], size_yen=size,
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="BUY", acc=acc)
        print(f"[notifier-{acc['label']}-CLOSE] {len(sub_targets)}/{len(targets)} 件")


def send_close_signals_sell(targets: list[dict], today: date) -> None:
    """SELL（空売り）ポジションの大引け処分指示を SELL専用Webhook + 公開CLOSE Discordに送信する。"""
    if not targets:
        return

    embed = _build_close_embed(targets, today, sell=True)
    _post_sell({"embeds": [embed]}, mirror=PUBLIC_CLOSE)
    print(f"[notifier] SELL大引け処分指示を Discord に送信しました（{len(targets)}件）")

    # サブ口座
    for acc in SUB_ACCOUNTS:
        if not acc.get("sell_url"):
            continue
        size = acc["size"]
        sub_targets = [t for t in targets if _affordable(t.get("entry_open") or 0, size)]
        if not sub_targets:
            continue
        sub_embed = _build_close_embed(
            sub_targets, today, sell=True,
            label=acc["label"], emoji_acc=acc["emoji"], size_yen=size,
        )
        _post_to_subaccount({"embeds": [sub_embed]}, side="SELL", acc=acc)
        print(f"[notifier-{acc['label']}-CLOSE-SELL] {len(sub_targets)}/{len(targets)} 件")

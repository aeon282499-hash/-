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

CAPITAL  = 3_000_000   # 総資金（円）
WEIGHT   = 1 / 3       # 1トレード投入比率（100万 / 300万）
MAX_HOLD = 3           # 最大保有営業日数（tracker.py と一致）


def _get_webhook_url() -> str:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        raise ValueError("DISCORD_WEBHOOK_URL が未設定です。")
    return url


def _post(payload: dict) -> None:
    url  = _get_webhook_url()
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord送信失敗: HTTP {resp.status_code}\n{resp.text}")


def _macro_description(macro: dict) -> str:
    """マクロ環境の説明文を生成する。"""
    dow = macro.get("dow")
    nas = macro.get("nasdaq")
    bias = macro.get("bias", "neutral")

    dow_str = f"S&P500(SPY) {dow:+.1f}%" if dow is not None else "S&P500 取得不可"
    nas_str = f"ナスダック総合 {nas:+.1f}%" if nas is not None else "ナスダック 取得不可"

    if bias == "bearish":
        env = "⚠️ 米国株安"
    elif bias == "bullish":
        env = "🌕 米国株高"
    else:
        env = "⚖️ 米国市場はほぼ横ばい"

    return f"{dow_str} ／ {nas_str}\n{env}"


def _nth_trading_day(d, n: int):
    """d から n 営業日後を返す。"""
    cur = d
    count = 0
    while count < n:
        cur += timedelta(days=1)
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            count += 1
    return cur


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

    sep = "─" * 24
    lines = [
        f"🎯 **9:00 寄り付き成行**・1件100万円",
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
            shares     = max(100, int(1_000_000 / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            line1      = f"**#{i} {name}** ({ticker}) 前日{prev_close:,.0f}円 {shares:,}株/約{invest_amt/1e4:.0f}万"
        else:
            line1      = f"**#{i} {name}** ({ticker}) 100万円目安"

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

    embed = {
        "title":       f"📊【スイング】{date_str} — 買い{len(buy_signals)}銘柄",
        "description": "\n".join(lines).rstrip(),
        "color":       COLOR_BUY,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }
    _post({"embeds": [embed]})
    print(f"[notifier] {len(buy_signals)} 件のシグナルを Discord に送信しました。")


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


def send_no_signal(today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    _send_no_signal(date_str, time_str, {})


def send_skip(reason: str, today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    payload = {
        "embeds": [{
            "title":       f"🗓️ {date_str} — 配信スキップ",
            "description": reason,
            "color":       COLOR_NONE,
        }]
    }
    _post(payload)


def send_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """前日シグナルの損益結果を Discord に送信する。"""
    if not closed and not still_open:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    lines = []

    if closed:
        lines.append("**── 🔔 本日寄り付きで売却してください ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            dir_str = "買い" if p["direction"] == "BUY" else "売り"
            reason = {
                "RSI":     "RSI回復（≥50）",
                "TP":      "利確（+5%）",
                "STOP":    "損切り（-3%）",
                "MAXHOLD": "最大保有日数",
            }.get(etype, etype)
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）{dir_str} "
                f"→ **{pnl:+.2f}%** ／ 理由: {reason}"
            )

    if still_open:
        if lines:
            lines.append("")
        lines.append("**── 保有中（持ち越し） ──**")
        for p in still_open:
            upnl    = p.get("unrealized_pnl", 0) or 0
            hold    = p.get("hold_days", 0)
            emoji   = "📈" if upnl >= 0 else "📉"
            dir_str = "買い" if p["direction"] == "BUY" else "売り"

            # 処分期限日（entry_dateから5営業日目）を計算
            try:
                entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                cur = entry_dt
                biz_count = 0
                while biz_count < MAX_HOLD:
                    cur += timedelta(days=1)
                    if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                        biz_count += 1
                deadline = cur
                deadline_str = deadline.strftime("%m月%d日")
                remaining = MAX_HOLD - hold
                warn = f"⚠️ **{deadline_str} 大引けに処分**" if remaining <= 1 else f"（あと{remaining}日／{deadline_str}までに処分）"
            except Exception:
                warn = ""

            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）{dir_str} "
                f"含み **{upnl:+.2f}%** — {hold}日目 {warn}"
            )

    # 合計損益
    if closed:
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    title = f"⚡【売却指示】{date_str}" if closed else f"📋【スイング結果】{date_str}"
    payload = {
        "embeds": [{
            "title":       title,
            "description": "\n".join(lines),
            "color":       COLOR_WIN if any((p.get("pnl_pct") or 0) > 0 for p in closed) else COLOR_ERROR,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print(f"[notifier] 結果レポートを Discord に送信しました（決済{len(closed)}件 / 保有中{len(still_open)}件）")


def send_sell_signals(signals: list[dict], today: date, entry_date=None) -> None:
    """空売りシグナルを SELL専用Webhookに送信する。"""
    url = os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip()
    if not url:
        print("[notifier] DISCORD_WEBHOOK_SELL_URL が未設定 → SELL通知スキップ")
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    if entry_date is None:
        entry_date = today
    exit_date     = _nth_trading_day(entry_date, 2)
    exit_date_str = exit_date.strftime("%m月%d日")

    if not signals:
        resp = requests.post(url, json={
            "embeds": [{
                "title":       f"📉【スイング空売り】{date_str} — シグナルなし",
                "description": "本日の空売りシグナルは0件です。",
                "color":       COLOR_NONE,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]
        }, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier] SELL シグナルなし送信失敗: HTTP {resp.status_code}")
        else:
            print("[notifier] SELL シグナルなし を送信しました。")
        return

    sep = "─" * 24
    exit_date_short = exit_date.strftime("%m/%d")
    lines = [
        f"🎯 **9:00 寄り付き成行（信用売り）**・1件100万円",
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
            shares     = max(100, int(1_000_000 / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            line1      = f"**#{i} {name}** ({ticker}) 前日{prev_close:,.0f}円 {shares:,}株/約{invest_amt/1e4:.0f}万"
        else:
            line1      = f"**#{i} {name}** ({ticker}) 100万円目安"

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

    payload = {
        "embeds": [{
            "title":       f"📉【スイング空売り】{date_str} — 売り{len(signals)}銘柄",
            "description": "\n".join(lines).rstrip(),
            "color":       COLOR_SELL,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    }
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[notifier] SELL Discord送信失敗: HTTP {resp.status_code}")
    print(f"[notifier] SELL {len(signals)} 件のシグナルを Discord に送信しました。")


def send_monthly_report(positions: list[dict], today: date) -> None:
    """月別・年間損益をDiscordに送信する。"""
    from collections import defaultdict

    closed = [
        p for p in positions
        if p.get("status") == "closed" and p.get("pnl_pct") is not None
    ]
    if not closed:
        return

    # exit_date で月別集計
    monthly = defaultdict(list)
    for p in closed:
        ym = (p.get("exit_date") or "")[:7]
        if ym:
            monthly[ym].append(p["pnl_pct"])

    current_year = str(today.year)
    year_months  = {ym: pnls for ym, pnls in monthly.items() if ym.startswith(current_year)}

    if not year_months:
        return

    lines = []
    for ym in sorted(year_months.keys()):
        pnls  = year_months[ym]
        mr    = sum(pnls) * WEIGHT
        wins  = sum(1 for p in pnls if p > 0)
        yen   = mr / 100 * CAPITAL
        sign  = "+" if mr >= 0 else ""
        lines.append(
            f"`{ym}` {len(pnls)}件 勝率{wins}/{len(pnls)} "
            f"**月利{sign}{mr:.1f}%**（{sign}{yen/10000:.1f}万円）"
        )

    year_pnls  = [p for pnls in year_months.values() for p in pnls]
    annual_pct = sum(year_pnls) * WEIGHT
    annual_yen = annual_pct / 100 * CAPITAL
    a_sign     = "+" if annual_pct >= 0 else ""

    desc = "\n".join(lines)
    desc += f"\n\n**{current_year}年合計: {a_sign}{annual_pct:.1f}%（{a_sign}{annual_yen/10000:.1f}万円）**"

    color = COLOR_WIN if annual_pct >= 0 else COLOR_ERROR
    _post({
        "embeds": [{
            "title":       f"📈 {current_year}年 月別・年間損益（スイング）",
            "description": desc,
            "color":       color,
            "footer":      {"text": "※資金300万・1トレード100万基準"},
        }]
    })
    print(f"[notifier] 月別・年間損益レポートを送信しました")


def _post_sell(payload: dict) -> None:
    url = os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip()
    if not url:
        print("[notifier] DISCORD_WEBHOOK_SELL_URL が未設定 → SELL通知スキップ")
        return
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[notifier] SELL Discord送信失敗: HTTP {resp.status_code}")


def send_sell_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """空売りポジションの損益結果をSELL専用Webhookに送信する。"""
    if not closed and not still_open:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    lines = []

    if closed:
        lines.append("**── 決済済み（買い戻し） ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）空売り "
                f"→ **{pnl:+.2f}%** ［{etype}］"
            )

    if still_open:
        if lines:
            lines.append("")
        lines.append("**── 保有中（売りポジション持ち越し） ──**")
        for p in still_open:
            upnl = p.get("unrealized_pnl", 0) or 0
            hold = p.get("hold_days", 0)
            emoji = "📈" if upnl >= 0 else "📉"
            try:
                entry_dt  = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                cur       = entry_dt
                biz_count = 0
                while biz_count < MAX_HOLD:
                    cur += timedelta(days=1)
                    if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                        biz_count += 1
                deadline_str = cur.strftime("%m月%d日")
                remaining    = MAX_HOLD - hold
                warn = f"⚠️ **{deadline_str} 大引けに買戻し**" if remaining <= 1 else f"（あと{remaining}日／{deadline_str}までに買戻し）"
            except Exception:
                warn = ""
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）含み **{upnl:+.2f}%** — {hold}日目 {warn}"
            )

    if closed:
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    _post_sell({
        "embeds": [{
            "title":       f"📋【スイング空売り結果】{date_str}",
            "description": "\n".join(lines),
            "color":       0x43A047 if any((p.get("pnl_pct") or 0) > 0 for p in closed) else 0xFDD835,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    })
    print(f"[notifier] SELL結果レポートを送信しました（決済{len(closed)}件 / 保有中{len(still_open)}件）")


def send_sell_monthly_report(positions: list[dict], today: date) -> None:
    """空売りの月別・年間損益をSELL専用Webhookに送信する。"""
    from collections import defaultdict

    closed = [
        p for p in positions
        if p.get("status") == "closed" and p.get("pnl_pct") is not None
    ]
    if not closed:
        return

    monthly = defaultdict(list)
    for p in closed:
        ym = (p.get("exit_date") or "")[:7]
        if ym:
            monthly[ym].append(p["pnl_pct"])

    current_year = str(today.year)
    year_months  = {ym: pnls for ym, pnls in monthly.items() if ym.startswith(current_year)}

    if not year_months:
        return

    lines = []
    for ym in sorted(year_months.keys()):
        pnls = year_months[ym]
        mr   = sum(pnls) * WEIGHT
        wins = sum(1 for p in pnls if p > 0)
        yen  = mr / 100 * CAPITAL
        sign = "+" if mr >= 0 else ""
        lines.append(
            f"`{ym}` {len(pnls)}件 勝率{wins}/{len(pnls)} "
            f"**月利{sign}{mr:.1f}%**（{sign}{yen/10000:.1f}万円）"
        )

    year_pnls  = [p for pnls in year_months.values() for p in pnls]
    annual_pct = sum(year_pnls) * WEIGHT
    annual_yen = annual_pct / 100 * CAPITAL
    a_sign     = "+" if annual_pct >= 0 else ""

    desc  = "\n".join(lines)
    desc += f"\n\n**{current_year}年合計: {a_sign}{annual_pct:.1f}%（{a_sign}{annual_yen/10000:.1f}万円）**"

    _post_sell({
        "embeds": [{
            "title":       f"📉 {current_year}年 月別・年間損益（スイング空売り）",
            "description": desc,
            "color":       0x43A047 if annual_pct >= 0 else 0xFDD835,
            "footer":      {"text": "※資金300万・1トレード100万基準"},
        }]
    })
    print(f"[notifier] SELL月別・年間損益レポートを送信しました")


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


def send_close_signals(targets: list[dict], today: date) -> None:
    """大引け前にRSI≥50またはMAXHOLDで処分すべきポジションをDiscord通知する。

    targets: close_check.py が生成した sell_targets リスト
      [{"ticker", "name", "reason_type": "RSI"|"MAXHOLD", "reason",
        "today_hold", "rsi_now", "current_price", "entry_open"}]
    """
    if not targets:
        return

    date_str = today.strftime("%m/%d")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    sep = "─" * 22

    lines = [
        f"🛒 **15:25-15:30 クロージング**で成行売り（SBI証券）",
        f"対象: **{len(targets)}銘柄**",
        sep,
    ]

    for i, t in enumerate(targets, 1):
        ticker  = t["ticker"].replace(".T", "")
        name    = t["name"]
        rtype   = t["reason_type"]
        hold    = t.get("today_hold", "?")
        rsi     = t.get("rsi_now")
        price   = t.get("current_price")
        entry   = t.get("entry_open")

        if rtype == "RSI":
            icon = "🔔"
            tag  = f"RSI回復(RSI={rsi:.1f})" if rsi is not None else "RSI回復"
        else:
            icon = "⏰"
            tag  = f"{hold}日目MAXHOLD"

        line = f"{icon} **#{i} {name}** ({ticker}) — {tag}"
        if price is not None and entry is not None and entry > 0:
            pnl_now = (price - entry) / entry * 100
            line += f" / {pnl_now:+.2f}%"
        lines.append(line)

    color = COLOR_WIN if any(
        t.get("current_price") and t.get("entry_open") and t["current_price"] > t["entry_open"]
        for t in targets
    ) else COLOR_ERROR

    payload = {
        "embeds": [{
            "title":       f"⚡【大引け処分指示】{date_str}",
            "description": "\n".join(lines),
            "color":       color,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print(f"[notifier] 大引け処分指示を Discord に送信しました（{len(targets)}件）")


def send_close_signals_sell(targets: list[dict], today: date) -> None:
    """SELL（空売り）ポジションの大引け処分指示を SELL専用Webhookに送信する。

    targets: close_check.py が生成した sell_targets（direction="SELL"）
    """
    if not targets:
        return

    url = os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip()
    if not url:
        print("[notifier] DISCORD_WEBHOOK_SELL_URL 未設定 → SELL大引け処分通知スキップ")
        return

    date_str = today.strftime("%m/%d")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    sep = "─" * 22

    lines = [
        f"🛒 **15:25-15:30 クロージング**で成行買戻し（SBI証券・信用）",
        f"対象: **{len(targets)}銘柄**",
        sep,
    ]

    for i, t in enumerate(targets, 1):
        ticker  = t["ticker"].replace(".T", "")
        name    = t["name"]
        rtype   = t["reason_type"]
        hold    = t.get("today_hold", "?")
        rsi     = t.get("rsi_now")
        price   = t.get("current_price")
        entry   = t.get("entry_open")

        if rtype == "RSI":
            icon = "🔔"
            tag  = f"RSI回復(RSI={rsi:.1f})" if rsi is not None else "RSI回復"
        else:
            icon = "⏰"
            tag  = f"{hold}日目MAXHOLD"

        line = f"{icon} **#{i} {name}** ({ticker}) — {tag}"
        if price is not None and entry is not None and entry > 0:
            # SELL は entry > current で利益
            pnl_now = (entry - price) / entry * 100
            line += f" / {pnl_now:+.2f}%"
        lines.append(line)

    color = COLOR_WIN if any(
        t.get("current_price") and t.get("entry_open") and t["entry_open"] > t["current_price"]
        for t in targets
    ) else COLOR_ERROR

    payload = {
        "embeds": [{
            "title":       f"⚡【空売り大引け処分指示】{date_str}",
            "description": "\n".join(lines),
            "color":       color,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    }
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[notifier] SELL大引け処分通知 失敗: HTTP {resp.status_code}")
    else:
        print(f"[notifier] SELL大引け処分指示を Discord に送信しました（{len(targets)}件）")

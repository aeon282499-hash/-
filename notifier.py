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
    nas_str = f"ナスダック(QQQ) {nas:+.1f}%" if nas is not None else "ナスダック 取得不可"

    if bias == "bearish":
        env = "⚠️ 米国株安 → **売りバイアス**（買いシグナルは見送り）"
        strategy = "本日は地合い悪化のため売りシグナルのみ採用します。"
    elif bias == "bullish":
        env = "🌕 米国株高 → **買いバイアス**（売りシグナルは見送り）"
        strategy = "本日は地合い良好のため買いシグナルのみ採用します。"
    else:
        env = "⚖️ 米国市場はほぼ横ばい → **中立**"
        strategy = "買い・売り双方のシグナルを採用します。"

    return f"{dow_str} ／ {nas_str}\n{env}\n{strategy}"


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
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    macro = macro or {}

    # 処分日（エントリー日から3営業日後）
    if entry_date is None:
        _d = today + timedelta(days=1)
        while _d.weekday() >= 5 or jpholiday.is_holiday(_d):
            _d += timedelta(days=1)
        entry_date = _d
    exit_date = _nth_trading_day(entry_date, 3)
    exit_date_str = exit_date.strftime("%m月%d日")

    if not signals:
        _send_no_signal(date_str, time_str, macro)
        return

    macro_desc = _macro_description(macro)

    # ── ヘッダーEmbed（相場環境） ────────────────────────
    header_embed = {
        "title": f"📊【スイング】{date_str} — 相場環境",
        "description": macro_desc,
        "color": COLOR_NONE,
    }

    # ── 各銘柄のEmbed ────────────────────────────────────
    embeds = [header_embed]
    for i, sig in enumerate(signals, 1):
        direction    = sig["direction"]
        prev_close   = sig.get("prev_close", 0)

        bb_upper = sig.get("bb_upper")
        bb_lower = sig.get("bb_lower")

        if direction == "BUY":
            action_str = "🔴 **買い**（9:00以降、指値推奨）"
            color      = COLOR_BUY
            stop_price = prev_close * 0.97
            tp_price   = prev_close * 1.05
            stop_str   = f"**{stop_price:,.0f}円**（前日終値-3%）"
            tp_str     = f"**{tp_price:,.0f}円**（前日終値+5%）"
            if bb_lower:
                entry_str = f"**{bb_lower:,.0f}円**（BB下限）付近に指値\n※寄り付き後に値が下限に近ければ有効"
            else:
                entry_str = f"**{prev_close:,.0f}円**付近（前日終値）"

        # 株数・投入金額（100万円基準）
        if prev_close > 0:
            shares      = max(100, int(1_000_000 / prev_close / 100) * 100)
            invest_amt  = shares * prev_close
            invest_str  = f"**{shares:,}株**（約{invest_amt/1e4:.0f}万円）※前日終値{prev_close:,.0f}円基準"
        else:
            invest_str  = "**100万円目安**"

        reason_text  = "\n".join(f"・{r}" for r in sig["reason"])
        turnover_str = f"{sig['turnover']/1e8:.0f}億円"

        embed = {
            "title": f"📊【スイング】#{i}  {sig['name']}（{sig['ticker']}）",
            "color": color,
            "fields": [
                {
                    "name":   "📌 アクション",
                    "value":  action_str,
                    "inline": False,
                },
                {
                    "name":   "🎯 エントリー目安（指値）",
                    "value":  entry_str,
                    "inline": False,
                },
                {
                    "name":   "💴 推奨株数・投入金額",
                    "value":  invest_str,
                    "inline": False,
                },
                {
                    "name":   "🛑 損切りライン（目安）",
                    "value":  stop_str,
                    "inline": True,
                },
                {
                    "name":   "✅ 利確ライン（目安）",
                    "value":  tp_str,
                    "inline": True,
                },
                {
                    "name":   "📅 保有ルール・処分日",
                    "value":  f"最大**3営業日**保有\nRSI回復（≧50）で早期決済\n⏰ **処分期限: {exit_date_str}**（3営業日後終値）",
                    "inline": False,
                },
                {
                    "name":   "🛡️ 流動性",
                    "value":  f"売買代金 **{turnover_str}**（スリッページ軽微）",
                    "inline": False,
                },
                {
                    "name":   "📊 シグナル根拠",
                    "value":  reason_text,
                    "inline": False,
                },
            ],
            "footer": {"text": f"配信時刻: {time_str}"},
        }
        embeds.append(embed)

    payload = {
        "content": (
            f"## 📊【スイング】自動売買シグナル｜{date_str}\n"
            f"> 本日の買いシグナル: **{len(signals)}銘柄**"
        ),
        "embeds": embeds[:10],  # Discord上限10
    }
    _post(payload)
    print(f"[notifier] {len(signals)} 件のシグナルを Discord に送信しました。")


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
        lines.append("**── 決済済み ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            dir_str = "買い" if p["direction"] == "BUY" else "売り"
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）{dir_str} "
                f"→ **{pnl:+.2f}%** ［{etype}］"
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
                while biz_count < 5:
                    cur += timedelta(days=1)
                    if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                        biz_count += 1
                deadline = cur
                deadline_str = deadline.strftime("%m月%d日")
                remaining = 5 - hold
                warn = "⚠️ **本日処分！**" if remaining <= 1 else f"（あと{remaining}日／{deadline_str}までに処分）"
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

    payload = {
        "embeds": [{
            "title":       f"📋【スイング結果】{date_str}",
            "description": "\n".join(lines),
            "color":       COLOR_WIN if any((p.get("pnl_pct") or 0) > 0 for p in closed) else COLOR_ERROR,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print(f"[notifier] 結果レポートを Discord に送信しました（決済{len(closed)}件 / 保有中{len(still_open)}件）")


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

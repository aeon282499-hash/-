"""
notifier_premium.py — 至高版 Discord Webhook 通知
================================================
別チャンネル送信。環境変数 DISCORD_WEBHOOK_URL_PREMIUM を使用。

【特徴】
  - BUYシグナルのみ
  - 1件あたり 250万円ベースで株数を提示
  - 至高（厳選大ロット）であることを明示するレイアウト
"""

import os
from datetime import date, datetime, timedelta
import zoneinfo

import jpholiday
import requests

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

COLOR_PRM_BUY = 0xFFB300   # 至高はゴールド
COLOR_NONE    = 0x757575
COLOR_ERROR   = 0xFDD835
COLOR_WIN     = 0x43A047

POSITION_BUDGET_JPY = 1_500_000   # 1件あたり投入額（v2: 250万→150万・3並列＝450万）
MAX_HOLD_PRM        = 5           # 最大保有営業日数（tracker_premium.py と一致）


def _get_webhook_url() -> str:
    url = os.getenv("DISCORD_WEBHOOK_URL_PREMIUM", "").strip()
    if not url:
        raise ValueError("DISCORD_WEBHOOK_URL_PREMIUM が未設定です。")
    return url


def _post(payload: dict) -> None:
    url  = _get_webhook_url()
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord送信失敗: HTTP {resp.status_code}\n{resp.text}")


def _macro_description(macro: dict) -> str:
    dow  = macro.get("dow")
    nas  = macro.get("nasdaq")
    bias = macro.get("bias", "neutral")

    dow_str = f"S&P500(SPY) {dow:+.1f}%" if dow is not None else "S&P500 取得不可"
    nas_str = f"ナスダック総合 {nas:+.1f}%" if nas is not None else "ナスダック 取得不可"
    env = "⚠️ 米国株安" if bias == "bearish" else "🌕 米国株高" if bias == "bullish" else "⚖️ 米国市場はほぼ横ばい"
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


def send_signals_premium(signals: list[dict], today: date, macro: dict | None = None, entry_date=None) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    macro    = macro or {}

    if entry_date is None:
        entry_date = today
    exit_date     = _nth_trading_day(entry_date, MAX_HOLD_PRM)
    exit_date_str = exit_date.strftime("%m月%d日")

    if not signals:
        _send_no_signal(date_str, time_str, macro)
        return

    embeds = []
    for i, sig in enumerate(signals, 1):
        prev_close = sig.get("prev_close", 0)
        action_str = "🟡 **買い**（9:00 寄り付き成行）"
        stop_str   = "**寄り値 × 0.97**（-3%）"
        tp_str     = "**寄り値 × 1.03**（+3%）"
        entry_str  = f"**9:00 寄り付き成行**\n参考: 前日終値 {prev_close:,.0f}円"

        if prev_close > 0:
            shares     = max(100, int(POSITION_BUDGET_JPY / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            invest_str = f"**{shares:,}株**（約{invest_amt/1e4:.0f}万円）※前日終値{prev_close:,.0f}円基準・250万予算"
        else:
            invest_str = "**250万円目安**"

        reason_text  = "\n".join(f"・{r}" for r in sig["reason"])
        turnover_str = f"{sig['turnover']/1e8:.0f}億円"

        embed = {
            "title": f"👑【極み】#{i}  {sig['name']}（{sig['ticker']}）",
            "color": COLOR_PRM_BUY,
            "fields": [
                {"name": "📌 アクション",          "value": action_str, "inline": False},
                {"name": "🎯 エントリー",          "value": entry_str,  "inline": False},
                {"name": "💴 推奨株数・投入金額",   "value": invest_str, "inline": False},
                {"name": "🛑 損切りライン（目安）", "value": stop_str,   "inline": True},
                {"name": "✅ 利確ライン（目安）",   "value": tp_str,     "inline": True},
                {"name": "📅 保有ルール・処分日",
                 "value": f"最大**{MAX_HOLD_PRM}営業日**保有\nRSI回復（≧50）で早期決済\n⏰ **処分期限: {exit_date_str}**（{MAX_HOLD_PRM}営業日後終値）",
                 "inline": False},
                {"name": "🛡️ 流動性",             "value": f"売買代金 **{turnover_str}**（大型株・スリッページ極小）", "inline": False},
                {"name": "📊 シグナル根拠",        "value": reason_text, "inline": False},
            ],
            "footer": {"text": f"配信時刻: {time_str}"},
        }
        embeds.append(embed)

    payload = {
        "content": (
            f"## 👑【買いスイング極み】｜{date_str}\n"
            f"> 本日の極みシグナル: **{len(signals)}銘柄**（最大3件・1件{POSITION_BUDGET_JPY//10000}万円）"
        ),
        "embeds": embeds[:10],
    }
    _post(payload)
    print(f"[notifier_prm] {len(signals)} 件のシグナルを Discord(極み) に送信しました。")


def _send_no_signal(date_str: str, time_str: str, macro: dict) -> None:
    macro_desc = _macro_description(macro)
    payload = {
        "embeds": [{
            "title": f"👑【買いスイング極み】{date_str} — シグナルなし",
            "description": (
                "本日は極み水準を満たす銘柄が存在しません。\n"
                "厳選モデルとして資金を温存し、**0銘柄（見送り）** とします。\n\n"
                f"**【本日の相場環境】**\n{macro_desc}"
            ),
            "color":  COLOR_NONE,
            "footer": {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print("[notifier_prm] 極みシグナル 0 件の通知を送信しました。")


def send_no_signal_premium(today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    _send_no_signal(date_str, time_str, {})


def send_skip_premium(reason: str, today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    payload = {
        "embeds": [{
            "title":       f"👑【買いスイング極み】{date_str} — 配信スキップ",
            "description": reason,
            "color":       COLOR_NONE,
        }]
    }
    _post(payload)


def send_results_premium(closed: list[dict], still_open: list[dict], today: date) -> None:
    """前日シグナルの損益結果を Discord(Premium) に送信する。"""
    if not closed and not still_open:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    lines: list[str] = []

    if closed:
        lines.append("**── 🔔 本日寄り付きで売却してください ──**")
        for p in closed:
            pnl   = p.get("pnl_pct", 0) or 0
            etype = p.get("exit_type", "?")
            emoji = "✅" if pnl > 0 else "❌"
            reason = {
                "RSI":     "RSI回復（≥50）",
                "TP":      "利確（+3%）",
                "STOP":    "損切り（-3%）",
                "MAXHOLD": "最大保有日数",
            }.get(etype, etype)
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）買い "
                f"→ **{pnl:+.2f}%** ／ 理由: {reason}"
            )

    if still_open:
        if lines:
            lines.append("")
        lines.append("**── 保有中（持ち越し） ──**")
        for p in still_open:
            upnl  = p.get("unrealized_pnl", 0) or 0
            hold  = p.get("hold_days", 0)
            emoji = "📈" if upnl >= 0 else "📉"
            try:
                entry_dt  = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                cur       = entry_dt
                biz_count = 0
                while biz_count < MAX_HOLD_PRM:
                    cur += timedelta(days=1)
                    if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
                        biz_count += 1
                deadline_str = cur.strftime("%m月%d日")
                remaining    = MAX_HOLD_PRM - hold
                warn = (f"⚠️ **{deadline_str} 大引けに処分**" if remaining <= 1
                        else f"（あと{remaining}日／{deadline_str}までに処分）")
            except Exception:
                warn = ""
            lines.append(
                f"{emoji} **{p['name']}**（{p['ticker']}）買い "
                f"含み **{upnl:+.2f}%** — {hold}日目 {warn}"
            )

    if closed:
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    payload = {
        "content": f"## 👑【買いスイング極み】売買結果｜{date_str}\n" + "\n".join(lines),
    }
    _post(payload)
    print(f"[notifier_prm] 結果通知を Discord(極み) に送信しました。")


def send_error_premium(err: Exception, today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    payload = {
        "embeds": [{
            "title":       f"👑【買いスイング極み】{date_str} — エラー",
            "description": f"```\n{type(err).__name__}: {err}\n```",
            "color":       COLOR_ERROR,
        }]
    }
    try:
        _post(payload)
    except Exception:
        pass

"""
notifier.py — Discord Webhook 通知モジュール
=============================================

Embed カラー:
  買い (BUY)  → 赤  #E53935
  売り (SELL) → 青  #1E88E5
  0件・スキップ → グレー #757575
  エラー      → 黄  #FDD835
"""

import os
import requests
from datetime import date, datetime
import zoneinfo

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

COLOR_BUY   = 0xE53935
COLOR_SELL  = 0x1E88E5
COLOR_NONE  = 0x757575
COLOR_ERROR = 0xFDD835


def _get_webhook_url() -> str:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        raise ValueError(
            "DISCORD_WEBHOOK_URL が未設定です。"
            ".env ファイルまたは GitHub Secrets を確認してください。"
        )
    return url


def _post(payload: dict) -> None:
    url  = _get_webhook_url()
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"Discord への送信に失敗しました: HTTP {resp.status_code}\n{resp.text}"
        )


def send_signals(signals: list[dict], today: date) -> None:
    """
    スクリーニング結果を Discord に送信する。

    Parameters
    ----------
    signals : screener.run_screener() の戻り値
    today   : 実行日（date オブジェクト）
    """
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    if not signals:
        _send_no_signal(date_str, time_str)
        return

    embeds = []
    for sig in signals:
        if sig["direction"] == "BUY":
            dir_label = "🔴 成行 **買い** エントリー（寄り付き9:00）"
            color     = COLOR_BUY
        else:
            dir_label = "🔵 成行 **売り**（空売り）エントリー（寄り付き9:00）"
            color     = COLOR_SELL

        reason_text = "\n".join(f"・{r}" for r in sig["reason"])

        embed = {
            "title": f"{sig['name']}　{sig['ticker']}",
            "color": color,
            "fields": [
                {
                    "name":   "📌 売買方向",
                    "value":  dir_label,
                    "inline": False,
                },
                {
                    "name":   "📊 選定根拠",
                    "value":  reason_text,
                    "inline": False,
                },
                {
                    "name":   "RSI(14)",
                    "value":  f"`{sig['rsi']}`",
                    "inline": True,
                },
                {
                    "name":   "25MA乖離率",
                    "value":  f"`{sig['deviation']:+.2f}%`",
                    "inline": True,
                },
                {
                    "name":   "前日値幅/ATR",
                    "value":  f"`{sig['range_ratio']}`",
                    "inline": True,
                },
                {
                    "name":   "⚠️ 決済リマインド",
                    "value":  "**15:30 の大引けで必ず決済してください**（寄り引けデイトレ）",
                    "inline": False,
                },
            ],
            "footer": {"text": f"配信時刻: {time_str}"},
        }
        embeds.append(embed)

    payload = {
        "content": (
            f"## 📈 自動売買シグナル｜{date_str}\n"
            f"> 以下 **{len(signals)} 銘柄** が本日の条件を満たしました。"
            f"9:00 寄り付きでエントリー、**15:30 大引けで全決済**してください。"
        ),
        "embeds": embeds,
    }
    _post(payload)
    print(f"[notifier] {len(signals)} 件のシグナルを Discord に送信しました。")


def send_no_signal(today: date) -> None:
    """条件に合致する銘柄が 0 件だったときの通知。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    payload = {
        "embeds": [{
            "title":       f"📊 {date_str} — 本日のシグナル結果",
            "description": (
                "本日の条件を満たす銘柄は **0 件** でした。\n\n"
                "ノートレードを推奨します。\n"
                "システムは正常に稼働しています。"
            ),
            "color":  COLOR_NONE,
            "footer": {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print("[notifier] シグナル 0 件の通知を送信しました。")


def _send_no_signal(date_str: str, time_str: str) -> None:
    """send_signals から内部的に呼ばれる 0 件通知。"""
    payload = {
        "embeds": [{
            "title":       f"📊 {date_str} — 本日のシグナル結果",
            "description": (
                "本日の条件を満たす銘柄は **0 件** でした。\n\n"
                "ノートレードを推奨します。\n"
                "システムは正常に稼働しています。"
            ),
            "color":  COLOR_NONE,
            "footer": {"text": f"配信時刻: {time_str}"},
        }]
    }
    _post(payload)
    print("[notifier] シグナル 0 件の通知を送信しました。")


def send_skip(reason: str, today: date) -> None:
    """休場日スキップ通知（デバッグ用。本番では省略可）。"""
    date_str = today.strftime("%Y年%m月%d日")
    payload = {
        "embeds": [{
            "title":       f"🗓️ {date_str} — 配信スキップ",
            "description": reason,
            "color":       COLOR_NONE,
        }]
    }
    _post(payload)


def send_error(error_message: str, today: date) -> None:
    """エラー発生時の通知。"""
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

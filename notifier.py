"""
notifier.py — Discord Webhook 通知モジュール
"""

import os
import requests
from datetime import date, datetime
import zoneinfo

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

COLOR_BUY   = 0xE53935   # 赤
COLOR_SELL  = 0x1E88E5   # 青
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

    dow_str = f"NYダウ {dow:+.1f}%" if dow is not None else "NYダウ 取得不可"
    nas_str = f"ナスダック {nas:+.1f}%" if nas is not None else "ナスダック 取得不可"

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


def send_signals(signals: list[dict], today: date, macro: dict | None = None) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    macro = macro or {}

    if not signals:
        _send_no_signal(date_str, time_str, macro)
        return

    buys  = sum(1 for s in signals if s["direction"] == "BUY")
    sells = len(signals) - buys

    macro_desc = _macro_description(macro)

    # ── ヘッダーEmbed（相場環境） ────────────────────────
    header_embed = {
        "title": f"📊 {date_str} — 本日の相場環境と基本戦略",
        "description": macro_desc,
        "color": COLOR_NONE,
    }

    # ── 各銘柄のEmbed ────────────────────────────────────
    embeds = [header_embed]
    for i, sig in enumerate(signals, 1):
        if sig["direction"] == "BUY":
            action_str = "🔴 **寄り成り 買い**（9:00 エントリー）"
            color      = COLOR_BUY
        else:
            action_str = "🔵 **寄り成り 売り**（空売り）（9:00 エントリー）"
            color      = COLOR_SELL

        reason_text = "\n".join(f"・{r}" for r in sig["reason"])
        turnover_str = f"{sig['turnover']/1e8:.0f}億円"

        embed = {
            "title": f"#{i}  {sig['name']}（{sig['ticker']}）",
            "color": color,
            "fields": [
                {
                    "name":   "📌 アクション",
                    "value":  action_str,
                    "inline": False,
                },
                {
                    "name":   "💴 投入金額目安",
                    "value":  "**100万〜150万円**（寄り成り、15:30大引け決済）",
                    "inline": False,
                },
                {
                    "name":   "🛡️ 安全性の証明",
                    "value":  f"売買代金 **{turnover_str}**（150万の決済でスリッページ軽微）",
                    "inline": False,
                },
                {
                    "name":   "📊 極限吟味のロジック",
                    "value":  reason_text,
                    "inline": False,
                },
                {
                    "name":   "⚠️ 決済リマインド",
                    "value":  "**15:30 大引けで必ず決済**（寄り引けデイトレ）",
                    "inline": False,
                },
            ],
            "footer": {"text": f"配信時刻: {time_str}"},
        }
        embeds.append(embed)

    payload = {
        "content": (
            f"## 📈 自動売買シグナル｜{date_str}\n"
            f"> 本日の実行銘柄数: **{len(signals)}銘柄**"
            f"（買い {buys} / 売り {sells}）"
        ),
        "embeds": embeds[:10],  # Discord上限10
    }
    _post(payload)
    print(f"[notifier] {len(signals)} 件のシグナルを Discord に送信しました。")


def _send_no_signal(date_str: str, time_str: str, macro: dict) -> None:
    macro_desc = _macro_description(macro)
    payload = {
        "embeds": [{
            "title":       f"📊 {date_str} — 本日のシグナル結果",
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

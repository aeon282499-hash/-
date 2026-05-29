"""
notifier_sector_theme.py — セクター×テーマ新システム Discord 通知

簡易版: BUY シグナルのみを単一 Webhook に Embed 形式で送信する。
"""
import os
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK = (os.getenv("DISCORD_WEBHOOK_URL_SECTOR_THEME", "") or "").strip()
_VERIFY_SSL = os.getenv("DISCORD_VERIFY_SSL", "true").lower() not in ("0", "false", "no")
COLOR_BUY = 0x8E24AA   # 紫 (既存BUYの赤と差別化)
COLOR_SELL = 0xE53935  # 赤 (空売り)
COLOR_NONE = 0x757575


def _post(payload: dict, tag: str = "") -> None:
    if not WEBHOOK:
        print(f"[notifier_st{tag}] DISCORD_WEBHOOK_URL_SECTOR_THEME 未設定 → スキップ")
        return
    try:
        r = requests.post(WEBHOOK, json=payload, timeout=10, verify=_VERIFY_SSL)
        if r.status_code not in (200, 204):
            print(f"[notifier_st{tag}] HTTP {r.status_code} {r.text[:200]}")
        else:
            print(f"[notifier_st{tag}] 送信OK")
    except Exception as e:
        print(f"[notifier_st{tag}] failed: {e}")


def _fmt_signal_embed(s: dict, rank: int) -> dict:
    flags = []
    if s.get("in_sector_top"):
        sec = s.get("sector", "?")
        flags.append(f"🏭 セクター上位 [{sec}]")
    if s.get("in_theme"):
        flags.append("🎯 テーマ銘柄")

    desc_lines = [
        f"**RSI({14})**: `{s['rsi']}` (≦45・売られすぎ)",
        f"**25MA乖離**: `{s['deviation']:+.1f}%` (≦-1.5%・押し目)",
        f"**売買代金**: `{s['turnover']/1e8:.0f}億円`",
        f"**直近終値**: `{s.get('prev_close','-'):,.0f}円`",
        "",
        " / ".join(flags) if flags else "_(フィルタ通過根拠なし)_",
    ]
    return {
        "title": f"#{rank}  [{s['ticker']}] {s['name']}",
        "description": "\n".join(desc_lines),
        "color": COLOR_BUY,
    }


def _fmt_sell_signal_embed(s: dict, rank: int) -> dict:
    sec = s.get("sector", "?")
    desc_lines = [
        f"**RSI({14})**: `{s['rsi']}` (≧60・買われすぎ)",
        f"**25MA乖離**: `{s['deviation']:+.1f}%` (≧+4%・上がりすぎ)",
        f"**前日比**: `{s.get('day_change', 0):+.1f}%` (≧+3%・急騰)",
        f"**売買代金**: `{s['turnover']/1e8:.0f}億円`",
        f"**直近終値**: `{s.get('prev_close','-'):,.0f}円`",
        "",
        f"🔻 最弱セクター [{sec}] の急騰 → 反落狙いの空売り",
        "⚠️ エントリー前にSBIで**貸借区分・在庫(空売り可否)**を確認",
    ]
    return {
        "title": f"#{rank}  [{s['ticker']}] {s['name']}",
        "description": "\n".join(desc_lines),
        "color": COLOR_SELL,
    }


def send_signals(signals: list[dict], sell_signals: list[dict] | None,
                 macro: dict, diag: dict | None = None) -> None:
    today = date.today().strftime("%Y-%m-%d (%a)")
    sell_signals = sell_signals or []

    if not signals and not sell_signals:
        _post({"embeds": [{
            "title": f"🟣 本日のシグナル — {today}",
            "description": "本日は買い・空売りともシグナル0件です。",
            "color": COLOR_NONE,
        }]}, tag="-empty")
        return

    embeds = []
    if signals:
        embeds.append({
            "title": f"🟣 本日の買いシグナル — {today}",
            "description": f"本日の買いシグナルは {len(signals)} 件です。",
            "color": COLOR_BUY,
        })
        for i, s in enumerate(signals, 1):
            embeds.append(_fmt_signal_embed(s, i))

    if sell_signals:
        embeds.append({
            "title": f"🔻 本日の空売りシグナル — {today}",
            "description": (f"最弱セクターの急騰後反落 {len(sell_signals)} 件。"
                            "翌寄り成りでショート・損切+3%/利確-5%/最大3日。"),
            "color": COLOR_SELL,
        })
        for i, s in enumerate(sell_signals, 1):
            embeds.append(_fmt_sell_signal_embed(s, i))

    # Discord は 1 メッセージに embed 10 個まで
    for chunk_start in range(0, len(embeds), 10):
        chunk = embeds[chunk_start:chunk_start + 10]
        _post({"embeds": chunk}, tag=f"-sig{chunk_start}")


def send_error(msg: str) -> None:
    _post({"embeds": [{
        "title": "⚠️ スイングセクターローテ エラー",
        "description": msg[:1900],
        "color": 0xFDD835,
    }]}, tag="-err")

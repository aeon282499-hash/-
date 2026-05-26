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


def send_signals(signals: list[dict], macro: dict, diag: dict | None = None) -> None:
    today = date.today().strftime("%Y-%m-%d (%a)")
    title = f"🟣 本日のシグナル — {today}"
    if not signals:
        body = "本日は買いシグナル0件です。"
        _post({"embeds": [{
            "title": title, "description": body, "color": COLOR_NONE
        }]}, tag="-empty")
        return

    embeds = []
    head = {
        "title": title,
        "description": f"本日の買いシグナルは {len(signals)} 件です。",
        "color": COLOR_BUY,
    }
    embeds.append(head)
    for i, s in enumerate(signals, 1):
        embeds.append(_fmt_signal_embed(s, i))

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

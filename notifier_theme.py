"""
notifier_theme.py — テーマトラッカー Discord 通知

「🔥 今ホットなテーマ一覧 ＋ 点火中テーマ(heat>=floor)の出遅れ初動候補」を Embed で送信する。
DISCORD_WEBHOOK_URL_THEME を使用(未設定なら DISCORD_WEBHOOK_URL_SECTOR_THEME にフォールバック)。
"""
import os
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK = (
    os.getenv("DISCORD_WEBHOOK_URL_THEME", "")
    or os.getenv("DISCORD_WEBHOOK_URL_SECTOR_THEME", "")
    or ""
).strip()
_VERIFY_SSL = os.getenv("DISCORD_VERIFY_SSL", "true").lower() not in ("0", "false", "no")

COLOR_HOT = 0xFB8C00   # オレンジ (テーマ熱)
COLOR_NONE = 0x757575


def _post(payload: dict, tag: str = "") -> None:
    if not WEBHOOK:
        print(f"[notifier_theme{tag}] DISCORD_WEBHOOK_URL_THEME 未設定 → スキップ")
        return
    try:
        r = requests.post(WEBHOOK, json=payload, timeout=10, verify=_VERIFY_SSL)
        if r.status_code not in (200, 204):
            print(f"[notifier_theme{tag}] HTTP {r.status_code} {r.text[:200]}")
        else:
            print(f"[notifier_theme{tag}] 送信OK")
    except Exception as e:
        print(f"[notifier_theme{tag}] failed: {e}")


def _drivers_str(drivers: list[str]) -> str:
    return "/".join(drivers) if drivers else "国内発(米連動薄)"


def _ranking_overview(ranked: list[dict], top: int = 8) -> str:
    lines = []
    for i, r in enumerate(ranked[:top], 1):
        lines.append(
            f"`{i:2d}.` **{r['theme']}** heat`{r['heat']:.0f}` "
            f"5d`{r['avg_r5']:+.1f}%` 25MA上`{r['pct_above_ma25']*100:.0f}%`"
        )
    return "\n".join(lines) if lines else "_データなし_"


def _hot_theme_embed(r: dict) -> dict:
    early = r.get("early", [])
    lines = [
        f"**heat** `{r['heat']:.0f}`  ｜ 1d`{r['avg_r1']:+.1f}%` 5d`{r['avg_r5']:+.1f}%` 20d`{r['avg_r20']:+.1f}%`",
        f"**breadth** 25MA上`{r['pct_above_ma25']*100:.0f}%`  出来高ブレイク`{r['breakout']}/{r['n']}`",
        f"**米震源** {_drivers_str(r['us_drivers'])}",
        "",
    ]
    if early:
        lines.append("__出遅れ初動候補(出来高点火・乖離小)__")
        for m in early:
            rsi = m["rsi"] if m["rsi"] is not None else "-"
            lines.append(
                f"・**[{m['ticker']}] {m['name']}** "
                f"出来高`{m['vr']:.1f}倍` 25MA乖離`{m['dev']:+.1f}%` "
                f"RSI`{rsi}` 5d`{m['r5']*100:+.1f}%`\n　〔{m['role']}〕"
            )
    else:
        lines.append("_点火中だが個別は伸びきり or 出来高待ち → 押し目待ち_")

    return {
        "title": f"🔥 {r['theme']}",
        "description": "\n".join(lines),
        "color": COLOR_HOT,
    }


def send_theme_signals(ranked: list[dict], hot: list[dict]) -> None:
    today = date.today().strftime("%Y-%m-%d (%a)")

    if not ranked:
        _post({"embeds": [{
            "title": f"🔥 テーマトラッカー — {today}",
            "description": "データ取得失敗 or 対象テーマなし。",
            "color": COLOR_NONE,
        }]}, tag="-empty")
        return

    embeds = [{
        "title": f"🔥 今ホットなテーマ — {today}",
        "description": _ranking_overview(ranked),
        "color": COLOR_HOT,
    }]
    for r in hot:
        embeds.append(_hot_theme_embed(r))

    for chunk_start in range(0, len(embeds), 10):
        _post({"embeds": embeds[chunk_start:chunk_start + 10]}, tag=f"-{chunk_start}")


def send_error(msg: str) -> None:
    _post({"embeds": [{
        "title": "⚠️ テーマトラッカー エラー",
        "description": msg[:1900],
        "color": 0xFDD835,
    }]}, tag="-err")

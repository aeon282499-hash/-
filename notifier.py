"""
notifier.py — Discord Webhook 通知モジュール（Phase 2: tier対応）

各 send_* 関数は `tier` 引数を受け取る。
- tier=None もしくは tier['key']=='main' → 大資金（DISCORD_WEBHOOK_URL/SELL_URL）+公開ミラー
- tier={'buy_webhook': ..., 'sell_webhook': ..., 'public_mirror': False, ...} → サブ口座
"""

import os
import json
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

VOL_MULT_THRESHOLD = 2.0
MAX_HOLD = 3   # 最大保有営業日数（tracker.py と一致）

# 公開（note メンバー向け）Discord Webhook URLs
PUBLIC_BUY     = os.getenv("DISCORD_WEBHOOK_URL_PUBLIC", "").strip()
PUBLIC_SELL    = os.getenv("DISCORD_WEBHOOK_SELL_URL_PUBLIC", "").strip()
PUBLIC_CLOSE   = os.getenv("DISCORD_WEBHOOK_CLOSE_URL_PUBLIC", "").strip()
PUBLIC_MONTHLY = os.getenv("DISCORD_WEBHOOK_MONTHLY_URL_PUBLIC", "").strip()


# ── 階層解決ヘルパー ────────────────────────────────────────
def _default_tier() -> dict:
    """tier未指定時の既定（大資金）。"""
    return {
        "key":           "main",
        "label":         "大資金",
        "emoji":         "",
        "size":          1_000_000,
        "buy_webhook":   os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":  os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
        # 2026-05-21: DISCORD_WEBHOOK_URL_PUBLIC が大資金チャンネルを指していて
        # 二重投稿状態になっていたため一時無効化。note専用チャンネルWebhook用意後にTrueへ戻す。
        "public_mirror": False,
    }


def _tier(tier: dict | None) -> dict:
    return tier if tier is not None else _default_tier()


def _is_main_tier(tier: dict) -> bool:
    return tier.get("key") == "main"


# セッション内 dedup — 同じURLに同じpayloadを2回POSTするのを防ぐ
# (webhook URL重複登録/同チャンネル多重登録などの設定ミス耐性)
import hashlib as _hashlib
_SENT_KEYS: set[tuple[str, str]] = set()


def _post(url: str, payload: dict, log_tag: str) -> None:
    if not url:
        print(f"[notifier-{log_tag}] webhook未設定 → スキップ")
        return
    payload_hash = _hashlib.md5(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    key = (url, payload_hash)
    if key in _SENT_KEYS:
        print(f"[notifier-{log_tag}] 同URL同payload 重複検知 → スキップ")
        return
    _SENT_KEYS.add(key)
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier-{log_tag}] HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"[notifier-{log_tag}] failed: {e}")


def _dispatch(payload: dict, *, tier: dict, side: str, public_url: str = "") -> None:
    """送信本体。tierのwebhookに送り、public_mirror=Trueなら公開チャンネルにもミラー。"""
    url     = tier["buy_webhook"] if side == "BUY" else tier["sell_webhook"]
    log_tag = f"{tier['label']}-{side}"
    _post(url, payload, log_tag)
    if tier.get("public_mirror") and public_url:
        _post(public_url, payload, f"public-{side}")


# ── ユーティリティ ──────────────────────────────────────────
def _affordable(price: float, size_yen: int) -> bool:
    if not price or price <= 0:
        return False
    return price * 100 <= size_yen


def _macro_description(macro: dict) -> str:
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
    cur = d
    count = 0
    while count < n:
        cur += timedelta(days=1)
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            count += 1
    return cur


def _calc_today_hold_day(entry_date_str: str, today: date) -> int:
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


def _tier_title_prefix(tier: dict) -> str:
    """大資金以外のtierにはラベルプレフィックスを付ける。"""
    if _is_main_tier(tier):
        return ""
    return f"{tier.get('emoji', '')}【{tier['label']}】"


def _tier_header_line(tier: dict) -> str:
    """サブ口座のembed冒頭に「💼 中資金口座（1件50万円枠）」を入れる。"""
    if _is_main_tier(tier):
        return ""
    return f"💼 **{tier['label']}口座（1件{tier['size']//10000}万円枠）**"


# ── 朝のBUYシグナル ────────────────────────────────────────
def _build_buy_embed(buy_signals: list[dict], tier: dict, *, date_str: str,
                     time_str: str, exit_date_str: str) -> dict:
    size_yen = tier["size"]
    size_man = size_yen // 10000
    sep = "─" * 24

    header = _tier_header_line(tier)
    lines = []
    if header:
        lines.append(header)
        lines.append("")
    lines += [
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

    title = f"{_tier_title_prefix(tier)}📊【スイング】{date_str} — 買い{len(buy_signals)}銘柄"
    return {
        "title":       title,
        "description": "\n".join(lines).rstrip(),
        "color":       COLOR_BUY,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_signals(signals: list[dict], today: date, macro: dict | None = None,
                 entry_date=None, *, tier: dict | None = None) -> None:
    tier = _tier(tier)
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    macro = macro or {}

    if entry_date is None:
        entry_date = today
    exit_date     = _nth_trading_day(entry_date, 2)
    exit_date_str = exit_date.strftime("%m/%d")

    buy_signals = [s for s in signals if s.get("direction") == "BUY"]

    if not buy_signals:
        _send_no_signal(tier, date_str, time_str, macro)
        return

    embed = _build_buy_embed(buy_signals, tier, date_str=date_str,
                             time_str=time_str, exit_date_str=exit_date_str)
    _dispatch({"embeds": [embed]}, tier=tier, side="BUY", public_url=PUBLIC_BUY)
    print(f"[notifier-{tier['label']}] BUY {len(buy_signals)}件 配信")


def _send_no_signal(tier: dict, date_str: str, time_str: str, macro: dict) -> None:
    if _is_main_tier(tier):
        desc = (
            "本日は極限まで吟味した結果、確実に勝てる優位性を持つ銘柄が存在しません。\n"
            "大切な資金の防衛を優先し、本日のトレードは **0銘柄（見送り）** とします。\n\n"
            f"**【本日の相場環境】**\n{_macro_description(macro)}"
        )
    else:
        desc = f"💼 1件{tier['size']//10000}万円枠 — 本日は買いシグナル0件です。"

    title = f"{_tier_title_prefix(tier)}📊【スイング】{date_str} — シグナルなし"
    payload = {"embeds": [{
        "title":       title,
        "description": desc,
        "color":       COLOR_NONE,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }]}
    _dispatch(payload, tier=tier, side="BUY", public_url=PUBLIC_BUY)
    print(f"[notifier-{tier['label']}] シグナルなし配信")


# ── 朝の決済結果＋保有中 ────────────────────────────────
def _build_results_embed(closed: list[dict], still_open: list[dict], today: date,
                         tier: dict) -> dict:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    header = _tier_header_line(tier)
    lines = []
    if header:
        lines.append(header)
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

    title = (
        f"{_tier_title_prefix(tier)}📋【スイング決済結果】{date_str}"
        if closed else
        f"{_tier_title_prefix(tier)}📋【スイング保有中】{date_str}"
    )
    color = COLOR_WIN if any((p.get("pnl_pct") or 0) > 0 for p in closed) else COLOR_ERROR
    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       color,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_results(closed: list[dict], still_open: list[dict], today: date,
                 *, tier: dict | None = None) -> None:
    if not closed and not still_open:
        return
    tier = _tier(tier)
    embed = _build_results_embed(closed, still_open, today, tier)
    _dispatch({"embeds": [embed]}, tier=tier, side="BUY", public_url=PUBLIC_BUY)
    print(f"[notifier-{tier['label']}] 結果レポート 決済{len(closed)}/保有中{len(still_open)}")


# ── 朝のSELLシグナル ────────────────────────────────────────
def _build_sell_embed(signals: list[dict], tier: dict, *, date_str: str,
                      time_str: str, exit_date_short: str) -> dict:
    size_yen = tier["size"]
    size_man = size_yen // 10000
    sep = "─" * 24

    header = _tier_header_line(tier)
    lines = []
    if header:
        lines.append(header)
        lines.append("")
    lines += [
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

    title = f"{_tier_title_prefix(tier)}📉【スイング空売り】{date_str} — 売り{len(signals)}銘柄"
    return {
        "title":       title,
        "description": "\n".join(lines).rstrip(),
        "color":       COLOR_SELL,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_sell_signals(signals: list[dict], today: date, entry_date=None,
                      *, tier: dict | None = None) -> None:
    tier = _tier(tier)
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    if entry_date is None:
        entry_date = today
    exit_date       = _nth_trading_day(entry_date, 2)
    exit_date_short = exit_date.strftime("%m/%d")

    if not signals:
        if _is_main_tier(tier):
            desc = "本日の空売りシグナルは0件です。"
        else:
            desc = f"💼 1件{tier['size']//10000}万円枠 — 本日は空売りシグナル0件です。"
        payload = {"embeds": [{
            "title":       f"{_tier_title_prefix(tier)}📉【スイング空売り】{date_str} — シグナルなし",
            "description": desc,
            "color":       COLOR_NONE,
            "footer":      {"text": f"配信時刻: {time_str}"},
        }]}
        _dispatch(payload, tier=tier, side="SELL", public_url=PUBLIC_SELL)
        print(f"[notifier-{tier['label']}] SELL シグナルなし配信")
        return

    embed = _build_sell_embed(signals, tier, date_str=date_str,
                              time_str=time_str, exit_date_short=exit_date_short)
    _dispatch({"embeds": [embed]}, tier=tier, side="SELL", public_url=PUBLIC_SELL)
    print(f"[notifier-{tier['label']}] SELL {len(signals)}件 配信")


# ── SELL決済結果 ────────────────────────────────────────────
def _build_sell_results_embed(closed: list[dict], still_open: list[dict], today: date,
                              tier: dict) -> dict:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    header = _tier_header_line(tier)
    lines = []
    if header:
        lines.append(header)
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

    title = f"{_tier_title_prefix(tier)}📋【スイング空売り結果】{date_str}"
    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       0x43A047 if any((p.get("pnl_pct") or 0) > 0 for p in closed) else 0xFDD835,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_sell_results(closed: list[dict], still_open: list[dict], today: date,
                      *, tier: dict | None = None) -> None:
    if not closed and not still_open:
        return
    tier = _tier(tier)
    embed = _build_sell_results_embed(closed, still_open, today, tier)
    _dispatch({"embeds": [embed]}, tier=tier, side="SELL", public_url=PUBLIC_SELL)
    print(f"[notifier-{tier['label']}] SELL結果 決済{len(closed)}/保有中{len(still_open)}")


# ── 月別・年間損益 ─────────────────────────────────────────
def _build_monthly_embed(closed: list[dict], today: date, tier: dict, *, sell: bool) -> dict | None:
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

    size_yen = tier["size"]
    capital  = size_yen * 5
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

    kind  = "空売り" if sell else "スイング"
    title = f"{_tier_title_prefix(tier)}{'📉' if sell else '📈'} {current_year}年 月別・年間損益（{kind}）"
    if sell:
        color = 0x43A047 if annual_pct >= 0 else 0xFDD835
    else:
        color = COLOR_WIN if annual_pct >= 0 else COLOR_ERROR

    return {
        "title":       title,
        "description": desc,
        "color":       color,
        "footer":      {"text": f"※資金{capital//10000}万・1トレード{size_yen//10000}万・MAX5並列基準"},
    }


def send_monthly_report(positions: list[dict], today: date,
                        *, tier: dict | None = None) -> None:
    tier = _tier(tier)
    closed = [p for p in positions
              if p.get("status") == "closed" and p.get("pnl_pct") is not None]
    if not closed:
        return
    embed = _build_monthly_embed(closed, today, tier, sell=False)
    if embed:
        _dispatch({"embeds": [embed]}, tier=tier, side="BUY", public_url=PUBLIC_MONTHLY)
        print(f"[notifier-{tier['label']}] 月別・年間損益送信")


def send_sell_monthly_report(positions: list[dict], today: date,
                             *, tier: dict | None = None) -> None:
    tier = _tier(tier)
    closed = [p for p in positions
              if p.get("status") == "closed" and p.get("pnl_pct") is not None]
    if not closed:
        return
    embed = _build_monthly_embed(closed, today, tier, sell=True)
    if embed:
        _dispatch({"embeds": [embed]}, tier=tier, side="SELL", public_url=PUBLIC_MONTHLY)
        print(f"[notifier-{tier['label']}] SELL月別・年間損益送信")


# ── 15:00 大引け処分指示 ──────────────────────────────────
def _build_close_embed(targets: list[dict], today: date, tier: dict, *, sell: bool) -> dict:
    date_str = today.strftime("%m/%d")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    sep = "─" * 22

    if sell:
        header_action = "🛒 **15:25-15:30 クロージング**で成行買戻し（SBI証券・信用）"
        title_kind = "空売り大引け処分指示"
    else:
        header_action = "🛒 **15:25-15:30 クロージング**で成行売り（SBI証券）"
        title_kind = "大引け処分指示"

    tier_header = _tier_header_line(tier)
    lines = []
    if tier_header:
        lines.append(tier_header)
    lines += [header_action, f"対象: **{len(targets)}銘柄**", sep]

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

    title = f"{_tier_title_prefix(tier)}⚡【{title_kind}】{date_str}"
    return {
        "title":       title,
        "description": "\n".join(lines),
        "color":       color,
        "footer":      {"text": f"配信時刻: {time_str}"},
    }


def send_close_signals(targets: list[dict], today: date,
                       *, tier: dict | None = None) -> None:
    if not targets:
        return
    tier = _tier(tier)
    embed = _build_close_embed(targets, today, tier, sell=False)
    _dispatch({"embeds": [embed]}, tier=tier, side="BUY", public_url=PUBLIC_CLOSE)
    print(f"[notifier-{tier['label']}] 大引け処分 {len(targets)}件")


def send_close_signals_sell(targets: list[dict], today: date,
                            *, tier: dict | None = None) -> None:
    if not targets:
        return
    tier = _tier(tier)
    embed = _build_close_embed(targets, today, tier, sell=True)
    _dispatch({"embeds": [embed]}, tier=tier, side="SELL", public_url=PUBLIC_CLOSE)
    print(f"[notifier-{tier['label']}] SELL大引け処分 {len(targets)}件")


# ── エラー通知（tier非対応・常にメイン宛て）────────────────────
def send_error(error_message: str, today: date) -> None:
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    payload = {"embeds": [{
        "title":       f"⚠️ {date_str} — シグナル配信エラー",
        "description": f"```\n{error_message[:1800]}\n```",
        "color":       COLOR_ERROR,
        "footer":      {"text": time_str},
    }]}
    main_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    _post(main_url, payload, "error")

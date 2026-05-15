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

# 中資金（2026-05-15開始）: 50万 × 5並列 = 250万 / 同じシグナルを別Discord通知ミラー
MID_SIZE_PER_TRADE = 500_000


# 公開（note メンバー向け）Discord Webhook URLs
# 副業運用：株AI Discord サーバーの各チャンネルに並行送信する
PUBLIC_BUY     = os.getenv("DISCORD_WEBHOOK_URL_PUBLIC", "").strip()
PUBLIC_SELL    = os.getenv("DISCORD_WEBHOOK_SELL_URL_PUBLIC", "").strip()
PUBLIC_CLOSE   = os.getenv("DISCORD_WEBHOOK_CLOSE_URL_PUBLIC", "").strip()
PUBLIC_MONTHLY = os.getenv("DISCORD_WEBHOOK_MONTHLY_URL_PUBLIC", "").strip()

# 中資金（個人運用）: 既存BUY/結果/大引け処分通知をすべて同じ内容でミラー
BUY_MID = os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip()

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


def _mirror_to_mid(payload: dict) -> None:
    """中資金口座用Discordへミラー。embedに『中資金 50万円/件』を併記する。"""
    if not BUY_MID:
        return
    import copy
    mid_payload = copy.deepcopy(payload)
    for embed in mid_payload.get("embeds", []):
        title = embed.get("title", "")
        if title and "中資金" not in title:
            embed["title"] = f"🔵【中資金】{title}"
        desc = embed.get("description", "") or ""
        mid_header = f"💼 **中資金口座用：1件 {MID_SIZE_PER_TRADE//10000}万円 × 5並列（資金{MID_SIZE_PER_TRADE*5//10000}万）**\n"
        embed["description"] = mid_header + desc
    try:
        resp = requests.post(BUY_MID, json=mid_payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[notifier-mid] mirror HTTP {resp.status_code}")
    except Exception as e:
        print(f"[notifier-mid] mirror failed: {e}")


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
    # 中資金口座ミラー（BUY系の通知に「中資金 50万円/件」併記版を送信）
    _mirror_to_mid(payload)


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


def send_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """前日シグナルの損益結果を Discord に送信する。"""
    if not closed and not still_open:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    lines = []

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
            upnl    = p.get("unrealized_pnl", 0) or 0
            today_hold = _calc_today_hold_day(p.get("entry_date", ""), today)
            emoji   = "📈" if upnl >= 0 else "📉"
            dir_str = "買い" if p["direction"] == "BUY" else "売り"

            # 処分期限日: entry日含めて MAX_HOLD 営業日目（= entry + (MAX_HOLD-1) 営業日後）
            try:
                entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
                deadline = _nth_trading_day(entry_dt, MAX_HOLD - 1)
                deadline_str = deadline.strftime("%m月%d日")
                remaining = MAX_HOLD - today_hold  # 今日を除く残り営業日
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

    # 合計損益
    if closed:
        avg_pnl = sum(p.get("pnl_pct", 0) or 0 for p in closed) / len(closed)
        wins    = sum(1 for p in closed if (p.get("pnl_pct") or 0) > 0)
        lines.append(f"\n合計: {len(closed)}件決済 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    title = f"📋【スイング決済結果】{date_str}" if closed else f"📋【スイング保有中】{date_str}"
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
    """空売りシグナルを SELL専用Webhook + 公開SELL Discordに送信する。"""
    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    if entry_date is None:
        entry_date = today
    exit_date     = _nth_trading_day(entry_date, 2)
    exit_date_str = exit_date.strftime("%m月%d日")

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
    _post_sell(payload)
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
            "footer":      {"text": "※資金500万・1トレード100万・MAX5並列基準"},
        }]
    }, mirror=PUBLIC_MONTHLY)
    print(f"[notifier] 月別・年間損益レポートを送信しました")


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


def send_sell_results(closed: list[dict], still_open: list[dict], today: date) -> None:
    """空売りポジションの損益結果をSELL専用Webhookに送信する。"""
    if not closed and not still_open:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    lines = []

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
            upnl = p.get("unrealized_pnl", 0) or 0
            today_hold = _calc_today_hold_day(p.get("entry_date", ""), today)
            emoji = "📈" if upnl >= 0 else "📉"
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
            "footer":      {"text": "※資金500万・1トレード100万・MAX5並列基準"},
        }]
    }, mirror=PUBLIC_MONTHLY)
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
    _post(payload, mirror=PUBLIC_CLOSE)
    print(f"[notifier] 大引け処分指示を Discord に送信しました（{len(targets)}件）")


def send_close_signals_sell(targets: list[dict], today: date) -> None:
    """SELL（空売り）ポジションの大引け処分指示を SELL専用Webhook + 公開CLOSE Discordに送信する。

    targets: close_check.py が生成した sell_targets（direction="SELL"）
    """
    if not targets:
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
    _post_sell(payload, mirror=PUBLIC_CLOSE)
    print(f"[notifier] SELL大引け処分指示を Discord に送信しました（{len(targets)}件）")

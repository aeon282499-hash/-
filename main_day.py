"""
main_day.py — デイトレシグナル配信エントリーポイント
======================================================
毎営業日 8:05 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 今日が営業日か判定
  2. 前日のデイトレシグナル結果を確認 → Discord に送信
  3. screener_day.run_screener_day() で新規シグナル選定
  4. Discord にシグナル送信
  5. シグナルを day_signals.json に保存
"""

import sys
import json
import os
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
DAY_SIGNALS_FILE = "day_signals.json"


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def load_day_signals() -> list[dict]:
    if os.path.exists(DAY_SIGNALS_FILE):
        with open(DAY_SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_day_signals(signals_data: list[dict]) -> None:
    with open(DAY_SIGNALS_FILE, "w", encoding="utf-8") as f:
        json.dump(signals_data, f, ensure_ascii=False, indent=2)


def check_yesterday_results(yesterday_signals: list[dict], today: date) -> list[dict]:
    """前日のシグナル結果を計算（v2: 寄り→引け・MAX指値の執行可否判定）。"""
    if not yesterday_signals:
        return []

    from screener import batch_download_jquants, _jquants_id_token
    tickers = [s["ticker"] for s in yesterday_signals]
    end_str   = today.strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")

    token    = _jquants_id_token()
    all_data = batch_download_jquants(token, start=start_str, end=end_str, tickers=tickers)

    results = []
    for sig in yesterday_signals:
        ticker          = sig["ticker"]
        entry_date      = sig.get("entry_date")
        max_entry_price = sig.get("max_entry_price")
        df = all_data.get(ticker)
        if df is None or df.empty or not entry_date:
            continue

        entry_rows = df[df.index.strftime("%Y-%m-%d") == entry_date]
        if entry_rows.empty:
            continue

        entry_open  = float(entry_rows["Open"].iloc[0])
        entry_close = float(entry_rows["Close"].iloc[0])
        direction   = sig.get("direction", "BUY")

        if direction == "BUY":
            # MAX指値判定: 寄り > MAX → 見送り
            if max_entry_price is not None and entry_open > max_entry_price:
                results.append({
                    **sig,
                    "entry_open":  entry_open,
                    "entry_close": entry_close,
                    "pnl_pct":     0.0,
                    "exit_type":   "SKIP",
                    "win":         False,
                })
                continue
            pnl_pct = (entry_close - entry_open) / entry_open * 100
        else:
            # SELL: MIN指値判定 寄り < MIN → 見送り（ギャップダウン済）
            min_entry_price = sig.get("min_entry_price")
            if min_entry_price is not None and entry_open < min_entry_price:
                results.append({
                    **sig,
                    "entry_open":  entry_open,
                    "entry_close": entry_close,
                    "pnl_pct":     0.0,
                    "exit_type":   "SKIP",
                    "win":         False,
                })
                continue
            # SELL: (寄値 - 引値) / 寄値 が利益
            pnl_pct = (entry_open - entry_close) / entry_open * 100

        results.append({
            **sig,
            "entry_open":  entry_open,
            "entry_close": entry_close,
            "pnl_pct":     round(pnl_pct, 3),
            "exit_type":   "CLOSE",
            "win":         pnl_pct > 0,
        })

    return results


def send_day_results(results: list[dict], today: date) -> None:
    """デイトレ結果を Discord に送信（v2: 寄り→引け、SKIP含む）。"""
    if not results:
        return

    import os, requests
    url = (os.getenv("DISCORD_WEBHOOK_DAY_URL") or os.getenv("DISCORD_WEBHOOK_URL_DAY") or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
    if not url:
        return

    date_str = today.strftime("%Y年%m月%d日")
    lines    = ["**── 前日デイトレ結果（v2）──**"]

    for r in results:
        pnl   = r["pnl_pct"]
        etype = r["exit_type"]
        direction = r.get("direction", "BUY")
        dir_emoji = "🟢買" if direction == "BUY" else "🔴売"
        if etype == "SKIP":
            entry_open = r.get("entry_open", 0)
            if direction == "BUY":
                limit_p = r.get("max_entry_price", 0)
                lines.append(
                    f"⏭️{dir_emoji} **{r['name']}**（{r['ticker']}）見送り "
                    f"寄{entry_open:,.0f} > MAX指値{limit_p:,.0f}"
                )
            else:
                limit_p = r.get("min_entry_price", 0)
                lines.append(
                    f"⏭️{dir_emoji} **{r['name']}**（{r['ticker']}）見送り "
                    f"寄{entry_open:,.0f} < MIN指値{limit_p:,.0f}（ギャップダウン）"
                )
        else:
            emoji = "✅" if pnl > 0 else "❌"
            entry_open  = r.get("entry_open", 0)
            entry_close = r.get("entry_close", 0)
            lines.append(
                f"{emoji}{dir_emoji} **{r['name']}**（{r['ticker']}）"
                f"寄{entry_open:,.0f}→引{entry_close:,.0f} → **{pnl:+.2f}%**"
            )

    executed = [r for r in results if r["exit_type"] != "SKIP"]
    skipped  = [r for r in results if r["exit_type"] == "SKIP"]
    if executed:
        wins    = sum(1 for r in executed if r["win"])
        avg_pnl = sum(r["pnl_pct"] for r in executed) / len(executed)
        lines.append(f"\n執行: {len(executed)}件 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}% / 見送り{len(skipped)}件")
        color = 0x43A047 if avg_pnl > 0 else 0xFDD835
    else:
        lines.append(f"\n全件見送り: {len(skipped)}件（全銘柄MAX指値超過）")
        color = 0x757575

    payload = {
        "embeds": [{
            "title":       f"📋【デイトレv2結果】{date_str}",
            "description": "\n".join(lines),
            "color":       color,
        }]
    }
    requests.post(url, json=payload, timeout=10)
    print(f"[main_day] デイトレ結果を Discord に送信しました（{len(results)}件）")


def send_day_signals(signals: list[dict], today: date, macro: dict) -> None:
    """デイトレシグナルを Discord に送信する。"""
    import os, requests
    from datetime import datetime as _dt
    import zoneinfo as _zi

    # 診断: どのenv varが見つかったか出力（URL本体は出さない）
    found_var = None
    for name in ("DISCORD_WEBHOOK_DAY_URL", "DISCORD_WEBHOOK_URL_DAY", "DISCORD_WEBHOOK_URL"):
        v = os.getenv(name, "").strip()
        if v:
            found_var = name
            break
    print(f"[diag] webhook env: found={found_var} url_len={len(os.getenv(found_var, '')) if found_var else 0}")

    url = (os.getenv("DISCORD_WEBHOOK_DAY_URL") or os.getenv("DISCORD_WEBHOOK_URL_DAY") or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
    if not url:
        print("[diag] webhook URL未設定 → 通知スキップ")
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = _dt.now(_zi.ZoneInfo("Asia/Tokyo")).strftime("%H:%M JST")

    if not signals:
        payload = {
            "embeds": [{
                "title":       f"⚡【デイトレ】{date_str} — シグナルなし",
                "description": "本日は条件を満たす銘柄がありません。",
                "color":       0x757575,
            }]
        }
        resp = requests.post(url, json=payload, timeout=10)
        print(f"[main_day] シグナルなし通知を送信: HTTP {resp.status_code} body={resp.text[:200]}")
        return

    embeds = []
    for i, sig in enumerate(signals, 1):
        direction  = sig.get("direction", "BUY")
        prev_close = sig.get("prev_close", 0)
        reason_text = "\n".join(f"・{r}" for r in sig["reason"])

        if direction == "BUY":
            high_20         = sig.get("high_20", 0)
            max_entry_price = sig.get("max_entry_price", 0)
            action_str = (
                f"🟢 **MAX指値 ¥{max_entry_price:,.0f} で寄成買い**\n"
                f"（寄り≦指値なら執行・超えたら見送り）\n"
                f"→ 同日 **15:00 引成 返済売り** で決済"
            )
            if max_entry_price > 0:
                shares     = max(100, int(4_000_000 / max_entry_price / 100) * 100)
                invest_amt = shares * max_entry_price
                invest_str = f"**{shares:,}株** × ¥{max_entry_price:,.0f} = 約{invest_amt/1e4:.0f}万円"
            else:
                invest_str = "**400万円目安**"
            embeds.append({
                "title": f"🚀【デイトレv2 BUY】#{i}  {sig['name']}（{sig['ticker']}）",
                "color": 0xE53935,
                "fields": [
                    {"name": "📌 アクション",     "value": action_str, "inline": False},
                    {"name": "💰 MAX指値",        "value": f"**¥{max_entry_price:,.0f}**（20日高値+{int((max_entry_price/high_20-1)*100)}%）" if high_20 else f"¥{max_entry_price:,.0f}", "inline": True},
                    {"name": "🎯 20日高値",       "value": f"¥{high_20:,.0f}", "inline": True},
                    {"name": "📈 前日終値",       "value": f"¥{prev_close:,.0f}", "inline": True},
                    {"name": "💴 推奨株数",       "value": invest_str, "inline": False},
                    {"name": "⚠️ 当日決済必須",  "value": "**15:00 引成 返済売り**", "inline": False},
                    {"name": "📊 根拠",           "value": reason_text, "inline": False},
                ],
                "footer": {"text": f"配信時刻: {time_str}"},
            })
        else:
            # SELL（信用売り）
            min_entry_price = sig.get("min_entry_price", 0)
            daily_gain      = sig.get("daily_gain", 0)
            action_str = (
                f"🔴 **MIN指値 ¥{min_entry_price:,.0f} で寄成 信用売り**\n"
                f"（寄り≧指値なら執行・下回ったら見送り）\n"
                f"→ 同日 **15:00 引成 返済買い** で決済\n"
                f"※ 信用売り規制チェック必須"
            )
            if min_entry_price > 0:
                shares     = max(100, int(4_000_000 / min_entry_price / 100) * 100)
                invest_amt = shares * min_entry_price
                invest_str = f"**{shares:,}株** × ¥{min_entry_price:,.0f} = 約{invest_amt/1e4:.0f}万円"
            else:
                invest_str = "**400万円目安**"
            embeds.append({
                "title": f"🚀【デイトレv2 SELL】#{i}  {sig['name']}（{sig['ticker']}）",
                "color": 0x1E88E5,
                "fields": [
                    {"name": "📌 アクション",     "value": action_str, "inline": False},
                    {"name": "💰 MIN指値",        "value": f"**¥{min_entry_price:,.0f}**（前日終値）", "inline": True},
                    {"name": "🔥 前日急騰",       "value": f"+{daily_gain:.1f}%", "inline": True},
                    {"name": "📈 前日終値",       "value": f"¥{prev_close:,.0f}", "inline": True},
                    {"name": "💴 推奨株数",       "value": invest_str, "inline": False},
                    {"name": "⚠️ 当日決済必須",  "value": "**15:00 引成 返済買い**（連続S高リスクあり）", "inline": False},
                    {"name": "📊 根拠",           "value": reason_text, "inline": False},
                ],
                "footer": {"text": f"配信時刻: {time_str}"},
            })

    n_buy  = sum(1 for s in signals if s.get("direction", "BUY") == "BUY")
    n_sell = sum(1 for s in signals if s.get("direction") == "SELL")
    payload = {
        "content": (
            f"## 🚀【デイトレv2】シグナル｜{date_str}\n"
            f"> 本日: **{len(signals)}銘柄**（🟢買い {n_buy} / 🔴売り {n_sell}）"
        ),
        "embeds": embeds[:10],
    }
    requests.post(url, json=payload, timeout=10)
    print(f"[main_day] {len(signals)} 件のデイトレシグナルを Discord に送信しました")


def main() -> None:
    today = datetime.now(JST).date()
    print(f"[main_day] 実行日: {today}")

    if not is_trading_day(today):
        print("[main_day] 休場日 → スキップ")
        sys.exit(0)

    from screener_day import run_screener_day
    from screener_sell_day import run_screener_sell_day

    try:
        # ── ① 前日シグナルの結果チェック ──────────────────
        all_saved   = load_day_signals()
        yesterday   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_signals = [s for s in all_saved if s.get("signal_date") == yesterday]
        print(f"[main_day] 前日シグナル: {len(prev_signals)}件")

        if prev_signals:
            results = check_yesterday_results(prev_signals, today)
            send_day_results(results, today)

        # ── ② 新規スクリーニング（BUY + SELL）─────────────
        buy_signals, macro  = run_screener_day()
        sell_signals, _     = run_screener_sell_day()
        signals = buy_signals + sell_signals
        print(f"[main_day] BUY {len(buy_signals)}件 / SELL {len(sell_signals)}件")

        # ── ③ シグナルを保存 ──────────────────────────────
        from datetime import timedelta as _td
        entry_date = today + _td(days=1)
        # 翌営業日を計算
        while not is_trading_day(entry_date):
            entry_date += _td(days=1)

        new_records = [
            {
                "signal_date": today.strftime("%Y-%m-%d"),
                "entry_date":  entry_date.strftime("%Y-%m-%d"),
                **{k: v for k, v in s.items() if k != "reason"},
            }
            for s in signals
        ]
        # 直近30日分だけ保持
        cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        kept   = [s for s in all_saved if s.get("signal_date", "") >= cutoff]
        save_day_signals(kept + new_records)

        # ── ④ Discord にシグナル送信 ─────────────────────
        send_day_signals(signals, today, macro)

        # ── ⑤ Twitter に投稿 ────────────────────────────────
        from twitter_notifier import post_day_signals
        post_day_signals(signals, today)

        print("[main_day] 正常終了")

    except Exception as e:
        import traceback, os, requests as req
        err_msg = traceback.format_exc()
        print(f"[main_day] エラー:\n{err_msg}", file=sys.stderr)
        url = (os.getenv("DISCORD_WEBHOOK_DAY_URL") or os.getenv("DISCORD_WEBHOOK_URL_DAY") or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
        if url:
            req.post(url, json={"content": f"[デイトレ] エラー発生:\n```{err_msg[:1500]}```"}, timeout=10)
        sys.exit(1)


if __name__ == "__main__":
    main()

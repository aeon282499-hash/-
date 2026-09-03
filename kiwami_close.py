# -*- coding: utf-8 -*-
"""kiwami_close.py — 「売買シグナル極み」の15時 大引けチェック（2026-07-26）。

■ なぜ要るか
  極みは損切りが銘柄ごと（ATR%×2.0・下限2.0%）なので、通常版の15時チェック（一律-3%）
  では判定がズレる。極みを実弾で回すなら極み専用の出口管理が要る、という本人判断で新設。

■ 何をするか
  極みの帳簿（shadow_exit_main.json）のうち保有中の玉について、
    ①ザラ場OCO約定（銘柄ごとの損切り / 利確+5%）→ 決済済みとして報告
    ②RSI50以上に回復 → 大引けで処分
    ③保有3日目 → 大引けで処分（期限）
  を判定して極みの買いチャンネルへ通知する。

■ 非破壊の担保
  ・通常版の positions*.json / close_check の配信には一切触れない（読むだけ）。
  ・判定は close_check.collect_targets をそのまま呼ぶ＝寄指の約定確認・OCO優先・
    yfinance誤差の緩衝など、通常版で5つの穴を塞いだ実績のあるロジックを共有する。
    唯一の違いは各玉の stop_pct を渡すこと（通常版のポジションには無い＝既定3.0%）。
  ・帳簿の更新（決済の記帳）は行わない。記帳は従来どおり翌朝 shadow_exit.update_ledger の
    単一経路のまま＝二重記帳が構造的に起きない。

実行: python -X utf8 kiwami_close.py [--force] [--dry]
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
LEDGER = "shadow_exit_main.json"
# 2026-07-29: 極みの売りは踏み上げ損切りが+2.5%（通常版は+3.0%）になったので、
# 通常版の positions_sell.json では判定がズレる。専用台帳へ切り替えた。
# 旧台帳が残っていて新台帳が空のうちは、通常版を読んで従来どおり通知する（移行期の取りこぼし防止）。
SELL_LEDGER = "kiwami_sell.json"
SELL_LEDGER_FALLBACK = "positions_sell.json"
MARKER = "kiwami_close_last_run.json"
SIZE = 1_000_000
WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_URL"
SELL_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_SELL_URL"
_COLOR_ACT, _COLOR_OK, _COLOR_DONE = 0xE67E22, 0x95A5A6, 0x2ECC71


TIER_LEDGERS = {"main": ("大資金", "shadow_exit_main.json",  "DISCORD_WEBHOOK_SHADOW_URL"),
                "mid":   ("中資金", "shadow_exit_mid.json",   "DISCORD_WEBHOOK_SHADOW_MID_URL"),
                "small": ("小資金", "shadow_exit_small.json", "DISCORD_WEBHOOK_SHADOW_SMALL_URL")}


def load_open(key: str = "main") -> list[dict]:
    """極み帳簿の保有中（pending/open）だけを close_check が読める形に整えて返す。"""
    ledger = LEDGER if key == "main" else TIER_LEDGERS[key][1]   # mainは従来のLEDGER定数（テスト互換）
    if not os.path.exists(ledger):
        return []
    with open(ledger, encoding="utf-8") as f:
        rows = json.load(f)
    out = []
    for r in rows:
        if r.get("status") not in ("pending", "open"):
            continue
        out.append({
            "ticker": r["ticker"], "name": r.get("name", r["ticker"]),
            "direction": "BUY",
            "signal_date": r["signal_date"], "entry_date": r["entry_date"],
            "prev_close": r.get("prev_close"), "limit_price": r.get("limit_price"),
            "entry_open": r.get("entry_open"),
            "status": r["status"],
            "stop_pct": r.get("stop_pct") or 3.0,     # ← 極みの銘柄別損切り
            "atr_pct": r.get("atr_pct"),
        })
    return out


def load_open_sell() -> list[dict]:
    """極みの売り玉（保有中）を返す。読むだけ・書かない。

    2026-07-29: 極み専用台帳 kiwami_sell.json（踏み上げ損切り+2.5%・3枠）へ切替。
    移行期に新台帳がまだ無い/空の間だけ通常版 positions_sell.json を読む（取りこぼし防止）。
    その場合は通常版の玉なので stop_pct は既定3.0のまま＝従来と同じ判定になる。"""
    def _read(path: str) -> list[dict]:
        if not os.path.exists(path):
            return []
        try:
            with open(path, encoding="utf-8") as f:
                rows = json.load(f)
        except Exception as e:
            print(f"[kiwami_close] {path} 読込失敗: {e}")
            return []
        rows = rows if isinstance(rows, list) else rows.get("positions", [])
        return [r for r in rows
                if r.get("status") in ("pending", "open")
                and r.get("direction", "SELL") == "SELL"]

    # 2026-09-03監査: 極み売り台帳が空の日に通常版台帳(損切り3.0%)へフォールバックすると、極みでは
    # +2.5%で損切り済みの玉を「保有継続」と通知してしまう。移行期(2026-07-29)は終わったので撤去。
    return _read(SELL_LEDGER)


def _post(embeds: list[dict], env: str = WEBHOOK_ENV) -> bool:
    import requests

    url = os.getenv(env, "").strip()
    if not url:
        print(f"[kiwami_close] {env} 未設定 → 無送信")
        return False
    verify = os.getenv("DISCORD_VERIFY_SSL", "true").lower() != "false"
    for i, wait in enumerate((0, 2, 4)):
        if wait:
            import time
            time.sleep(wait)
        try:
            r = requests.post(url, json={"embeds": embeds}, timeout=10, verify=verify)
            if r.status_code in (200, 204):
                return True
            print(f"[kiwami_close] HTTP {r.status_code} {r.text[:150]}（試行{i + 1}）")
        except Exception as e:
            print(f"[kiwami_close] 送信失敗: {e}（試行{i + 1}）")
    return False


def build_embeds(targets: list[dict], checked: list[dict], today: date,
                 positions: list[dict], sell: bool = False) -> list[dict]:
    """通常版 notifier._build_close_embed / _build_close_no_targets_embed と同一の書体
    （2026-08-09改装・本人「わかりずらい・売買シグナルと一緒の書体にして」）。
    極み固有の情報は損切りが通常版と違う時だけ行末に付ける（買いは現在一律-3%＝表示不要、
    売りは+2.5%＝通常版+3.0%と違うので明示）。"""
    stop_of = {p["ticker"]: p.get("stop_pct") or 3.0 for p in positions}
    date_str = today.strftime("%m/%d")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    sep = "─" * 22
    embeds = []

    def _stop_note(tk: str) -> str:
        sp = stop_of.get(tk, 3.0)
        if abs(sp - 3.0) < 0.01:
            return ""                     # 通常版と同じ幅なら書かない
        return f"・損切り{'+' if sell else '-'}{sp:.1f}%"

    if targets:
        header_action = ("🛒 **15:25-15:30 クロージング**で成行買戻し（SBI証券・信用）" if sell
                         else "🛒 **15:25-15:30 クロージング**で成行売り（SBI証券）")
        lines = [header_action, f"対象: **{len(targets)}銘柄**", sep]
        for i, t in enumerate(targets, 1):
            ticker = t["ticker"].replace(".T", "")
            name = t.get("name", ticker)
            rtype = t.get("reason_type")
            hold = t.get("today_hold", "?")
            rsi = t.get("rsi_now")
            price = t.get("current_price")
            entry = t.get("entry_open")
            if rtype == "RSI":
                icon = "🔔"
                tag = f"RSI回復(RSI={rsi:.1f})" if rsi is not None else "RSI回復"
            elif rtype:
                icon = "⏰"
                tag = f"{hold}日目MAXHOLD"
            else:
                icon = "🔔"
                tag = t.get("reason") or t.get("exit_type") or "処分"
            line = f"{icon} **#{i} {name}** ({ticker}) — {tag}"
            if price is not None and entry:
                pnl_now = (entry - price) / entry * 100 if sell else (price - entry) / entry * 100
                line += f" / {pnl_now:+.2f}%"
            lines.append(line + _stop_note(t["ticker"]))
            if t.get("warn"):            # OCO水準に誤差幅内で到達（2026-09-03監査・口座確認を促す）
                lines.append(f"　{t['warn']}")
        title_kind = "空売り大引け処分指示" if sell else "大引け処分指示"
        embeds.append({
            "title": f"⚡【極み {title_kind}】{date_str}",
            "description": "\n".join(lines),
            "color": _COLOR_ACT,
            "footer": {"text": f"配信時刻: {time_str}"},
        })

    settled = [c for c in checked if c.get("note") and "OCO" in str(c.get("note"))]
    holds = [c for c in checked if c not in settled]
    if settled or holds:
        action = "買戻し" if sell else "処分"
        lines = [f"15:00判定: {action}条件未達 → **保有継続**（OCO注文はそのまま）", sep]
        warn = False
        for c in settled:
            ticker = c["ticker"].replace(".T", "")
            hold_str = f"{c['today_hold']}日目 — " if c.get("today_hold") else ""
            lines.append(f"✅ **{c.get('name', ticker)}** ({ticker}) {hold_str}{c['note']}")
        for c in holds:
            ticker = c["ticker"].replace(".T", "")
            name = c.get("name", ticker)
            hold = c.get("today_hold")
            if c.get("note"):
                warn = True
                hold_str = f"{hold}日目 — " if hold else ""
                lines.append(f"⚠️ **{name}** ({ticker}) {hold_str}{c['note']}{_stop_note(c['ticker'])}")
                continue
            rsi = c.get("rsi_now")
            price = c.get("current_price")
            rsi_str = (f"RSI {rsi:.1f}＞50（反転待ち）" if sell else f"RSI {rsi:.1f}＜50（回復待ち）") \
                if rsi is not None else "RSI —"
            rest = 3 - (hold or 0)
            rest_str = "明日が処分期限" if rest == 1 else f"期限まであと{rest}日"
            px_str = f"・現在 {price:,.0f}円" if price is not None else ""
            lines.append(f"📊 **{name}** ({ticker}) {hold}日目 — "
                         f"{rsi_str}{px_str}・{rest_str}{_stop_note(c['ticker'])}")
            if c.get("warn"):
                warn = True
                lines.append(f"　{c['warn']}")
        title_kind = "売り保有チェック" if sell else "大引けチェック"
        suffix = "" if targets else f" — {action}対象なし"
        embeds.append({
            "title": f"🔍【極み {title_kind}】{date_str}{suffix}",
            "description": "\n".join(lines),
            "color": 0xE74C3C if warn else (_COLOR_DONE if settled else _COLOR_OK),
            "footer": {"text": f"配信時刻: {time_str}"},
        })
    return embeds


def main() -> None:
    now = datetime.now(JST)
    today = now.date()
    force = "--force" in sys.argv
    dry = "--dry" in sys.argv
    print(f"[kiwami_close] 実行 {now:%Y-%m-%d %H:%M JST}")

    import close_check as CC

    if not force:
        if not CC.is_trading_day(today):
            print("[kiwami_close] 休場日 → スキップ")
            return
        if not (14 <= now.hour <= 17):
            print(f"[kiwami_close] 時間外スキップ（{now:%H:%M}）")
            return
        if os.path.exists(MARKER):
            try:
                if json.load(open(MARKER, encoding="utf-8")).get("date") == today.isoformat():
                    print("[kiwami_close] 本日分は送信済み → スキップ")
                    return
            except Exception as e:
                print(f"[kiwami_close] マーカー読込失敗: {e} → 続行")

    # 買いと売りは独立に判定する。片方が0件でももう片方は必ず処理する
    # （旧: 買いが0件だと return していて、売りだけ保有している日に処分指示が消えていた）
    positions = load_open("main")
    tier_pos = {k2: load_open(k2) for k2 in ("mid", "small")}   # 2026-08-28 中/小も判定
    sells = load_open_sell()
    if not positions and not sells and not any(tier_pos.values()):
        print("[kiwami_close] 極みの保有玉なし（買い0/売り0）→ 無送信")
        return
    print(f"[kiwami_close] 保有 買い{len(positions)}件 / 売り{len(sells)}件")
    if positions:
        print("  買い: " + ", ".join(f"{p['name']}(損切り-{p['stop_pct']:.1f}%)" for p in positions))
    if sells:
        print("  売り: " + ", ".join(p.get("name", p["ticker"]) for p in sells))

    # 通常版と同じ判定エンジンを共有（stop_pct だけが玉ごとに違う）
    from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS
    from datetime import timedelta
    start = (today - timedelta(days=RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
    data = batch_download_jquants(_jquants_id_token(), start=start,
                                  end=today.strftime("%Y-%m-%d"))

    sent = False
    # scope=close_decisions の記録キー。階層別にしないと大/中/小が同じ銘柄の判定を上書きし合う
    # （2026-09-03監査: 9/3に小資金の積水化学が kiwami:BUY:4204 として記録されていた）。売りは台帳1本。
    for label, pos, direction, env, is_sell, scope in (
            ("買い", positions, "BUY", WEBHOOK_ENV, False, "kiwami"),
            ("買い・中資金", tier_pos["mid"], "BUY", TIER_LEDGERS["mid"][2], False, "kiwami_mid"),
            ("買い・小資金", tier_pos["small"], "BUY", TIER_LEDGERS["small"][2], False, "kiwami_small"),
            ("売り", sells, "SELL", SELL_WEBHOOK_ENV, True, "kiwami"),
            ("売り・中資金", sells, "SELL", "DISCORD_WEBHOOK_SHADOW_SELL_MID_URL", True, "kiwami"),
            ("売り・小資金", sells, "SELL", "DISCORD_WEBHOOK_SHADOW_SELL_SMALL_URL", True, "kiwami")):
        if not pos:
            continue
        try:
            tg, ck = CC.collect_targets(pos, direction, today, data)
            print(f"[kiwami_close] {label}: 処分対象 {len(tg)}件 / 判定済み {len(ck)}件")
            if not dry:   # 14:55判定を記録＝翌朝の shadow_exit.advance がこれに従う（2026-08-28）
                import close_decisions
                close_decisions.record(today, scope, direction, tg, ck)
            emb = build_embeds(tg, ck, today, pos, sell=is_sell)
            if not emb:
                continue
            if dry:
                print(f"--- {label} ---")
                print(json.dumps(emb, ensure_ascii=False, indent=1)[:1800])
                continue
            if _post(emb, env=env):
                sent = True
                print(f"[kiwami_close] {label}の送信OK")
        except Exception as e:
            # 片方がコケてももう片方は出す（実弾の決済指示なので落とさない）
            print(f"[kiwami_close] {label}の処分通知スキップ: {e}")

    if sent and not dry:
        with open(MARKER, "w", encoding="utf-8") as f:
            json.dump({"date": today.isoformat()}, f)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()

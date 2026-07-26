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
MARKER = "kiwami_close_last_run.json"
SIZE = 1_000_000
WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_URL"
_COLOR_ACT, _COLOR_OK, _COLOR_DONE = 0xE67E22, 0x95A5A6, 0x2ECC71


def load_open() -> list[dict]:
    """極み帳簿の保有中（pending/open）だけを close_check が読める形に整えて返す。"""
    if not os.path.exists(LEDGER):
        return []
    with open(LEDGER, encoding="utf-8") as f:
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


def _post(embeds: list[dict]) -> bool:
    import requests

    url = os.getenv(WEBHOOK_ENV, "").strip()
    if not url:
        print(f"[kiwami_close] {WEBHOOK_ENV} 未設定 → 無送信")
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
                 positions: list[dict]) -> list[dict]:
    stop_of = {p["ticker"]: p["stop_pct"] for p in positions}
    atr_of = {p["ticker"]: p.get("atr_pct") for p in positions}
    d = today.strftime("%Y-%m-%d")
    embeds = []

    if targets:
        lines = []
        for t in targets:
            tk = t["ticker"]
            reason = t.get("reason") or t.get("exit_type") or "処分"
            px = t.get("current_price")
            pnl = t.get("unrealized_pnl")
            lines.append(
                f"**{t.get('name', tk)}**（{tk[:4]}）{reason}\n"
                f"　現在値 {px:,.1f} / 含み {pnl:+.2f}%".replace("None", "—")
                if px is not None else f"**{t.get('name', tk)}**（{tk[:4]}）{reason}")
        embeds.append({
            "title": f"🔔 極み｜大引けで処分 — {d}",
            "description": ("**15:00〜15:30に成行で決済**（極みの玉）。\n\n" + "\n".join(lines)),
            "color": _COLOR_ACT,
        })

    settled = [c for c in checked if c.get("note") and "OCO" in str(c.get("note"))]
    holds = [c for c in checked if c not in settled]
    if settled:
        embeds.append({
            "title": "✅ 極み｜本日OCO約定済み",
            "description": "\n".join(
                f"**{c.get('name', c['ticker'])}**（{c['ticker'][:4]}）{c['note']}"
                for c in settled),
            "color": _COLOR_DONE,
        })
    if holds:
        lines = []
        for c in holds:
            tk = c["ticker"]
            sp = stop_of.get(tk, 3.0)
            atr = atr_of.get(tk)
            note = f"　⚠️ {c['note']}" if c.get("note") else ""
            rsi = c.get("rsi_now")
            px = c.get("current_price")
            lines.append(
                f"**{c.get('name', tk)}**（{tk[:4]}）保有継続"
                + (f" RSI {rsi:.1f}" if rsi is not None else "")
                + (f" / 現在値 {px:,.1f}" if px is not None else "")
                + f"\n　損切り **-{sp:.1f}%**"
                + (f"（ATR {atr:.2f}%）" if atr else "")
                + (f" / 残り{3 - c['today_hold']}日" if c.get("today_hold") else "")
                + note)
        embeds.append({
            "title": "🔍 極み｜保有継続（処分なし）",
            "description": "\n".join(lines),
            "color": _COLOR_OK,
            "footer": {"text": "損切りは銘柄ごとに違う＝OCOの数字を通常版と取り違えないこと"},
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

    positions = load_open()
    if not positions:
        print("[kiwami_close] 極みの保有玉なし → 無送信")
        return
    print(f"[kiwami_close] 保有 {len(positions)}件: "
          + ", ".join(f"{p['name']}(損切り-{p['stop_pct']:.1f}%)" for p in positions))

    # 通常版と同じ判定エンジンを共有（stop_pct だけが玉ごとに違う）
    from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS
    from datetime import timedelta
    start = (today - timedelta(days=RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
    data = batch_download_jquants(_jquants_id_token(), start=start,
                                  end=today.strftime("%Y-%m-%d"))
    targets, checked = CC.collect_targets(positions, "BUY", today, data)
    print(f"[kiwami_close] 処分対象 {len(targets)}件 / 判定済み {len(checked)}件")

    embeds = build_embeds(targets, checked, today, positions)
    if not embeds:
        print("[kiwami_close] 送るものなし")
        return
    if dry:
        print(json.dumps(embeds, ensure_ascii=False, indent=1)[:2500])
        return
    if _post(embeds):
        with open(MARKER, "w", encoding="utf-8") as f:
            json.dump({"date": today.isoformat()}, f)
        print("[kiwami_close] 送信OK")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()

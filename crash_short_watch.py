# -*- coding: utf-8 -*-
"""crash_short_watch.py — モメンタム崩壊デイショートの紙運用（2026-07-20新設・実弾なし）。

毎営業日夕方に実行:
  1. 前回までの検出の答え合わせ（翌営業日の寄り→引けを記帳・紙のデイショート損益）
  2. 当日の精鋭条件ヒットを検出して crash_short_log.json に記録
  3. ヒットがあれば Discord 通知（DISCORD_WEBHOOK_CRASH_SHORT_URL 設定時のみ・無ければログのみ）

精鋭条件（10年BT: 貸借のみ n=436・平均+0.846%/件・勝率58.3%・PF1.52・陽性9/10年）:
  直近1ヶ月+30%以上の急騰 × 終値5MA割れ初日 × 当日-5%以上 × 出来高20日平均2倍以上
  × 代金20日平均5億以上 × 株価100円以上。貸借区分・5MA向きも記録（forward検証用）。
想定執行: 翌営業日 寄り成行空売り → 当日引け買戻し（デイ・持ち越しなし）。
注意: 紙運用専用。逆日歩・貸株料は含まない（紙で実測するのが目的）。
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, timedelta
from pathlib import Path

import jpholiday
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
from screener import (batch_download_jquants, fetch_tse_universe,
                      _jquants_get, _jquants_id_token, is_etf_ticker, _today_jst)

LOG_FILE = Path("crash_short_log.json")
RUNUP_MIN, R1_MAX, VOLX_MIN, TOV_MIN_OKU, PRICE_MIN = 30.0, -5.0, 2.0, 5.0, 100.0
PAPER_SIZE = 300_000   # 紙の建玉サイズ（円）


def _load_log() -> list[dict]:
    if LOG_FILE.exists():
        return json.load(open(LOG_FILE, encoding="utf-8"))
    return []


def _save_log(log: list[dict]) -> None:
    json.dump(log, open(LOG_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def _iss_map(token: str) -> dict:
    """直近公表週の信用残高から {code4: IssType}。取れなければ空dict（貸借判定は'?'）。"""
    try:
        cur = _today_jst() - timedelta(days=4)
        for _ in range(14):
            if cur.weekday() < 5:
                d = _jquants_get("/markets/margin-interest", token,
                                 {"date": cur.strftime("%Y-%m-%d")})
                rows = d.get("data", [])
                if rows:
                    from screener import is_common_stock_code
                    return {str(r.get("Code", ""))[:4]: str(r.get("IssType", ""))
                            for r in rows if is_common_stock_code(r.get("Code", ""))}
            cur -= timedelta(days=1)
    except Exception as e:
        print(f"[iss] 取得失敗: {e}")
    return {}


def _notify(hits: list[dict], today_str: str) -> None:
    url = os.getenv("DISCORD_WEBHOOK_CRASH_SHORT_URL", "").strip()
    if not url or not hits:
        return
    lines = []
    for h in hits:
        lines.append(f"**{h['name']}** ({h['code']}) 終値¥{h['close']:,.0f} "
                     f"前日比{h['r1']:+.1f}% 出来高{h['volx']:.1f}倍 1ヶ月+{h['runup']:.0f}% "
                     f"5MA{'上向き' if h['ma5_up'] else '下向き'} 貸借{h['loanable']}")
    embed = {
        "title": f"🩸 崩壊ショート紙運用 — 検出 {len(hits)}件（{today_str}終値）",
        "description": ("\n".join(lines) +
                        "\n\n📝 **紙運用**: 翌営業日 寄り成行空売り→当日引け買戻し想定（実弾ではありません）"
                        "\n実務メモ: 貸借○のみ空売り可 / 50単元以下は価格規制の成行制限対象外 / 逆日歩は要実測"),
        "color": 0x8B0000,
        "footer": {"text": "10年BT: 貸借のみ 平均+0.85%/件・勝率58%・PF1.52・陽性9/10年（コスト前）"},
    }
    try:
        r = requests.post(url, json={"embeds": [embed]}, timeout=15)
        print(f"[discord] 通知 HTTP {r.status_code}")
    except Exception as e:
        print(f"[discord] 送信失敗: {e}")


def main() -> None:
    today = _today_jst()
    if (today.weekday() >= 5 or jpholiday.is_holiday(today)
            or (today.month == 12 and today.day == 31)
            or (today.month == 1 and today.day <= 3)):   # 年末年始は東証休業（2026-08-02統一）
        print(f"[crash_short] {today} は休場 → スキップ")
        return

    token = _jquants_id_token()
    data = batch_download_jquants(token, lookback_trading_days=45)
    if not data:
        print("[crash_short] データ取得失敗（鮮度ガード）→ 今回はスキップ")
        return
    universe = fetch_tse_universe(token)
    name_map = {t: n for t, n in universe}

    # 最新営業日（データ側の実日付・祝日ズレに頑健）
    any_df = next(iter(data.values()))
    last_day = max(df.index.max() for df in data.values() if len(df))
    today_str = last_day.strftime("%Y-%m-%d")

    log = _load_log()

    # ── 1) 答え合わせ: 未採点の過去検出に「検出翌営業日の寄り→引け」を記帳 ──
    scored = 0
    for e in log:
        if e.get("result_pct") is not None or e["d0"] >= today_str:
            continue
        df = data.get(e["ticker"])
        if df is None or df.empty:
            continue
        after = df[df.index.strftime("%Y-%m-%d") > e["d0"]]
        if after.empty:
            continue
        bar = after.iloc[0]
        o1, c1 = float(bar["Open"]), float(bar["Close"])
        if not (o1 > 0 and c1 > 0):
            continue
        e["exec_date"] = after.index[0].strftime("%Y-%m-%d")
        e["exec_open"] = o1
        e["exec_close"] = c1
        e["result_pct"] = round((o1 - c1) / o1 * 100, 3)
        e["result_yen"] = round(PAPER_SIZE * e["result_pct"] / 100)
        scored += 1
    if scored:
        done = [e for e in log if e.get("result_pct") is not None]
        tot = sum(e["result_yen"] for e in done)
        wins = sum(1 for e in done if e["result_pct"] > 0)
        print(f"[score] {scored}件を採点 → 通算 {len(done)}件 勝率{wins/len(done)*100:.0f}% {tot:+,}円（紙・30万/件）")

    # ── 2) 当日の検出 ──
    iss = _iss_map(token)
    known = {(e["ticker"], e["d0"]) for e in log}
    hits = []
    for tk, df in data.items():
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):
            continue
        if len(df) < 30:
            continue
        o = df["Open"].astype(float); c = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        if df.index.max() != last_day:
            continue
        ma5 = c.rolling(5).mean()
        below = c < ma5
        fresh = bool(below.iloc[-1]) and not bool(below.iloc[-2])
        if not fresh:
            continue
        r1 = float(c.iloc[-1] / c.iloc[-2] - 1) * 100
        if r1 > R1_MAX:
            continue
        v20p = float(v.iloc[-21:-1].mean())
        volx = float(v.iloc[-1] / v20p) if v20p > 0 else 0.0
        if volx < VOLX_MIN:
            continue
        tov20 = float((c * v).tail(20).mean() / 1e8)
        if tov20 < TOV_MIN_OKU or c.iloc[-1] < PRICE_MIN:
            continue
        c21 = float(c.iloc[-21]) if len(c) >= 21 else 0.0
        runup = (float(c.tail(20).max()) / c21 - 1) * 100 if c21 > 0 else 0.0
        if runup < RUNUP_MIN:
            continue
        if (tk, today_str) in known:
            continue
        hits.append({
            "ticker": tk, "code": tk[:4], "name": name, "d0": today_str,
            "close": float(c.iloc[-1]), "r1": round(r1, 2), "volx": round(volx, 2),
            "runup": round(runup, 1), "tov_oku": round(tov20, 1),
            "ma5_up": bool(ma5.iloc[-1] >= ma5.iloc[-2]),
            "loanable": "○" if iss.get(tk[:4]) == "2" else ("×" if iss.get(tk[:4]) else "?"),
            "result_pct": None, "result_yen": None,
        })

    log.extend(hits)
    _save_log(log)
    print(f"[crash_short] {today_str} 検出 {len(hits)}件 / ログ累計 {len(log)}件")
    for h in hits:
        print(f"  {h['code']} {h['name']} 前日比{h['r1']:+.1f}% 出来高{h['volx']:.1f}倍 "
              f"+{h['runup']:.0f}% 5MA{'上' if h['ma5_up'] else '下'} 貸借{h['loanable']}")
    _notify(hits, today_str)


if __name__ == "__main__":
    main()

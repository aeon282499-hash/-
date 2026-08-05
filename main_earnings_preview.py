# -*- coding: utf-8 -*-
"""main_earnings_preview.py — 決算持ち越しの朝の予備リスト自動配信（2026-08-06・本人依頼）。

15:06の本配信の「先読み」を毎朝8:20頃に決算チャンネルへ送る。本人が日中忙しくても
「今日は何本来そうか・どの銘柄が有力か」を朝のうちに把握できる。

⚠️これは予備リスト＝非公式。RSI/5日騰落は昨日終値ベースで、当日の値動きで入れ替わる。
場中発表銘柄は15:06のTDnet照会で自動除外される。**このリストで事前発注はしない**
（人間の先回り取捨選択はランダム−ロケットにしかならない＝7月実測）。

判定は本体ルールAの近似: 発表予定(JPX) × 決算ボラゲート(本体と同じvol_pass) ×
株価≤5,000 × 20日代金中央値≥7.5億 × RSI14≤55 × 5日騰落<-3%（すべて昨日終値まで）。
実行: python main_earnings_preview.py [--dry] [--force]
"""
from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

import main_earnings_hold as eh
from main_pead_paper import jst_today

STATE_PATH = "earnings_preview_state.json"
PRICE_CAP = 5_000.0
TOV_MIN = 7.5e8
RSI_MAX = 55.0
RUNUP_MAX = -3.0
SLOTS_TOTAL = 8


def rsi14(closes: list[float]) -> float | None:
    if len(closes) < 15:
        return None
    gains, losses = [], []
    for a, b in zip(closes[:-1], closes[1:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    n = 14
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    return 50.0 if ag + al == 0 else 100.0 * ag / (ag + al)


def open_slots(today_str: str) -> int:
    """15:06時点の空き枠見込み。pendingは今朝の寄りで決済=解放。extendedは
    売却日当日まで枠占有（本体と同じ保守ルール）なので ext_exit_date>=今日 だけ数える。"""
    try:
        with open("positions_earnings.json", encoding="utf-8") as f:
            ps = json.load(f).get("positions", [])
        used = sum(1 for p in ps if p.get("status") == "extended"
                   and str(p.get("ext_exit_date", "9999-99-99")) >= today_str)
        return max(SLOTS_TOTAL - used, 0)
    except Exception:
        return SLOTS_TOTAL


def scan(all_data: dict, codes: list[dict], upto: str) -> tuple[list, list, int]:
    """圏内(RSI≤55×5日<-3%)とボーダー(RSI≤62×5日<0)を返す。upto=昨日(判定基準日)。"""
    hits, border, n_gate = [], [], 0
    for r in codes:
        tk = str(r.get("code", "")) + ".T"
        okv, ev = eh.vol_pass(tk)
        if not okv:
            continue
        n_gate += 1
        df = all_data.get(tk)
        if df is None or "Close" not in df or len(df) < 21:
            continue
        df = df[df.index.strftime("%Y-%m-%d") <= upto]
        if len(df) < 21:
            continue
        closes = df["Close"].astype(float).tolist()
        px = closes[-1]
        if px <= 0 or px > PRICE_CAP:
            continue
        tov = (df["Close"].astype(float) * df["Volume"].astype(float)).tail(20).median()
        if tov < TOV_MIN:
            continue
        runup5 = (closes[-1] / closes[-6] - 1) * 100
        rsi = rsi14(closes[-40:])
        if rsi is None:
            continue
        row = {"ticker": tk, "name": str(r.get("name", tk))[:10], "px": px,
               "rsi": rsi, "runup5": runup5, "evol": ev}
        if rsi <= RSI_MAX and runup5 < RUNUP_MAX:
            hits.append(row)
        elif rsi <= 62 and runup5 < 0:
            border.append(row)
    hits.sort(key=lambda x: x["rsi"])
    border.sort(key=lambda x: x["rsi"])
    return hits, border, n_gate


def _line(r: dict) -> str:
    ev = f"{r['evol']:.1f}" if r.get("evol") is not None else "n/a"
    return (f"{r['name']} ({r['ticker'][:-2]}) {r['px']:,.0f}円 "
            f"RSI{r['rsi']:.0f} 5日{r['runup5']:+.1f}% ボラ{ev}")


def build_embed(hits: list, border: list, n_sched: int, n_gate: int,
                slots: int, today) -> dict:
    lines = [f"発表予定**{n_sched}件** → ゲート通過{n_gate}件 → 圏内**{len(hits)}件**"
             f"／空き枠**{slots}**（15:06はRSI昇順で上位{slots}本まで）", ""]
    if hits:
        lines.append("**圏内（昨日終値でルールA相当・RSI昇順=配信順の見込み）**")
        lines += [f"`{i}.` {_line(r)}" for i, r in enumerate(hits[:8], 1)]
        if len(hits) > 8:
            lines.append(f"…ほか{len(hits) - 8}件")
    else:
        lines.append("圏内なし（当日下げで入る可能性はある）")
    if border:
        lines.append("**ボーダー（今日下げれば入る）**")
        lines += [f"・{_line(r)}" for r in border[:4]]
    lines += ["", "⚠️ **予備リスト＝非公式。当日値で入替あり・場中発表は自動除外。**",
              "⚠️ **最終判定は15:06の本配信。このリストで事前発注しない。**"]
    return {"title": f"🔮 決算持ち越し 朝の予備リスト {today.strftime('%m/%d')}",
            "description": "\n".join(lines), "color": 0x3498DB,
            "footer": {"text": "自動先読み（昨日終値ベース）・本配信は15:06"}}


def run(dry: bool = False, force: bool = False) -> None:
    today = jst_today()
    if not force and not eh.is_trading_day(today):
        print("[preview] 休場日スキップ")
        return
    ts = today.strftime("%Y-%m-%d")
    state = {}
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}
    if not force and state.get("last_date") == ts:
        print("[preview] 本日送信済み")
        return
    try:
        with open("jpx_earnings_schedule.json", encoding="utf-8") as f:
            codes = json.load(f).get("schedule", {}).get(ts, [])
    except Exception:
        codes = []
    if not codes:
        print("[preview] 本日の発表予定なし → 配信スキップ")
        return
    from screener import _jquants_id_token, batch_download_jquants
    token = _jquants_id_token()
    start = (today - timedelta(days=70)).strftime("%Y-%m-%d")
    all_data = batch_download_jquants(token, start=start, end=ts)
    yday = today - timedelta(days=1)
    while not eh.is_trading_day(yday):
        yday -= timedelta(days=1)
    hits, border, n_gate = scan(all_data, codes, yday.strftime("%Y-%m-%d"))
    embed = build_embed(hits, border, len(codes), n_gate, open_slots(ts), today)
    if dry:
        print(json.dumps(embed, ensure_ascii=False, indent=1))
    else:
        url = os.getenv("DISCORD_WEBHOOK_EARNINGS_URL", "").strip()
        if url:
            eh.send_discord([embed], url, "earnings-preview")
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"last_date": ts}, f)
    print(f"[preview] 予定{len(codes)} ゲート後{n_gate} 圏内{len(hits)} ボーダー{len(border)}")


if __name__ == "__main__":
    run(dry="--dry" in sys.argv, force="--force" in sys.argv)

# -*- coding: utf-8 -*-
"""
fibo_live.py — フィボ押し目反発のライブモード（P1）。fibo_daytrade.py --live から呼ばれる。

  tachibana_live_flow.py が毎分追記する live_flow/minutes/YYYY-MM-DD.jsonl（1行=1巡・全銘柄の
  現在値/始値/高値/安値/出来高/代金/VWAP/前日終値）を読み、1分断面→5分足を作って、確定した足だけを
  WaveEngine に渡す。通知(Discord: DISCORD_WEBHOOK_FIBO_URL)・記録(fibo_daytrade_log.csv)・
  live_flow/fibo_live.json（LiveFlow が payload["fibo"] に同梱→アプリ🔥場中ライブに出る）。
  発注はしない。紙の建玉は1分断面で決済ルールを追跡し、2連敗停止/30分クールダウン/HALF_LOT を数える。
"""
from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from fibo_daytrade import (Bar, DayContext, WaveEngine, Signal, Skip, hm, minutes_between, load_cache, load_name_map,
                           trading_days, bars_of, ROOT, LOG_CSV, STATE_JSON, TIME_STOP_MIN, FLAT_ALL_AT,
                           MAX_CONSEC_LOSS, COOLDOWN_MIN, HALF_LOT_TRADES)

try:   # Discord webhook は .env（ローカルのPCタスクで動くので GitHub Secrets ではなく .env に置く）
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

MINUTES_DIR = ROOT / "live_flow" / "minutes"
FIBO_LIVE_JSON = ROOT / "live_flow" / "fibo_live.json"
WEBHOOK_ENV = "DISCORD_WEBHOOK_FIBO_URL"
LIVE_POLL_SEC = 20
LIVE_END = "15:05"
MIN_TOV_LIVE = 1e8          # 当日代金1億未満は見ない（9:30以降）


class BarBuilder:
    """1銘柄の1分断面 → 5分足。確定した足（次の5分枠の断面が来た時点）だけを返す。"""

    def __init__(self):
        self.cur_key: datetime | None = None
        self.cur: dict | None = None
        self.vol_at_bar_start = 0.0
        self.last_vol = 0.0
        self.last_dayhigh: float | None = None
        self.last_daylow: float | None = None

    def push(self, ts: datetime, last, high, low, vol) -> Bar | None:
        if last is None or last <= 0:
            return None
        key = ts.replace(second=0, microsecond=0, minute=(ts.minute // 5) * 5)
        done: Bar | None = None
        if self.cur_key is not None and key > self.cur_key:
            c = self.cur
            done = Bar(self.cur_key, c["o"], c["h"], c["l"], c["c"], max(0.0, self.last_vol - self.vol_at_bar_start))
            self.vol_at_bar_start = self.last_vol
            self.cur_key = None; self.cur = None
        if self.cur_key is None:
            self.cur_key = key
            self.cur = {"o": last, "h": last, "l": last, "c": last}
        else:
            self.cur["c"] = last; self.cur["h"] = max(self.cur["h"], last); self.cur["l"] = min(self.cur["l"], last)
        # 当日高値/安値の更新はその5分枠の高安に反映（1分断面の取りこぼしを補う）
        if high and self.last_dayhigh is not None and high > self.last_dayhigh:
            self.cur["h"] = max(self.cur["h"], high)
        if low and self.last_daylow is not None and 0 < low < self.last_daylow:
            self.cur["l"] = min(self.cur["l"], low)
        if high:
            self.last_dayhigh = high
        if low:
            self.last_daylow = low
        if vol is not None:
            self.last_vol = vol
        return done


def read_minutes(path: Path, offset: int) -> tuple[list[dict], int]:
    rows = []
    if not path.exists():
        return rows, offset
    with open(path, "r", encoding="utf-8") as f:
        f.seek(offset)
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
        offset = f.tell()
    return rows, offset


def bars_from_minutes(path: Path) -> dict[str, list[Bar]]:
    rows, _ = read_minutes(path, 0)
    builders: dict[str, BarBuilder] = {}
    out: dict[str, list[Bar]] = {}
    for row in rows:
        ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
        for code, v in row["s"].items():
            b = builders.setdefault(code, BarBuilder())
            bar = b.push(ts, v[0], v[2], v[3], v[4])
            if bar:
                out.setdefault(code, []).append(bar)
    return out


_PREV_MIN: dict | None = None


def prev_context(day: str, code: str, prev_close, yahoo) -> tuple[float, float, list[Bar]]:
    """前日終値・前日高値・前日5分足。minutes の前日ファイル → Yahooキャッシュ の順。"""
    global _PREV_MIN
    if _PREV_MIN is None:
        _PREV_MIN = {}
        d = datetime.strptime(day, "%Y-%m-%d").date()
        for k in range(1, 6):
            p = MINUTES_DIR / f"{(d - timedelta(days=k)).strftime('%Y-%m-%d')}.jsonl"
            if p.exists():
                _PREV_MIN = bars_from_minutes(p); break
    prev_bars: list[Bar] = []
    if code in _PREV_MIN:
        prev_bars = _PREV_MIN[code]
    elif yahoo is not None and (code + ".T") in yahoo:
        df = yahoo[code + ".T"]
        prevs = [x for x in trading_days(df) if x < day]
        if prevs:
            prev_bars = [b for b in bars_of(df, prevs[-1]) if b.v > 0]
    pc = float(prev_close or 0) or (prev_bars[-1].c if prev_bars else 0.0)
    ph = max((b.h for b in prev_bars), default=pc)
    return pc, ph, prev_bars


def discord_signal(sig: Signal, lot_note: str) -> bool:
    url = os.getenv(WEBHOOK_ENV, "").strip()
    if not url:
        return False
    try:
        import requests
    except Exception:
        return False
    f = sig.fib
    vr = f"{sig.vol_ratio:.1f}倍" if sig.vol_ratio else "?"
    lines = [
        f"**{sig.name}**（{sig.code}）ラベル: {sig.label}／初動ランク: {sig.rank}（起点{sig.origin:,.0f}→高値{sig.high:,.0f} +{sig.rise_pct:.1f}%・{sig.rise_min}分・出来高倍率{vr}）",
        f"フィボ: 23.6={f['23.6']:,.0f} 38.2={f['38.2']:,.0f} 50={f['50']:,.0f} 61.8={f['61.8']:,.0f} 78.6={f['78.6']:,.0f} ／ 127.2={f['127.2']:,.0f} 161.8={f['161.8']:,.0f}",
        f"押し: {sig.retrace_pct:.0f}%（{sig.stopped_at}）で停止・中期MA {sig.ma_mid_dir}・重なり **{sig.overlap}点** {' '.join(sig.overlap_items) or '—'}{'　★本命' if '本命' in sig.flags else ''}",
        f"**エントリー {sig.entry:,.0f}／損切り {sig.stop:,.0f}／利確 {sig.tp1:,.0f}（半分）→{sig.tp2:,.0f}／RR {sig.rr:.2f}**　{lot_note}",
        "決済: +3%で半分→損切りを建値へ／30分動かなければ撤退／15:00までに全決済。発注は自動ではありません（通知のみ）",
    ]
    if sig.flags:
        lines.append("フラグ: " + "・".join(sig.flags))
    payload = {"embeds": [{"title": f"📐 フィボ押し目 {sig.day} {sig.time}", "description": "\n".join(lines), "color": 0x1ABC9C,
                           "footer": {"text": "fibo_daytrade.py（紙・通知のみ）"}}]}
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code < 300
    except Exception:
        return False


CSV_FIELDS = ["date", "code", "name", "label", "rank", "rise_pct", "rise_min", "vol_ratio", "stopped_at", "retrace_pct", "ma_mid_dir",
              "overlap", "overlap_items", "entry", "stop", "tp1", "tp2", "rr", "time", "lot", "exit_price", "exit_time", "exit_type",
              "pnl_pct", "hold_min", "result", "skip_reason", "flags"]


def csv_append(sig: Signal | None, skip: Skip | None = None):
    new = not LOG_CSV.exists()
    with open(LOG_CSV, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new:
            w.writeheader()
        if sig:
            row = {k: getattr(sig, k, "") for k in CSV_FIELDS if hasattr(sig, k)}
            row["date"] = sig.day; row["overlap_items"] = "|".join(sig.overlap_items); row["flags"] = "|".join(sig.flags)
            row["result"] = "" if sig.pnl_pct is None else ("win" if sig.pnl_pct > 0 else "loss" if sig.pnl_pct < 0 else "flat")
            w.writerow(row)
        elif skip:
            w.writerow({"date": skip.day, "code": skip.code, "name": skip.name, "time": skip.time, "skip_reason": skip.reason})


class RiskState:
    """2連敗で当日停止・負け後30分クールダウン・HALF_LOT_MODE（最初の10トレードは半ロット・fibo_state.json で通算）。"""

    def __init__(self):
        self.consec_loss = 0
        self.stopped = False
        self.cooldown_until: datetime | None = None
        try:
            self.total_trades = int(json.loads(STATE_JSON.read_text(encoding="utf-8")).get("total_trades", 0))
        except Exception:
            self.total_trades = 0

    def can_enter(self, now: datetime) -> tuple[bool, str]:
        if self.stopped:
            return False, "2連敗で当日停止"
        if self.cooldown_until and now < self.cooldown_until:
            return False, f"クールダウン中（{hm(self.cooldown_until)}まで）"
        return True, ""

    def lot_note(self) -> str:
        return "半ロット（HALF_LOT_MODE: 最初の10トレード）" if self.total_trades < HALF_LOT_TRADES else "通常ロット"

    def on_close(self, pnl: float, now: datetime):
        self.total_trades += 1
        STATE_JSON.write_text(json.dumps({"total_trades": self.total_trades}), encoding="utf-8")
        if pnl < 0:
            self.consec_loss += 1
            self.cooldown_until = now + timedelta(minutes=COOLDOWN_MIN)
            if self.consec_loss >= MAX_CONSEC_LOSS:
                self.stopped = True
        else:
            self.consec_loss = 0


def track_open(ot: dict, last: float, ts: datetime, risk: RiskState):
    sig: Signal = ot["sig"]; ot["last"] = last
    entry = sig.entry
    if last <= ot["stop"]:
        close_trade(ot, ot["stop"], ts, "STOP" if not ot["half"] else "BE_STOP", risk); return
    if not ot["half"] and last >= sig.tp1:
        if sig.tp1 == sig.tp2:
            close_trade(ot, sig.tp1, ts, "TP", risk); return
        ot["half"] = True; ot["realized"] = (sig.tp1 / entry - 1) * 100 * 0.5; ot["stop"] = entry
        print(f"[live] 利確1 {sig.name} {sig.tp1} → 損切りを建値{entry}へ", flush=True)
        return
    if ot["half"] and last >= sig.tp2:
        close_trade(ot, sig.tp2, ts, "TP2", risk); return
    if not ot["half"] and minutes_between(ot["t0"], ts) >= TIME_STOP_MIN and last < entry * 1.01:
        close_trade(ot, last, ts, "TIME", risk); return
    if hm(ts) >= FLAT_ALL_AT:
        close_trade(ot, last, ts, "CLOSE", risk)


def close_trade(ot: dict, px: float, ts: datetime, kind: str, risk: RiskState):
    sig: Signal = ot["sig"]
    pnl = ot["realized"] + (px / sig.entry - 1) * 100 * (0.5 if ot["half"] else 1.0)
    sig.exit_price = round(px, 1); sig.exit_time = hm(ts); sig.exit_type = kind
    sig.pnl_pct = round(pnl, 3); sig.hold_min = minutes_between(ot["t0"], ts)
    ot["closed"] = True
    csv_append(sig)
    risk.on_close(pnl, ts)
    print(f"[live] 決済 {sig.name} {kind} {px} {pnl:+.2f}%（連敗{risk.consec_loss}）", flush=True)


def write_fibo_live(engines: dict[str, WaveEngine], open_trades: dict[str, dict], now: datetime):
    """アプリ同梱用: 高値確定済みの波（フィボ・押し・状態）と、紙の建玉。"""
    cands = []
    for code, e in engines.items():
        if not e.confirmed or not e.fib:
            continue
        status = "entered" if e.signal else ("skip" if e.done else "watch")
        last_ev = e.events[-1][6:] if e.events else ""
        w = getattr(e, "wave", {}) or {}
        cands.append({"code": code, "name": e.ctx.name, "label": e.label, "rank": w.get("rank") or "-",
                      "origin": round(e.origin, 1), "high": round(e.high, 1), "rise": round(w.get("rise", 0), 1), "mins": w.get("mins"),
                      "vol_ratio": (round(w["vol_ratio"], 1) if w.get("vol_ratio") else None),
                      "fib": {k: round(v, 1) for k, v in e.fib.items()},
                      "pull_low": (round(e.pull_low, 1) if e.pull_low else None),
                      "retrace": (round((e.high - e.pull_low) / (e.high - e.origin) * 100) if e.pull_low and e.high > e.origin else None),
                      "status": status, "note": last_ev[:60],
                      "signal": (asdict(e.signal) if e.signal else None)})
    order = {"entered": 0, "watch": 1, "skip": 2}
    cands.sort(key=lambda c: (order[c["status"]], -c["rise"]))
    trades = [{"code": c, "name": ot["sig"].name, "entry": ot["sig"].entry, "stop": ot["stop"], "half": ot["half"],
               "last": ot.get("last"), "closed": bool(ot.get("closed")), "exit_type": ot["sig"].exit_type, "pnl_pct": ot["sig"].pnl_pct}
              for c, ot in open_trades.items()]
    out = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "n_watch": sum(1 for c in cands if c["status"] == "watch"),
           "candidates": cands[:40], "trades": trades, "note": "フィボ押し目反発（紙・通知のみ・発注はしない）"}
    try:
        FIBO_LIVE_JSON.parent.mkdir(parents=True, exist_ok=True)
        FIBO_LIVE_JSON.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def run_live(day: str | None = None, once: bool = False):
    day = day or datetime.now().strftime("%Y-%m-%d")
    path = MINUTES_DIR / f"{day}.jsonl"
    print(f"[live] {day} 監視開始 {path}", flush=True)
    d0 = datetime.strptime(day, "%Y-%m-%d").date()
    yahoo = None
    if not any((MINUTES_DIR / f"{(d0 - timedelta(days=k)).strftime('%Y-%m-%d')}.jsonl").exists() for k in range(1, 6)):
        try:
            yahoo = load_cache()   # 前日の minutes が無い初日だけ Yahoo 5分足で前日文脈を補う
            print("[live] 前日文脈: Yahoo 5分足キャッシュ", flush=True)
        except Exception as e:
            print(f"[live] Yahooキャッシュ読込失敗: {e}", flush=True)
    builders: dict[str, BarBuilder] = {}
    engines: dict[str, WaveEngine] = {}
    names = {k[:-2]: v for k, v in load_name_map().items()}
    risk = RiskState()
    open_trades: dict[str, dict] = {}
    offset = 0
    while True:
        now = datetime.now()
        rows, offset = read_minutes(path, offset)
        for row in rows:
            ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
            for code, v in row["s"].items():
                last, opn, high, low, vol, tov, vwap, prev = (v + [None] * 8)[:8]
                if not last or (tov is not None and tov < MIN_TOV_LIVE and hm(ts) > "09:30"):
                    continue
                b = builders.setdefault(code, BarBuilder())
                bar = b.push(ts, last, high, low, vol)
                ot = open_trades.get(code)
                if ot and not ot.get("closed"):
                    track_open(ot, last, ts, risk)
                if bar is None:
                    continue
                eng = engines.get(code)
                if eng is None:
                    pc, ph, prev_bars = prev_context(day, code, prev, yahoo)
                    if pc <= 0:
                        continue
                    cum = 0.0; cum_at = {}
                    for pb in prev_bars:
                        cum += pb.v; cum_at[hm(pb.end)] = cum
                    eng = WaveEngine(DayContext(code, names.get(code, code), day, pc, ph, prev_bars, cum_at))
                    engines[code] = eng
                if eng.done:
                    continue
                eng.on_bar(bar)
                if eng.signal and code not in open_trades:
                    sig = eng.signal
                    ok, why = risk.can_enter(ts)
                    if not ok:
                        k = Skip(code, sig.name, day, sig.time, why); eng.skips.append(k); csv_append(None, k)
                        continue
                    sig.lot = "half" if risk.total_trades < HALF_LOT_TRADES else "full"
                    open_trades[code] = {"sig": sig, "half": False, "stop": sig.stop, "t0": ts, "realized": 0.0}
                    sent = discord_signal(sig, risk.lot_note())
                    csv_append(sig)
                    print(f"[live] ★{sig.time} {sig.name}({code}) entry{sig.entry} stop{sig.stop} tp{sig.tp1}/{sig.tp2} 重なり{sig.overlap} Discord={'OK' if sent else 'skip'}", flush=True)
        write_fibo_live(engines, open_trades, now)
        if once or hm(now) >= LIVE_END:
            break
        time.sleep(LIVE_POLL_SEC)
    for code, ot in open_trades.items():
        if not ot.get("closed"):
            close_trade(ot, ot.get("last", ot["sig"].entry), now, "CLOSE", risk)
    write_fibo_live(engines, open_trades, now)
    print(f"[live] 終了 銘柄{len(engines)} 高値確定{sum(1 for e in engines.values() if e.confirmed)} シグナル{len(open_trades)}", flush=True)

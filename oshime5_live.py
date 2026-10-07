# -*- coding: utf-8 -*-
"""
oshime5_live.py — ✂️上ヒゲ刈り取りの場中ライン（フェーズ2・2026-10-05）。発注はしない（表示・通知・紙トレード記録だけ）。

  前夜の oshime5_candidates.json（target_date＝今日）の10本だけを、tachibana_live_flow.py が毎分追記する
  live_flow/minutes/YYYY-MM-DD.jsonl から5分足にして判定する（fibo_live.py と同じ入力・同じ BarBuilder）。

ルール（trade_bt RULE_15m.md・RESULTS_1004〜1005a）
  見送り   9:00〜9:30の高値が前日終値+3%以上／最初の15分足（9:00〜9:15）が始値比+1%以上／株価2,000〜10,000円の外
  ライン   5分足が1本確定するたびに: 25MA上向き（確定済みの足の MA[j] > MA[j-1]）・その足が陰線・安値＞25MA・
           幅（高値÷安値−1）0.8%以下 → 次の5分足のあいだだけ「陰線の高値＋1ティック」に買いの逆指値（TOPIX500の呼値）
           次の足が始まるのが 9:30〜11:25 のときだけ（12:30以降は出さない）。条件が崩れたらラインは消す。1銘柄1日1回
  出口     損切り −3%・利確 +1.5%（参考に +3% も記録）・14:45 に手じまい
  停止     紙の実現損 −4万円・2連敗・1日8回 のどれかで、その日は新しいラインを出さない（含み損は入れない・10/6 本人指示書）
  記録     oshime5_log.csv（滑り0.1%の列を末尾に追加）＋ oshime5_random_log.csv（同じ候補を9:30〜11:29/12:30〜12:59のランダムな1分に買った紙・ルールとの差を見る）
  候補     oshime5_candidates.json（target_date＝今日）が無ければ oshime5_watch.txt（4桁コードを1行ずつ）
  株数     2万円 ÷（建値×3%）を100株単位で切り捨て

出力
  live_flow/oshime5_live.json（tachibana_live_flow.py が payload["oshime5"] に同梱 → チンパン✂️上ヒゲ タブ）
  oshime5_log.csv（fibo_oct_log.csv と同じ列・exit1=+1.5%・exit2=+3%）
  Discord: DISCORD_WEBHOOK_OSHIME5_URL（.env・未設定なら無言）

実行
  python oshime5_live.py --live                 平日 9:00 起動・15:05 まで毎分追読み
  python oshime5_live.py --replay 2026-10-02 [--cands path.json] [--no-log]   minutes の再生（検証用）
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from fibo_daytrade import ROOT, DATA_ROOT, hm
from fibo_live import BarBuilder, read_minutes

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

MINUTES_DIR = DATA_ROOT / "live_flow" / "minutes"
LIVE_JSON = ROOT / "live_flow" / "oshime5_live.json"
LOG_CSV = ROOT / "oshime5_log.csv"
CANDS_JSON = ROOT / "oshime5_candidates.json"
WEBHOOK_ENV = "DISCORD_WEBHOOK_OSHIME5_URL"

BAR_MIN = 5
MA_N = 25
WIDTH_MAX = 0.8          # 陰線の幅（%）
FIRST_START, LAST_START = "09:30", "11:25"   # ラインを出す足（次の足）の開始時刻（前場）
PM_START, PM_LAST = "12:30", "12:55"          # 後場も12:30〜12:59は入る（10/7 本人「午後も入れるはず」→60日: 12:30〜12:59 +0.10%/PF1.43/損切り0.2%・13:00〜14:00は+0.04%で入らない）
def in_entry_window(t: str) -> bool:
    return FIRST_START <= t <= LAST_START or PM_START <= t <= PM_LAST
FLAT_AT = "14:45"
GAP_MAX = 3.0            # 9:30までの高値が前日比これ以上なら見送り
FIRST15_MAX = 1.0        # 最初の15分足（始値比）
PX_LO, PX_HI = 2_000, 10_000
SL_PCT, TP_PCT, TP2_PCT = 3.0, 1.5, 3.0
RISK_YEN = 20_000
STOP_CONSEC, STOP_TRADES, STOP_YEN = 2, 8, -40_000   # 10/6 本人指示書: −4万円／2連敗／8回（実現損だけで判定・含み損は入れない）
SLIP_PCT = 0.1           # 紙の約定に乗せる滑り（別列で記録・生の値も残す）
RANDOM_LOG_CSV = ROOT / "oshime5_random_log.csv"   # 同じ候補を 9:30〜11:29 / 12:30〜12:59 のランダムな1分に買った紙（ルールとの差を見る）
WATCH_TXT = ROOT / "oshime5_watch.txt"             # 候補JSONが無い日の手書きリスト（コードを1行ずつ）
LIVE_POLL_SEC = 20
LIVE_END = "15:05"
LOG_FIELDS = ["date", "code", "name", "armed_at", "origin", "high", "entry", "stop", "stop_kind", "shares", "max_loss", "gap_pct", "priority",
              "overlap", "fill_time", "exit1_type", "exit1_time", "exit1_price", "pnl1_yen", "exit2_type", "exit2_time", "exit2_price", "pnl2_yen", "skip_reason",
              "entry_slip", "pnl1_slip_yen", "pnl2_slip_yen"]   # fibo_oct_log.csv と同じ列＋滑り0.1%を乗せた列（末尾）


def tick500(px: float) -> float:
    for lim, t in ((1000, 0.1), (3000, 0.5), (10000, 1), (30000, 5), (100000, 10)):
        if px <= lim:
            return t
    return 50


def rnd(v: float) -> float:
    return round(v + 1e-9, 1)


def rtick(v: float) -> float:
    """TOPIX500 の呼値に丸める（損切り・利確の表示用。紙の損益もこの値で計算）"""
    t = tick500(v)
    return rnd(round(v / t) * t)


def shares_for(entry: float) -> int:
    """2万円 ÷（建値×3%）を100株単位で切り捨て。0株なら100株（6,667円超の銘柄・損失は最大約3万円）"""
    return max(100, int(RISK_YEN / (entry * SL_PCT / 100)) // 100 * 100) if entry > 0 else 0


class Risk:
    def __init__(self):
        self.n = 0; self.consec = 0; self.pnl = 0.0; self.wins = 0; self.losses = 0

    def reason(self) -> str | None:
        if self.consec >= STOP_CONSEC:
            return f"{self.consec}連敗で本日終了"
        if self.n >= STOP_TRADES:
            return f"{self.n}回で本日終了"
        if self.pnl <= STOP_YEN:
            return f"紙の損益{self.pnl:+,.0f}円で本日終了"
        return None

    def close(self, pnl: float):
        self.pnl += pnl
        if pnl < 0:
            self.consec += 1; self.losses += 1
        else:
            self.consec = 0; self.wins += pnl > 0


class Engine:
    """1銘柄・1日。push(ts, 断面) を時刻順に呼ぶ。events を返す"""

    def __init__(self, code: str, name: str, rank: int, prev_bars: list):
        self.code, self.name, self.rank = code, name, rank
        self.closes = [b.c for b in prev_bars][-(MA_N + 5):]
        self.ma = self._ma_series()
        self.bld = BarBuilder(BAR_MIN)
        self.prev_close = None; self.day_open = None
        self.pre_high = None; self.first15_close = None
        self.status = "待機"; self.skip = ""
        self.line = None; self.line_bar = None; self.armed_at = None; self.src = None   # src=(陰線の安値, 高値)
        self.trade = None
        self.last = None; self.n_lines = 0; self.notified = set()

    def _ma_series(self):
        c = self.closes
        return [sum(c[i - MA_N + 1:i + 1]) / MA_N if i >= MA_N - 1 else None for i in range(len(c))]

    def _push_close(self, c: float):
        self.closes.append(c)
        self.ma.append(sum(self.closes[-MA_N:]) / MA_N if len(self.closes) >= MA_N else None)

    def row(self) -> dict:
        r = {"code": self.code, "name": self.name, "rank": self.rank, "status": self.status, "skip_reason": self.skip,
             "last": self.last, "prev_close": self.prev_close, "ma": rnd(self.ma[-1]) if self.ma and self.ma[-1] else None,
             "line": self.line, "line_bar": self.line_bar, "armed_at": self.armed_at, "n_lines": self.n_lines}
        if self.line:
            r.update(tp=rtick(self.line * (1 + TP_PCT / 100)), sl=rtick(self.line * (1 - SL_PCT / 100)), shares=shares_for(self.line))
        if self.trade:
            r["trade"] = {k: v for k, v in self.trade.items() if k != "books"} | {"books": self.trade["books"]}
        return r

    # ── 断面を1つ受け取る ──
    def push(self, ts: datetime, v: list, risk: Risk) -> list[dict]:
        last, opn, high, low, vol, tov, vwap, prev = (list(v) + [None] * 8)[:8]
        ev = []
        if not last:
            return ev
        t = hm(ts)
        if t < "09:00" or "11:30" < t < "12:30" or t > "15:30":
            return ev
        if t in ("11:30", "15:30"):
            ts = ts.replace(second=0) - timedelta(seconds=1)
        self.last = last
        if prev and not self.prev_close:
            self.prev_close = float(prev)
        if opn and not self.day_open:
            self.day_open = float(opn)
        if t < "09:30" and high:
            self.pre_high = max(self.pre_high or 0, float(high), float(last))
        done = self.bld.push(ts, last, high, low, vol)
        if done:
            ev += self._on_bar(done, ts, risk)
        ev += self._on_tick(ts, last, risk)
        return ev

    def _day_skip(self) -> str:
        pc, o = self.prev_close, self.day_open
        if not pc or not o:
            return ""
        if not (PX_LO <= pc <= PX_HI):
            return f"株価{pc:,.0f}円（2,000〜10,000円の外）"
        if self.pre_high and (self.pre_high / pc - 1) * 100 >= GAP_MAX:
            return f"9:30までに前日比+{(self.pre_high / pc - 1) * 100:.1f}%（+3%以上は見送り）"
        if self.first15_close and (self.first15_close / o - 1) * 100 >= FIRST15_MAX:
            return f"最初の15分足が+{(self.first15_close / o - 1) * 100:.1f}%の大陽線（見送り）"
        return ""

    def _on_bar(self, b, ts: datetime, risk: Risk) -> list[dict]:
        ev = []
        self._push_close(b.c)
        bt = hm(b.t)
        if bt == "09:10":
            self.first15_close = b.c
        nxt = hm(b.t + timedelta(minutes=BAR_MIN))
        expired = False
        if self.line and not self.trade:                     # 前の足のラインは時間切れ
            self.line = None; self.line_bar = None; expired = True
            if self.status == "ライン点灯":
                self.status = "待機"
        if self.trade or self.skip:
            return ev
        if bt >= "09:25" and not self.skip:
            self.skip = self._day_skip()
            if self.skip:
                self.status = "見送り"; ev.append({"kind": "skip", "t": ts, "code": self.code, "why": self.skip}); return ev
        if not in_entry_window(nxt):
            if nxt > PM_LAST and self.status in ("待機",):
                self.status = "終了（13:00以降は入らない）"
                if expired:
                    ev.append({"kind": "cancel", "t": ts, "code": self.code, "why": "13:00で取消"})
            elif LAST_START < nxt < PM_START and expired:
                ev.append({"kind": "cancel", "t": ts, "code": self.code, "why": "11:30で取消（12:30から見直す）"})
            return ev
        r = risk.reason()
        if r:
            self.status = "本日終了"; self.skip = r; return ev
        m1, m0 = self.ma[-1], (self.ma[-2] if len(self.ma) >= 2 else None)
        if m1 is None or m0 is None:
            self.status = "待機（25MAの足が足りない）"; return ev
        width = (b.h / b.l - 1) * 100 if b.l > 0 else 99
        why = "25MAが下向き" if not m1 > m0 else ("陰線でない" if not b.c < b.o else ("安値が25MAの下" if not b.l > m1 else (f"陰線の幅{width:.2f}%が0.8%超" if width > WIDTH_MAX else "")))
        if m1 > m0 and b.c < b.o and b.l > m1 and width <= WIDTH_MAX:
            self.line = rnd(b.h + tick500(b.h)); self.line_bar = nxt; self.armed_at = bt; self.src = (b.l, b.h)
            self.status = "ライン点灯"; self.n_lines += 1
            ev.append({"kind": "rearm" if expired else "armed", "t": ts, "code": self.code, "line": self.line, "bar": nxt, "width": round(width, 2), "ma": rnd(m1)})
        else:
            self.status = "待機"
            if expired:
                ev.append({"kind": "cancel", "t": ts, "code": self.code, "why": f"次の足が{why}"})
        return ev

    def _on_tick(self, ts: datetime, last: float, risk: Risk) -> list[dict]:
        ev = []
        cur = self.bld.cur
        t = hm(ts)
        if self.line and not self.trade and cur is not None and hm(self.bld.cur_key) == self.line_bar:
            if cur["h"] >= self.line:                         # 逆指値が刺さった（寄りが上なら始値）
                entry = rnd(max(cur["o"], self.line))
                sh = shares_for(entry)
                self.trade = {"fill_time": t, "entry": entry, "entry_slip": rnd(entry * (1 + SLIP_PCT / 100)), "shares": sh, "sl": rtick(entry * (1 - SL_PCT / 100)),
                              "lo": last, "hi": last, "src_low": self.src[0], "src_high": self.src[1], "armed_at": self.armed_at,
                              "books": {f"{p:g}": {"tp": rtick(entry * (1 + p / 100)), "exit_type": None, "exit_time": None, "exit_price": None, "pnl_yen": None}
                                        for p in (TP_PCT, TP2_PCT)}}
                risk.n += 1
                self.status = "約定中"; self.line = None
                ev.append({"kind": "fill", "t": ts, "code": self.code, "entry": entry, "shares": sh})
                return ev
        tr = self.trade
        if tr and any(b["exit_type"] is None for b in tr["books"].values()):
            hi = max(last, cur["h"] if cur else last); lo = min(last, cur["l"] if cur else last)
            if t != tr["fill_time"]:                         # 約定した分の高安は約定前かもしれないので使わない
                tr["hi"] = max(tr["hi"], hi); tr["lo"] = min(tr["lo"], lo)
            for k, b in tr["books"].items():
                if b["exit_type"]:
                    continue
                if t >= FLAT_AT:
                    self._exit(b, "14:45手じまい", t, last)
                elif tr["lo"] <= tr["sl"]:
                    self._exit(b, "損切り", t, tr["sl"])
                elif tr["hi"] >= b["tp"]:
                    self._exit(b, "利確", t, b["tp"])
            main = tr["books"][f"{TP_PCT:g}"]
            if main["exit_type"] and self.status == "約定中":
                self.status = "決済済み"; risk.close(main["pnl_yen"])
                ev.append({"kind": "closed", "t": ts, "code": self.code, "exit": main["exit_type"], "price": main["exit_price"], "pnl": main["pnl_yen"]})
        return ev

    def _exit(self, b: dict, kind: str, t: str, px: float):
        b.update(exit_type=kind, exit_time=t, exit_price=rnd(px), pnl_yen=round((px - self.trade["entry"]) * self.trade["shares"]),
                 pnl_slip_yen=round((px - self.trade["entry_slip"]) * self.trade["shares"]))

    def log_row(self, day: str) -> dict:
        tr = self.trade or {}
        bk = tr.get("books", {})
        r = {"date": day, "code": self.code, "name": self.name, "armed_at": tr.get("armed_at", self.armed_at or ""),
             "origin": tr.get("src_low", ""), "high": tr.get("src_high", ""), "entry": tr.get("entry", ""), "stop": tr.get("sl", ""),
             "stop_kind": "−3%", "shares": tr.get("shares", ""), "max_loss": round(tr["entry"] * SL_PCT / 100 * tr["shares"]) if tr else "",
             "gap_pct": round((self.day_open / self.prev_close - 1) * 100, 2) if self.day_open and self.prev_close else "",
             "priority": self.rank, "overlap": "", "fill_time": tr.get("fill_time", ""), "skip_reason": self.skip, "entry_slip": tr.get("entry_slip", "")}
        for i, k in enumerate((f"{TP_PCT:g}", f"{TP2_PCT:g}"), 1):
            b = bk.get(k, {})
            r.update({f"exit{i}_type": b.get("exit_type") or "", f"exit{i}_time": b.get("exit_time") or "",
                      f"exit{i}_price": b.get("exit_price") or "", f"pnl{i}_yen": b.get("pnl_yen") if b.get("pnl_yen") is not None else "",
                      f"pnl{i}_slip_yen": b.get("pnl_slip_yen") if b.get("pnl_slip_yen") is not None else ""})
        return r


class RandomEngine:
    """比較用: 同じ銘柄を 9:30〜11:29 のランダムな1分（日付＋コードで固定）に買い、同じ出口（−3%／+1.5%・+3%／14:45）で紙を付ける。
    見送り（9:30の判定）は本線と同じ＝同じ銘柄日だけを比べる。1日の停止は掛けない。"""

    def __init__(self, day: str, code: str, name: str, rank: int):
        import random
        self.code, self.name, self.rank = code, name, rank
        k = random.Random(f"{day}-{code}").randrange(0, 150)           # 9:30〜11:29(120分) + 12:30〜12:59(30分) の中の1分
        m = (30 + k) if k < 120 else (12 * 60 + 30 + (k - 120)) - 9 * 60
        self.buy_at = f"{9 + m // 60:02d}:{m % 60:02d}"
        self.trade = None; self.last = None; self.skip = ""; self.day_open = None; self.prev_close = None

    def push(self, ts: datetime, v: list, main: "Engine"):
        last, opn, high, low, vol, tov, vwap, prev = (list(v) + [None] * 8)[:8]
        if not last:
            return
        t = hm(ts)
        if t < "09:00" or "11:30" < t < "12:30" or t > "15:30":
            return
        self.last = last; self.day_open = self.day_open or opn; self.prev_close = self.prev_close or prev
        if self.trade is None:
            if main.skip:
                self.skip = main.skip; return
            if self.buy_at <= t and (t <= "11:29" or "12:30" <= t <= "12:59") and main.prev_close:
                entry = rnd(last); sh = shares_for(entry)
                self.trade = {"fill_time": t, "entry": entry, "entry_slip": rnd(entry * (1 + SLIP_PCT / 100)), "shares": sh,
                              "sl": rtick(entry * (1 - SL_PCT / 100)), "lo": last, "hi": last, "src_low": "", "src_high": "", "armed_at": self.buy_at,
                              "books": {f"{p:g}": {"tp": rtick(entry * (1 + p / 100)), "exit_type": None, "exit_time": None, "exit_price": None, "pnl_yen": None}
                                        for p in (TP_PCT, TP2_PCT)}}
            return
        tr = self.trade
        if t == tr["fill_time"]:
            return
        tr["hi"] = max(tr["hi"], last); tr["lo"] = min(tr["lo"], last)
        for b in tr["books"].values():
            if b["exit_type"]:
                continue
            if t >= FLAT_AT:
                Engine._exit(self, b, "14:45手じまい", t, last)
            elif tr["lo"] <= tr["sl"]:
                Engine._exit(self, b, "損切り", t, tr["sl"])
            elif tr["hi"] >= b["tp"]:
                Engine._exit(self, b, "利確", t, b["tp"])

    def finish(self):
        if self.trade:
            for b in self.trade["books"].values():
                if b["exit_type"] is None and self.last:
                    Engine._exit(self, b, "14:45手じまい", FLAT_AT, self.last)

    log_row = Engine.log_row


def prev_bars(day: str, codes: set[str]) -> dict[str, list]:
    """前の営業日の minutes → 5分足（25MAを前日から続けるため）。休場日の断面は飛ばす"""
    from fibo_oct import is_stale_day
    from arena_watchlist import is_trading_day
    d = datetime.strptime(day, "%Y-%m-%d").date()
    pd_ = d - timedelta(days=1)
    while not is_trading_day(pd_):
        pd_ -= timedelta(days=1)
    p = MINUTES_DIR / f"{pd_.isoformat()}.jsonl"
    if not p.exists() or is_stale_day(p):              # 前の営業日の断面が無い（10/1のように巡回が止まった日）→ Yahoo 5分足で補う
        return yahoo_prev_bars(day, codes)
    rows, _ = read_minutes(p, 0)
    bld: dict[str, BarBuilder] = {}; out: dict[str, list] = {c: [] for c in codes}
    for row in rows:
        ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S"); t = hm(ts)
        if t < "09:00" or "11:30" < t < "12:30" or t > "15:30":
            continue
        if t in ("11:30", "15:30"):
            ts = ts.replace(second=0) - timedelta(seconds=1)
        for code in codes:
            v = row["s"].get(code)
            if not v:
                continue
            b = bld.setdefault(code, BarBuilder(BAR_MIN))
            bar = b.push(ts, v[0], v[2], v[3], v[4])
            if bar:
                out[code].append(bar)
    for code, b in bld.items():
        if b.cur is not None:
            from fibo_daytrade import Bar
            c = b.cur; out[code].append(Bar(b.cur_key, c["o"], c["h"], c["l"], c["c"], 0.0))
    print(f"[oshime5] 前日の5分足: {p.name} {sum(1 for v in out.values() if len(v) >= MA_N)}/{len(codes)}銘柄が25本以上", flush=True)
    return out


def yahoo_prev_bars(day: str, codes: set[str]) -> dict[str, list]:
    """前日までの Yahoo 5分足（直近60本）。取れない銘柄は空（その日は25MAが当日の足でそろうまで待つ）"""
    from fibo_oct import _yahoo_chart, _yahoo_bars
    out = {}
    for code in codes:
        try:
            bars = [b for b in _yahoo_bars(_yahoo_chart(code, "5m", "5d"), 5) if b.t.strftime("%Y-%m-%d") < day]
            out[code] = bars[-60:]
        except Exception as e:
            print(f"[oshime5] {code} Yahoo 5分足が取れない: {e}", flush=True)
            out[code] = []
    print(f"[oshime5] 前日の5分足: 前の営業日の断面が無いので Yahoo で補った {sum(1 for v in out.values() if len(v) >= MA_N)}/{len(codes)}銘柄", flush=True)
    return out


def send(text: str) -> bool:
    url = os.environ.get(WEBHOOK_ENV, "").strip()
    if not url:
        return False
    try:
        import requests
        return requests.post(url, json={"content": text[:1990]}, timeout=10).status_code in (200, 204)
    except Exception:
        return False


class Session:
    def __init__(self, day: str, cands: dict, notify: bool):
        self.day = day; self.notify = notify; self.risk = Risk(); self.cands = cands
        rows = cands.get("rows", [])
        pb = prev_bars(day, {r["code"] for r in rows})
        self.eng = {r["code"]: Engine(r["code"], r["name"], i + 1, pb.get(r["code"], [])) for i, r in enumerate(rows)}
        self.rnd = {r["code"]: RandomEngine(day, r["code"], r["name"], i + 1) for i, r in enumerate(rows)}   # 比較用の紙
        self.msgs: list[str] = []
        self.stopped_sent = False

    def feed(self, row: dict):
        ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
        skips = []
        for code, e in self.eng.items():
            v = row["s"].get(code)
            if v:
                for ev in e.push(ts, v, self.risk):
                    if ev["kind"] == "skip":
                        skips.append((e, ev["why"]))
                    else:
                        self._event(ev, e)
                self.rnd[code].push(ts, v, e)
        if skips:                                            # 9:30の見送りは1通にまとめる
            msg = f"⛔ {ts.strftime('%H:%M')} 見送り（今日は入らない）\n" + "\n".join(f"・{e.name}({e.code}) {why}" for e, why in skips)
            self._say(msg, None, ("skip",), ts)
        r = self.risk.reason()
        if r and not self.stopped_sent:
            self.stopped_sent = True
            self._say(f"⛔ {ts.strftime('%H:%M')} {r}\n紙 {self.risk.n}回 {self.risk.wins}勝{self.risk.losses}敗 {self.risk.pnl:+,.0f}円（新しいラインは出さない）", None, ("stopped",), ts)

    def _say(self, msg: str, e, key, ts):
        self.msgs.append(f"[{ts.strftime('%H:%M:%S')}] {msg}")
        sent = False
        if self.notify and (e is None or key not in e.notified):
            if e is not None:
                e.notified.add(key)
            sent = send(msg)
        print(f"[oshime5] {msg}{' Discord' if sent else ''}", flush=True)

    def _event(self, ev: dict, e: Engine):
        t = ev["t"].strftime("%H:%M")
        head = f"**{e.name}**({e.code})"
        if ev["kind"] in ("armed", "rearm"):
            tp, sl, sh = rtick(ev["line"] * (1 + TP_PCT / 100)), rtick(ev["line"] * (1 - SL_PCT / 100)), shares_for(ev["line"])
            icon = "🎯 準備" if ev["kind"] == "armed" else "🔁 置き直し"
            msg = (f"{icon} {t} {head} 逆指値の買い **{ev['line']:,g}円**（{ev['bar']}の足だけ有効）\n"
                   f"損切り {sl:,g}（−3%）／ 利確 {tp:,g}（+1.5%）／ {sh}株\n"
                   f"陰線の幅 {ev['width']}%・25MA {ev['ma']:,g} 上向き")
            key = (ev["kind"], ev["line"])
        elif ev["kind"] == "cancel":
            msg = f"✖ {t} {head} ライン取消（{ev['why']}）"; key = ("cancel", t)
        elif ev["kind"] == "fill":
            tr = e.trade
            msg = (f"✅ {t} {head} 約定想定 **{ev['entry']:,g}円** × {ev['shares']}株（紙）\n"
                   f"損切り {tr['sl']:,g}（−3%）／ 利確 {tr['books'][f'{TP_PCT:g}']['tp']:,g}（+1.5%）／ 14:45 手じまい")
            key = ("fill",)
        else:
            msg = (f"🏁 {t} {head} {ev['exit']} {ev['price']:,g}円 → **{ev['pnl']:+,}円**（紙）\n"
                   f"本日 {self.risk.n}回 {self.risk.wins}勝{self.risk.losses}敗 {self.risk.pnl:+,.0f}円")
            key = ("closed",)
        self._say(msg, e, key, ev["t"])

    def finish(self, log: bool):
        for e in self.eng.values():
            tr = e.trade
            if tr:
                for b in tr["books"].values():
                    if b["exit_type"] is None:
                        e._exit(b, "14:45手じまい", FLAT_AT, e.last)
                if e.status == "約定中":
                    e.status = "決済済み"; self.risk.close(tr["books"][f"{TP_PCT:g}"]["pnl_yen"])
        for r in self.rnd.values():
            r.finish()
        if log:
            for path, engs in ((LOG_CSV, self.eng.values()), (RANDOM_LOG_CSV, self.rnd.values())):
                new = not path.exists()
                with open(path, "a", newline="", encoding="utf-8-sig") as f:
                    w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
                    if new:
                        w.writeheader()
                    for e in engs:
                        if e.trade or e.skip:
                            w.writerow(e.log_row(self.day))

    def random_summary(self) -> dict:
        tr = [r.trade for r in self.rnd.values() if r.trade]
        k = f"{TP_PCT:g}"
        pnl = [t["books"][k]["pnl_yen"] for t in tr if t["books"][k]["pnl_yen"] is not None]
        return {"trades": len(pnl), "wins": sum(1 for x in pnl if x > 0), "pnl_yen": round(sum(pnl))}

    def json(self, now: datetime) -> dict:
        order = {"約定中": 0, "ライン点灯": 1, "待機": 2, "決済済み": 3}
        rows = sorted((e.row() for e in self.eng.values()), key=lambda r: (order.get(r["status"], 5), r["rank"]))
        rs = self.risk.reason()
        return {"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "date": self.day, "target_date": self.cands.get("target_date"),
                "rows": rows, "paper": {"trades": self.risk.n, "wins": self.risk.wins, "losses": self.risk.losses,
                                        "pnl_yen": round(self.risk.pnl), "stopped": rs is not None, "stop_reason": rs or ""},
                "random": self.random_summary(),
                "rules": {"width_max": WIDTH_MAX, "tp": TP_PCT, "sl": SL_PCT, "last_start": LAST_START, "pm": f"{PM_START}〜{PM_LAST}",
                          "stop": f"{STOP_CONSEC}連敗・1日{STOP_TRADES}回・{STOP_YEN:,}円で終了"},
                "note": "ラインは次の5分足のあいだだけ有効（足が変わったら置き直し/取り消し）。発注はしません。"}


def load_watch_txt(day: str) -> dict | None:
    """候補JSONが無い/古い日の手書きリスト oshime5_watch.txt（4桁コードを1行ずつ・#以降はメモ）"""
    if not WATCH_TXT.exists():
        return None
    codes = []
    for line in WATCH_TXT.read_text(encoding="utf-8").splitlines():
        c = line.split("#")[0].strip().replace(".T", "")
        if c.isdigit() and len(c) == 4 or (len(c) == 4 and c[:3].isdigit()):
            codes.append(c)
    if not codes:
        return None
    print(f"[oshime5] 手書きリスト {WATCH_TXT.name} の {len(codes)}銘柄を使う", flush=True)
    return {"date": "", "target_date": day, "rows": [{"code": c, "name": c, "close": None} for c in codes], "source": "watch.txt"}


def load_cands(day: str, path: Path | None) -> dict | None:
    p = path or CANDS_JSON
    c = None
    try:
        c = json.load(open(p, encoding="utf-8"))
    except Exception as e:
        print(f"[oshime5] 候補が読めない: {p} {e}", flush=True)
    if c is not None and path is None and str(c.get("target_date")) != day:
        print(f"[oshime5] 候補は {c.get('target_date')} 分（今日 {day} の分ではない）", flush=True); c = None
    if c is None and path is None:
        c = load_watch_txt(day)
        if c is None:
            print("[oshime5] 候補も手書きリストも無い → 何もしない", flush=True)
    return c


def write_json(ses: Session, now: datetime, path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(ses.json(now), ensure_ascii=False, default=str), encoding="utf-8")
    except Exception as e:
        print(f"[oshime5] json 書込失敗: {e}", flush=True)


def run_live(day: str | None = None):
    day = day or datetime.now().strftime("%Y-%m-%d")
    cands = load_cands(day, None)
    if not cands:
        return
    ses = Session(day, cands, notify=True)
    path = MINUTES_DIR / f"{day}.jsonl"
    print(f"[oshime5] {day} 監視開始 {len(ses.eng)}銘柄 {path}", flush=True)
    offset = 0
    while True:
        now = datetime.now()
        rows, offset = read_minutes(path, offset)
        for row in rows:
            ses.feed(row)
        write_json(ses, now, LIVE_JSON)
        if hm(now) >= LIVE_END:
            break
        time.sleep(LIVE_POLL_SEC)
    ses.finish(log=True); write_json(ses, datetime.now(), LIVE_JSON)
    p = ses.risk
    print(f"[oshime5] 終了 紙トレード{p.n}回 {p.wins}勝{p.losses}敗 {p.pnl:+,.0f}円 {p.reason() or ''}", flush=True)


def run_replay(day: str, cands_path: Path | None, log: bool, out: Path | None):
    cands = load_cands(day, cands_path)
    if not cands:
        return None
    ses = Session(day, cands, notify=False)
    rows, _ = read_minutes(MINUTES_DIR / f"{day}.jsonl", 0)
    for row in rows:
        ses.feed(row)
    ses.finish(log=log)
    now = datetime.strptime(rows[-1]["ts"], "%Y-%m-%d %H:%M:%S") if rows else datetime.now()
    if out:
        write_json(ses, now, out)
    rs = ses.random_summary()
    print(f"[oshime5] 再生 {day} 断面{len(rows)} 紙トレード{ses.risk.n}回 {ses.risk.wins}勝{ses.risk.losses}敗 {ses.risk.pnl:+,.0f}円"
          f" ／ ランダム買い{rs['trades']}回 {rs['wins']}勝 {rs['pnl_yen']:+,}円", flush=True)
    for e in ses.eng.values():
        tr = e.trade
        print(f"  {e.code} {e.name}: {e.status}{('・' + e.skip) if e.skip else ''}"
              + (f" 約定{tr['fill_time']} {tr['entry']:,g} → {tr['books'][f'{TP_PCT:g}']['exit_type']} {tr['books'][f'{TP_PCT:g}']['exit_price']:,g}"
                 f"（{tr['books'][f'{TP_PCT:g}']['pnl_yen']:+,}円）" if tr else "") + f" ライン{e.n_lines}回", flush=True)
    return ses


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--replay")
    ap.add_argument("--cands")
    ap.add_argument("--no-log", action="store_true")
    ap.add_argument("--out")
    a = ap.parse_args()
    if a.replay:
        run_replay(a.replay, Path(a.cands) if a.cands else None, not a.no_log, Path(a.out) if a.out else None)
    else:
        run_live()

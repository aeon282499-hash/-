# -*- coding: utf-8 -*-
"""
fibo_oct.py — ボスの「10月デイトレルール」専用のフィボ押し目判定・紙トレード（2026-10-01）

  定数は fibo_daytrade.py の「10月ルールの定数」欄。旧ルール（fibo_daytrade.WaveEngine）は消さずに残してあり、
  FIBO_RULESET=legacy で戻せる。発注はしない（通知・表示・記録だけ）。

ルール
  対象   買いのみ・38.2%の指値が2,000〜10,000円・寄りの窓6%未満（2%未満は「優先」）
  見送り 寄り付き後に高値更新なし／最初の1分足が大陰線かつ大出来高／38.2%が5分足75MAの下／5分足MAが下向きの並び(5<25<75)
  波     5分足で見る朝の波。起点=高値より前の最安値、高値=高値の5分足が終わり次の足が超えていない間は「確定」（超えたら引き直し）
         起点→高値が+4%以上・高値の足が11:00までに終わった波だけ
  入る   38.2%に指値1回だけ（ナンピンなし）
  重なり 38.2%の±0.5%以内に5分足25MA/75MA（2本=最優先）
  損切り 起点割れ と 建値−3% の近い方／株数=2万円÷(建値−逆指値)を100株単位で切り捨て・建玉130万円まで
  利確   +1% と +2% の両方を表示・紙トレードも両方記録（本線は fibo_oct_settings.json / FIBO_TP_PCT）
  時間   約定から30分で利確に届かなければ撤退・11:30で全決済
  停止   紙トレード（本線）で −4万円／2連敗／8回 のどれかで、その日は新しい候補を出さない

入力は「時刻つきの値動きの切れ端」（1分足1本、または立花の毎分断面1つ）。live と replay は同じ処理を通る。
"""
from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fibo_daytrade as FD
from fibo_daytrade import Bar, hm

MINUTES_DIR = FD.DATA_ROOT / "live_flow" / "minutes"
OCT_LOG_CSV = FD.ROOT / "fibo_oct_log.csv"
REPLAY_CACHE = FD.ROOT / "fibo_replay_cache"   # 再生用にYahooから取った足（日付つき・消してよい）


# ── 値の丸め・株数 ─────────────────────────────────────────
def tick_size(px: float) -> float:
    """呼値（一般の銘柄）。TOPIX500 の細かい呼値は使わない（表示用の目安）。"""
    for lim, t in ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3_000_000, 1000)):
        if px <= lim:
            return t
    return 5000


def round_tick(px: float) -> float:
    t = tick_size(px)
    return round(px / t) * t


def calc_shares(entry: float, stop: float) -> int:
    risk = entry - stop
    if risk <= 0:
        return 0
    n = int(FD.OCT_RISK_YEN / risk) // 100 * 100
    cap = int(FD.OCT_MAX_POSITION_YEN / entry) // 100 * 100
    return max(0, min(n, cap))


def key5(t: datetime) -> datetime:
    """観測時刻 t（足の終わり or 断面の時刻）が属する5分足の開始時刻"""
    u = t - timedelta(seconds=1)
    return u.replace(second=0, microsecond=0, minute=(u.minute // 5) * 5)


def _yen(v) -> str:
    return f"{v:,.0f}"


# ── 計画・建玉・1日の停止 ────────────────────────────────────
@dataclass
class Plan:
    code: str
    name: str
    day: str
    origin: float
    high: float
    level: float            # 38.2% の生の値
    entry: float            # 指値（呼値に丸め）
    stop: float
    stop_kind: str          # 起点割れ / −3%
    shares: int
    max_loss: float
    tps: dict               # {"1.0": 価格, "2.0": 価格}
    gap_pct: float
    priority: bool          # 窓2%未満
    overlap_items: list
    armed_at: str
    ma: dict = field(default_factory=dict)
    skips: list = field(default_factory=list)
    touched: bool = False   # 見送り/停止中のまま38.2%に触れた

    @property
    def overlap(self) -> int:
        return len(self.overlap_items)

    @property
    def overlap_label(self) -> str:
        return "あり・最優先（" + "・".join(self.overlap_items) + "）" if self.overlap >= 2 else ("あり（" + self.overlap_items[0] + "）" if self.overlap == 1 else "なし")


@dataclass
class Book:
    tp_pct: float
    tp: float
    exit_price: float | None = None
    exit_time: str | None = None
    exit_type: str | None = None
    pnl_yen: float | None = None


@dataclass
class Trade:
    plan: Plan
    fill_t: datetime
    books: dict             # {1.0: Book, 2.0: Book}
    stop: float
    last: float | None = None

    def open_books(self):
        return [b for b in self.books.values() if b.exit_type is None]


class DayRisk:
    """1日の停止: 本線の紙トレードで −4万円／2連敗／8回。回数は約定で数える。"""

    def __init__(self, tp_main: float):
        self.tp_main = tp_main
        self.n = 0
        self.consec = 0
        self.pnl = 0.0
        self.wins = 0
        self.losses = 0

    def stop_reason(self) -> str | None:
        if self.pnl <= FD.OCT_DAY_STOP_YEN:
            return f"紙の損益{self.pnl:+,.0f}円（−4万円）で停止"
        if self.consec >= FD.OCT_DAY_STOP_CONSEC:
            return f"{self.consec}連敗で停止"
        if self.n >= FD.OCT_DAY_STOP_TRADES:
            return f"{self.n}回で停止"
        return None

    def can_enter(self) -> tuple[bool, str]:
        r = self.stop_reason()
        return (r is None, r or "")

    def on_fill(self):
        self.n += 1

    def on_close(self, pnl_yen: float):
        self.pnl += pnl_yen
        if pnl_yen < 0:
            self.consec += 1; self.losses += 1
        else:
            self.consec = 0
            if pnl_yen > 0:
                self.wins += 1

    def summary(self) -> dict:
        r = self.stop_reason()
        return {"tp_pct": self.tp_main, "trades": self.n, "wins": self.wins, "losses": self.losses, "pnl_yen": round(self.pnl),
                "consec_loss": self.consec, "stopped": r is not None, "stop_reason": r or "",
                "limits": {"pnl_yen": FD.OCT_DAY_STOP_YEN, "consec": FD.OCT_DAY_STOP_CONSEC, "trades": FD.OCT_DAY_STOP_TRADES}}


# ── 判定エンジン（1銘柄・1日）───────────────────────────────
class OctEngine:
    def __init__(self, code: str, name: str, day: str, prev_close: float, prev_bars5: list[Bar],
                 prev_day_vol: float | None = None, day_open: float | None = None, first1m_known: bool = True,
                 risk: DayRisk | None = None, ignore_price_band: bool = False):
        self.code, self.name, self.day = code, name, day
        self.prev_close = prev_close
        self.closes = [b.c for b in prev_bars5][-200:]
        self.prev_day_vol = prev_day_vol
        self.open = day_open
        self.first1m_known = first1m_known
        self.risk = risk or DayRisk(FD.oct_tp_pct())
        self.ignore_price_band = ignore_price_band
        self.t0: datetime | None = None
        self.first1m: dict | None = None
        self.first_ref_high: float | None = None
        self.gap: float | None = None
        self.cur: list | None = None           # [key, o, h, l, c, v]
        self.high = self.high_key = self.origin = self.low_so_far = None
        self.state = "wait"                     # wait / armed / filled / done
        self.done_reason = ""
        self.out: str | None = None             # その日ずっと対象外の理由
        self.plan: Plan | None = None
        self.trade: Trade | None = None
        self.last: float | None = None
        self.events: list[str] = []
        self._sent: set = set()

    # 補助
    def _log(self, t: datetime, msg: str):
        self.events.append(f"{t.strftime('%H:%M:%S')} {msg}")

    def _ma(self, n: int) -> float | None:
        return sum(self.closes[-n:]) / n if len(self.closes) >= n else None

    def _done(self, t: datetime, why: str):
        if self.state != "done":
            self.state = "done"; self.done_reason = why; self._log(t, f"終了: {why}")

    # 入口
    def feed(self, t: datetime, o: float, h: float, l: float, c: float, dv: float | None = None) -> list[dict]:
        ev: list[dict] = []
        if not c or c <= 0:
            return ev
        o = o or c; h = max(h or c, c, o); l = min(l or c, c, o)
        self.last = c
        if self.t0 is None:
            self._first(t, o, h, l, c, dv)
        k = key5(t)
        if self.cur is not None and k > self.cur[0]:
            self.closes.append(self.cur[4]); self.cur = None
            if self.state == "wait" and not self.out:
                self._try_arm(t)
            elif self.state == "armed":
                self._refresh(t)
        if self.cur is None:
            self.cur = [k, o, h, l, c, dv or 0.0]
        else:
            self.cur[2] = max(self.cur[2], h); self.cur[3] = min(self.cur[3], l); self.cur[4] = c; self.cur[5] += dv or 0.0
        if self.out or self.state == "done":
            return ev
        if self.state == "filled":
            return self._track(t, o, h, l, c)
        # 押しの指値（約定を先に見るのは、この切れ端の始まりが高値より下だった時だけ）
        if self.state == "armed" and o <= self.high and l <= self.plan.entry:
            ev += self._touch(t, l)
            if self.state == "filled":
                return ev
        if h > self.high:
            if self.state == "armed":
                self._log(t, f"高値更新 {_yen(h)} → 指値取消・引き直し"); self.plan = None
            self.origin = min(self.low_so_far, o)
            self.high = h; self.high_key = k; self.state = "wait"
        if l < self.origin and self.state in ("wait", "armed"):
            if self.state == "armed":     # 高値確定前に起点を割るのは「起点が下がった」だけなので記録しない
                self._log(t, f"起点{_yen(self.origin)}割れ → 指値取消・波を引き直し"); self.plan = None
            self.origin = l; self.high = c; self.high_key = k; self.state = "wait"
        self.low_so_far = min(self.low_so_far, l)
        tod = hm(t)
        if self.state == "armed" and tod >= FD.OCT_ENTRY_DEADLINE:
            self._done(t, f"{FD.OCT_ENTRY_DEADLINE}までに38.2%に届かず")
        elif self.state == "wait" and tod >= FD.OCT_WAVE_DEADLINE and hm(self.high_key + timedelta(minutes=5)) > FD.OCT_WAVE_DEADLINE:
            self._done(t, f"{FD.OCT_WAVE_DEADLINE}までに波が確定せず")
        # 🟡押し目接近
        p = self.plan
        if self.state == "armed" and not p.skips and p.entry < c <= p.entry * (1 + FD.OCT_APPROACH_PCT / 100):
            key = ("approach", p.armed_at, p.entry)
            ok, _ = self.risk.can_enter()
            if ok and key not in self._sent:
                self._sent.add(key)
                self._log(t, f"🟡押し目接近 現在{_yen(c)}（38.2%={_yen(p.entry)}まで{(c / p.entry - 1) * 100:.1f}%）")
                ev.append({"kind": "approach", "t": t, "plan": p, "last": c})
        return ev

    def _first(self, t, o, h, l, c, dv):
        """最初の切れ端＝寄り直後の1分（立花の断面なら最初の断面）"""
        self.t0 = t
        if self.open is None:
            self.open = o
        self.gap = (self.open / self.prev_close - 1) * 100 if self.prev_close else 0.0
        self.high = h; self.high_key = key5(t); self.origin = min(self.open, l); self.low_so_far = self.origin
        pri = "（優先）" if self.gap < FD.OCT_GAP_PRIORITY_PCT else ""
        self._log(t, f"寄り {_yen(self.open)}（前日終値{_yen(self.prev_close)}・窓{self.gap:+.1f}%{pri}）")
        if self.gap >= FD.OCT_GAP_MAX_PCT:
            self.out = f"窓{self.gap:+.1f}%（{FD.OCT_GAP_MAX_PCT:.0f}%以上）"; self._log(t, "対象外: " + self.out); return
        if not self.first1m_known:
            self.first1m = {"verdict": "不明（寄り直後の足がデータに無い）"}
            self.first_ref_high = self.open
            self._log(t, "最初の1分足: 不明（寄り直後の足がデータに無い）→ 見送り判定なし・寄り値を基準にする")
            return
        self.first_ref_high = h
        drop = (c / self.open - 1) * 100
        avg1m = (self.prev_day_vol / 300) if self.prev_day_vol else None
        big_body = c < self.open and drop <= -FD.OCT_FIRST1M_DROP_PCT
        vol_x = (dv / avg1m) if (dv and avg1m) else None
        if big_body and vol_x is not None and vol_x >= FD.OCT_FIRST1M_VOL_X:
            verdict = "大陰線＋大出来高"
        elif big_body:
            verdict = "大陰線（出来高は" + (f"{vol_x:.1f}倍で基準未満" if vol_x is not None else "不明") + "）"
        else:
            verdict = "問題なし"
        self.first1m = {"o": self.open, "h": h, "l": l, "c": c, "v": dv, "vol_x": vol_x, "verdict": verdict}
        self._log(t, f"最初の1分足: {_yen(self.open)}→{_yen(c)}（{drop:+.1f}%・出来高{('%.1f倍' % vol_x) if vol_x is not None else '不明'}）{verdict}")
        if verdict == "大陰線＋大出来高":
            self.out = "最初の1分足が大陰線＋大出来高"; self._log(t, "対象外: " + self.out)

    def _skips(self, entry: float, ma5, ma25, ma75, shares: int) -> list[str]:
        s = []
        if not self.ignore_price_band and not (FD.OCT_PRICE_MIN <= entry <= FD.OCT_PRICE_MAX):
            s.append(f"株価{_yen(entry)}円（{FD.OCT_PRICE_MIN:,}〜{FD.OCT_PRICE_MAX:,}円の外）")
        if self.first_ref_high is not None and self.high <= self.first_ref_high:
            s.append("寄り付き後に高値更新なし")
        if ma75 is not None and entry < ma75 * (1 - FD.OCT_OVERLAP_TOL_PCT / 100):
            s.append(f"5分足75MA({_yen(ma75)})の下")
        if None not in (ma5, ma25, ma75) and ma5 < ma25 < ma75:
            s.append("5分足MAが下向きの並び")
        if shares <= 0:
            s.append("株数0（損切り幅が広すぎる）")
        return s

    def _overlap(self, level: float, ma25, ma75) -> list[str]:
        tol = level * FD.OCT_OVERLAP_TOL_PCT / 100
        return [nm for nm, v in (("25MA", ma25), ("75MA", ma75)) if v is not None and abs(v - level) <= tol]

    def _try_arm(self, t: datetime):
        if self.high_key >= key5(t):
            return
        if hm(self.high_key + timedelta(minutes=5)) > FD.OCT_WAVE_DEADLINE:
            self._done(t, f"高値の確定が{FD.OCT_WAVE_DEADLINE}を過ぎた"); return
        rise = (self.high / self.origin - 1) * 100
        if rise < FD.OCT_WAVE_MIN_PCT:
            return
        level = self.high - FD.OCT_FIB_ENTRY * (self.high - self.origin)
        entry = round_tick(level)
        s_org = self.origin - tick_size(self.origin); s_pct = entry * (1 - FD.OCT_STOP_PCT / 100)
        stop = round_tick(max(s_org, s_pct)); kind = "起点割れ" if s_org >= s_pct else f"−{FD.OCT_STOP_PCT:.0f}%"
        shares = calc_shares(entry, stop)
        ma5, ma25, ma75 = self._ma(FD.OCT_MA_SHORT), self._ma(FD.OCT_MA_MID), self._ma(FD.OCT_MA_LONG)
        self.plan = Plan(code=self.code, name=self.name, day=self.day, origin=self.origin, high=self.high, level=level, entry=entry,
                         stop=stop, stop_kind=kind, shares=shares, max_loss=round(shares * (entry - stop)),
                         tps={str(p): round_tick(entry * (1 + p / 100)) for p in FD.OCT_TP_PCTS},
                         gap_pct=round(self.gap, 2), priority=self.gap < FD.OCT_GAP_PRIORITY_PCT,
                         overlap_items=self._overlap(level, ma25, ma75), armed_at=hm(t),
                         ma={"ma5": ma5, "ma25": ma25, "ma75": ma75}, skips=self._skips(entry, ma5, ma25, ma75, shares))
        self.state = "armed"
        p = self.plan
        self._log(t, f"高値確定 {_yen(self.high)}（起点{_yen(self.origin)}→+{rise:.1f}%）→ 38.2%={_yen(level)} 指値{_yen(entry)} 逆指値{_yen(stop)}（{kind}）"
                     f" {shares}株・最大損失{_yen(p.max_loss)}円・重なり{p.overlap_label}"
                     f"・5分足MA 5/25/75={'/'.join(_yen(x) if x else '—' for x in (ma5, ma25, ma75))}"
                     + (f" ／見送り: {'・'.join(p.skips)}" if p.skips else ""))

    def _refresh(self, t: datetime):
        """指値待ちの間、5分足が1本終わるたびにMAの条件を見直す"""
        p = self.plan
        ma5, ma25, ma75 = self._ma(FD.OCT_MA_SHORT), self._ma(FD.OCT_MA_MID), self._ma(FD.OCT_MA_LONG)
        new = self._skips(p.entry, ma5, ma25, ma75, p.shares)
        p.overlap_items = self._overlap(p.level, ma25, ma75); p.ma = {"ma5": ma5, "ma25": ma25, "ma75": ma75}
        if new != p.skips:
            self._log(t, "見送り条件: " + ("・".join(new) if new else "なし（解除）"))
            p.skips = new

    def _touch(self, t: datetime, low: float) -> list[dict]:
        p = self.plan
        if p.skips or not self.risk.can_enter()[0]:
            if not p.touched:
                p.touched = True
                why = "・".join(p.skips) if p.skips else self.risk.can_enter()[1]
                self._log(t, f"38.2%({_yen(p.entry)})に到達・見送り（{why}）")
            return []
        self.trade = Trade(plan=p, fill_t=t, books={pct: Book(pct, p.tps[str(pct)]) for pct in FD.OCT_TP_PCTS}, stop=p.stop)
        self.state = "filled"
        self.risk.on_fill()
        self._log(t, f"🟢約定 {_yen(p.entry)} × {p.shares}株（逆指値{_yen(p.stop)}・利確+1% {_yen(p.tps['1.0'])}／+2% {_yen(p.tps['2.0'])}）")
        ev = [{"kind": "zone", "t": t, "plan": p, "last": p.entry}]
        if low <= p.stop:    # 同じ切れ端で逆指値まで届いていたら損切り（保守的）
            ev += self._exit_all(t, p.stop, "損切り")
        return ev

    def _exit(self, t: datetime, b: Book, px: float, kind: str):
        p = self.plan
        b.exit_price = px; b.exit_time = t.strftime("%H:%M"); b.exit_type = kind; b.pnl_yen = round((px - p.entry) * p.shares)
        self._log(t, f"決済[+{b.tp_pct:g}%]: {kind} {_yen(px)}（{b.pnl_yen:+,}円）")

    def _exit_all(self, t, px, kind) -> list[dict]:
        for b in self.trade.open_books():
            self._exit(t, b, px, kind)
        return self._closed(t)

    def _closed(self, t) -> list[dict]:
        if self.trade.open_books():
            return []
        self.state = "done"; self.done_reason = "決済済み"
        main = self.trade.books.get(self.risk.tp_main) or list(self.trade.books.values())[0]
        self.risk.on_close(main.pnl_yen)
        return [{"kind": "closed", "t": t, "plan": self.plan, "trade": self.trade}]

    def _track(self, t, o, h, l, c) -> list[dict]:
        tr = self.trade; tr.last = c
        for b in tr.open_books():
            if l <= tr.stop:
                self._exit(t, b, tr.stop, "損切り")
            elif t > tr.fill_t and h >= b.tp:
                self._exit(t, b, b.tp, "利確")
            elif t >= tr.fill_t + timedelta(minutes=FD.OCT_TIME_STOP_MIN):
                self._exit(t, b, c, "30分撤退")
            elif hm(t) >= FD.OCT_FLAT_AT:
                self._exit(t, b, c, "11:30決済")
        return self._closed(t)

    # 画面・記録用
    def status(self) -> str:
        p = self.plan
        if self.out or (p and any(x.startswith("株価") for x in p.skips) and not self.ignore_price_band):
            return "対象外"
        if self.state == "filled":
            return "約定中"
        if self.trade:
            return "決済済み"
        if self.state == "armed":
            p = self.plan
            if p.skips:
                return "見送り"
            if self.last is not None and self.last <= p.entry * (1 + FD.OCT_APPROACH_PCT / 100):
                return "接近"
            return "指値待ち"
        if self.state == "done" and p and (p.skips or p.touched):
            return "見送り"
        return "終了" if self.state == "done" else "波待ち"

    def to_row(self) -> dict | None:
        p = self.plan or (self.trade.plan if self.trade else None)
        if p is None:
            return None
        d = {"code": self.code, "name": self.name, "status": self.status(), "last": self.last,
             "entry": p.entry, "stop": p.stop, "stop_kind": p.stop_kind, "shares": p.shares, "max_loss": p.max_loss,
             "tp1": p.tps["1.0"], "tp2": p.tps["2.0"], "gap": p.gap_pct, "priority": p.priority,
             "overlap": p.overlap, "overlap_items": p.overlap_items, "skip_reason": "・".join(p.skips),
             "origin": round(p.origin, 1), "high": round(p.high, 1), "armed_at": p.armed_at,
             # 今の🔥場中ライブの📐欄（旧画面）がそのまま読める項目
             "label": "優先" if p.priority else "通常", "rank": "-", "rise": round((p.high / p.origin - 1) * 100, 1), "mins": None, "vol_ratio": None,
             "fib": {"38.2": round(p.level, 1), "50": round(p.high - .5 * (p.high - p.origin), 1), "61.8": round(p.high - .618 * (p.high - p.origin), 1),
                     "78.6": round(p.high - .786 * (p.high - p.origin), 1)},
             "pull_low": None, "retrace": None, "note": (self.events[-1][9:69] if self.events else ""), "signal": None}
        d["status_legacy"] = {"約定中": "entered", "決済済み": "entered", "見送り": "skip", "対象外": "skip", "終了": "skip"}.get(d["status"], "watch")
        if self.trade:
            d["trade"] = {"fill_time": self.trade.fill_t.strftime("%H:%M"),
                          "books": {f"{k:g}": asdict(b) for k, b in self.trade.books.items()}}
        return d


def sort_key(row: dict):
    """優先度順: 重なり2本→1本→なし、同じなら窓2%未満を上。見送り・対象外は下。"""
    bad = row["status"] in ("見送り", "対象外", "終了")
    return (bad, -row["overlap"], 0 if row["priority"] else 1, row["code"])


# ── 通知（Discord: 2種類だけ）──────────────────────────────
def message(kind: str, p: Plan, last: float | None, tp_main: float) -> dict:
    head = "🟡 押し目接近" if kind == "approach" else "🟢 指値ゾーン（38.2%到達）"
    lines = [
        f"**38.2%の指値 {_yen(p.entry)}円**　逆指値 {_yen(p.stop)}円（{p.stop_kind}）",
        f"株数 {p.shares:,}株　最大損失 {_yen(p.max_loss)}円",
        f"利確 +1% {_yen(p.tps['1.0'])}円 ／ +2% {_yen(p.tps['2.0'])}円（本線 +{tp_main:g}%）",
        f"窓 {p.gap_pct:+.1f}%{'（優先）' if p.priority else ''}　重なり {p.overlap_label}",
    ]
    if kind == "approach" and last:
        lines.append(f"現在 {_yen(last)}円（38.2%まで あと{(last / p.entry - 1) * 100:.1f}%）")
    lines.append("約定から30分で撤退・11:30で全決済。発注はしません（通知のみ）")
    return {"embeds": [{"title": f"{head}　{p.name}（{p.code}）", "description": "\n".join(lines),
                        "color": 0xF1C40F if kind == "approach" else 0x2ECC71,
                        "footer": {"text": f"10月ルール・{p.day} {p.armed_at}高値確定（起点{_yen(p.origin)}→高値{_yen(p.high)}）"}}]}


def message_text(payload: dict) -> str:
    e = payload["embeds"][0]
    return f"{e['title']}\n{e['description']}\n（{e['footer']['text']}）"


def send_discord(payload: dict) -> bool:
    url = os.getenv("DISCORD_WEBHOOK_FIBO_URL", "").strip()
    if not url:
        return False
    try:
        import requests
        return requests.post(url, json=payload, timeout=15).status_code < 300
    except Exception:
        return False


# ── 記録（CSV）───────────────────────────────────────────
LOG_FIELDS = ["date", "code", "name", "armed_at", "origin", "high", "entry", "stop", "stop_kind", "shares", "max_loss", "gap_pct", "priority",
              "overlap", "fill_time", "exit1_type", "exit1_time", "exit1_price", "pnl1_yen", "exit2_type", "exit2_time", "exit2_price", "pnl2_yen", "skip_reason"]


def log_csv(p: Plan, trade: Trade | None = None, path: Path = OCT_LOG_CSV):
    new = not path.exists()
    row = {"date": p.day, "code": p.code, "name": p.name, "armed_at": p.armed_at, "origin": round(p.origin, 1), "high": round(p.high, 1),
           "entry": p.entry, "stop": p.stop, "stop_kind": p.stop_kind, "shares": p.shares, "max_loss": p.max_loss, "gap_pct": p.gap_pct,
           "priority": int(p.priority), "overlap": "|".join(p.overlap_items), "skip_reason": "・".join(p.skips)}
    if trade:
        row["fill_time"] = trade.fill_t.strftime("%H:%M")
        for i, pct in enumerate(FD.OCT_TP_PCTS, 1):
            b = trade.books[pct]
            row.update({f"exit{i}_type": b.exit_type, f"exit{i}_time": b.exit_time, f"exit{i}_price": b.exit_price, f"pnl{i}_yen": b.pnl_yen})
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


# ── 立花の毎分断面（live_flow/minutes）──────────────────────
def read_rows(path: Path, offset: int = 0) -> tuple[list[dict], int]:
    rows = []
    if not path.exists():
        return rows, offset
    with open(path, "r", encoding="utf-8") as f:
        f.seek(offset)
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
        offset = f.tell()
    return rows, offset


def prev_minutes_context(day: str, codes: set | None = None, n_days: int = 2) -> dict:
    """前の営業日（最大2日）の断面 → {code: {"bars": [5分足], "prev_vol": 前日出来高}}。MA75 を前日から連続で作るため。"""
    from fibo_live import BarBuilder
    d = datetime.strptime(day, "%Y-%m-%d").date()
    files = [p for p in (MINUTES_DIR / f"{(d - timedelta(days=k)).isoformat()}.jsonl" for k in range(1, 10)) if p.exists()][:n_days]
    out: dict[str, dict] = {}
    for i, p in enumerate(reversed(files)):          # 古い日から
        rows, _ = read_rows(p)
        bld: dict[str, BarBuilder] = {}
        vol: dict[str, float] = {}
        for row in rows:
            ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
            for code, v in row["s"].items():
                if codes and code not in codes:
                    continue
                b = bld.setdefault(code, BarBuilder())
                bar = b.push(ts, v[0], v[2], v[3], v[4])
                if bar:
                    out.setdefault(code, {"bars": [], "prev_vol": None})["bars"].append(bar)
                if v[4]:
                    vol[code] = v[4]
        for code, b in bld.items():                    # 最後の足を閉じる
            if b.cur is not None:
                c = b.cur
                out.setdefault(code, {"bars": [], "prev_vol": None})["bars"].append(Bar(b.cur_key, c["o"], c["h"], c["l"], c["c"], max(0.0, b.last_vol - b.vol_at_bar_start)))
        if i == len(files) - 1:
            for code, v in vol.items():
                out.setdefault(code, {"bars": [], "prev_vol": None})["prev_vol"] = v
    return out


class OctSession:
    """断面を時刻順に流して、全銘柄のエンジン・1日の停止・通知・画面用JSON をまとめる（live と replay で共通）"""

    def __init__(self, day: str, names: dict | None = None, codes: set | None = None, notify: bool = False,
                 tp_main: float | None = None, ignore_price_band: bool = False, min_tov: float = 1e8, log_path: Path | None = OCT_LOG_CSV):
        self.day = day
        self.names = names or {}
        self.codes = codes
        self.notify = notify
        self.tp_main = tp_main if tp_main is not None else FD.oct_tp_pct()
        self.risk = DayRisk(self.tp_main)
        self.ignore_price_band = ignore_price_band
        self.min_tov = min_tov
        self.log_path = log_path
        self.engines: dict[str, OctEngine] = {}
        self.snap: dict[str, tuple] = {}
        self.prev = prev_minutes_context(day, codes)
        self.messages: list[str] = []

    def feed_row(self, row: dict):
        ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
        if hm(ts) < "09:00":
            return
        for code, v in row["s"].items():
            if self.codes and code not in self.codes:
                continue
            last, opn, high, low, vol, tov, vwap, prev = (list(v) + [None] * 8)[:8]
            if not last or not prev:
                continue
            eng = self.engines.get(code)
            if eng is None:
                if self.min_tov and (tov or 0) < self.min_tov and hm(ts) > "09:30":
                    continue
                pv = self.prev.get(code, {})
                eng = OctEngine(code, self.names.get(code, code), self.day, float(prev), pv.get("bars", []), pv.get("prev_vol"),
                                day_open=opn, risk=self.risk, ignore_price_band=self.ignore_price_band)
                self.engines[code] = eng
            ps = self.snap.get(code)
            if ps is None:
                o, h, l, dv = (opn or last), (high or last), (low or last), vol
            else:
                pl, ph, plo, pvol = ps
                o = pl; h = max(pl, last); l = min(pl, last)
                if high and ph and high > ph:
                    h = max(h, high)
                if low and plo and low < plo:
                    l = min(l, low)
                dv = (vol - pvol) if (vol is not None and pvol is not None) else None
            self.snap[code] = (last, high, low, vol)
            for e in eng.feed(ts, o, h, l, last, dv):
                self._event(e)

    def _event(self, e: dict):
        if e["kind"] in ("approach", "zone"):
            payload = message(e["kind"], e["plan"], e.get("last"), self.tp_main)
            self.messages.append(f"[{e['t'].strftime('%H:%M:%S')}] " + message_text(payload))
            sent = send_discord(payload) if self.notify else False
            print(f"[oct] {e['t'].strftime('%H:%M:%S')} {e['kind']} {e['plan'].name}({e['plan'].code}) 指値{e['plan'].entry} Discord={'OK' if sent else '-'}", flush=True)
        elif e["kind"] == "closed" and self.log_path:
            log_csv(e["plan"], e["trade"], self.log_path)

    def finish(self):
        """引け: 見送りのまま38.2%に触れた候補も記録（採否の検証用）"""
        if not self.log_path:
            return
        for eng in self.engines.values():
            if eng.plan and eng.plan.touched and not eng.trade:
                log_csv(eng.plan, None, self.log_path)

    def live_json(self, now: datetime) -> dict:
        rows = [r for r in (e.to_row() for e in self.engines.values()) if r and r["status"] != "対象外"]
        rows.sort(key=sort_key)
        return {"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "ruleset": "oct", "tp_main": self.tp_main,
                "n_watch": sum(1 for r in rows if r["status"] in ("指値待ち", "接近")),
                "candidates": [dict(r, status=r["status_legacy"], status_jp=r["status"]) for r in rows[:40]],
                "paper": self.risk.summary(),
                "trades": [{"code": r["code"], "name": r["name"], "entry": r["entry"], "stop": r["stop"], "half": False, "last": r["last"],
                            "closed": r["status"] == "決済済み",
                            "exit_type": (r.get("trade", {}).get("books", {}).get(f"{self.tp_main:g}", {}) or {}).get("exit_type"),
                            "pnl_pct": None} for r in rows if r.get("trade")],
                "note": "10月ルール（紙・通知のみ・発注はしない）"}


# ── Yahoo（APIキー不要）で再生する時のデータ ─────────────────
def _yahoo_chart(code: str, interval: str, rng: str) -> dict:
    REPLAY_CACHE.mkdir(parents=True, exist_ok=True)
    p = REPLAY_CACHE / f"{code}_{interval}_{rng}_{datetime.now():%Y%m%d}.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    try:
        import truststore
        truststore.inject_into_ssl()      # ウイルス対策のSSL検査を越える
    except Exception:
        pass
    import requests
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}.T?range={rng}&interval={interval}"
    for a in range(3):
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if r.status_code == 429:
            time.sleep(5 * (a + 1)); continue
        res = r.json()["chart"]["result"][0]
        q = res["indicators"]["quote"][0]
        out = {"t": res.get("timestamp") or [], **{k: q.get(k) or [] for k in ("open", "high", "low", "close", "volume")}}
        p.write_text(json.dumps(out), encoding="utf-8")
        return out
    raise RuntimeError(f"Yahoo {code} {interval}: 取得できません")


def _yahoo_bars(j: dict, minutes: int) -> list[Bar]:
    out = []
    for t, o, h, l, c, v in zip(j["t"], j["open"], j["high"], j["low"], j["close"], j["volume"]):
        if None in (o, h, l, c):
            continue
        ts = datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
        out.append(Bar(ts, float(o), float(h), float(l), float(c), float(v or 0)))
    return out


def yahoo_day(code: str, day: str) -> dict:
    """1分足（その日）・5分足（前2営業日＝MA用）・日足（寄り値・前日終値・前日出来高）"""
    d1 = _yahoo_chart(code, "1d", "3mo")
    days = [(datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None) + timedelta(hours=9)).strftime("%Y-%m-%d") for t in d1["t"]]
    if day not in days:
        raise RuntimeError(f"{code}: {day} の日足が無い")
    i = days.index(day)
    day_open, prev_close, prev_vol = d1["open"][i], d1["close"][i - 1], d1["volume"][i - 1]
    m1 = [b for b in _yahoo_bars(_yahoo_chart(code, "1m", "7d"), 1) if b.t.strftime("%Y-%m-%d") == day]
    m5 = _yahoo_bars(_yahoo_chart(code, "5m", "60d"), 5)
    prev_days = sorted({b.t.strftime("%Y-%m-%d") for b in m5 if b.t.strftime("%Y-%m-%d") < day})[-2:]
    prev_bars = [b for b in m5 if b.t.strftime("%Y-%m-%d") in prev_days]
    return {"day_open": day_open, "prev_close": prev_close, "prev_vol": prev_vol, "m1": m1, "prev_bars": prev_bars}


# ── 再生（1銘柄ずつ・時系列を表示）──────────────────────────
def replay_codes(day: str, codes: list[str], src: str = "auto", names: dict | None = None, ignore_price_band: bool = False,
                 tp_main: float | None = None):
    names = names or {}
    tp_main = tp_main if tp_main is not None else FD.oct_tp_pct()
    mpath = MINUTES_DIR / f"{day}.jsonl"
    want = set(codes)
    have_min: set = set()
    rows: list[dict] = []
    if src in ("auto", "minutes") and mpath.exists():
        rows, _ = read_rows(mpath)
        have_min = {c for r in rows[-5:] for c in r["s"] if c in want}
    results = {}
    for code in codes:
        use = "minutes" if (src != "yahoo" and code in have_min) else "yahoo"
        if src == "minutes" and use != "minutes":
            print(f"\n■ {code}: {day} の立花断面に無い"); continue
        risk = DayRisk(tp_main)
        if use == "minutes":
            ses = OctSession(day, names, {code}, notify=False, tp_main=tp_main, ignore_price_band=ignore_price_band, min_tov=0, log_path=None)
            ses.risk = risk
            for r in rows:
                ses.feed_row(r)
            eng = ses.engines.get(code); msgs = ses.messages
            srcnote = f"立花の毎分断面（live_flow/minutes/{day}.jsonl・{len(rows)}断面）＋前日断面でMA"
        else:
            y = yahoo_day(code, day)
            first_ok = bool(y["m1"]) and abs(y["m1"][0].o - y["day_open"]) < 1e-6
            eng = OctEngine(code, names.get(code, code), day, y["prev_close"], y["prev_bars"], y["prev_vol"], day_open=y["day_open"],
                            first1m_known=first_ok, risk=risk, ignore_price_band=ignore_price_band)
            msgs = []
            for b in y["m1"]:
                for e in eng.feed(b.t + timedelta(minutes=1), b.o, b.h, b.l, b.c, b.v or None):
                    if e["kind"] in ("approach", "zone"):
                        msgs.append(f"[{e['t'].strftime('%H:%M')}] " + message_text(message(e["kind"], e["plan"], e.get("last"), tp_main)))
            srcnote = (f"Yahoo 1分足（{y['m1'][0].t:%H:%M}〜 {len(y['m1'])}本）＋日足の寄り値{y['day_open']:,.0f}・前日終値{y['prev_close']:,.0f}＋前2日の5分足でMA"
                       if y["m1"] else "Yahoo 1分足なし")
        results[code] = eng
        nm = eng.name if eng else code
        print(f"\n■ {nm}（{code}）{day}  データ: {srcnote}")
        if eng is None:
            print("  （断面なし）"); continue
        for e in eng.events:
            print("  " + e)
        if eng.trade:
            print("  → 紙トレード結果: " + " ／ ".join(f"+{k:g}%利確なら {b.exit_type} {_yen(b.exit_price)}（{b.exit_time}・{b.pnl_yen:+,}円）"
                                                    for k, b in eng.trade.books.items()))
        elif eng.out:
            print(f"  → 対象外: {eng.out}")
        else:
            print(f"  → 約定なし（{eng.status()}{('・' + eng.done_reason) if eng.done_reason else ''}）")
        if msgs:
            print("  ── 送られる通知（再生なので送信はしない）──")
            for m in msgs:
                print("    " + m.replace("\n", "\n    "))
    return results

# -*- coding: utf-8 -*-
"""
fibo_daytrade.py — デイトレ用「フィボ押し目反発」通知・記録・再生（2026-09-17 本人指示書・設計 _design_fibo_daytrade_0917.md）

  発注はしない。通知（Discord）と記録（CSV/JSON）のみ。スイング用ロジックには一切触れない。

モード
  python fibo_daytrade.py --replay 2026-09-16 --codes 6208,338A          # 保存5分足(yfinance)で再生・判定の時系列を表示
  python fibo_daytrade.py --replay 2026-09-16 [--min-tov 3]              # 全銘柄再生（前日代金≥3億）→ fibo_signals/ に保存
  python fibo_daytrade.py --replay-range 2026-06-24 2026-09-16           # 期間再生（採否判定用）
  python fibo_daytrade.py --report                                       # ランク別/ラベル別/スコア別の集計
  python fibo_daytrade.py --live                                         # 場中（LiveFlowの毎分断面から5分足を作って判定・P1で実装）

ルール（指示書→実装。定数は下に集約）
  起点=寄り後最安値（GU=寄り≥前日終値+1%は前日終値・+10%超は除外）。初動ランクA/B/C/警戒/見送り。出来高3倍（前日同時間帯比・無ければ判定保留）。
  高値確定=高値足の次の5分足が高値未更新かつ陰線。高値更新で引き直し・起点割れで破棄・その日最初の波だけ・11:30以降の初波は対象外。
  エントリー=押し38.2〜61.8停止 × 押し出来高<上昇出来高 × 中期MA上向きかつ価格上 × MA5上抜け足の確定 × RCI9が-80以下から上向き。
  重なりスコア=±0.5%内の VWAP/中期MA(上向き)/前日高値/前日終値/価格帯出来高の山/キリ番/寄り足高値(上抜け済み) → 3点以上=本命。
  決済=損切り78.6%線（-3%より遠ければ不可）／+3%で半分→建値ストップ→+5%か127.2%の近い方／+4〜6%は直近高値手前でRR<1.5見送り／+4%未満見送り／30分無反応撤退／14:30以降新規なし・15:00全決済。
  MA/RCIは前日の足から連続で計算（チャートツールと同じ）。9時台は中期MA=15本、10時以降=25本。

データ
  再生: _intraday_cache_5m.pkl（yfinance 5分足・足ラベル=開始時刻・9:00足は出来高0の気配足のことがある→出来高0の先頭足は捨てる）
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import pickle
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CACHE_5M = ROOT / "_intraday_cache_5m.pkl"
SIG_DIR = ROOT / "fibo_signals"          # 日別の全判定（見送り含む）JSON
LOG_CSV = ROOT / "fibo_daytrade_log.csv"  # 指示書9の記録
STATE_JSON = ROOT / "fibo_state.json"     # HALF_LOT_MODE のトレード数など

# ── 定数（指示書）──────────────────────────────────────────
GAP_UP_PCT = 1.0            # 寄り≥前日終値+1% を「ギャップアップ」扱い（起点=前日終値）
GAP_EXCLUDE_PCT = 10.0      # ギャップ+10%超は除外
VOL_RATIO_MIN = 3.0         # 出来高倍率（前日同時間帯比）3倍以上を候補条件
RANK_A = (15, 5.0, 10.0)    # 15分以内に+5〜10%
RANK_B = (30, 5.0, 8.0)     # 15〜30分で+5〜8%
RANK_C = (60, 6.0)          # 30〜60分で+6%（中期MA上向き必須）
RANK_WARN = (5, 10.0)       # 5分以内に+10%超＝警戒
RANK_SKIP = (60, 4.0)       # 60分経過で+4%未満＝見送り
FIRST_WAVE_DEADLINE = "11:30"   # これ以降に初めて出た波は対象外
NO_NEW_AFTER = "14:30"
FLAT_ALL_AT = "15:00"
FIB_ZONE = (0.382, 0.618)   # 押しの停止ゾーン
FIB_STOP = 0.786            # 損切り線
FIB_TP2 = 1.272
STOP_MAX_PCT = 3.0          # 損切りが-3%より遠いならエントリー不可
TP1_PCT = 3.0               # +3%で半分
TP2_PCT = 5.0               # 残りは+5%か127.2%の近い方
RR_MIN_SMALL = 1.5          # 初動+4〜6%はRR1.5未満見送り
RISE_MIN = 4.0              # 初動+4%未満は見送り
RISE_NORMAL = 6.0           # +6%以上は通常ルール
TIME_STOP_MIN = 30          # エントリー後30分動かなければ撤退（+1%未満）
OVERLAP_TOL_PCT = 0.5       # 重なり判定 ±0.5%
OVERLAP_MAIN = 3            # 3点以上=本命
MA_SHORT, MA_MID_AM, MA_MID = 5, 15, 25
MA_SLOPE_BARS = 5           # 中期MAの傾き＝直近5本
RCI_N, RCI_OVERSOLD = 9, -80.0
SOFT_SKIPS = os.environ.get("FIBO_SOFT_SKIPS") == "1"   # 診断用: 中期MA向き/押し出来高/戻り高値切り下げを確定見送りにしない
# リスク管理（既存に無いので新規）
MAX_CONSEC_LOSS = 2         # 2連敗で当日停止
COOLDOWN_MIN = 30           # 負け後30分クールダウン
HALF_LOT_TRADES = 10        # 最初の10トレードは半ロット
LOT_FULL = 100              # 表示用の想定ロット（株）
JST_OPEN, JST_CLOSE = "09:00", "15:00"


def hm(t: datetime) -> str:
    return t.strftime("%H:%M")


def minutes_between(a: datetime, b: datetime) -> int:
    return int((b - a).total_seconds() // 60)


def round_number_near(price: float) -> float:
    """キリ番: 500円未満は50円刻み・それ以上は100円刻み（1万円以上は1,000円）。最も近い値を返す。"""
    step = 50 if price < 500 else 100 if price < 10000 else 1000
    return round(price / step) * step


def sma(vals: list[float], n: int) -> float | None:
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def rci(closes: list[float], n: int = RCI_N) -> float | None:
    """RCI（順位相関）。時間順位と価格順位のスピアマン。-100〜+100。"""
    if len(closes) < n:
        return None
    seg = closes[-n:]
    # 価格順位（高い方が1位）・時間順位（新しい方が1位）
    order = sorted(range(n), key=lambda i: -seg[i])
    prank = [0] * n
    for r, i in enumerate(order):
        prank[i] = r + 1
    d2 = sum((( n - i) - prank[i]) ** 2 for i in range(n))
    return (1 - 6 * d2 / (n * (n * n - 1))) * 100


@dataclass
class Bar:
    t: datetime   # 足の開始時刻
    o: float
    h: float
    l: float
    c: float
    v: float

    @property
    def end(self) -> datetime:
        return self.t + timedelta(minutes=5)


@dataclass
class DayContext:
    code: str
    name: str
    day: str
    prev_close: float
    prev_high: float
    prev_bars: list[Bar]            # 前日の5分足（MA/RCIの連続計算・前日同時間帯出来高）
    prev_cum_vol_at: dict[str, float] = field(default_factory=dict)   # "HH:MM"(足の終了) → 前日累計出来高


@dataclass
class Signal:
    code: str
    name: str
    day: str
    label: str              # 通常 / ギャップ / 寄り陰線 / 寄り天反発
    rank: str               # A/B/C/警戒
    rise_pct: float
    rise_min: int
    vol_ratio: float | None
    origin: float
    high: float
    fib: dict               # {"23.6":..,"38.2":..,...}
    stopped_at: str         # 止まったフィボ（"50" など）
    retrace_pct: float
    ma_mid_dir: str         # up/flat/down
    overlap: int
    overlap_items: list[str]
    entry: float
    stop: float
    tp1: float
    tp2: float
    rr: float
    time: str               # シグナル足の終了時刻
    flags: list[str] = field(default_factory=list)   # 寄り足高値上抜け済み など
    lot: str = "full"
    # 紙の決済（再生で埋める）
    exit_price: float | None = None
    exit_time: str | None = None
    exit_type: str | None = None
    pnl_pct: float | None = None
    hold_min: int | None = None


@dataclass
class Skip:
    code: str
    name: str
    day: str
    time: str
    reason: str
    detail: dict = field(default_factory=dict)


class WaveEngine:
    """1銘柄・1日の状態機械。5分足を1本ずつ食わせる（on_bar）。最初の大きな波だけを扱う。"""

    def __init__(self, ctx: DayContext):
        self.ctx = ctx
        self.bars: list[Bar] = []               # 当日
        self.series_c: list[float] = [b.c for b in ctx.prev_bars]   # 前日から連続
        self.open_bar: Bar | None = None
        self.gap_pct: float | None = None
        self.label = "通常"
        self.origin: float | None = None
        self.origin_floor: float | None = None
        self.high: float | None = None
        self.high_bar_i: int | None = None
        self.confirmed = False                   # 高値確定
        self.fib: dict | None = None
        self.done = False                        # 破棄/エントリー済み/見送り確定
        self.signal: Signal | None = None
        self.skips: list[Skip] = []
        self.pull_low: float | None = None
        self.pull_low_i: int | None = None
        self.rci_hit_oversold = False
        self.bounce_highs: list[float] = []      # 押しの間の戻り高値（切り下げ判定）
        self.open_high_broken = False
        self.first_wave_started = False
        self.events: list[str] = []              # 再生表示用

    # ── 補助 ─────────────────────────────────────────
    def _log(self, bar: Bar, msg: str):
        self.events.append(f"{hm(bar.end)} {msg}")

    def _ma(self, n: int, upto: int | None = None) -> float | None:
        s = self.series_c if upto is None else self.series_c[:upto]
        return sma(s, n)

    def _mid_n(self, bar: Bar) -> int:
        return MA_MID_AM if bar.end.hour < 10 else MA_MID

    def _mid_dir(self, bar: Bar) -> str:
        n = self._mid_n(bar)
        cur = self._ma(n)
        prev = self._ma(n, upto=len(self.series_c) - MA_SLOPE_BARS)
        if cur is None or prev is None:
            return "flat"
        slope = (cur - prev) / prev * 100
        return "up" if slope > 0.05 else "down" if slope < -0.05 else "flat"

    def _vwap(self) -> float | None:
        tv = sum(((b.h + b.l + b.c) / 3) * b.v for b in self.bars)
        vv = sum(b.v for b in self.bars)
        return tv / vv if vv > 0 else None

    def _vol_profile_peak(self) -> float | None:
        """当日＋前日の5分足を価格帯(0.5%刻み)に積んで最大の帯の中心。"""
        bars = self.ctx.prev_bars + self.bars
        if not bars:
            return None
        ref = bars[-1].c
        step = max(ref * 0.005, 1.0)
        bins: dict[int, float] = {}
        for b in bars:
            k = int(((b.h + b.l) / 2) / step)
            bins[k] = bins.get(k, 0.0) + b.v
        k = max(bins, key=bins.get)
        return (k + 0.5) * step

    def _vol_ratio(self, bar: Bar) -> float | None:
        cum = sum(b.v for b in self.bars)
        base = self.ctx.prev_cum_vol_at.get(hm(bar.end))
        if not base:
            return None
        return cum / base

    def _fib_levels(self) -> dict:
        o, h = self.origin, self.high
        d = h - o
        return {"23.6": h - d * 0.236, "38.2": h - d * 0.382, "50": h - d * 0.5, "61.8": h - d * 0.618,
                "78.6": h - d * 0.786, "100": o, "127.2": o + d * 1.272, "161.8": o + d * 1.618}

    def _rank(self, rise: float, mins: int, mid_dir: str) -> str | None:
        if mins <= RANK_WARN[0] and rise > RANK_WARN[1]:
            return "警戒"
        if mins <= RANK_A[0] and RANK_A[1] <= rise <= RANK_A[2]:
            return "A"
        if RANK_A[0] < mins <= RANK_B[0] and RANK_B[1] <= rise <= RANK_B[2]:
            return "B"
        if RANK_B[0] < mins <= RANK_C[0] and rise >= RANK_C[1] and mid_dir == "up":
            return "C"
        if mins <= RANK_B[0] and rise > RANK_A[2]:
            return "強"      # 表に無い「30分以内に+10%超」（警戒ほど速くない）
        return None

    # ── 本体 ─────────────────────────────────────────
    def on_bar(self, bar: Bar):
        if self.done:
            return
        # 先頭の出来高0足（気配足）は捨てる
        if self.open_bar is None and bar.v <= 0:
            return
        self.bars.append(bar)
        self.series_c.append(bar.c)
        ctx = self.ctx
        if self.open_bar is None:
            self.open_bar = bar
            self.gap_pct = (bar.o / ctx.prev_close - 1) * 100
            if self.gap_pct > GAP_EXCLUDE_PCT:
                self._skip(bar, "ギャップ+10%超", {"gap_pct": round(self.gap_pct, 2)}); return
            if self.gap_pct >= GAP_UP_PCT:
                # ギャップアップ: 起点＝寄り後最安値と前日終値の高い方（例: 共栄タンカー=寄り後安値1,806 / Zenmu=前日終値3,110）
                self.label = "ギャップ"; self.origin = max(bar.l, ctx.prev_close); self.origin_floor = ctx.prev_close
            else:
                self.origin = bar.l; self.origin_floor = None
                if bar.c < bar.o:
                    self.label = "寄り陰線"
            self.high = bar.h; self.high_bar_i = 0
            self._log(bar, f"寄り {bar.o:.0f}（前日比{self.gap_pct:+.1f}%）ラベル={self.label} 起点={self.origin:.0f}")
            return

        i = len(self.bars) - 1
        if not self.confirmed:
            # 寄り後最安値を起点として更新（高値確定前まで・GU日は前日終値が下限）
            if bar.l < self.origin:
                self.origin = max(bar.l, self.origin_floor) if self.origin_floor is not None else bar.l
        if self.label == "寄り陰線" and not self.open_high_broken and bar.c > self.open_bar.h:
            self.open_high_broken = True
            self._log(bar, "寄り足高値を上抜け")

        # 起点割れ → 破棄
        if bar.c < self.origin and self.confirmed:
            self._skip(bar, "起点割れ（シナリオ破棄）", {"origin": self.origin, "close": bar.c}); return

        # 高値更新
        if bar.h > self.high:
            if self.confirmed:
                self._log(bar, f"高値更新 {bar.h:.0f} → フィボ引き直し")
            self.high = bar.h; self.high_bar_i = i; self.confirmed = False; self.fib = None
            self.pull_low = None; self.pull_low_i = None; self.rci_hit_oversold = False; self.bounce_highs = []
            return

        # 高値確定判定: 高値足の次の足が高値未更新かつ陰線
        if not self.confirmed and i > self.high_bar_i and bar.c < bar.o and not getattr(self, "_rejected_at", None) == self.high_bar_i:
            rise = (self.high / self.origin - 1) * 100
            high_bar = self.bars[self.high_bar_i]
            mins = minutes_between(self.open_bar.t, high_bar.end)
            if hm(high_bar.end) > FIRST_WAVE_DEADLINE:
                self._skip(bar, "11:30以降の初波", {"high_time": hm(high_bar.end)}); return
            mid_dir = self._mid_dir(bar)
            vr = self._vol_ratio(high_bar)
            rank = self._rank(rise, mins, mid_dir)
            self.confirmed = True
            self.fib = self._fib_levels()
            self.wave = {"rise": rise, "mins": mins, "vol_ratio": vr, "rank": rank, "mid_dir": mid_dir,
                         "impulse_vol": sum(b.v for b in self.bars[:self.high_bar_i + 1])}
            self._log(bar, f"高値確定 {self.high:.0f}（起点{self.origin:.0f}→+{rise:.1f}%・{mins}分・出来高倍率{('%.1f' % vr) if vr else '?'}・ランク{rank or '-'}・中期MA{mid_dir}）"
                           f" フィボ 38.2={self.fib['38.2']:.0f} 50={self.fib['50']:.0f} 61.8={self.fib['61.8']:.0f} 78.6={self.fib['78.6']:.0f}")
            if rise < RISE_MIN:
                # まだ「大きな波」ではない: 60分経過なら見送り確定、それまでは高値確定を取り消して追跡を続ける
                if mins >= RANK_SKIP[0]:
                    self._skip(bar, "60分経過で+4%未満", {"rise": round(rise, 2), "mins": mins}); return
                self.confirmed = False; self.fib = None; self._rejected_at = self.high_bar_i
                self._log(bar, f"（初動+{rise:.1f}%は小さい→追跡継続）")
                return
            if vr is not None and vr < VOL_RATIO_MIN:
                self._skip(bar, "出来高倍率3倍未満", {"vol_ratio": round(vr, 2)}); return
            self.pull_low = bar.l; self.pull_low_i = i
            return

        if not self.confirmed:
            return

        # ── 押しの追跡 ──
        f = self.fib
        if bar.l < self.pull_low:
            self.pull_low = bar.l; self.pull_low_i = i
        if bar.c < f["78.6"]:
            reason = "寄り天（78.6%割れ）" if self.high_bar_i <= 1 else "78.6%割れ"
            self._skip(bar, reason, {"close": bar.c, "fib786": round(f["78.6"], 1)}); return
        r = rci(self.series_c)
        if r is not None and r <= RCI_OVERSOLD:
            self.rci_hit_oversold = True
        # 戻り高値（押しの間に前の足より高い高値→戻り）
        if i > self.pull_low_i and bar.h > self.bars[i - 1].h:
            self.bounce_highs.append(bar.h)

        # ── エントリー判定（この足の確定で）──
        retrace = (self.high - self.pull_low) / (self.high - self.origin)
        if hm(bar.end) >= NO_NEW_AFTER:
            self._skip(bar, "14:30以降は新規なし", {}); return
        ma5_now = self._ma(MA_SHORT); ma5_prev = self._ma(MA_SHORT, upto=len(self.series_c) - 1)
        cross5 = (ma5_now is not None and ma5_prev is not None and self.bars[i - 1].c <= ma5_prev and bar.c > ma5_now)
        if not cross5:
            return
        mid_n = self._mid_n(bar); mid = self._ma(mid_n); mid_dir = self._mid_dir(bar)
        r_prev = rci(self.series_c[:-1]); r_now = rci(self.series_c)
        pull_bars = self.bars[self.high_bar_i + 1:i]
        pull_vol = (sum(b.v for b in pull_bars) / len(pull_bars)) if pull_bars else 0.0     # 1本あたり平均
        imp_vol = self.wave["impulse_vol"] / (self.high_bar_i + 1)                          # 1本あたり平均
        checks = {
            # 停止ゾーンは価格で±0.5%の遊びを持たせる（61.8%線を数円割った「61.8%付近で停止」を拾う）
            "押し38.2〜61.8で停止": (self.pull_low <= f["38.2"] * (1 + OVERLAP_TOL_PCT / 100)) and (self.pull_low >= f["61.8"] * (1 - OVERLAP_TOL_PCT / 100)),
            "押し出来高<上昇出来高": pull_vol < imp_vol,
            f"中期MA{mid_n}上向きかつ価格上": mid_dir == "up" and mid is not None and bar.c > mid,
            "MA5上抜け足の確定": cross5,
            "RCI-80以下から上向き": self.rci_hit_oversold and r_prev is not None and r_now is not None and r_now > r_prev,
        }
        failed = [k for k, ok in checks.items() if not ok]
        if failed:
            # 見送りは確定しない（次の足で再判定）。ただし明確な見送り条件は確定
            if retrace > FIB_ZONE[1] and bar.c < f["61.8"]:
                pass
            if mid_dir != "up" and not SOFT_SKIPS:
                self._skip(bar, "中期MAが横ばい〜下向き", {"mid_dir": mid_dir}); return
            if pull_vol >= imp_vol and not SOFT_SKIPS:
                self._skip(bar, "押しの間に出来高が増加", {"pull_vol_per_bar": round(pull_vol), "impulse_vol_per_bar": round(imp_vol)}); return
            self._log(bar, f"MA5上抜けだが未達: {', '.join(failed)}（押し{retrace*100:.0f}%）")
            return
        if len(self.bounce_highs) >= 2 and self.bounce_highs[-1] < self.bounce_highs[-2] and not SOFT_SKIPS:
            self._skip(bar, "戻り高値の切り下げ", {"bounce_highs": self.bounce_highs[-2:]}); return

        # ── 決済プラン ──
        entry = bar.c
        # 損切り＝78.6%線と-3%の「早い方」（先に触れる＝高い方）。78.6%線が-3%より遠い日は-3%が損切りになる。
        stop_786 = f["78.6"]; stop_pct = entry * (1 - STOP_MAX_PCT / 100)
        stop = max(stop_786, stop_pct); stop_kind = "78.6%" if stop_786 >= stop_pct else "-3%"
        rise = self.wave["rise"]
        if rise >= RISE_NORMAL:
            tp1 = entry * (1 + TP1_PCT / 100)
            tp2 = min(entry * (1 + TP2_PCT / 100), f["127.2"])
        else:
            tp1 = tp2 = self.high * 0.995   # 直近高値の手前
        rr = (tp1 - entry) / (entry - stop) if entry > stop else 0
        if rise < RISE_NORMAL and rr < RR_MIN_SMALL:
            self._skip(bar, "初動+4〜6%でRR1.5未満", {"rr": round(rr, 2)}); return
        # 上値の壁: エントリーと利確の間に前日高値/寄り足高値
        walls = [w for w in (ctx.prev_high, self.open_bar.h if self.label == "寄り陰線" and not self.open_high_broken else None)
                 if w and entry < w < tp1]
        if walls:
            self._skip(bar, "押し目ゾーンの近くに上値の壁", {"walls": [round(w, 1) for w in walls]}); return

        # ── 重なりスコア ──
        tol = self.pull_low * OVERLAP_TOL_PCT / 100
        items = []
        vw = self._vwap()
        if vw and abs(vw - self.pull_low) <= tol: items.append("VWAP")
        if mid and mid_dir == "up" and abs(mid - self.pull_low) <= tol: items.append(f"MA{mid_n}")
        if abs(ctx.prev_high - self.pull_low) <= tol: items.append("前日高値")
        if abs(ctx.prev_close - self.pull_low) <= tol: items.append("前日終値")
        vp = self._vol_profile_peak()
        if vp and abs(vp - self.pull_low) <= tol: items.append("価格帯出来高")
        if abs(round_number_near(self.pull_low) - self.pull_low) <= tol: items.append("キリ番")
        if self.label == "寄り陰線" and self.open_high_broken and abs(self.open_bar.h - self.pull_low) <= tol: items.append("寄り足高値")
        stopped = "38.2" if retrace < 0.44 else "50" if retrace < 0.56 else "61.8"
        self.signal = Signal(
            code=ctx.code, name=ctx.name, day=ctx.day, label=self.label, rank=self.wave["rank"] or "-",
            rise_pct=round(rise, 2), rise_min=self.wave["mins"], vol_ratio=(round(self.wave["vol_ratio"], 2) if self.wave["vol_ratio"] else None),
            origin=round(self.origin, 1), high=round(self.high, 1), fib={k: round(v, 1) for k, v in f.items()},
            stopped_at=stopped, retrace_pct=round(retrace * 100, 1), ma_mid_dir=mid_dir, overlap=len(items), overlap_items=items,
            entry=round(entry, 1), stop=round(stop, 1), tp1=round(tp1, 1), tp2=round(tp2, 1), rr=round(rr, 2), time=hm(bar.end),
            flags=(["寄り足高値上抜け済み"] if self.open_high_broken else []) + (["本命"] if len(items) >= OVERLAP_MAIN else []) + [f"損切り={stop_kind}"],
        )
        self._log(bar, f"★エントリー {entry:.0f}（押し{retrace*100:.0f}%={stopped}・重なり{len(items)}点{items}・損切{stop:.0f}・利確{tp1:.0f}/{tp2:.0f}・RR{rr:.2f}）")
        self.done = True

    def _skip(self, bar: Bar, reason: str, detail: dict):
        self.skips.append(Skip(self.ctx.code, self.ctx.name, self.ctx.day, hm(bar.end), reason, detail))
        self._log(bar, f"見送り: {reason} {detail if detail else ''}")
        self.done = True


def settle_paper(sig: Signal, bars: list[Bar]) -> Signal:
    """紙の決済（再生用）: エントリー足の次から、損切り→利確1（半分・建値ストップ）→利確2／30分無反応／15:00。同一足で両方触れたら損切り優先。"""
    after = [b for b in bars if hm(b.end) > sig.time]
    entry, stop, tp1, tp2 = sig.entry, sig.stop, sig.tp1, sig.tp2
    half_done = False; realized = 0.0; t0 = None
    for b in after:
        if t0 is None:
            t0 = b.t
        if b.l <= stop:
            pnl = (stop / entry - 1) * 100 * (0.5 if half_done else 1.0)
            return _fill(sig, stop, b, "STOP" if not half_done else "BE_STOP", realized + pnl, t0)
        if not half_done and b.h >= tp1:
            if sig.tp1 == sig.tp2:   # 初動+4〜6%: 直近高値手前で全決済
                return _fill(sig, tp1, b, "TP", (tp1 / entry - 1) * 100, t0)
            half_done = True; realized = (tp1 / entry - 1) * 100 * 0.5; stop = entry
            continue
        if half_done and b.h >= tp2:
            return _fill(sig, tp2, b, "TP2", realized + (tp2 / entry - 1) * 100 * 0.5, t0)
        if not half_done and minutes_between(t0, b.end) >= TIME_STOP_MIN and b.c < entry * 1.01:
            return _fill(sig, b.c, b, "TIME", (b.c / entry - 1) * 100, t0)
        if hm(b.end) >= FLAT_ALL_AT:
            pnl = (b.c / entry - 1) * 100 * (0.5 if half_done else 1.0)
            return _fill(sig, b.c, b, "CLOSE", realized + pnl, t0)
    if after:
        b = after[-1]
        pnl = (b.c / entry - 1) * 100 * (0.5 if half_done else 1.0)
        return _fill(sig, b.c, b, "CLOSE", realized + pnl, t0)
    return sig


def _fill(sig: Signal, px: float, b: Bar, kind: str, pnl: float, t0: datetime) -> Signal:
    sig.exit_price = round(px, 1); sig.exit_time = hm(b.end); sig.exit_type = kind
    sig.pnl_pct = round(pnl, 3); sig.hold_min = minutes_between(t0, b.end)
    return sig


# ── 再生（yfinance 5分足）──────────────────────────────────
_CACHE = None


def load_cache() -> dict:
    global _CACHE
    if _CACHE is None:
        _CACHE = pickle.load(open(CACHE_5M, "rb"))
    return _CACHE


_DAYBARS: dict[int, dict[str, list[Bar]]] = {}


def _daybars(df) -> dict[str, list[Bar]]:
    """DataFrame → {day: [Bar...]} を一度だけ作る（id をキーにキャッシュ）。"""
    k = id(df)
    d = _DAYBARS.get(k)
    if d is None:
        d = {}
        ts = df["dt"].dt.tz_localize(None) if getattr(df["dt"].dt, "tz", None) is not None else df["dt"]
        days = ts.dt.strftime("%Y-%m-%d").tolist()
        for day, t, o, h, l, c, v in zip(days, ts.tolist(), df["o"].tolist(), df["h"].tolist(), df["l"].tolist(), df["c"].tolist(), df["v"].tolist()):
            d.setdefault(day, []).append(Bar(t.to_pydatetime(), float(o), float(h), float(l), float(c), float(v)))
        _DAYBARS[k] = d
    return d


def bars_of(df, day: str) -> list[Bar]:
    return list(_daybars(df).get(day, []))


def trading_days(df) -> list[str]:
    return sorted(_daybars(df).keys())


def make_ctx(code: str, name: str, df, day: str) -> DayContext | None:
    days = trading_days(df)
    if day not in days:
        return None
    k = days.index(day)
    if k == 0:
        return None
    prev = bars_of(df, days[k - 1])
    prev = [b for b in prev if b.v > 0] or prev
    if not prev:
        return None
    cum = 0.0; cum_at = {}
    for b in prev:
        cum += b.v; cum_at[hm(b.end)] = cum
    return DayContext(code=code, name=name, day=day, prev_close=prev[-1].c, prev_high=max(b.h for b in prev),
                      prev_bars=prev, prev_cum_vol_at=cum_at)


def replay_one(code: str, day: str, verbose: bool = True, name_map: dict | None = None):
    cache = load_cache()
    key = code if code.endswith(".T") else code + ".T"
    if key not in cache:
        if verbose:
            print(f"{code}: 5分足キャッシュに無い")
        return None
    name = (name_map or {}).get(key, key)
    ctx = make_ctx(key[:-2], name, cache[key], day)
    if ctx is None:
        if verbose:
            print(f"{code}: {day} の足が無い")
        return None
    eng = WaveEngine(ctx)
    bars = bars_of(cache[key], day)
    for b in bars:
        eng.on_bar(b)
    if eng.signal:
        settle_paper(eng.signal, bars)
    if verbose:
        print(f"\n■ {name}（{key[:-2]}）{day}  前日終値{ctx.prev_close:.0f} 前日高値{ctx.prev_high:.0f}")
        for e in eng.events:
            print("  " + e)
        if eng.signal:
            s = eng.signal
            print(f"  → 紙の決済: {s.exit_type} {s.exit_price} @{s.exit_time}  損益{s.pnl_pct:+.2f}%  保有{s.hold_min}分")
    return eng


def replay_day(day: str, min_tov_oku: float = 3.0, name_map: dict | None = None) -> tuple[list[Signal], list[Skip]]:
    cache = load_cache()
    sigs: list[Signal] = []; skips: list[Skip] = []
    for key, df in cache.items():
        ctx = make_ctx(key[:-2], (name_map or {}).get(key, key), df, day)
        if ctx is None:
            continue
        tov = sum(b.c * b.v for b in ctx.prev_bars) / 1e8
        if tov < min_tov_oku:
            continue
        eng = WaveEngine(ctx)
        bars = bars_of(df, day)
        for b in bars:
            eng.on_bar(b)
        if eng.signal:
            sigs.append(settle_paper(eng.signal, bars))
        elif eng.confirmed and not eng.done:
            # 高値確定まで行ったが5条件が揃わず終了＝「条件待ちで終了」（見送り理由の分析用）
            skips.append(Skip(ctx.code, ctx.name, day, hm(bars[-1].end) if bars else "", "条件待ちで終了",
                              {"rank": eng.wave.get("rank"), "rise": round(eng.wave["rise"], 1), "last": eng.events[-1][:80] if eng.events else ""}))
        skips.extend(eng.skips)
    SIG_DIR.mkdir(exist_ok=True)
    with open(SIG_DIR / f"{day}{'_soft' if SOFT_SKIPS else ''}.json", "w", encoding="utf-8") as f:
        json.dump({"day": day, "signals": [asdict(s) for s in sigs], "skips": [asdict(k) for k in skips]}, f, ensure_ascii=False, indent=1)
    return sigs, skips


def summarize(sigs: list[Signal], title: str = ""):
    if not sigs:
        print(f"{title}: シグナル0件"); return
    done = [s for s in sigs if s.pnl_pct is not None]
    def pf(x):
        g = sum(v for v in x if v > 0); l = -sum(v for v in x if v < 0)
        return g / l if l else float("inf")
    p = [s.pnl_pct for s in done]
    print(f"{title}: n={len(done)} 勝率{sum(1 for v in p if v > 0)/len(p)*100:.0f}% 平均{sum(p)/len(p):+.2f}% PF{pf(p):.2f} 最悪{min(p):+.1f}% 最良{max(p):+.1f}%")
    for key, lab in (("rank", "ランク"), ("label", "ラベル"), ("overlap", "重なり")):
        groups: dict = {}
        for s in done:
            groups.setdefault(getattr(s, key), []).append(s.pnl_pct)
        print("  " + lab + ": " + " / ".join(f"{k}: n{len(v)} 勝率{sum(1 for x in v if x>0)/len(v)*100:.0f}% 平均{sum(v)/len(v):+.2f}% PF{pf(v):.2f}" for k, v in sorted(groups.items(), key=lambda kv: str(kv[0]))))


def load_name_map() -> dict:
    try:
        c = pickle.load(open(ROOT / "jquants_cache.pkl", "rb"))
        return dict(c.get("name_map", {}) or {})
    except Exception:
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", metavar="DATE")
    ap.add_argument("--replay-range", nargs=2, metavar=("FROM", "TO"))
    ap.add_argument("--codes", default="")
    ap.add_argument("--min-tov", type=float, default=3.0, help="前日代金(億)の下限・全銘柄再生時")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--once", action="store_true", help="--live: 今あるminutesを1回だけ処理して終了（動作確認用）")
    a = ap.parse_args()
    nm = load_name_map()
    if a.replay and a.codes:
        for c in a.codes.split(","):
            replay_one(c.strip(), a.replay, True, nm)
        return
    if a.replay:
        sigs, skips = replay_day(a.replay, a.min_tov, nm)
        summarize(sigs, a.replay)
        reasons: dict = {}
        for k in skips:
            reasons[k.reason] = reasons.get(k.reason, 0) + 1
        print("  見送り理由:", dict(sorted(reasons.items(), key=lambda kv: -kv[1])))
        wait = [k for k in skips if k.reason == "条件待ちで終了"]; print(f"  条件待ちで終了: {len(wait)}件（ランクあり{sum(1 for k in wait if k.detail.get('rank'))}）")
        for k in wait[:12]:
            print(f"    条件待ち: {k.name}({k.code}) ランク{k.detail['rank']} +{k.detail['rise']}% … {k.detail['last']}")
        return
    if a.replay_range:
        cache = load_cache()
        days = sorted(set(d for df in list(cache.values())[:50] for d in trading_days(df)))
        days = [d for d in days if a.replay_range[0] <= d <= a.replay_range[1]]
        allsig: list[Signal] = []
        for d in days:
            sigs, _ = replay_day(d, a.min_tov, nm)
            allsig.extend(sigs)
            print(f"  {d}: {len(sigs)}件", flush=True)
        summarize(allsig, f"{a.replay_range[0]}〜{a.replay_range[1]}")
        half = len(days) // 2
        summarize([s for s in allsig if s.day < days[half]], "  前半")
        summarize([s for s in allsig if s.day >= days[half]], "  後半")
        return
    if a.report:
        allsig = []
        for p in sorted(SIG_DIR.glob("*.json")):
            j = json.load(open(p, encoding="utf-8"))
            allsig.extend(Signal(**s) for s in j["signals"])
        summarize(allsig, "保存済み全日")
        return
    if a.live:
        import fibo_live
        fibo_live.run_live(day=a.replay, once=a.once)
        return
    ap.print_help()


if __name__ == "__main__":
    main()

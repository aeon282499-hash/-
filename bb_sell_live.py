# -*- coding: utf-8 -*-
"""bb_sell_live.py — GU株のBB上抜け売り（前場）の場中通知と紙トレード台帳（2026-11-22・第1段階＝発注しない）。

本人「これって自動でできる？」「楽天」→ 第1段階: 合図を Discord に通知し、紙の台帳を自動で付ける。
第2段階（12月の紙の成績が良ければ）: 楽天マーケットスピードII RSS（Excel）で注文を出す部分を足す。

■ ルール（検証済み・kabuai/bb_forward_1122.py / bb_filters_1122.py / bb_period_compare_1122.py）
  対象   寄値 ÷ 前日終値 ≥ +3%・前日の売買代金 1億以上・決算発表の当日と翌営業日は除く
  足     5分足・BB 期間40・2σ（単純移動平均・母集団の標準偏差）。前日の足から続けて計算する
  合図   10:00〜11:25 に始まった足が確定した時点で ①終値 ≥ 上バンド×1.005 ②終値 ≥ 寄値×1.02（1銘柄1日1回・最初の合図だけ）
  売り   合図の次の足の始値（＝次の足の最初の断面の現在値）で、いちにち信用の新規売り（紙）
  買戻し 確定した足の終値 ≤ 中心線（40本MA）→ 次の足の始値／戻らなければ大引け（15:30の値）
  保険   売値+5% に逆指値（検証には無い。台帳では「保険あり」の損益を別の列に記録）
  上限   1日2回・同時に1銘柄・1日の損失−2万円（100株換算）。上限で見送った合図も「見送り」として記録する
  検証の成績（確認期間 10/23〜11/20・前場・寄値+2%・中心線回帰）: 12回・勝率75%・PF4.02（件数が少ないので参考）

■ データ
  tachibana_live_flow.py が毎分追記する live_flow/minutes/YYYY-MM-DD.jsonl
    1行 = {"ts": "...", "s": {code: [現在値, 始値, 高値, 安値, 出来高, 代金, VWAP, 前日終値]}}
  前日の同ファイルで BB40 を前日から続けて計算し、前日の代金を取る。決算日は earnings_calendar.json。
  ⚠ 検証（Yahoo 5分足）は 9:00 の足を捨てていたが、ここでは立花の正式な 9:00 の足を使う（BB の値がわずかに違う）。

■ 出力
  Discord: DISCORD_WEBHOOK_BB_URL（.env・未設定なら画面に出すだけ）
  台帳:    bb_sell_paper_log.csv（1合図1行）
  状態:    live_flow/bb_sell_live.json（当日の候補・合図・建玉）

実行
  python -X utf8 bb_sell_live.py --live              平日 9:00 起動・15:35 まで20秒ごとに追読み
  python -X utf8 bb_sell_live.py --replay 2026-11-20 その日の minutes を最初から再生（通知しない・台帳に書かない）
  python -X utf8 bb_sell_live.py --replay 2026-11-20 --log   再生して台帳にも書く
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

MINUTES_DIR = Path(os.getenv("BB_MINUTES_DIR", str(ROOT / "live_flow" / "minutes")))
STATE_JSON = ROOT / "live_flow" / "bb_sell_live.json"
LOG_CSV = ROOT / "bb_sell_paper_log.csv"
EARN_JSON = ROOT / "earnings_calendar.json"
WEBHOOK_ENV = "DISCORD_WEBHOOK_BB_URL"

# ── ルールの定数 ──
GU_MIN = 0.03
PREV_TOV_MIN = 1e8
PERIOD, SIGMA, BAND_OUT, OPEN_UP = 40, 2.0, 0.005, 0.02
SIG_FROM, SIG_TO = "10:00", "11:25"        # 合図の足の開始時刻
STOP_PCT = 0.05                             # 保険の逆指値（検証には無い）
COST_PCT = 0.10                             # 往復（＋不利1tick×2 は tick() で引く）
SHARES = 100
MAX_TRADES, MAX_LOSS_YEN = 2, 20000
POLL_SEC, END_AT = 20, "15:35"

LOG_FIELDS = ["date", "code", "name", "gu_pct", "signal_bar", "signal_close", "upper", "ma", "open_up_pct", "vwap_dev_pct",
              "vol_ratio", "taken", "skip_reason", "entry_time", "entry", "exit_time", "exit", "exit_reason",
              "pnl_pct", "pnl_yen", "stop_hit", "pnl_pct_with_stop", "pnl_yen_with_stop"]


def tick(px: float) -> float:
    for lim, t in ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3e6, 1000)):
        if px <= lim:
            return t
    return 5000


def hm(ts: datetime) -> str:
    return ts.strftime("%H:%M")


# ───────────────────────── 5分足 ─────────────────────────
@dataclass
class Bar:
    t: datetime
    o: float
    h: float
    l: float
    c: float
    v: float


class Builder:
    """1銘柄の1分断面 → 5分足（確定した足は次の枠の断面が来た時点で返す）。チャートと同じく昼休み・引け後は足にしない。"""

    def __init__(self):
        self.key = None; self.cur = None; self.v0 = 0.0; self.vlast = 0.0

    def push(self, ts: datetime, last, vol) -> Bar | None:
        if last is None or last <= 0:
            return None
        k = ts.replace(second=0, microsecond=0, minute=(ts.minute // 5) * 5)
        done = None
        if self.key is not None and k > self.key:
            c = self.cur
            done = Bar(self.key, c["o"], c["h"], c["l"], c["c"], max(0.0, self.vlast - self.v0))
            self.v0 = self.vlast; self.key = None
        if self.key is None:
            self.key = k; self.cur = {"o": last, "h": last, "l": last, "c": last}
        else:
            self.cur["c"] = last; self.cur["h"] = max(self.cur["h"], last); self.cur["l"] = min(self.cur["l"], last)
        if vol is not None:
            self.vlast = vol
        return done


def norm_ts(ts: datetime) -> datetime | None:
    """足にしない時間を捨て、11:30/15:30 の断面（前場/大引けの値）はその前の足に入れる。"""
    t = hm(ts)
    if t < "09:00" or "11:30" < t < "12:30" or t > "15:30":
        return None
    if t in ("11:30", "15:30"):
        return ts.replace(second=0) - timedelta(seconds=1)
    return ts


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


def is_stale_day(path: Path) -> bool:
    """休場日の断面（値が朝から引けまで同じ）を見分ける。"""
    rows, _ = read_rows(path)
    if len(rows) < 2:
        return True
    a, b = rows[0]["s"], rows[-1]["s"]
    same = sum(1 for k, v in b.items() if a.get(k) == v)
    return bool(b) and same >= 0.95 * len(b)


def prev_day_context(day: str) -> tuple[dict, dict]:
    """前の営業日の {code: [5分足]} と {code: 当日代金}。"""
    d = datetime.strptime(day, "%Y-%m-%d").date()
    for k in range(1, 10):
        p = MINUTES_DIR / f"{(d - timedelta(days=k)).isoformat()}.jsonl"
        if p.exists() and not is_stale_day(p):
            rows, _ = read_rows(p)
            bld: dict[str, Builder] = {}; bars: dict[str, list[Bar]] = {}; tov: dict[str, float] = {}
            for row in rows:
                ts = norm_ts(datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S"))
                if ts is None:
                    continue
                for code, v in row["s"].items():
                    b = bld.setdefault(code, Builder()).push(ts, v[0], v[4])
                    if b:
                        bars.setdefault(code, []).append(b)
                    if v[5]:
                        tov[code] = v[5]
            for code, b in bld.items():   # 最後の足を閉じる
                if b.key is not None:
                    c = b.cur; bars.setdefault(code, []).append(Bar(b.key, c["o"], c["h"], c["l"], c["c"], max(0.0, b.vlast - b.v0)))
            return bars, tov
    return {}, {}


JPX_JSON = ROOT / "jpx_earnings_schedule.json"


def earnings_block(day: str) -> set:
    """決算発表の当日と翌営業日に当たる銘柄（4桁コード）。翌営業日は「前の平日が発表日」で近似。
    earnings_calendar.json（J-Quants）と jpx_earnings_schedule.json（JPX公式）の両方を見る。"""
    d = datetime.strptime(day, "%Y-%m-%d").date()
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    want = {day, prev.isoformat()}
    out = set()
    try:
        cal = json.load(open(EARN_JSON, encoding="utf-8"))
        out |= {tk.split(".")[0] for tk, ds in cal.items() if want & set(ds)}
    except Exception:
        pass
    try:
        sch = json.load(open(JPX_JSON, encoding="utf-8")).get("schedule", {})
        out |= {x["code"] for ds in want for x in sch.get(ds, [])}
    except Exception:
        pass
    return out


def load_names() -> dict:
    """銘柄名（取れる範囲で）: JPX決算予定の名前。無ければコードのまま表示する。"""
    names = {}
    try:
        sch = json.load(open(JPX_JSON, encoding="utf-8")).get("schedule", {})
        for lst in sch.values():
            for x in lst:
                names[x["code"]] = x.get("name") or x["code"]
    except Exception:
        pass
    return names


# ───────────────────────── 本体 ─────────────────────────
@dataclass
class Stock:
    code: str
    name: str
    prev_close: float
    open_px: float
    bars: list = field(default_factory=list)       # 前日＋当日の5分足
    builder: Builder = field(default_factory=Builder)
    state: str = "watch"                           # watch / short / done
    rec: dict = field(default_factory=dict)
    today_n: int = 0


class Session:
    def __init__(self, day: str, notify: bool, write_log: bool):
        self.day, self.notify, self.write_log = day, notify, write_log
        self.prev_bars, self.prev_tov = prev_day_context(day)
        self.earn = earnings_block(day)
        self.names = load_names()
        self.stocks: dict[str, Stock] = {}
        self.cands_sent = False
        self.trades_taken = 0; self.realized_yen = 0.0; self.open_code: str | None = None
        self.records: list[dict] = []

    # ── 通知 ──
    def send(self, text: str):
        print(text, flush=True)
        url = os.getenv(WEBHOOK_ENV, "").strip()
        if not (self.notify and url):
            return
        try:
            req = urllib.request.Request(url, data=json.dumps({"content": text[:1900]}).encode(),
                                         headers={"Content-Type": "application/json", "User-Agent": "bb-sell/1.0"})
            urllib.request.urlopen(req, timeout=15).read()
        except Exception as e:  # noqa: BLE001
            print(f"[discord] 失敗: {e}", flush=True)

    # ── 1行（1巡の断面）を処理 ──
    def on_row(self, row: dict):
        ts_raw = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
        ts = norm_ts(ts_raw)
        if ts is None:
            if hm(ts_raw) > "15:30":
                self.close_all(row, "大引け")
            return
        t = hm(ts)
        for code, v in row["s"].items():
            last, opn, vol, vwap, pc = v[0], v[1], v[4], v[6], v[7]
            st = self.stocks.get(code)
            if st is None:
                if not (opn and pc and last) or t < "09:00":
                    continue
                if opn / pc - 1 < GU_MIN or code in self.earn or self.prev_tov.get(code, 0) < PREV_TOV_MIN:
                    continue
                st = Stock(code, self.names.get(code) or code, pc, opn, bars=list(self.prev_bars.get(code, []))[-PERIOD * 2:])
                self.stocks[code] = st
            # 建玉の約定・決済（足の確定を待つものは、確定させた断面＝次の足の最初の断面の現在値で約定）
            bar = st.builder.push(ts, last, vol)
            if st.state == "short" and last >= st.rec["entry"] * (1 + STOP_PCT) and not st.rec.get("stop_hit"):
                st.rec["stop_hit"] = True; st.rec["stop_px"] = st.rec["entry"] * (1 + STOP_PCT)
                self.send(f"⚠️【BB売り・紙】{st.code} {st.name} 保険の逆指値（+5%）に到達 {st.rec['stop_px']:,.0f}円 {t}"
                          "（検証のルールは保険なし＝台帳は両方記録）")
            if bar is None:
                continue
            st.bars.append(bar)
            if bar.t.date().isoformat() == self.day:
                st.today_n += 1
            closes = np.array([b.c for b in st.bars[-PERIOD:]])
            if len(closes) < PERIOD:
                continue
            ma, sd = closes.mean(), closes.std(ddof=0)
            up = ma + SIGMA * sd
            bt = hm(bar.t)
            if st.state == "watch" and SIG_FROM <= bt <= SIG_TO and bar.c >= up * (1 + BAND_OUT) and bar.c >= st.open_px * (1 + OPEN_UP):
                today = [b for b in st.bars if b.t.date().isoformat() == self.day]
                prior = [b.v for b in today[:-1]]
                st.rec = {"date": self.day, "code": code, "name": st.name, "gu_pct": round((st.open_px / st.prev_close - 1) * 100, 2),
                          "signal_bar": bt, "signal_close": bar.c, "upper": round(up, 2), "ma": round(ma, 2),
                          "open_up_pct": round((bar.c / st.open_px - 1) * 100, 2),
                          "vwap_dev_pct": round((bar.c / vwap - 1) * 100, 2) if vwap else "",
                          "vol_ratio": round(bar.v / (sum(prior) / len(prior)), 2) if prior and sum(prior) > 0 else ""}
                ok, why = self.can_take()
                st.rec["taken"] = ok; st.rec["skip_reason"] = why
                self.send(f"🔻【BB売り・紙】{code} {st.name}  合図 {bt}の足  終値{bar.c:,.0f}（上バンド{up:,.0f}・寄値比+{st.rec['open_up_pct']}%）\n"
                          f"  → 次の足の始値で いちにち信用の新規売り（成行）" + ("" if ok else f"  ※上限で見送り（{why}）") +
                          f"\n  買戻し: 5分足の終値が中心線（今{ma:,.0f}円）以下で次の足／戻らなければ大引け"
                          f"\n  保険: 売値+5%に逆指値  ｜GU+{st.rec['gu_pct']}%・VWAP乖離{st.rec['vwap_dev_pct']}%・出来高×{st.rec['vol_ratio']}")
                # 足を確定させたこの断面＝次の足の最初の値 → ここで売り（紙）
                self.enter(st, last, ts)
            elif st.state == "short" and bar.c <= ma:
                # 中心線以下で確定 → 次の足の最初の値（この断面）で買い戻し
                self.exit(st, last, ts, "中心線回帰")

    def can_take(self) -> tuple[bool, str]:
        if self.trades_taken >= MAX_TRADES:
            return False, f"1日{MAX_TRADES}回まで"
        if self.open_code is not None:
            return False, "同時に1銘柄まで"
        if self.realized_yen <= -MAX_LOSS_YEN:
            return False, f"1日の損失−{MAX_LOSS_YEN:,}円に到達"
        return True, ""

    def enter(self, st: Stock, px: float, ts: datetime):
        st.state = "short"
        st.rec["entry_time"] = hm(ts); st.rec["entry"] = px
        if st.rec["taken"]:
            if self.open_code is not None or self.trades_taken >= MAX_TRADES:    # 合図と約定の間に枠が埋まった
                st.rec["taken"] = False; st.rec["skip_reason"] = "約定時に枠なし"
            else:
                self.trades_taken += 1; self.open_code = st.code
        self.send(f"✅【BB売り・紙】{st.code} {st.name} 売り {px:,.0f}円 {hm(ts)}" + ("" if st.rec["taken"] else "（見送り扱い・成績だけ記録）"))

    def exit(self, st: Stock, px: float, ts: datetime, why: str):
        r = st.rec
        ep = r["entry"]
        def pnl(x):
            pct = -((x + tick(x)) - (ep - tick(ep))) / (ep - tick(ep)) * 100 - COST_PCT
            return round(pct, 3), round(pct / 100 * ep * SHARES)
        r["exit_time"] = hm(ts); r["exit"] = px; r["exit_reason"] = why
        r["pnl_pct"], r["pnl_yen"] = pnl(px)
        r["stop_hit"] = bool(r.get("stop_hit"))
        r["pnl_pct_with_stop"], r["pnl_yen_with_stop"] = pnl(r["stop_px"]) if r["stop_hit"] else (r["pnl_pct"], r["pnl_yen"])
        st.state = "done"
        if r.get("taken"):
            self.realized_yen += r["pnl_yen"]
            if self.open_code == st.code:
                self.open_code = None
        self.records.append(dict(r))
        self.send(f"🏁【BB売り・紙】{st.code} {st.name} 買戻し {px:,.0f}円 {hm(ts)}（{why}）  "
                  f"{r['pnl_pct']:+.2f}%・{r['pnl_yen']:+,}円/100株" + ("" if r.get("taken") else "（見送り扱い）")
                  + (f"  ｜保険ありなら {r['pnl_yen_with_stop']:+,}円" if r["stop_hit"] else ""))
        if self.write_log:
            new = not LOG_CSV.exists()
            with open(LOG_CSV, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow(r)

    def close_all(self, row: dict, why: str):
        ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
        for code, st in self.stocks.items():
            if st.state == "short":
                v = row["s"].get(code)
                px = v[0] if v and v[0] else (st.bars[-1].c if st.bars else st.rec["entry"])
                self.exit(st, px, ts, why)

    def candidates_message(self):
        if self.cands_sent:
            return
        self.cands_sent = True
        lst = sorted(self.stocks.values(), key=lambda s: -(s.open_px / s.prev_close))
        if not lst:
            self.send(f"📋【BB売り・紙】{self.day} 今日の候補（GU+3%・前日代金1億+・決算除外）なし")
            return
        lines = [f"・{s.code} {s.name}  GU+{(s.open_px / s.prev_close - 1) * 100:.1f}%  寄値{s.open_px:,.0f}" for s in lst[:20]]
        self.send(f"📋【BB売り・紙】{self.day} 今日の候補 {len(lst)}銘柄（10:00〜11:25 に合図を見る・いちにち信用の在庫を確認）\n" + "\n".join(lines))

    def summary(self):
        tk = [r for r in self.records if r.get("taken")]
        all_ = self.records
        tot = sum(r["pnl_yen"] for r in tk)
        self.send(f"📊【BB売り・紙】{self.day} まとめ  合図{len(all_)}件・建てた{len(tk)}件  合計 {tot:+,}円/100株"
                  + (f"（見送り分も入れると {sum(r['pnl_yen'] for r in all_):+,}円）" if len(all_) > len(tk) else ""))
        STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
        json.dump({"date": self.day, "records": self.records}, open(STATE_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def run(day: str, live: bool, write_log: bool):
    S = Session(day, notify=live, write_log=write_log)
    path = MINUTES_DIR / f"{day}.jsonl"
    off = 0
    print(f"[bb_sell] {day} 前日の足 {len(S.prev_bars)}銘柄・決算除外 {len(S.earn)}銘柄・minutes={path}", flush=True)
    while True:
        rows, off = read_rows(path, off)
        for row in rows:
            S.on_row(row)
            if not S.cands_sent and row["ts"][11:16] >= "09:58":
                S.candidates_message()
        now = datetime.now()
        if not live or now.strftime("%H:%M") >= END_AT:
            break
        time.sleep(POLL_SEC)
    if not S.cands_sent:
        S.candidates_message()
    # 引け後の断面が無いまま終わった場合も手仕舞う
    rows, _ = read_rows(path)
    if rows:
        S.close_all(rows[-1], "大引け")
    S.summary()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--replay")
    ap.add_argument("--log", action="store_true", help="再生でも台帳に書く")
    a = ap.parse_args()
    if a.live:
        day = datetime.now().strftime("%Y-%m-%d")
        if datetime.now().weekday() >= 5:
            print("週末 → 終了"); return
        run(day, live=True, write_log=True)
    elif a.replay:
        run(a.replay, live=False, write_log=a.log)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()

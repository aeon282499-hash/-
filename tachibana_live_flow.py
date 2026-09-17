# -*- coding: utf-8 -*-
"""tachibana_live_flow.py — 場中の「資金がいま来ているテーマ／セクター／銘柄」を立花APIの時価で巡回集計し、
Cloudflare の chimp-live ハブへ毎分配信する（モメンタムチンパン v5 🔥ライブの供給元・2026-09-14 本人依頼）。

本人の要望: 「デイトレ用に特化。資金が入っているテーマやセクターを高頻度で更新して、そのテーマの銘柄を
  何%上がってる／資金が今来てる、で分かりやすく。情報は一番正確なところから」
  → 一番正確＝取引所リアルタイム（立花e支店API）。株探/SNSは20分遅延か伝聞なので使わない。

巡回: 流動性上位（20日平均代金 ≥ UNIVERSE_MIN_OKU 億）＋テーマ構成銘柄 ≈ 2,000銘柄を120銘柄/要求で
  約17要求、要求間隔 --interval 秒（既定3秒）＝約1分で一巡 → 一巡ごとに集計して POST /publish。
  共用APIへの配慮: 板レコーダー(1要求/10秒)と同程度の密度に抑え、注文系は一切呼ばない。
  同時刻に走る板レコーダーとは p_no をファイルロックで共有（tachibana/client.py 2026-09-14）。

指標（すべて当日・取引所値）:
  chg      前日終値比 %（pDPP / pPRP）
  tov      当日累計売買代金（円）
  flow     資金流入倍率＝当日累計代金 ÷ (20日平均代金 × 時刻別の想定進捗)  1.0=いつも通り
  flow5    直近5分の流入倍率＝5分間の代金増分 ÷ (20日平均代金 ÷ 66)  ← 「今来てる」はこっち
  d5       直近5分の値動き %
  vwap_dev VWAP乖離 %
  hi       当日高値更新中（現在値==高値）
テーマ/セクターは構成銘柄の Σ代金 ÷ Σ(平均代金×進捗) と、代金加重の騰落・中央値・上昇比率。

実行: python -X utf8 tachibana_live_flow.py            # 08:58〜15:35 を巡回（場外なら即終了）
      python -X utf8 tachibana_live_flow.py --once     # 1巡だけ集計して配信（場外でも動く・動作確認用）
      python -X utf8 tachibana_live_flow.py --once --no-publish --dump live_flow/test.json
Windowsタスク: TachibanaLiveFlow 平日 08:58（ログオン中のみ・7時間上限）
出力: live_flow/latest.json（ローカル控え）・logs/live_flow.log
配信先: https://chimp-live.aeon282499.workers.dev/publish（トークン .tachibana/chimp_live_token.txt）
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
from tachibana import TachibanaClient, TachibanaError  # noqa: E402

OUT_DIR = ROOT / "live_flow"
LOG = ROOT / "logs" / "live_flow.log"
PAGES_INDEX = "https://aeon282499-hash.github.io/-/data/search_index.json"
LOCAL_INDEX = ROOT / "kabuai" / "data" / "search_index.json"
THEMES = ROOT / "theme_members.json"
SECTOR_MAP = ROOT / "sector33_map.json"
HUB_URL = os.environ.get("CHIMP_LIVE_URL", "https://chimp-live.aeon282499.workers.dev/publish")
TOKEN_FILE = ROOT / ".tachibana" / "chimp_live_token.txt"

UNIVERSE_MIN_OKU = 0.5          # 20日平均代金 0.5億円以上（≈2,500銘柄・21要求/一巡）
KABUTAN = ROOT / "kabutan_themes.json"   # 株探テーマ辞書（kabutan_themes.py・週次）
KABUTAN_MIN_MEMBERS = 3         # 株探テーマは巡回対象の構成銘柄が3以上のものだけ集計
TOP_MEMBERS = 40                # 構成銘柄リストを載せるテーマ数（並び上位）＋手作り全部（株探10本で169KB→上限を絞る）
MEMBERS_CAP = 20                # 1テーマの構成銘柄リスト上限（代金順）
TOP_FULL = 150                  # 完全な形で載せるテーマ数（それ以外は themes_rest に圧縮行で載せる・初日実測455KB/分→削減）
TOP_SERIES = 40                 # スパークライン系列を載せるテーマ数＋手作り全部
SECTOR_MEMBER_MIN_TOV = 1e8     # セクター構成銘柄は当日代金1億以上を全部載せる（2026-09-17・全銘柄だと660KB/分）
SECTOR_MEMBERS_CAP = 80         # 1セクターの上限（代金順）
PRICE_COLS = ("pDPP", "tDPP:T", "pDOP", "pDHP", "pDLP", "pDV", "pDJ", "pVWAP", "pPRP")
SESSION_START, SESSION_END = "08:58", "15:35"
# 時刻別の想定進捗（累計代金が1日の何割まで来ているか・U字カーブの近似）。flow の分母に使う。
PROGRESS = [("09:00", 0.0), ("09:15", 0.13), ("09:30", 0.21), ("10:00", 0.31), ("10:30", 0.38),
            ("11:00", 0.44), ("11:30", 0.49), ("12:30", 0.49), ("13:00", 0.57), ("13:30", 0.64),
            ("14:00", 0.71), ("14:30", 0.78), ("15:00", 0.86), ("15:20", 0.93), ("15:30", 1.0)]
BUCKETS_5MIN = 66               # 330分（9:00-11:30, 12:30-15:30）÷5
SERIES_STEP_SEC = 300           # スパークライン用の系列は5分刻み


def _log_setup() -> logging.Logger:
    LOG.parent.mkdir(exist_ok=True)
    lg = logging.getLogger("live_flow")
    lg.setLevel(logging.INFO)
    if not lg.handlers:
        fh = logging.FileHandler(LOG, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        lg.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        lg.addHandler(sh)
    return lg


def _f(v) -> float | None:
    try:
        if v in ("", None):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def hhmm(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%H:%M")


def progress_at(t: str) -> float:
    """hh:mm → 想定進捗（0〜1）。折れ線補間。"""
    def mins(s: str) -> int:
        h, m = s.split(":")
        return int(h) * 60 + int(m)
    x = mins(t)
    if x <= mins(PROGRESS[0][0]):
        return 0.0
    for (t0, p0), (t1, p1) in zip(PROGRESS, PROGRESS[1:]):
        a, b = mins(t0), mins(t1)
        if a <= x <= b:
            return p0 if b == a else p0 + (p1 - p0) * (x - a) / (b - a)
    return 1.0


def session_state(t: str) -> str:
    if t < "09:00":
        return "pre"
    if t < "11:30":
        return "am"
    if t < "12:30":
        return "lunch"
    if t <= "15:30":
        return "pm"
    return "closed"


def load_universe(log) -> tuple[dict[str, dict], dict[str, dict]]:
    """search_index（Pages公開の20日平均代金・名前・業種）とテーマ辞書から巡回対象を作る。"""
    idx = None
    try:
        with urllib.request.urlopen(PAGES_INDEX, timeout=30) as r:
            idx = json.loads(r.read().decode("utf-8"))
        log.info(f"search_index: Pages {idx.get('data_date')} {idx.get('count')}銘柄")
    except Exception as e:  # noqa: BLE001
        log.warning(f"Pages の search_index 取得失敗→ローカル: {e}")
        if LOCAL_INDEX.exists():
            idx = json.load(open(LOCAL_INDEX, encoding="utf-8"))
    stocks = (idx or {}).get("stocks", [])
    sec_map = {}
    if SECTOR_MAP.exists():
        sec_map = {k.removesuffix(".T"): v for k, v in json.load(open(SECTOR_MAP, encoding="utf-8")).items()}
    themes_raw = json.load(open(THEMES, encoding="utf-8"))["themes"] if THEMES.exists() else {}
    themes = {}
    theme_codes: set[str] = set()
    for key, t in themes_raw.items():
        members = [m["ticker"].removesuffix(".T") for m in t.get("members", [])]
        themes[key] = {"label": key.replace("_", " "), "desc": t.get("desc", ""), "members": members, "source": "hand"}
        theme_codes |= set(members)
    # 株探テーマ（2026-09-14 本人「やれや」）: 構成銘柄は巡回対象（代金0.5億以上）に絞る＝要求数を増やさない
    n_kab = 0
    if KABUTAN.exists():
        try:
            kj = json.load(open(KABUTAN, encoding="utf-8"))
            for name, t in (kj.get("themes") or {}).items():
                if name in themes:
                    continue
                themes["k:" + name] = {"label": name, "desc": "", "members": list(t.get("members") or []), "source": "kabutan"}
                n_kab += 1
            log.info(f"株探テーマ辞書: {n_kab}本（{kj.get('fetched')}）")
        except Exception as e:  # noqa: BLE001
            log.warning(f"株探テーマ辞書の読込失敗: {e}")
    uni: dict[str, dict] = {}
    for r in stocks:
        code = str(r.get("code", ""))
        tov = float(r.get("turnover_oku") or 0) * 1e8
        if code and (tov >= UNIVERSE_MIN_OKU * 1e8 or code in theme_codes):
            uni[code] = {"name": r.get("name") or code, "sector": r.get("sector") or sec_map.get(code),
                         "avg_tov": tov, "r20": r.get("r20"), "rng20": r.get("rng20")}
    for c in theme_codes:
        if c not in uni:
            uni[c] = {"name": c, "sector": sec_map.get(c), "avg_tov": 0.0}
    for k, t in themes.items():           # 株探テーマの構成銘柄は巡回対象内だけに絞る
        if t.get("source") == "kabutan":
            t["members"] = [c for c in t["members"] if c in uni]
    themes = {k: t for k, t in themes.items() if t.get("source") == "hand" or len(t["members"]) >= KABUTAN_MIN_MEMBERS}
    log.info(f"巡回対象 {len(uni)}銘柄（代金{UNIVERSE_MIN_OKU}億以上＋手作りテーマ構成{len(theme_codes)}）・テーマ{len(themes)}（株探{sum(1 for t in themes.values() if t.get('source')=='kabutan')}）")
    return uni, themes


class FlowState:
    """5分前との差分用リングバッファと、5分刻みの系列。"""

    def __init__(self):
        self.hist: dict[str, deque] = {}      # code → deque[(epoch, last, tov)]
        self.series_ts: list[str] = []
        self.series: dict[str, dict[str, list]] = {"themes": {}, "sectors": {}}
        self._last_series_epoch = 0.0

    def push(self, code: str, epoch: float, last: float | None, tov: float | None) -> tuple[float | None, float | None]:
        dq = self.hist.setdefault(code, deque(maxlen=60))
        dq.append((epoch, last, tov))
        # 5分（300秒）前に最も近い点
        ref = None
        for e, l, t in dq:
            if epoch - e >= 300:
                ref = (l, t)
            else:
                break
        if ref is None:
            return None, None
        d5 = ((last / ref[0] - 1) * 100) if (last and ref[0]) else None
        dt = (tov - ref[1]) if (tov is not None and ref[1] is not None) else None
        return d5, dt

    def maybe_series(self, epoch: float, ts: str, groups: dict[str, list[dict]]):
        if epoch - self._last_series_epoch < SERIES_STEP_SEC:
            return
        self._last_series_epoch = epoch
        self.series_ts.append(ts)
        for kind in ("themes", "sectors"):
            for g in groups[kind]:
                arr = self.series[kind].setdefault(g["key"], {"flow": [], "chg": []})
                arr["flow"].append(round(g["flow"], 2) if g["flow"] is not None else None)
                arr["chg"].append(round(g["chg_w"], 2) if g["chg_w"] is not None else None)


def sweep(client: TachibanaClient, codes: list[str], interval: float, log) -> dict[str, dict]:
    """全銘柄を120件ずつ取得。要求間に interval 秒。失敗チャンクは飛ばす。"""
    out: dict[str, dict] = {}
    chunks = [codes[i:i + 120] for i in range(0, len(codes), 120)]
    for i, ch in enumerate(chunks):
        t0 = time.monotonic()
        try:
            rows = client.market_price(ch, PRICE_COLS)
            for r in rows:
                c = str(r.get("sIssueCode", "")).strip()
                if c:
                    out[c] = r
        except TachibanaError as e:
            log.warning(f"chunk{i} 取得失敗: {e}")
        except Exception as e:  # noqa: BLE001
            log.warning(f"chunk{i} 例外: {e}")
        if i < len(chunks) - 1:
            time.sleep(max(0.0, interval - (time.monotonic() - t0)))
    return out


def aggregate(raw: dict[str, dict], uni: dict[str, dict], themes: dict[str, dict], st: FlowState,
              now: datetime) -> dict:
    t = hhmm(now)
    epoch = time.time()
    prog = progress_at(t)
    state = session_state(t)
    per5 = 1.0 / BUCKETS_5MIN
    stocks: dict[str, dict] = {}
    for code, meta in uni.items():
        r = raw.get(code)
        if not r:
            continue
        last = _f(r.get("pDPP")); prev = _f(r.get("pPRP")); tov = _f(r.get("pDJ")); vol = _f(r.get("pDV"))
        high = _f(r.get("pDHP")); low = _f(r.get("pDLP")); vwap = _f(r.get("pVWAP")); opn = _f(r.get("pDOP"))
        if not last or last <= 0:
            last = None
        chg = ((last / prev - 1) * 100) if (last and prev) else None
        avg = meta.get("avg_tov") or 0.0
        flow = (tov / (avg * prog)) if (tov and avg > 0 and prog > 0.02) else None
        d5, dtov = st.push(code, epoch, last, tov)
        flow5 = (dtov / (avg * per5)) if (dtov is not None and avg > 0) else None
        stocks[code] = {
            "code": code, "name": meta.get("name") or code, "sector": meta.get("sector"),
            "last": last, "chg": None if chg is None else round(chg, 2),
            "tov": int(tov or 0), "avg_tov": int(avg),
            "flow": None if flow is None else round(flow, 2),
            "flow5": None if flow5 is None else round(flow5, 2),
            "d5": None if d5 is None else round(d5, 2),
            "vwap_dev": round((last / vwap - 1) * 100, 2) if (last and vwap) else None,
            "hi": bool(last and high and last >= high),
            "gap": round((opn / prev - 1) * 100, 2) if (opn and prev) else None,
        }

    def group(key: str, label: str, members: list[str], desc: str = "", source: str = "") -> dict | None:
        ms = [stocks[c] for c in members if c in stocks]
        if not ms:
            return None
        tov = sum(m["tov"] for m in ms)
        avg = sum(m["avg_tov"] for m in ms)
        chgs = [m["chg"] for m in ms if m["chg"] is not None]
        w = [(m["chg"], m["tov"]) for m in ms if m["chg"] is not None and m["tov"] > 0]
        chg_w = (sum(c * x for c, x in w) / sum(x for _, x in w)) if w and sum(x for _, x in w) > 0 else None
        d5s = [(m["d5"], m["tov"]) for m in ms if m["d5"] is not None and m["tov"] > 0]
        d5_w = (sum(c * x for c, x in d5s) / sum(x for _, x in d5s)) if d5s else None
        f5 = [m for m in ms if m["flow5"] is not None and m["avg_tov"] > 0]
        flow5 = (sum(m["flow5"] * m["avg_tov"] for m in f5) / sum(m["avg_tov"] for m in f5)) if f5 else None
        return {
            "key": key, "label": label, "desc": desc, "n": len(ms), "src": source,
            "tov": tov, "avg_tov": avg,
            "flow": round(tov / (avg * prog), 2) if (avg > 0 and prog > 0.02 and tov) else None,
            "flow5": None if flow5 is None else round(flow5, 2),
            "chg_w": None if chg_w is None else round(chg_w, 2),
            "chg_med": round(statistics.median(chgs), 2) if chgs else None,
            "d5_w": None if d5_w is None else round(d5_w, 2),
            "up_ratio": round(sum(1 for c in chgs if c > 0) / len(chgs), 2) if chgs else None,
            "members": sorted((m["code"] for m in ms), key=lambda c: -stocks[c]["tov"]),
        }

    theme_groups = [g for g in (group(k, v["label"], v["members"], v.get("desc", ""), v.get("source", "")) for k, v in themes.items()) if g]
    theme_groups = [g for g in theme_groups if g["src"] == "hand" or g["n"] >= KABUTAN_MIN_MEMBERS]
    sec_members: dict[str, list[str]] = {}
    for c, m in stocks.items():
        if m.get("sector"):
            sec_members.setdefault(m["sector"], []).append(c)
    sector_groups = [g for g in (group(s, s, cs) for s, cs in sec_members.items()) if g]
    for g in sector_groups:
        # 2026-09-17 本人「ちゃんと銘柄を入れて」→ 代金1億以上は全部載せる（上限80・代金順）。1億未満は n との差で「省略」と表示
        g["members"] = [c for c in g["members"] if stocks[c]["tov"] >= SECTOR_MEMBER_MIN_TOV][:SECTOR_MEMBERS_CAP]
    # 🎯土俵（前夜配信・板レコーダーが8:55にgit pull済み）の銘柄も常に載せる＝「土俵のいま」
    arena_codes: list[str] = []
    try:
        ar = ROOT / "arena_watchlist.json"
        if ar.exists():
            aj = json.load(open(ar, encoding="utf-8"))
            if str(aj.get("target_date", "")) >= now.strftime("%Y-%m-%d"):
                arena_codes = [str(r.get("code", "")).replace(".T", "") for r in aj.get("rows", [])]
    except Exception:  # noqa: BLE001
        arena_codes = []
    st.maybe_series(epoch, now.strftime("%H:%M"), {"themes": theme_groups, "sectors": sector_groups})
    # 並び（場中=直近5分・場外=当日）で上位だけ構成銘柄/系列を載せる＝株探1,500本でも毎分120KB級に収める
    intraday = state in ("am", "pm", "lunch")
    sort_key = "flow5" if intraday else "flow"
    theme_groups.sort(key=lambda g: -(g[sort_key] if g[sort_key] is not None else -9))
    # 2026-09-17 本人「テーマに銘柄が入ってない」: フロント(v5.1)の既定並びが騰落率(chg_w)になったので、
    # 資金の並び(flow5/flow)の上位だけに構成銘柄を載せると既定表示の上位が空になる。
    # → 構成銘柄は「資金・騰落率・5分の動き」それぞれの上位TOP_MEMBERSの和集合＋手作り全部に載せる。
    def _top_keys(k: str, n: int) -> set[str]:
        return set(g["key"] for g in sorted(theme_groups, key=lambda g: -(g.get(k) if g.get(k) is not None else -9))[:n])
    hand_keys = set(g["key"] for g in theme_groups if g["src"] == "hand")
    top_members = _top_keys(sort_key, TOP_MEMBERS) | _top_keys("chg_w", TOP_MEMBERS) | _top_keys("d5_w", TOP_MEMBERS) | hand_keys
    top_series = _top_keys(sort_key, TOP_SERIES) | hand_keys
    for g in theme_groups:
        g["members"] = g["members"][:MEMBERS_CAP] if g["key"] in top_members else []
    full_keys = _top_keys(sort_key, TOP_FULL) | _top_keys("chg_w", TOP_FULL) | hand_keys
    themes_rest = [[g["key"], g["label"], g["n"], g["flow5"], g["flow"], g["chg_w"], g["up_ratio"], g["tov"]]
                   for g in theme_groups if g["key"] not in full_keys]
    theme_groups = [g for g in theme_groups if g["key"] in full_keys]

    # 個別の「いま資金が来ている」上位（テーマ外も拾う）
    liquid = [m for m in stocks.values() if m["tov"] >= 1e8 and m["avg_tov"] > 0]
    hot5 = sorted((m for m in liquid if m["flow5"] is not None), key=lambda m: -m["flow5"])[:40]
    hot = sorted((m for m in liquid if m["flow"] is not None), key=lambda m: -m["flow"])[:40]
    gain = sorted((m for m in liquid if m["chg"] is not None), key=lambda m: -m["chg"])[:40]
    lose = sorted((m for m in liquid if m["chg"] is not None), key=lambda m: m["chg"])[:20]
    tovtop = sorted(liquid, key=lambda m: -m["tov"])[:40]
    keep: set[str] = set()
    for g in theme_groups:
        keep |= set(g["members"])
    for g in sector_groups:
        keep |= set(g["members"])
    for arr in (hot5, hot, gain, lose, tovtop):
        keep |= {m["code"] for m in arr}
    keep |= set(arena_codes)
    chg_all = [m["chg"] for m in stocks.values() if m["chg"] is not None]
    tov_all = sum(m["tov"] for m in stocks.values())
    avg_all = sum(m["avg_tov"] for m in stocks.values() if m["tov"] > 0)
    wall = [(m["chg"], m["tov"]) for m in stocks.values() if m["chg"] is not None and m["tov"] > 0]
    market = {
        "n": len(stocks), "adv": sum(1 for c in chg_all if c > 0), "dec": sum(1 for c in chg_all if c < 0),
        "unch": sum(1 for c in chg_all if c == 0),
        "tov": tov_all, "flow": round(tov_all / (avg_all * prog), 2) if (avg_all > 0 and prog > 0.02) else None,
        "chg_w": round(sum(c * x for c, x in wall) / sum(x for _, x in wall), 2) if wall else None,
        "chg_med": round(statistics.median(chg_all), 2) if chg_all else None,
    }
    return {
        "schema": "chimp-live-1",
        "ts": now.strftime("%Y-%m-%d %H:%M:%S"), "date": now.strftime("%Y-%m-%d"), "hhmm": t,
        "state": state, "progress": round(prog, 3),
        "source": "立花証券e支店API（取引所リアルタイム）", "interval_note": "全銘柄を約1分で一巡・毎分配信",
        "universe": len(uni), "got": len(raw),
        "market": market,
        "themes": theme_groups, "themes_rest": themes_rest,
        "n_themes_kabutan": sum(1 for g in theme_groups if g["src"] == "kabutan") + len(themes_rest),
        "sectors": sorted(sector_groups, key=lambda g: -(g["chg_w"] if g["chg_w"] is not None else -999)),
        "hot5": [m["code"] for m in hot5], "hot": [m["code"] for m in hot], "gain": [m["code"] for m in gain],
        "lose": [m["code"] for m in lose], "tovtop": [m["code"] for m in tovtop],
        "arena": [c for c in arena_codes if c in stocks],
        "stocks": {c: {k: v for k, v in stocks[c].items() if k != "avg_tov"} for c in keep if c in stocks},
        "series": {"ts": st.series_ts, "themes": {k: v for k, v in st.series["themes"].items() if k in top_series},
                   "sectors": st.series["sectors"]},
    }


MINUTES_DIR = OUT_DIR / "minutes"       # 毎分の全銘柄断面（1行=1巡・fibo_daytrade.py が5分足にする）
FIBO_LIVE = OUT_DIR / "fibo_live.json"   # fibo_daytrade.py --live が書く候補（payload["fibo"] に同梱）
_MIN_KEYS = ("pDPP", "pDOP", "pDHP", "pDLP", "pDV", "pDJ", "pVWAP", "pPRP")


def append_minutes(raw: dict[str, dict], ts: str, log) -> None:
    """1巡ぶんの断面を live_flow/minutes/YYYY-MM-DD.jsonl に1行追記（約2,500銘柄・約150KB/分）。失敗は握る。"""
    try:
        MINUTES_DIR.mkdir(parents=True, exist_ok=True)
        row = {"ts": ts, "s": {}}
        for code, r in raw.items():
            vals = []
            for k in _MIN_KEYS:
                v = r.get(k)
                try:
                    vals.append(float(v) if v not in (None, "") else None)
                except (TypeError, ValueError):
                    vals.append(None)
            row["s"][code] = vals
        with open(MINUTES_DIR / f"{ts[:10]}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception as e:  # noqa: BLE001
        log.warning(f"minutes追記失敗: {e}")


def attach_fibo(payload: dict, log) -> None:
    """fibo_daytrade.py --live の出力（3分以内の物だけ）を payload['fibo'] に同梱。銘柄は stocks に無くても良い（fibo側が値を持つ）。"""
    try:
        if not FIBO_LIVE.exists():
            return
        if time.time() - FIBO_LIVE.stat().st_mtime > 180:
            return
        payload["fibo"] = json.loads(FIBO_LIVE.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        log.warning(f"fibo同梱失敗: {e}")


def publish(payload: dict, log) -> bool:
    if not TOKEN_FILE.exists():
        log.warning("配信トークンなし（.tachibana/chimp_live_token.txt）→ 配信スキップ")
        return False
    tok = TOKEN_FILE.read_text(encoding="utf-8-sig").strip()
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(HUB_URL, data=body, method="POST",
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {tok}",
                                          # Cloudflareのbot判定(error 1010)はPython既定UAを弾くので明示
                                          "User-Agent": "chimp-live-flow/1.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                res = json.loads(r.read().decode("utf-8"))
            log.info(f"配信OK {len(body)/1024:.0f}KB pushed={res.get('pushed')} {res.get('updated_at')}")
            return True
        except Exception as e:  # noqa: BLE001
            log.warning(f"配信失敗({attempt+1}/3): {e}")
            time.sleep(5)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="1巡だけ集計して配信（場外でも動く）")
    ap.add_argument("--interval", type=float, default=3.0, help="要求間隔（秒）")
    ap.add_argument("--no-publish", action="store_true")
    ap.add_argument("--dump", default="", help="集計JSONの書き出し先（既定 live_flow/latest.json）")
    ap.add_argument("--limit", type=int, default=0, help="銘柄数を絞る（動作確認用）")
    ap.add_argument("--end", default=SESSION_END)
    a = ap.parse_args()
    log = _log_setup()
    OUT_DIR.mkdir(exist_ok=True)
    now = datetime.now()
    if not a.once and (now.weekday() >= 5 or hhmm(now) > a.end):
        log.info(f"場外（{now:%a %H:%M}）→ 終了")
        return 0
    # 止まらないための方針（2026-09-14 本人「エラーで止まったりしない？」）:
    #  ①ユニバース読込/ログインは失敗しても60秒おきに引け(--end)まで再試行 ②1巡ごとの取得・集計・配信は
    #  まるごと try/except で握って次の巡回へ ③連続失敗が続いても終了しない（ログに残す）
    #  ④タスク側も RestartCount=3（異常終了なら5分後に再起動）
    uni, themes, client = None, None, None
    while uni is None:
        try:
            uni, themes = load_universe(log)
        except Exception as e:  # noqa: BLE001
            log.error(f"ユニバース読込失敗（60秒後に再試行）: {e}")
            if a.once or hhmm() > a.end:
                return 1
            time.sleep(60)
    codes = sorted(uni.keys())
    if a.limit:
        codes = codes[:a.limit]
    while client is None:
        try:
            client = TachibanaClient()
            client.ensure_session()
        except Exception as e:  # noqa: BLE001
            log.error(f"ログイン失敗（60秒後に再試行）: {e}")
            client = None
            if a.once or hhmm() > a.end:
                return 1
            time.sleep(60)
    st = FlowState()
    try:   # 同日の再起動なら系列（スパークライン）を引き継ぐ
        prev = json.loads((OUT_DIR / "latest.json").read_text(encoding="utf-8"))
        if prev.get("date") == datetime.now().strftime("%Y-%m-%d") and prev.get("series", {}).get("ts"):
            st.series_ts = list(prev["series"]["ts"]); st.series = {"themes": dict(prev["series"].get("themes", {})), "sectors": dict(prev["series"].get("sectors", {}))}
            st._last_series_epoch = time.time(); log.info(f"系列を引き継ぎ {len(st.series_ts)}点")
    except Exception:  # noqa: BLE001
        pass
    n_sweep, n_fail = 0, 0
    while True:
        now = datetime.now()
        if not a.once and hhmm(now) < SESSION_START:
            time.sleep(15); continue
        t0 = time.monotonic()
        try:
            raw = sweep(client, codes, a.interval, log)
            if not raw:
                raise RuntimeError("時価が1件も取れない（API/セッション異常の疑い）")
            payload = aggregate(raw, uni, themes, st, datetime.now())
            n_sweep += 1
            append_minutes(raw, payload["ts"], log)        # 毎分の断面を日別ファイルへ（フィボ押し目の5分足素材）
            attach_fibo(payload, log)                        # fibo_daytrade.py が書く候補を同梱（無ければ何もしない）
            dump = Path(a.dump) if a.dump else OUT_DIR / "latest.json"
            dump.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            top = payload["themes"][:3]
            log.info(f"#{n_sweep} {payload['hhmm']} {payload['state']} got{payload['got']}/{len(codes)} "
                     f"{time.monotonic()-t0:.0f}s 市場flow{payload['market']['flow']} "
                     f"top: " + " / ".join(f"{g['label']} f5={g['flow5']} chg={g['chg_w']}" for g in top))
            if not a.no_publish:
                publish(payload, log)
            n_fail = 0
        except Exception as e:  # noqa: BLE001
            n_fail += 1
            log.exception(f"巡回で例外（{n_fail}回目・続行）: {e}")
            if a.once:
                return 1
            time.sleep(min(60, 10 * n_fail))
            if n_fail % 5 == 0:          # 5回続けて落ちたらセッションを取り直す
                try:
                    client.clear_session(); client.ensure_session(); log.info("再ログインした")
                except Exception as e2:  # noqa: BLE001
                    log.error(f"再ログイン失敗: {e2}")
            continue
        if a.once:
            break
        if hhmm(datetime.now()) > a.end:
            log.info("引け後の最終配信まで完了 → 終了")
            break
        # 昼休みは1巡/3分に落とす（値が動かない）
        if payload["state"] == "lunch":
            time.sleep(120)
    return 0


if __name__ == "__main__":
    sys.exit(main())

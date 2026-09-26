# -*- coding: utf-8 -*-
"""masutan_signal.py — 増担保規制「再点火」シグナル（夕方ジョブ・小額実弾）2026-09-27 新設

本人決定（2026-09-27）: 「②小さい額の実弾で始める（1件30〜50万）」。

ルール（2026-09-19 確定・_bt_masutan_exante_0918.py / 2026-09-26 立花で再現 _bt_masutan_synth26_0926.py）:
  1. 東証の増担保規制（/markets/margin-alert の PubReason.Restricted=1・区分003/004/005）がかかっている銘柄。
  2. 規制の公表日（エピソードの最初の公表日）の翌営業日から数えて、終値が25日移動平均の±15%以内に
     2日続いた日（エピソード内で最初の1回だけ）を「シグナル日」とする。
  3. シグナル日の翌営業日の寄りで買い、シグナル日から3営業日後の大引けで売る（買った日を1日目として3日目の引け）。
  4. 5日平均売買代金3億円以上だけ。翌日の寄りがストップ高に張り付いたら買えない＝見送り。
  成績（10年・コスト0.3%後）: 代金≥3億 557件 +1.88%/回・勝率61%・PF1.61・2016〜2026の11年すべて0以上。
  ⚠ 26年検証は不可能（株価だけで作った偽の規制では -0.36%/回＝儲けは「本当に規制された株」だけ・
    信用残データも2016年より前は無い）→ 小さい額で始める（1件30〜50万）。
  ⚠ 稼ぎの中身は「解除で上がる」ではなく「規制で冷えた急騰株の再点火」（的中=本当に3日後解除 の玉は+0.2%・
    外れて規制が続いた玉が+4.9%）。

データ: J-Quants /markets/margin-alert（日々公表・夕方公表。9/25は18:26 JSTのランで当日公表分が取れていた）
        と /equities/bars/daily（四本値は16:30頃公開）。当日の公表分が未反映なら前営業日の公表分で判定する
        （シグナルは「公表日の翌日から2日連続」なので最短でも公表日の2営業日後＝前日分の規制情報で足りる）。
状態: masutan_state.json（規制エピソード＝銘柄ごとの公表日の連続。穴が3公表日以内なら同じエピソード＝9/18 BTと同じ）
出力: masutan_signals.json（その日の判定の全部）・positions_masutan.json（帳簿）・Discord（DISCORD_WEBHOOK_MASUTAN_URL・未設定なら投稿しない）
実行: python masutan_signal.py [--date YYYY-MM-DD] [--dry] [--force] [--init N]
  夕方ジョブ(.github/workflows/schedule_evening.yml)から continue-on-error で呼ばれる。
一致の監査: _audit_masutan_parity.py ／ テスト: _test_masutan.py
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import date, datetime, timedelta
import zoneinfo

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

# ── ルール（根拠は上の docstring）──────────────────────────────────────────
MASUTAN_BAND_PCT = 15.0        # 25日線±15%以内（東証の増担保解除基準の株価条件と同じ幅）
MASUTAN_MA_DAYS = 25
MASUTAN_CALM_DAYS = 2          # 2日連続で「シグナル日」（3日連続=+1.26%・4日連続=-0.74%＝早いほど良い・9/19）
MASUTAN_EXIT_OFFSET = 3        # シグナル日から3営業日後の大引けで売る（保有1日+0.15/2日+0.90/3日+1.63%・9/19）
MASUTAN_TOV_MIN_OKU = 3.0      # 5日平均売買代金（億円）。全件+1.40%/PF1.47 → 代金≥3億 +1.88%/PF1.61（9/26 立花再現）
MASUTAN_EPISODE_GAP = 3        # 同じ銘柄の規制公表日の穴がこの公表日数以内なら同じエピソード（9/18 BT）
MASUTAN_SIZES = (300_000, 500_000)   # 本人「1件30〜50万」＝配信では両方の株数を出す
MASUTAN_COST_PCT = 0.3         # BTのコスト（往復）。帳簿は gross と net(-0.3%) を両方持つ
MASUTAN_BARS_LOOKBACK_DAYS = 90      # 25日線に要る過去分＋余裕（暦日）
MASUTAN_INIT_DAYS = 250              # 状態ファイルが無い時に遡る営業日数（規制は中央値14営業日だが200営業日近い長期もある＝開始日を取り違えると偽シグナルになるので長めに。通常は状態ファイルがあるので使わない）

STATE_FILE = "masutan_state.json"
SIG_FILE = "masutan_signals.json"
BOOK_FILE = "positions_masutan.json"
CAL_FILE = "market_calendar.csv"
WEBHOOK_ENV = "DISCORD_WEBHOOK_MASUTAN_URL"

# 値幅制限（普通株・前日終値→制限値幅）。ストップ高/安の張り付き判定に使う（9/26 BTと同じ表）
_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500),
        (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000),
        (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000), (200000, 40000),
        (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000), (float("inf"), 300000)]
REG_CLASS_LABEL = {"003": "区分003", "004": "区分004(引き上げ2段目)", "005": "区分005(引き上げ3段目)"}
WEEKDAY_JA = "月火水木金土日"


def limit_width(p: float) -> float:
    for b, w in _LIM:
        if p < b:
            return float(w)
    return 300000.0


# ── 営業日（東証の株式）──────────────────────────────────────────────────
_TRADING: list[str] | None = None


def _trading_days() -> list[str]:
    """market_calendar.csv（J-Quants公式・HolDiv=1が株式の営業日。3は祝日の先物取引のみ）"""
    global _TRADING
    if _TRADING is None:
        days = []
        try:
            with open(CAL_FILE, encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if str(r.get("HolDiv", "")).strip() == "1":
                        days.append(str(r["Date"])[:10])
        except FileNotFoundError:
            pass
        _TRADING = sorted(days)
    return _TRADING


def is_trading_day(d: date) -> bool:
    days = _trading_days()
    s = d.isoformat()
    if days and days[0] <= s <= days[-1]:
        import bisect
        i = bisect.bisect_left(days, s)
        return i < len(days) and days[i] == s
    import jpholiday                      # カレンダーの外（2028年〜）だけ祝日ライブラリで代用
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3):
        return False
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def add_trading_days(d: date, n: int) -> date:
    """d から n 営業日後（n>=1）。d 自体が休日でも「次の営業日が1日目」"""
    cur = d
    k = 0
    while k < n:
        cur += timedelta(days=1)
        if is_trading_day(cur):
            k += 1
    return cur


def trading_gap(a: str, b: str) -> int:
    """営業日インデックスの差（a<b なら正）。9/18 BT の「公表日インデックスの差」と同じ数え方"""
    da, db = date.fromisoformat(a), date.fromisoformat(b)
    if da == db:
        return 0
    sgn = 1 if db > da else -1
    lo, hi = (da, db) if db > da else (db, da)
    k = 0
    cur = lo
    while cur < hi:
        cur += timedelta(days=1)
        if is_trading_day(cur):
            k += 1
    return sgn * k


# ── 判定の中身（監査・テストからも使う純粋関数）─────────────────────────────
def calm_path(closes: list[float], dates: list[str], start: str) -> dict:
    """公表日 start の翌営業日から、終値が25日線±MASUTAN_BAND_PCT%以内の連続日数を数える。
    戻り値: {"first_hit": 最初に連続MASUTAN_CALM_DAYS日になった日(or None), "count": 最終日の連続日数,
             "dev": 最終日の25日線乖離%, "ok": startが価格データにあったか}
    9/18 BT と同じ: dev=NaN(25日に満たない)は「外」扱いで連続が切れる。"""
    n = len(closes)
    out = {"first_hit": None, "count": 0, "dev": float("nan"), "ok": False}
    if start not in dates:
        return out
    i0 = dates.index(start)
    out["ok"] = True
    ma = [float("nan")] * n
    s = 0.0
    for i in range(n):
        s += closes[i]
        if i >= MASUTAN_MA_DAYS:
            s -= closes[i - MASUTAN_MA_DAYS]
        if i >= MASUTAN_MA_DAYS - 1:
            ma[i] = s / MASUTAN_MA_DAYS
    dev = [((closes[i] / ma[i] - 1) * 100 if ma[i] and not math.isnan(ma[i]) else float("nan")) for i in range(n)]
    cnt = 0
    for i in range(i0 + 1, n):
        cnt = cnt + 1 if (not math.isnan(dev[i]) and abs(dev[i]) < MASUTAN_BAND_PCT) else 0
        if cnt == MASUTAN_CALM_DAYS and out["first_hit"] is None:
            out["first_hit"] = dates[i]
    out["count"] = cnt if n > i0 + 1 else 0
    out["dev"] = dev[-1] if n else float("nan")
    return out


def apply_flags(episodes: dict, pubdate: str, flags: dict) -> None:
    """その公表日に規制中(Restricted=1)の銘柄 {code4: 区分} をエピソードに反映する（9/18 BTのエピソード化と同じ）。
    直前の公表日から MASUTAN_EPISODE_GAP 公表日以内なら同じエピソード、それより空いたら新しいエピソード。"""
    for code, cls in flags.items():
        ep = episodes.get(code)
        if ep and 0 <= trading_gap(ep["last"], pubdate) <= MASUTAN_EPISODE_GAP:
            ep["last"] = pubdate
            ep["cls"] = cls
            ep["cls_max"] = max(ep.get("cls_max", cls), cls)
        else:
            episodes[code] = {"start": pubdate, "last": pubdate, "cls": cls, "cls_max": cls}


def prune_episodes(episodes: dict, closed: list, pubdate: str) -> None:
    """最後の公表日から MASUTAN_EPISODE_GAP 公表日を超えて空いたエピソードは終了（closedへ）"""
    for code in list(episodes):
        ep = episodes[code]
        if trading_gap(ep["last"], pubdate) > MASUTAN_EPISODE_GAP:
            closed.append({"code": code, **ep})
            del episodes[code]
    del closed[:-200]


def shares_for(size: int, price: float) -> int:
    if not price or price <= 0:
        return 0
    return int(size / price / 100) * 100


# ── J-Quants ─────────────────────────────────────────────────────────────
def _token() -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(".env")
    except Exception:
        pass
    return os.environ.get("JQUANTS_API_KEY", "")


def fetch_alert(token: str, d: str) -> tuple[int, dict]:
    """(その公表日の全行数, {code4: 区分}) 。全行数0＝未公表 or 休日。"""
    from screener import _jquants_get, is_common_stock_code
    r = _jquants_get("/markets/margin-alert", token, {"date": d})
    rows = r.get("data", []) or []
    flags = {}
    for x in rows:
        pr = x.get("PubReason") or {}
        if str(pr.get("Restricted", "0")) != "1":
            continue
        code = str(x.get("Code", ""))
        if not is_common_stock_code(code):
            continue                          # 優先株など本体以外は除く（daytrade_paper.fetch_alert_map と同じ）
        flags[code[:4]] = str(x.get("TSEMrgnRegCls") or "")
    return len(rows), flags


def fetch_bars(token: str, code4: str, d_from: date, d_to: date) -> list[dict]:
    from screener import _jquants_get
    out, key = [], None
    while True:
        p = {"code": code4 + "0", "from": d_from.strftime("%Y%m%d"), "to": d_to.strftime("%Y%m%d")}
        if key:
            p["pagination_key"] = key
        r = _jquants_get("/equities/bars/daily", token, p)
        out.extend(r.get("data", []) or [])
        key = r.get("pagination_key")
        if not key:
            break
        time.sleep(1.2)
    rows = []
    for x in out:
        try:
            if x.get("AdjC") in (None, "") or float(x["AdjC"]) <= 0:
                continue
            rows.append({"date": str(x["Date"])[:10],
                         "O": float(x["O"] or 0), "H": float(x["H"] or 0), "L": float(x["L"] or 0), "C": float(x["C"] or 0),
                         "AdjO": float(x.get("AdjO") or 0), "AdjC": float(x["AdjC"]),
                         "Vo": float(x.get("Vo") or 0), "UL": str(x.get("UL", "0")), "LL": str(x.get("LL", "0"))})
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda r: r["date"])
    return rows


def fetch_names(token: str, codes: list[str]) -> dict:
    from screener import _jquants_get
    names = {}
    for c in codes:
        try:
            m = _jquants_get("/equities/master", token, {"code": c + "0"}).get("data", []) or []
            if m:
                names[c] = {"name": m[0].get("CoName", c), "mkt": m[0].get("MktNm", ""), "mrgn": m[0].get("MrgnNm", "")}
        except Exception as e:
            print(f"[masutan] 銘柄名の取得失敗 {c}: {e}")
        time.sleep(0.3)
    return names


# ── 状態と帳簿 ─────────────────────────────────────────────────────────────
def _load(path: str, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def update_state(state: dict, token: str, today: date, fetch=fetch_alert, init_days: int = MASUTAN_INIT_DAYS) -> str | None:
    """state を today までの公表分で更新し、判定に使う「最新の公表日」を返す（未公表の日は進めない）。"""
    eps = state.setdefault("episodes", {})
    closed = state.setdefault("closed", [])
    last = state.get("last_pubdate")
    if last:
        cur = date.fromisoformat(last)
    else:
        cur = today
        k = 0
        while k < init_days:
            cur -= timedelta(days=1)
            if is_trading_day(cur):
                k += 1
        cur -= timedelta(days=1)
    todo = []
    d = cur + timedelta(days=1)
    while d <= today:
        if is_trading_day(d):
            todo.append(d.isoformat())
        d += timedelta(days=1)
    for ds in todo[-120:]:
        n_rows, flags = fetch(token, ds)
        if n_rows == 0:
            print(f"[masutan] {ds} の公表分はまだ無い → ここで止める（前の公表日で判定）")
            break
        apply_flags(eps, ds, flags)
        prune_episodes(eps, closed, ds)
        state["last_pubdate"] = ds
        time.sleep(0.6)
    return state.get("last_pubdate")


def settle_book(book: list, bars_by_code: dict, today: str) -> list[dict]:
    """帳簿の決済。戻り値=今日起きたイベント（建て/見送り/手仕舞い）。bars_by_code[code] は日付昇順の足。"""
    events = []
    for p in book:
        bars = bars_by_code.get(p["code"])
        if not bars:
            continue
        ds = [b["date"] for b in bars]
        if p["status"] == "pending" and p["entry_date"] <= today and p["entry_date"] in ds:
            i = ds.index(p["entry_date"])
            e = bars[i]
            prev = bars[i - 1] if i > 0 else None
            prev_c = prev["C"] if prev else p.get("close_sig_raw", 0)
            stuck_up = prev_c > 0 and e["O"] >= prev_c + limit_width(prev_c) - 1e-9 and e["H"] <= e["O"] + 1e-9
            if stuck_up:
                p["status"] = "skipped"
                p["note"] = "寄りがストップ高張り付きで買えず"
                events.append({"kind": "skip", **p})
            else:
                p["status"] = "open"
                p["entry_adj"] = e["AdjO"]
                p["entry_raw"] = e["O"]
                events.append({"kind": "entry", **p})
        if p["status"] == "open" and p["exit_date"] <= today:
            if p["exit_date"] not in ds:
                continue
            j = ds.index(p["exit_date"])
            x = bars[j]
            prev_c = bars[j - 1]["C"] if j > 0 else x["C"]
            stuck_dn = prev_c > 0 and x["C"] <= prev_c - limit_width(prev_c) + 1e-9 and x["L"] >= x["C"] - 1e-9
            if stuck_dn:
                if j + 1 >= len(bars):
                    p["note"] = "3日目の引けがストップ安張り付き→翌日の寄りで処分"
                    continue
                exit_adj, exit_raw, p["exit_note"] = bars[j + 1]["AdjO"], bars[j + 1]["O"], "ストップ安張り付き→翌日寄り"
            else:
                exit_adj, exit_raw = x["AdjC"], x["C"]
            # 建値は建てた時点の調整値。今回の取得で分割が入っていれば同じ取得の調整値で比べる
            i = ds.index(p["entry_date"]) if p["entry_date"] in ds else None
            ent = bars[i]["AdjO"] if i is not None else p["entry_adj"]
            pnl = (exit_adj / ent - 1) * 100 if ent else 0.0
            p.update(status="closed", exit_adj=exit_adj, exit_raw=exit_raw, pnl_pct=round(pnl, 3),
                     pnl_net_pct=round(pnl - MASUTAN_COST_PCT, 3),
                     yen={str(s): round(p["shares"][str(s)] * p.get("entry_raw", 0) * pnl / 100) for s in MASUTAN_SIZES})
            events.append({"kind": "exit", **p})
    return events


def evaluate(state: dict, token: str, today: date, latest_pub: str, fetch_bars_fn=fetch_bars) -> tuple[list, dict]:
    """規制中の銘柄を判定。戻り値=(判定行, {code: bars})"""
    rows, bars_by_code = [], {}
    eps = state.get("episodes", {})
    for code, ep in sorted(eps.items()):
        if ep["last"] != latest_pub:
            continue                                   # 最新の公表日に規制が付いていない＝今は規制中でない
        start = date.fromisoformat(ep["start"])
        try:
            bars = fetch_bars_fn(token, code, start - timedelta(days=MASUTAN_BARS_LOOKBACK_DAYS), today)
        except Exception as e:
            rows.append({"code": code, "status": "error", "note": f"株価の取得失敗: {e}", **ep})
            continue
        bars_by_code[code] = bars
        if not bars or bars[-1]["date"] != today.isoformat():
            rows.append({"code": code, "status": "nodata", "note": "今日の足がまだ無い", **ep})
            continue
        closes = [b["AdjC"] for b in bars]
        dates = [b["date"] for b in bars]
        cp = calm_path(closes, dates, ep["start"])
        last5 = bars[-5:]
        tov5 = sum(b["C"] * b["Vo"] for b in last5) / len(last5) / 1e8
        row = {"code": code, **ep, "count": cp["count"], "dev": round(cp["dev"], 2) if not math.isnan(cp["dev"]) else None,
               "first_hit": cp["first_hit"], "tov5_oku": round(tov5, 2), "close_raw": bars[-1]["C"], "close_adj": bars[-1]["AdjC"]}
        if not cp["ok"]:
            row["status"] = "nostart"
            row["note"] = "公表日の足が無い（売買停止など）"
        elif cp["first_hit"] == today.isoformat():
            row["status"] = "signal" if tov5 >= MASUTAN_TOV_MIN_OKU else "filtered"
            if row["status"] == "filtered":
                row["note"] = f"5日平均代金{tov5:.1f}億 < {MASUTAN_TOV_MIN_OKU:.0f}億"
        elif cp["first_hit"]:
            row["status"] = "fired_before"
            row["note"] = f"{cp['first_hit']} にシグナル済み（エピソード内で1回だけ）"
        elif cp["count"] == MASUTAN_CALM_DAYS - 1:
            row["status"] = "watch"
            row["note"] = "明日も±15%以内ならシグナル"
        else:
            row["status"] = "waiting"
        rows.append(row)
    return rows, bars_by_code


# ── 配信 ───────────────────────────────────────────────────────────────────
def _md(d: date) -> str:
    return f"{d.month}/{d.day}({WEEKDAY_JA[d.weekday()]})"


def _shares_text(r: dict) -> str:
    """30万/50万の株数。100株に届かないサイズは「100株で約◯万円」と出す（値がさ株で0株と出さない）"""
    parts = []
    for s in MASUTAN_SIZES:
        sh = r["shares"][str(s)]
        lab = f"{s // 10000}万"
        parts.append(f"{lab}なら **{sh:,}株**" if sh > 0
                     else f"{lab}では100株に届かない（100株=約{r['close_raw'] * 100 / 10000:,.0f}万円）")
    return " ／ ".join(parts)


def build_embed(today: date, rows: list, events: list, names: dict, n_reg: int, latest_pub: str) -> dict:
    sig = [r for r in rows if r["status"] == "signal"]
    lines = []
    for r in sig:
        nm = names.get(r["code"], {})
        ent = date.fromisoformat(r["entry_date"])
        ex = date.fromisoformat(r["exit_date"])
        cls = REG_CLASS_LABEL.get(r.get("cls", ""), f"区分{r.get('cls', '?')}")
        lines.append(
            f"**{nm.get('name', r['code'])}（{r['code']}）** {nm.get('mkt', '')}・{nm.get('mrgn', '')}｜{cls}（{date.fromisoformat(r['start']).month}/{date.fromisoformat(r['start']).day}公表〜）\n"
            f"終値 {r['close_raw']:,.0f}円・25日線{r['dev']:+.1f}%・±15%以内が2日連続｜5日平均代金 {r['tov5_oku']:.1f}億\n"
            f"→ **{_md(ent)}の寄りで買い（成行）** → **{_md(ex)}の大引けで売り（引成）**＝買った日を1日目として3日目\n"
            f"株数: {_shares_text(r)}（100株単位・今日の終値基準）\n"
            f"⚠️寄りがストップ高に張り付いたら見送り")
    ev_lines = []
    for e in events:
        nm = names.get(e["code"], {}).get("name", e.get("name") or e["code"])
        if e["kind"] == "exit":
            ev_lines.append(f"📕 手仕舞い {nm}（{e['code']}） {e.get('entry_raw', 0):,.0f}→{e['exit_raw']:,.0f}円 "
                            f"**{e['pnl_pct']:+.2f}%**（コスト0.3%前・30万で{e['yen']['300000']:+,}円／50万で{e['yen']['500000']:+,}円）"
                            + (f" ※{e['exit_note']}" if e.get("exit_note") else ""))
        elif e["kind"] == "entry":
            ev_lines.append(f"🟢 建て {nm}（{e['code']}） 寄り {e['entry_raw']:,.0f}円 → {_md(date.fromisoformat(e['exit_date']))}の大引けで売り")
        elif e["kind"] == "skip":
            ev_lines.append(f"⛔ 見送り {nm}（{e['code']}） {e.get('note', '')}")
    watch = [r for r in rows if r["status"] == "watch"]
    w_lines = [f"👀 {names.get(r['code'], {}).get('name', r['code'])}（{r['code']}） 25日線{r['dev']:+.1f}%・あと1日±15%以内ならシグナル"
               + ("" if r.get("tov5_oku", 0) >= MASUTAN_TOV_MIN_OKU else f"（ただし代金{r.get('tov5_oku', 0):.1f}億で基準未満）")
               for r in watch if r.get("dev") is not None]
    if sig:
        title = f"🔥【増担 再点火】{_md(date.fromisoformat(sig[0]['entry_date']))} 寄りで買い {len(sig)}件"
        color = 0xE67E22
    else:
        title = f"🔥【増担 再点火】{_md(today)} 引け時点 シグナルなし（増担保規制中 {n_reg}銘柄）"
        color = 0x95A5A6
    desc = "\n\n".join(lines) if lines else "今日の引けで条件を満たした銘柄はありません。"
    if ev_lines:
        desc += "\n\n" + "\n".join(ev_lines)
    if w_lines:
        desc += "\n\n" + "\n".join(w_lines)
    filt = [r for r in rows if r["status"] == "filtered"]
    if filt:
        desc += "\n\n" + "\n".join(f"・{names.get(r['code'], {}).get('name', r['code'])}（{r['code']}） 条件は満たしたが{r.get('note', '')}→撃たない" for r in filt)
    footer = ("ルール: 増担保規制中に終値が25日線±15%以内に2日続いた日→翌朝の寄りで買い→3日目の大引けで売り・"
              "5日平均代金3億以上だけ｜10年(代金≥3億): 557件 1回+1.88%(コスト0.3%後)・勝率61%・PF1.61・11年すべて0以上｜"
              "26年での検証はデータが無く不可＝小さい額(1件30〜50万)で｜信用買いは増担保の保証金率(50%以上・うち現金の割合も上がる)がかかる・現物なら全額｜"
              f"規制情報: {latest_pub} 公表分")
    return {"title": title[:256], "description": desc[:4000], "color": color, "footer": {"text": footer[:2000]}}


def post_discord(embed: dict, dry: bool) -> bool:
    url = os.environ.get(WEBHOOK_ENV, "").strip()
    if not url:
        print(f"[masutan] {WEBHOOK_ENV} 未設定 → 投稿しない（JSONとログだけ）")
        return False
    if dry:
        print("[masutan] --dry → 投稿しない")
        return False
    try:
        import requests
        r = requests.post(url, json={"embeds": [embed]}, timeout=20,
                          headers={"User-Agent": "Mozilla/5.0 (masutan-signal)"})
        ok = r.status_code in (200, 204)
        print(f"[masutan] Discord {'OK' if ok else 'NG'} HTTP {r.status_code}")
        return ok
    except Exception as e:
        print(f"[masutan] Discord 送信失敗: {e}")
        return False


# ── 本体 ───────────────────────────────────────────────────────────────────
def run(today: date, dry: bool = False, force: bool = False, token: str | None = None,
        fetch_alert_fn=fetch_alert, fetch_bars_fn=fetch_bars, fetch_names_fn=fetch_names,
        post_fn=post_discord, init_days: int = MASUTAN_INIT_DAYS) -> dict:
    token = token if token is not None else _token()
    state = _load(STATE_FILE, {})
    book = _load(BOOK_FILE, [])
    sent = state.setdefault("sent", [])
    if today.isoformat() in sent and not force:
        print(f"[masutan] {today} は配信済み → 何もしない（--force で再実行）")
        return {"skipped": "sent"}
    latest_pub = update_state(state, token, today, fetch=fetch_alert_fn, init_days=init_days)
    if not latest_pub:
        print("[masutan] 規制情報が取れない → 終了")
        return {"skipped": "noalert"}
    rows, bars_by_code = evaluate(state, token, today, latest_pub, fetch_bars_fn=fetch_bars_fn)
    if any(r["status"] == "nodata" for r in rows) and not any(r["status"] in ("signal", "filtered", "watch", "waiting", "fired_before") for r in rows):
        print("[masutan] 今日の足がまだ無い → 配信せず終了（後のランでやり直し）")
        _save(STATE_FILE, state)
        return {"skipped": "nodata"}
    # 帳簿の決済（保有中・予定の銘柄の足は evaluate で取っていない場合があるので追加で取る）
    need = {p["code"] for p in book if p["status"] in ("pending", "open")} - set(bars_by_code)
    for code in sorted(need):
        p0 = min(date.fromisoformat(p["signal_date"]) for p in book if p["code"] == code and p["status"] in ("pending", "open"))
        try:
            bars_by_code[code] = fetch_bars_fn(token, code, p0 - timedelta(days=10), today)
        except Exception as e:
            print(f"[masutan] 帳簿の足の取得失敗 {code}: {e}")
    events = settle_book(book, bars_by_code, today.isoformat())
    # 新しいシグナルを帳簿へ
    have = {(p["code"], p["signal_date"]) for p in book}
    for r in rows:
        if r["status"] != "signal":
            continue
        r["entry_date"] = add_trading_days(today, 1).isoformat()
        r["exit_date"] = add_trading_days(today, MASUTAN_EXIT_OFFSET).isoformat()
        r["shares"] = {str(s): shares_for(s, r["close_raw"]) for s in MASUTAN_SIZES}
        if (r["code"], today.isoformat()) not in have:
            book.append({"code": r["code"], "signal_date": today.isoformat(), "entry_date": r["entry_date"],
                         "exit_date": r["exit_date"], "start": r["start"], "cls": r.get("cls"), "dev_sig": r["dev"],
                         "tov5_oku": r["tov5_oku"], "close_sig_raw": r["close_raw"], "shares": r["shares"],
                         "status": "pending"})
    codes = sorted({r["code"] for r in rows} | {e["code"] for e in events})
    names = fetch_names_fn(token, codes) if codes else {}
    for p in book:
        if p["code"] in names and not p.get("name"):
            p["name"] = names[p["code"]]["name"]
    n_reg = sum(1 for r in rows)
    embed = build_embed(today, rows, events, names, n_reg, latest_pub)
    out = {"date": today.isoformat(), "latest_pubdate": latest_pub, "rows": rows,
           "signals": [r for r in rows if r["status"] == "signal"], "events": events, "embed": embed}
    if dry:
        print(json.dumps(embed, ensure_ascii=False, indent=1))
        return out
    _save(SIG_FILE, {k: v for k, v in out.items() if k != "embed"})
    _save(BOOK_FILE, book)
    posted = post_fn(embed, dry)
    if posted or not os.environ.get(WEBHOOK_ENV, "").strip():
        sent.append(today.isoformat())
        del sent[:-30]
    _save(STATE_FILE, state)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="判定する日(YYYY-MM-DD)。省略時は今日(JST)")
    ap.add_argument("--dry", action="store_true", help="ファイルを書かず・投稿せず、判定だけ表示")
    ap.add_argument("--force", action="store_true", help="配信済みでも再実行")
    ap.add_argument("--init", type=int, default=MASUTAN_INIT_DAYS, help="状態ファイルが無い時に遡る営業日数")
    a = ap.parse_args()
    now = datetime.now(JST)
    today = date.fromisoformat(a.date) if a.date else now.date()
    if not is_trading_day(today):
        print(f"[masutan] {today} は休場 → スキップ")
        return 0
    if not a.date and now.hour * 60 + now.minute < 16 * 60 + 45:
        print(f"[masutan] {now:%H:%M} JST は引け後の四本値公開(16:30頃)前 → スキップ")
        return 0
    run(today, dry=a.dry, force=a.force, init_days=a.init)
    return 0


if __name__ == "__main__":
    sys.exit(main())

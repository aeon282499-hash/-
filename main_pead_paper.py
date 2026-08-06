# -*- coding: utf-8 -*-
"""main_pead_paper.py — PEAD専業の紙運用（2026-08-05・本人「決算後に買って利益を積み上げたい」）。

2026-07-12のフルBTで実弾見送りとなった第3システム候補（決算爆騰の翌日買い→ドリフト取り）を、
当時決めた復活パス=「紙運用(配信のみ)でforward検証」として稼働させる。実弾は一切なし。

ルール（BTの正直バージョン・大100万×5枠・名目%ベース）:
  D0=決算発表日（JPX予定表）/ D1=翌営業日。
  入口: D1寄りギャップ(D1始値/D0終値-1) > +12% × D0終値≤2万円 × 20日代金中央値≥5億(D0まで)
        × 決算持ち越し本体が同じD0にシグナルした銘柄は除外（ルールA重複除外の近似）
  買い: D1大引け成行（紙）。ただしD1終値がストップ高張り付き＝買えない → D2寄り成行で追撃、
        D2寄りもS高なら見送り（2日連続張り付き=市場が渡さない玉）。
  売り: D1から4営業日後の大引け（=5営業日目・保有4営業日）。STOPなし。
  損益: 名目100万/枠の%ベース（BTと同一・株数丸めなし）。枠5。

実行は毎朝8:20頃（schedule.yml相乗り）。昨日=D1の確定足で記帳するため全て公式EODで再現できる。
実行: python main_pead_paper.py [--dry] [--force]
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))


def jst_today() -> date:
    return datetime.now(JST).date()

from dotenv import load_dotenv

load_dotenv()

import main_earnings_hold as eh  # is_trading_day / next_trading_day / nth_trading_day / send_discord

BOOK_PATH = "positions_pead_paper.json"
SCHEDULE_PATH = "jpx_earnings_schedule.json"
GAP_MIN = 12.0          # 厳密に「超」
PRICE_CAP = 20_000.0    # D0終値上限
TOV_MIN = 5e8           # 20日代金中央値（D0まで）
SLOTS = 5
NOTIONAL = 1_000_000    # 名目・%ベース
HOLD_TD = 4             # D1から4営業日後の大引けで売り（=5営業日目）

_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300),
        (2000, 400), (3000, 500), (5000, 700), (7000, 1000), (10000, 1500),
        (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000)]


def lim_up(base: float) -> float:
    """値幅制限の上限値（base=基準値段・通常は前日終値）。"""
    for hi, w in _LIM:
        if base < hi:
            return base + w
    return base + 10000.0


def is_stop_high(price: float, prev_close: float) -> bool:
    return price >= lim_up(prev_close) - 1e-9


def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while not eh.is_trading_day(p):
        p -= timedelta(days=1)
    return p


def load_book() -> dict:
    if os.path.exists(BOOK_PATH):
        with open(BOOK_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"last_run_date": None, "positions": []}


def save_book(book: dict) -> None:
    with open(BOOK_PATH, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=1)


def _bar(all_data: dict, tk: str, ds: str):
    df = all_data.get(tk)
    if df is None or not len(df):
        return None
    try:
        idx = df.index.strftime("%Y-%m-%d")
    except Exception:
        return None
    hit = df[idx == ds]
    return hit.iloc[-1] if len(hit) else None


def tov20_median(all_data: dict, tk: str, until: str) -> float | None:
    df = all_data.get(tk)
    if df is None or "Close" not in df or "Volume" not in df:
        return None
    df = df[df.index.strftime("%Y-%m-%d") <= until]
    if len(df) < 20:
        return None
    tov = (df["Close"].astype(float) * df["Volume"].astype(float)).tail(20)
    return float(tov.median())


def main_ledger_tickers(d0s: str) -> set:
    """決算持ち越し本体が同じD0にシグナルした銘柄（ルールA重複除外の近似）。"""
    out = set()
    for path in ("positions_earnings.json", "positions_earnings_mid.json",
                 "positions_earnings_small.json"):
        try:
            with open(path, encoding="utf-8") as f:
                for p in json.load(f).get("positions", []):
                    if p.get("date") == d0s:
                        out.add(p.get("ticker"))
        except Exception:
            continue
    return out


def settle_and_fill(book: dict, all_data: dict, upto: str) -> tuple[list, list, list]:
    """確定足が揃った分の決済と追撃約定。uptoまでの足が使える前提。"""
    closed, chased, dropped = [], [], []
    for p in book["positions"]:
        if p["status"] == "pending_d2" and p["chase_date"] <= upto:
            bar = _bar(all_data, p["ticker"], p["chase_date"])
            if bar is None:
                continue  # 足が取れるまで待つ（翌日再試行）
            o2 = float(bar["Open"])
            if is_stop_high(o2, p["d1_close"]):
                p["status"] = "dropped"
                p["drop_reason"] = "2日連続張り付き"
                dropped.append(p)
            else:
                p["status"] = "held"
                p["entry"] = o2
                p["entry_kind"] = "D2寄り追撃"
                chased.append(p)
        if p["status"] == "held" and p["exit_date"] <= upto:
            bar = _bar(all_data, p["ticker"], p["exit_date"])
            if bar is None:
                continue
            exit_px = float(bar["Close"])
            p["status"] = "closed"
            p["exit"] = exit_px
            p["pnl_pct"] = round((exit_px / p["entry"] - 1) * 100, 2)
            p["pnl_yen"] = round(NOTIONAL * (exit_px / p["entry"] - 1))
            closed.append(p)
    return closed, chased, dropped


def build_entries(book: dict, all_data: dict, d0s: str, d1: date) -> list:
    """D1=昨日の確定足で新規紙エントリーを作る。"""
    d1s = d1.strftime("%Y-%m-%d")
    try:
        with open(SCHEDULE_PATH, encoding="utf-8") as f:
            sched = json.load(f).get("schedule", {}).get(d0s, [])
    except Exception:
        sched = []
    if not sched:
        return []
    have = {p["ticker"] for p in book["positions"] if p["status"] in ("held", "pending_d2")}
    slots = SLOTS - len(have)
    if slots <= 0:
        return []
    excl = main_ledger_tickers(d0s)
    cands = []
    for r in sched:
        tk = str(r.get("code", "")) + ".T"
        if tk in have or tk in excl:
            continue
        b0 = _bar(all_data, tk, d0s)
        b1 = _bar(all_data, tk, d1s)
        if b0 is None or b1 is None:
            continue
        c0, o1, c1 = float(b0["Close"]), float(b1["Open"]), float(b1["Close"])
        if c0 <= 0 or c0 > PRICE_CAP:
            continue
        gap = (o1 / c0 - 1) * 100
        if gap <= GAP_MIN:
            continue
        tov = tov20_median(all_data, tk, d0s)
        if tov is None or tov < TOV_MIN:
            continue
        cands.append({"ticker": tk, "name": str(r.get("name", tk))[:12], "d0": d0s,
                      "d1": d1s, "gap_pct": round(gap, 2), "d0_close": c0,
                      "d1_close": c1, "exit_date": eh.nth_trading_day(d1, HOLD_TD).strftime("%Y-%m-%d")})
    cands.sort(key=lambda x: -x["gap_pct"])
    out = []
    for c in cands[:slots]:
        if is_stop_high(c["d1_close"], c["d0_close"]):
            c["status"] = "pending_d2"
            c["chase_date"] = eh.next_trading_day(d1).strftime("%Y-%m-%d")
        else:
            c["status"] = "held"
            c["entry"] = c["d1_close"]
            c["entry_kind"] = "D1大引け"
        book["positions"].append(c)
        out.append(c)
    return out


def stats(book: dict) -> dict:
    cl = [p for p in book["positions"] if p["status"] == "closed"]
    win = [p for p in cl if p["pnl_yen"] > 0]
    loss_sum = sum(-p["pnl_yen"] for p in cl if p["pnl_yen"] <= 0)
    gain_sum = sum(p["pnl_yen"] for p in win)
    return {"n": len(cl), "win": len(win), "yen": sum(p["pnl_yen"] for p in cl),
            "pf": (gain_sum / loss_sum) if loss_sum else float("inf") if gain_sum else 0.0}


def build_embed(new_e: list, closed: list, chased: list, dropped: list,
                book: dict, today: date) -> dict:
    st = stats(book)
    held = [p for p in book["positions"] if p["status"] == "held"]
    pend = [p for p in book["positions"] if p["status"] == "pending_d2"]
    lines = []
    if closed:
        lines.append("**■ 決済（紙）**")
        lines += [f"{p['name']} {p['pnl_pct']:+.2f}% ({p['pnl_yen']:+,}円)" for p in closed]
    if new_e:
        lines.append("**■ 新規紙エントリー（昨日D1）**")
        for p in new_e:
            tag = "D1大引け" if p["status"] == "held" else "⚠️張り付き→D2寄り追撃待ち"
            lines.append(f"{p['name']} gap+{p['gap_pct']:.1f}% {tag} 売り={p['exit_date']}")
    if chased:
        lines.append("**■ 追撃約定（D2寄り）**")
        lines += [f"{p['name']} @{p['entry']:,.0f}円" for p in chased]
    if dropped:
        lines.append("**■ 見送り（2日連続張り付き=買えない玉）**")
        lines += [f"{p['name']}" for p in dropped]
    if not lines:
        lines.append("本日の動きなし")
    if held or pend:
        lines.append(f"保有{len(held)}/追撃待ち{len(pend)}（枠{SLOTS}）")
    lines.append(f"**通算: {st['n']}件 勝率{(st['win']/st['n']*100 if st['n'] else 0):.0f}% "
                 f"PF{st['pf']:.2f} {st['yen']:+,}円**（名目100万×5枠）")
    return {"title": f"🧪 PEAD紙・答え合わせ {today.strftime('%m/%d')}",
            "description": "\n".join(lines), "color": 0x9B59B6,
            "footer": {"text": "紙運用＝実弾禁止。2026-07-12実弾見送り判定のforward検証。"
                               "BT正直版=年+79万/PF1.67が本物か8月〜で確認する"}}


def fetch_today_opens(tickers: list[str]) -> dict:
    """今日の寄り値をYahooチャートAPIから取得（requests+verify=False=ローカルAV対応・CI両用）。"""
    import requests
    import urllib3
    urllib3.disable_warnings()
    out = {}
    sess = requests.Session()
    for tk in tickers:
        try:
            r = sess.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{tk}",
                         params={"range": "1d", "interval": "1d"},
                         headers={"User-Agent": "Mozilla/5.0"}, verify=False, timeout=10)
            res = r.json()["chart"]["result"][0]
            o = res["indicators"]["quote"][0]["open"]
            if o and o[0]:
                out[tk] = float(o[0])
        except Exception:
            continue
        import time
        time.sleep(0.2)
    return out


def signal_scan(book: dict, all_data: dict, d0s: str, today) -> list[dict]:
    """今日=D1の寄りギャップで「今日の大引けで買う（紙）」候補を返す。約定はしない＝翌朝の
    記帳runが公式終値で確定する。速報はYahoo寄り値・記帳は公式値（僅差はあり得る）。"""
    try:
        with open(SCHEDULE_PATH, encoding="utf-8") as f:
            sched = json.load(f).get("schedule", {}).get(d0s, [])
    except Exception:
        sched = []
    if not sched:
        return []
    have = {p["ticker"] for p in book["positions"] if p["status"] in ("held", "pending_d2")}
    slots = SLOTS - len(have)
    if slots <= 0:
        return []
    excl = main_ledger_tickers(d0s)
    pre = []
    for r in sched:
        tk = str(r.get("code", "")) + ".T"
        if tk in have or tk in excl:
            continue
        b0 = _bar(all_data, tk, d0s)
        if b0 is None:
            continue
        c0 = float(b0["Close"])
        if c0 <= 0 or c0 > PRICE_CAP:
            continue
        tov = tov20_median(all_data, tk, d0s)
        if tov is None or tov < TOV_MIN:
            continue
        pre.append({"ticker": tk, "name": str(r.get("name", tk))[:10], "d0_close": c0})
    opens = fetch_today_opens([p["ticker"] for p in pre])
    cands = []
    for p in pre:
        o1 = opens.get(p["ticker"])
        if not o1:
            continue
        gap = (o1 / p["d0_close"] - 1) * 100
        if gap <= GAP_MIN:
            continue
        cands.append({**p, "open": o1, "gap_pct": round(gap, 2),
                      "exit_date": eh.nth_trading_day(today, HOLD_TD).strftime("%Y-%m-%d")})
    cands.sort(key=lambda x: -x["gap_pct"])
    return cands[:slots]


def build_signal_embed(sigs: list[dict], today) -> dict:
    if sigs:
        lines = ["**下の銘柄を、今日15:30の大引け成行で買う（紙）**", ""]
        for i, s in enumerate(sigs, 1):
            ref_sh = int(NOTIONAL / s["open"] // 100 * 100)
            ref = f"参考{ref_sh}株(約{ref_sh * s['open'] / 1e4:,.0f}万)" if ref_sh else "参考:1単元が100万超"
            lines.append(f"`{i}.` 🟣 **{s['name']}** ({s['ticker'][:-2]}) 寄り+{s['gap_pct']:.1f}%"
                         f"\n　　大引け成行で買い → **{s['exit_date']} の大引けで売り**・{ref}")
        lines += ["", "⚠️ 引けストップ高なら約定しない（その場合は明朝、寄り追撃を自動判定）",
                  "⚠️ **これは紙シグナル＝実弾禁止**。記帳は明朝、公式終値で自動"]
    else:
        lines = ["本日の買いシグナルなし（gap+12%超の該当なし or 枠フル）"]
    return {"title": f"🧪 PEAD紙・買いシグナル {today.strftime('%m/%d')}",
            "description": "\n".join(lines), "color": 0x9B59B6,
            "footer": {"text": "紙運用＝実弾禁止。forward検証がBT(年+79万/PF1.67)通りなら実弾昇格を判断"}}


def run_signal(dry: bool = False, force: bool = False) -> None:
    """9:30過ぎ実行: 今日の寄りギャップで「今日の大引けで買う（紙）」を配信。記帳はしない。"""
    today = jst_today()
    if not force and not eh.is_trading_day(today):
        print("[pead-sig] 休場日スキップ")
        return
    book = load_book()
    ts = today.strftime("%Y-%m-%d")
    if not force and book.get("last_signal_date") == ts:
        print("[pead-sig] 本日配信済み")
        return
    d0 = prev_trading_day(today)
    from screener import _jquants_id_token, batch_download_jquants
    token = _jquants_id_token()
    start = (d0 - timedelta(days=70)).strftime("%Y-%m-%d")
    all_data = batch_download_jquants(token, start=start, end=ts)
    sigs = signal_scan(book, all_data, d0.strftime("%Y-%m-%d"), today)
    embed = build_signal_embed(sigs, today)
    if dry:
        print(json.dumps(embed, ensure_ascii=False, indent=1))
    else:
        url = os.getenv("DISCORD_WEBHOOK_EARNINGS_URL", "").strip()
        if url:
            eh.send_discord([embed], url, "pead-signal")
        book["last_signal_date"] = ts
        save_book(book)
    print(f"[pead-sig] シグナル{len(sigs)}件")


def run(dry: bool = False, force: bool = False) -> None:
    today = jst_today()
    if not force and not eh.is_trading_day(today):
        print("[pead] 休場日スキップ")
        return
    book = load_book()
    ts = today.strftime("%Y-%m-%d")
    if not force and book.get("last_run_date") == ts:
        print("[pead] 本日実行済み")
        return
    d1 = prev_trading_day(today)
    d0 = prev_trading_day(d1)
    from screener import _jquants_id_token, batch_download_jquants
    token = _jquants_id_token()
    start = (d0 - timedelta(days=70)).strftime("%Y-%m-%d")
    all_data = batch_download_jquants(token, start=start, end=ts)
    closed, chased, dropped = settle_and_fill(book, all_data, d1.strftime("%Y-%m-%d"))
    new_e = build_entries(book, all_data, d0.strftime("%Y-%m-%d"), d1)
    embed = build_embed(new_e, closed, chased, dropped, book, today)
    if dry:
        print(json.dumps(embed, ensure_ascii=False, indent=1))
    else:
        url = os.getenv("DISCORD_WEBHOOK_EARNINGS_URL", "").strip()
        if url:
            eh.send_discord([embed], url, "pead-paper")
        book["last_run_date"] = ts
        save_book(book)
    print(f"[pead] 新規{len(new_e)} 決済{len(closed)} 追撃{len(chased)} 見送り{len(dropped)}")


if __name__ == "__main__":
    if "--signal" in sys.argv:
        run_signal(dry="--dry" in sys.argv, force="--force" in sys.argv)
    else:
        run(dry="--dry" in sys.argv, force="--force" in sys.argv)

# -*- coding: utf-8 -*-
"""arena_watchlist.py — 前夜の「土俵リスト」（デイトレの銘柄選定・方向は付けない）。2026-09-07 本人依頼。

■ 何を出すか
  勝っているデイトレーダー（テスタ/cis）が前夜にやっている「準備」だけを機械化する:
  「翌日、人と注文が集まる銘柄」を、当日の売買代金・代金の急増・値幅・材料（TDnet開示）で選ぶ。
  上がるか下がるかは出さない。前夜データで「翌日日中に上がる銘柄」は選べないことが
  26年日足+15分足+LightGBM OOSで確定している（_bt_support_volume_26y.py / _bt_dayup_ai_0907.py）。

■ 出力
  arena_watchlist.json（CIがコミット → チンパン🎯土俵タブ・板レコーダーの対象リスト）
  Discord: DISCORD_WEBHOOK_ARENA_URL（未設定なら標準出力のみ）

■ 実行
  python arena_watchlist.py [--date YYYY-MM-DD] [--dry] [--force]
  前夜配信ワークフロー(schedule_evening.yml)で main_day.py の後に走る。
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
JST = timezone(timedelta(hours=9))
OUT_FILE = "arena_watchlist.json"
TOV_MIN = 1.0e9          # 当日売買代金の下限（人が集まっている）
TOV_MIN_MATERIAL = 3e8   # 材料ありは代金3億まで緩める（翌日に人が来る）
PX_LO, PX_HI = 300, 10_000
TOP_N = 15               # 土俵の本数
BOARD_N = 20             # 板レコーダーに渡す上限
TDNET_LIST_URL = "https://www.release.tdnet.info/inbs/I_list_{page:03d}_{ymd}.html"
_TDNET_ROW_RE = re.compile(
    r"kjTime[^>]*>([^<]+)<.*?kjCode[^>]*>([^<]+)<.*?kjName[^>]*>([^<]*)<.*?kjTitle[^>]*>.*?>([^<]+)<", re.S)

# 材料の種別（表題のキーワード → 表示ラベル）。訂正・招集通知・ガバナンス等の事務開示は除外。
MATERIAL_KINDS = [
    ("決算短信", "決算"), ("上方修正", "上方修正"), ("下方修正", "下方修正"), ("業績予想の修正", "業績修正"),
    ("業績予想", "業績修正"), ("配当予想の修正", "配当修正"), ("自己株式", "自社株買い"), ("株式分割", "分割"),
    ("株主優待", "優待"), ("公開買付", "TOB"), ("業務提携", "提携"), ("資本提携", "提携"), ("買収", "M&A"),
    ("株式取得", "M&A"), ("子会社", "子会社"), ("新製品", "新製品"), ("採用", "採用"), ("受注", "受注"),
    ("契約", "契約"), ("承認", "承認"), ("特許", "特許"), ("月次", "月次"), ("市場区分", "市場変更"),
    ("上場", "上場"), ("増資", "増資"), ("新株予約権", "新株予約権"), ("売出", "売出"), ("特別損失", "特損"),
    ("特別利益", "特益"),
]
SKIP_TITLE = ("訂正", "招集", "コーポレート・ガバナンス", "コーポレートガバナンス", "有価証券報告書", "四半期報告書",
              "臨時報告書", "議決権", "株主総会", "定款", "役員の異動", "人事", "組織変更", "開示事項の経過", "貸借対照表")


def is_trading_day(d: date) -> bool:
    import jpholiday
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while not is_trading_day(n):
        n += timedelta(days=1)
    return n


def material_kind(title: str) -> str | None:
    if any(s in title for s in SKIP_TITLE):
        return None
    for kw, lab in MATERIAL_KINDS:
        if kw in title:
            return lab
    return None


def fetch_tdnet_all(ymd: str, max_pages: int = 40, deadline_sec: float = 150.0) -> dict[str, list[dict]] | None:
    """当日のTDnet開示を全件（引け後含む）。{code4: [{time, kind, title}]}。取得不能はNone。"""
    import urllib3
    urllib3.disable_warnings()
    t0 = time.monotonic()
    out: dict[str, list[dict]] = {}
    try:
        for page in range(1, max_pages + 1):
            if time.monotonic() - t0 > deadline_sec:
                print(f"  [tdnet] {deadline_sec:.0f}秒超過 → 材料なしで続行")
                return None
            r = requests.get(TDNET_LIST_URL.format(page=page, ymd=ymd),
                             headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}, verify=False, timeout=20)
            if r.status_code != 200:
                if page == 1:
                    print(f"  [tdnet] 1ページ目 HTTP {r.status_code}")
                    return None
                break
            rows = _TDNET_ROW_RE.findall(r.content.decode("utf-8", errors="replace"))
            if not rows:
                if page == 1:
                    print("  [tdnet] 1ページ目パース0行")
                    return None
                break
            for t, code, name, title in rows:
                title = title.strip()
                kind = material_kind(title)
                if kind is None:
                    continue
                out.setdefault(code.strip()[:4], []).append({"time": t.strip()[:5], "kind": kind, "title": title[:60]})
            if len(rows) < 100:
                break
        print(f"  [tdnet] {ymd} 材料あり {len(out)}社 {time.monotonic()-t0:.0f}s")
        return out
    except Exception as e:
        print(f"  [tdnet] 取得失敗: {e}")
        return None


def build(sig_date: date) -> dict:
    from screener import batch_download_jquants, _jquants_id_token, fetch_tse_universe, is_etf_ticker
    from daytrade_paper import fetch_iss_map

    token = _jquants_id_token()
    names = dict(fetch_tse_universe(token))
    start = (sig_date - timedelta(days=45)).strftime("%Y-%m-%d")
    end = sig_date.strftime("%Y-%m-%d")
    data = batch_download_jquants(token, start=start, end=end)
    iss = fetch_iss_map(token)
    mats = fetch_tdnet_all(sig_date.strftime("%Y%m%d")) or {}
    try:
        secmap = json.load(open("sector33_map.json", encoding="utf-8"))
    except Exception:
        secmap = {}

    rows = []
    latest_seen = None
    for tk, df in data.items():
        if df is None or len(df) < 22:
            continue
        name = names.get(tk, "")
        if not name or is_etf_ticker(tk, name):
            continue
        d0 = df.index[-1].date() if hasattr(df.index[-1], "date") else df.index[-1]
        if latest_seen is None or d0 > latest_seen:
            latest_seen = d0
        if d0 != sig_date:
            continue   # 当日の足が無い（未公開/出来ず）
        c = df["Close"].astype(float); o = df["Open"].astype(float)
        h = df["High"].astype(float); l = df["Low"].astype(float); v = df["Volume"].astype(float)
        close = float(c.iloc[-1]); pc = float(c.iloc[-2])
        if not (close > 0 and pc > 0) or not (PX_LO <= close <= PX_HI):
            continue
        tov = close * float(v.iloc[-1])
        tov_hist = (c * v).iloc[-21:-1]
        tov_med = float(tov_hist.median()) if len(tov_hist) >= 10 else float("nan")
        code4 = tk.replace(".T", "")
        material = mats.get(code4, [])
        if tov < (TOV_MIN_MATERIAL if material else TOV_MIN):
            continue
        tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr_pct = float(tr.iloc[-14:].mean()) / close * 100
        rng = (float(h.iloc[-1]) - float(l.iloc[-1])) / pc * 100
        chg = (close / pc - 1) * 100
        gap = (float(o.iloc[-1]) / pc - 1) * 100
        ma25 = float(c.iloc[-25:].mean()) if len(c) >= 25 else float("nan")
        dev25 = (close / ma25 - 1) * 100 if ma25 == ma25 else float("nan")
        r5 = (close / float(c.iloc[-6]) - 1) * 100 if len(c) >= 6 else float("nan")
        tov_ratio = tov / tov_med if tov_med and tov_med > 0 and len(tov_hist) >= 20 else float("nan")
        # 熱量: 代金の急増(対数・上限30倍)×値幅×材料×絶対代金。「人が集まる」順。方向は含まない。
        # 上場直後で20日履歴が無い玉は代金比を使わず絶対代金だけで評価（比が数百倍に化けるため）。
        heat = ((min(math.log10(max(tov_ratio, 0.1)), 1.5) * 30 if tov_ratio == tov_ratio else 0)
                + min(rng, 15) * 2 + min(atr_pct, 10) * 1.5 + (15 if material else 0)
                + math.log10(max(tov / 1e8, 1)) * 12)
        flags = []
        if chg >= 7:
            flags.append("前日+7%↑＝翌日は寄り天が平常（26年BT・買い向きでない）")
        elif chg >= 3:
            flags.append("前日+3%↑＝寄り高→日中垂れやすい")
        if chg <= -7:
            flags.append("前日-7%↓＝投げ継続も反発もある・板で判断")
        if rng <= 1.5:
            flags.append("値幅小＝デイトレ向きでない")
        if tov_ratio == tov_ratio and tov_ratio >= 6:
            flags.append("代金6倍超＝本物の材料と過熱の両面")
        sh = str(iss.get(code4, ""))
        rows.append({"code": code4, "name": name, "close": close, "chg": round(chg, 2), "gap": round(gap, 2),
                     "range_pct": round(rng, 2), "atr_pct": round(atr_pct, 2), "turnover_oku": round(tov / 1e8, 1),
                     "tov_ratio": round(tov_ratio, 2) if tov_ratio == tov_ratio else None,
                     "dev25": round(dev25, 2) if dev25 == dev25 else None, "r5": round(r5, 2) if r5 == r5 else None,
                     "shortable": "○" if sh == "2" else ("×" if sh == "1" else "?"),
                     "sector": secmap.get(tk, ""), "materials": material[:3], "flags": flags, "heat": round(heat, 1)})
    if not rows:
        raise RuntimeError(f"{sig_date} の当日足がありません（J-Quants最新={latest_seen}）")
    rows.sort(key=lambda r: -r["heat"])
    top = rows[:TOP_N]
    mat_only = sorted([r for r in rows[TOP_N:] if r["materials"]], key=lambda r: -r["turnover_oku"])[:10]
    target = next_trading_day(sig_date)
    return {"date": sig_date.strftime("%Y-%m-%d"), "target_date": target.strftime("%Y-%m-%d"),
            "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "universe": len(rows),
            "rows": top, "materials_extra": mat_only, "board_codes": [r["code"] for r in rows[:BOARD_N]],
            "note": ("方向は付けない。前夜データで『翌日日中に上がる銘柄』は選べない（26年BT+AI OOSで確定）。"
                     "ここは人と注文が集まる土俵の一覧。勝負は板と歩み値・9〜10時・最小単元・全記録。"),
            "rules": ["9:00〜10:00（後場は12:30〜13:00）以外は触らない", "買いは最小単元・損切りは即・1日-1万で終了",
                      "前日+7%以上の玉は買わない（寄り天が平常）", "全部 manual_daytrade_log.py に記録（30件で監査）"]}


def fmt_discord(w: dict) -> list[str]:
    hd = (f"🎯 **デイトレ土俵リスト** {w['target_date']} 分（{w['date']} 引けデータ・母集団{w['universe']}銘柄）\n"
          "上がる/下がるは付けない。人と注文が集まる順。勝負は板と歩み値・9〜10時のみ。\n")
    lines = []
    for i, r in enumerate(w["rows"], 1):
        m = " / ".join(x["kind"] for x in r["materials"]) if r["materials"] else "—"
        fl = (" ⚠️" + r["flags"][0]) if r["flags"] else ""
        tr = f"×{r['tov_ratio']}" if r["tov_ratio"] else "×?"
        lines.append(f"{i:2d}. **{r['name']}**({r['code']}) ¥{r['close']:,.0f} {r['chg']:+.1f}% "
                     f"代金{r['turnover_oku']}億({tr}) 値幅{r['range_pct']}% ATR{r['atr_pct']}% 貸借{r['shortable']} 材料:{m}{fl}")
    ex = ""
    if w["materials_extra"]:
        ex = "\n**材料あり（土俵外・代金順）**: " + "、".join(
            f"{r['name']}({r['code']}/{r['materials'][0]['kind']})" for r in w["materials_extra"])
    rules = "\n📏 " + " ／ ".join(w["rules"])
    body = hd + "\n".join(lines) + ex + rules
    chunks, cur = [], ""
    for ln in body.split("\n"):
        if len(cur) + len(ln) + 1 > 1900:
            chunks.append(cur)
            cur = ""
        cur += ln + "\n"
    if cur:
        chunks.append(cur)
    return chunks


def post_discord(chunks: list[str], dry: bool) -> bool:
    url = os.environ.get("DISCORD_WEBHOOK_ARENA_URL", "").strip()
    if not url:
        print("[arena] DISCORD_WEBHOOK_ARENA_URL 未設定 → 配信スキップ（JSONのみ）")
        return False
    if dry:
        print("[arena] dry → 配信しない")
        return False
    ok = True
    for ch in chunks:
        r = requests.post(url, json={"content": ch}, timeout=15)
        ok &= r.status_code in (200, 204)
        time.sleep(0.5)
    print(f"[arena] Discord {'OK' if ok else 'NG'} ({len(chunks)}通)")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    sig = date.fromisoformat(a.date) if a.date else datetime.now(JST).date()
    if not is_trading_day(sig):
        print(f"[arena] {sig} は休場 → スキップ")
        return 0
    if os.path.exists(OUT_FILE) and not a.force:
        try:
            prev = json.load(open(OUT_FILE, encoding="utf-8"))
            if prev.get("date") == sig.strftime("%Y-%m-%d"):
                print(f"[arena] {sig} 分は生成済み → スキップ（--forceで再生成）")
                return 0
        except Exception:
            pass
    t0 = time.time()
    try:
        w = build(sig)
    except Exception as e:
        print(f"[arena] 生成失敗: {e}")
        return 1
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(w, f, ensure_ascii=False, indent=1)
    chunks = fmt_discord(w)
    print("\n".join(chunks))
    post_discord(chunks, a.dry)
    print(f"[arena] {len(w['rows'])}本 / 母集団{w['universe']} / {time.time()-t0:.0f}s → {OUT_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

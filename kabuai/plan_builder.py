"""
plan_builder.py — 📋作戦（明日の作戦を1画面に）— 2026-09-17 本人依頼「デイトレ用にもっと特化・最強のツールに・
ニュースを拾って明日狙う銘柄」。

出す物（全部「検証済みのルール」から機械的に組む。新しい予測は一切足さない）:
  ① 実弾の注文     … 🩳フェード GO（①100/②50）・💥崩壊◎（50万・寄指）・👑極上（150万・寄指）・🔻極み売り（3×100万）
                       → 各行に「注文の書き方」をそのまま載せる（写すだけ）
  ② 紙の対照       … 極み買い3×100万（紙）・崩壊の20億未満（撃たない）
  ③ 📰 材料        … 当日のTDnet適時開示（決算/上方修正/自社株買い/提携…）を全社、貸借・代金・前日比・触れる系統つき
  ④ 明日の決算予定 … JPXの決算発表予定（jpx_earnings_schedule.json）。保有をまたぐ玉に⚠️
  ⑤ 資金ラダー     … 余力が衝突した時の降ろす順（フェード＞極み売り＞崩壊）と縮小ライン

データ源は親リポの既存JSON（today_signals_gokujo/today_sell_signals/today_signals_kiwami/jpx_earnings_schedule）と
arena_watchlist.fetch_tdnet_all（TDnet日別一覧・150秒上限）。取れない物は空で通す（ビルドを落とさない）。
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
JST = timezone(timedelta(hours=9))

# 実弾ルール（2026-09-17時点・数字は memory/project_* と同じ。ここは表示用の定数）
FADE_SIZES = ("①100万", "②50万")
CRASH_SIZE = "50万"
GOKUJO_SIZE = "150万"
KIWAMI_SELL_SIZE = "100万×最大3"
LADDER = [
    "余力が衝突する日は フェード ＞ 極み売り ＞ 崩壊 の順に残す（崩壊を先に降ろす）",
    "現金余力30万割れ: フェード②ゼロ＋極み売り新規停止／15万割れ: フェード①50万／戻すのは月末",
    "現金余力100万到達: フェード①130万・崩壊100万へ",
    "崩壊は◎(代金20億+)だけ・1日1本・100株が50万を超える高額株は撃たない",
    "極上: 初日の引けが建値-1%以下なら翌朝処分／2日目の終値が建値+1%超なら同額追加（余力150万ある時だけ）",
]


def _is_trading_day(d: date) -> bool:
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return False
    except Exception:
        pass
    if d.weekday() >= 5:
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def next_trading_day(d: date) -> date:
    cur = d + timedelta(days=1)
    while not _is_trading_day(cur):
        cur += timedelta(days=1)
    return cur


def _load_json(name: str):
    p = PARENT / name
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _fetch_tdnet(ymd: str) -> dict[str, list[dict]]:
    """{code4: [{time, kind, title}]}。arena_watchlist の実装をそのまま使う。失敗は {}。"""
    try:
        if str(PARENT) not in sys.path:
            sys.path.insert(0, str(PARENT))
        import arena_watchlist as aw
        t0 = time.time()
        out = aw.fetch_tdnet_all(ymd, deadline_sec=150.0) or {}
        print(f"[plan] TDnet {ymd}: 材料あり{len(out)}社 {time.time()-t0:.0f}s")
        return out
    except Exception as e:
        print(f"[plan] TDnet取得失敗（材料なしで続行）: {e}")
        return {}


# TDnetの種別の言い直し（arena_watchlist.material_kind は表題キーワードの先勝ち。作戦では事務開示を落とし、
# 「上場」を上場廃止/新規上場に分ける・報酬目的の自己株処分は自社株買いにしない）
_DROP_TITLE = ("上場維持基準", "譲渡制限付株式報酬", "株式報酬", "ストック・オプション", "ストックオプション",
               "自己株式の消却", "自己株式の処分", "自己株式の取得状況", "取得状況に関するお知らせ", "経過開示", "（経過開示）")


def _refine_kind(title: str, kind: str) -> str | None:
    if any(k in title for k in _DROP_TITLE):
        return None
    if kind == "上場":
        if "上場廃止" in title or "整理銘柄" in title or "監理銘柄" in title:
            return "上場廃止"
        if "新規上場" in title or "上場承認" in title:
            return "新規上場"
        return None
    return kind


def build_plan(rows: list[dict], sell_watch: dict | None, arena: dict | None, data_date: str,
               iss_map: dict | None, name_map: dict | None) -> dict:
    """latest.json に載せる plan を組む。全部 None安全・例外は各節で握ってビルドを落とさない。"""
    d0 = datetime.strptime(data_date, "%Y-%m-%d").date()
    target = next_trading_day(d0)
    target_s = target.strftime("%Y-%m-%d")
    by_code = {str(r.get("code")): r for r in rows if r.get("code")}
    iss_map = iss_map or {}
    name_map = name_map or {}

    def _iss(code4: str):
        v = iss_map.get(code4) or iss_map.get(code4 + ".T")
        return "○" if v == "2" else ("×" if v else "?")

    def _nm(code4: str, fallback: str = "") -> str:
        r = by_code.get(code4)
        return (r and r.get("name")) or name_map.get(code4 + ".T") or name_map.get(code4) or fallback or code4

    # ── 明日の決算予定（JPX予定表）──
    sched = (_load_json("jpx_earnings_schedule.json") or {}).get("schedule", {}) or {}
    earn_tomorrow = sched.get(target_s, []) or []
    earn_codes = {str(x.get("code", ""))[:4] for x in earn_tomorrow}
    earn_today = {str(x.get("code", ""))[:4] for x in (sched.get(data_date, []) or [])}

    orders: list[dict] = []
    paper: list[dict] = []

    # ① 🩳フェード（sell_watch.fade.picks の verdict==GO 上位2本＝①100/②50）
    try:
        fd = (sell_watch or {}).get("fade") or {}
        gos = [p for p in (fd.get("picks") or []) if p.get("verdict") == "GO"]
        for i, p in enumerate(gos[:2]):
            reg = p.get("reg_note") or ""
            orders.append({
                "system": "🩳フェード", "pri": 1, "code": p["code"], "name": p["name"],
                "side": "空売り", "size": FADE_SIZES[i],
                "order": f"寄指 売り {int(round(p['min_entry'])):,}円以上 → 大引け成行で買い戻し" if p.get("min_entry") else "寄り成行 空売り → 大引け成行で買い戻し",
                "note": " / ".join(x for x in (f"前日+{p.get('gain', 0):.1f}%", f"乖離+{p.get('dev25', 0):.0f}%", f"ATR{p.get('atr_pct', 0):.1f}%", reg) if x),
                "warn": ("🚫売り禁＝ハイカラ在庫が要る（料の帯は配信を見る）" if p.get("jsf_stop") else "") or ("⚠️明日決算発表" if p["code"] in earn_codes else ""),
                "iss": p.get("short_mark") or _iss(p["code"]),
            })
    except Exception as e:
        print(f"[plan] フェード節スキップ: {e}")

    # ② 💥崩壊（◎だけ実弾・20億未満は撃たない＝紙へ）
    try:
        for m in (sell_watch or {}).get("members") or []:
            if not m.get("crash"):
                continue
            row = {
                "system": "💥崩壊", "pri": 3, "code": m["code"], "name": m["name"], "side": "空売り",
                "size": CRASH_SIZE if m.get("strong") else "撃たない（代金20億未満）",
                "order": f"寄指 売り {int(m['entry_min']):,}円以上 → 大引け成行で買い戻し" if m.get("entry_min") else "寄り成行 空売り → 大引け成行で買い戻し",
                "note": " / ".join(x for x in (f"前日{m.get('r1', 0):+.1f}%", f"出来高{m.get('vol_x', 0):.1f}倍", f"代金{m.get('turnover_oku', 0):.0f}億", "◎" if m.get("strong") else "") if x),
                "warn": ("高額株＝100株が50万超なら撃たない" if (m.get("price") or 0) * 100 > 500000 else "") or ("⚠️明日決算発表" if m["code"] in earn_codes else ""),
                "iss": "○",
            }
            (orders if m.get("strong") else paper).append(row)
    except Exception as e:
        print(f"[plan] 崩壊節スキップ: {e}")

    # ③ 👑極上（150万・寄指上限）/ 極み買い（紙）/ 🔻極み売り（3×100万）
    def _sig_rows(fname: str, system: str, pri: int, size: str, side: str, how, to_list: list):
        j = _load_json(fname)
        if not j or str(j.get("date", "")) != target_s:
            return
        for s in j.get("signals") or []:
            c4 = str(s.get("ticker", "")).replace(".T", "")[:4]
            to_list.append({
                "system": system, "pri": pri, "code": c4, "name": s.get("name") or _nm(c4), "side": side, "size": size,
                "order": how(s), "note": " / ".join(x for x in (f"前日終値 {int(round(s.get('prev_close') or 0)):,}円", (f"RSI{s['rsi']:.0f}" if s.get("rsi") is not None else ""), (f"乖離{s['deviation']:+.1f}%" if s.get("deviation") is not None else "")) if x),
                "warn": "⚠️明日決算発表＝保有中に決算をまたぐ" if c4 in earn_codes else "",
                "iss": _iss(c4),
            })
    try:
        _sig_rows("today_signals_gokujo.json", "👑極上", 2, GOKUJO_SIZE, "買い",
                  lambda s: (f"寄指 買い {int(round(s['limit_price'])):,}円以下 → 3営業日目の大引けで売り（初日引け-1%以下なら翌朝処分）" if s.get("limit_price") else "寄り成行 買い → 3営業日目の大引けで売り"), orders)
        _sig_rows("today_sell_signals.json", "🔻極み売り", 2, KIWAMI_SELL_SIZE, "空売り",
                  lambda s: "寄り成行 空売り → 3営業日目の大引けで買い戻し（損切り+2.5%）", orders)
        _sig_rows("today_signals_kiwami.json", "極み買い（紙・対照）", 9, "100万×3（紙）", "買い",
                  lambda s: (f"寄指 買い {int(round(s['limit_price'])):,}円以下（紙）" if s.get("limit_price") else "寄り成行 買い（紙）"), paper)
    except Exception as e:
        print(f"[plan] 極み/極上節スキップ: {e}")
    orders.sort(key=lambda o: (o["pri"], o["system"], o["code"]))

    # ④ 📰 材料（TDnet当日開示・全社）
    news: list[dict] = []
    kinds: dict[str, int] = {}
    try:
        mats = _fetch_tdnet(d0.strftime("%Y%m%d"))
        if not mats and arena:   # フォールバック: 土俵JSONに入っている材料だけ
            for r in (arena.get("rows") or []) + (arena.get("materials_extra") or []):
                if r.get("materials"):
                    mats.setdefault(str(r["code"])[:4], []).extend(r["materials"])
        fade_codes = {p["code"] for p in ((sell_watch or {}).get("fade") or {}).get("picks") or []}
        crash_codes = {m["code"] for m in (sell_watch or {}).get("members") or [] if m.get("crash")}
        arena_codes = {str(r.get("code")) for r in (arena or {}).get("rows") or []}
        for c4, items in mats.items():
            r = by_code.get(c4) or {}
            touch = []
            if c4 in fade_codes: touch.append("🩳フェード候補")
            if c4 in crash_codes: touch.append("💥崩壊")
            if c4 in arena_codes: touch.append("🎯土俵")
            if c4 in earn_codes: touch.append("明日決算")
            ks = []
            kept = []
            for it in items:
                k = _refine_kind(it.get("title") or "", it.get("kind") or "")
                if not k:
                    continue
                kept.append({**it, "kind": k})
                if k not in ks:
                    ks.append(k)
            if not kept:
                continue
            items = kept
            for k in ks:
                kinds[k] = kinds.get(k, 0) + 1
            news.append({
                "code": c4, "name": _nm(c4, (items[0].get("name") if items else "") or ""),
                "kinds": ks, "time": (items[0].get("time") if items else "") or "",
                "title": (items[0].get("title") if items else "") or "",
                "price": r.get("price"), "r1": r.get("r1"), "turnover_oku": r.get("turnover_oku"),
                "sector": r.get("sector"), "iss": _iss(c4), "touch": touch,
                "after_close": bool((items[0].get("time") or "99:99") >= "15:00") if items else False,
            })
        # 並び: 触れる系統あり → 代金の大きい順（代金不明は最後）
        news.sort(key=lambda n: (0 if n["touch"] else 1, -(n["turnover_oku"] or 0)))
    except Exception as e:
        print(f"[plan] 材料節スキップ: {e}")

    # ⑤ 明日の決算予定（代金つき・代金順）
    earn_rows = []
    for x in earn_tomorrow:
        c4 = str(x.get("code", ""))[:4]
        r = by_code.get(c4) or {}
        earn_rows.append({"code": c4, "name": x.get("name") or _nm(c4), "type": x.get("type", ""),
                          "turnover_oku": r.get("turnover_oku"), "iss": _iss(c4), "r1": r.get("r1")})
    earn_rows.sort(key=lambda e: -(e["turnover_oku"] or 0))

    print(f"[plan] {target_s}分: 注文{len(orders)}件 紙{len(paper)}件 材料{len(news)}社 明日決算{len(earn_rows)}社")
    return {
        "date": data_date, "target_date": target_s,
        "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "orders": orders, "paper": paper,
        "news": news[:120], "news_total": len(news), "news_kinds": kinds,
        "earnings_tomorrow": earn_rows[:60], "earnings_tomorrow_total": len(earn_rows),
        "earnings_today_count": len(earn_today),
        "ladder": LADDER,
    }

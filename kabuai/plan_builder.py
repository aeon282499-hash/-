"""
plan_builder.py — 📋作戦（明日の作戦を1画面に）— 2026-09-17 本人依頼「デイトレ用にもっと特化・最強のツールに・
ニュースを拾って明日狙う銘柄」。

出す物（全部「検証済みのルール」から機械的に組む。新しい予測は一切足さない）:
  ① 実弾の注文     … 🩳フェード GO（①100/②50・寄り成行）・💥崩壊◎（50万・寄指）・👑極上（200万・寄指）・🔻極み売り（3×100万）
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
# フェードの執行＝寄り成行（daytrade_paper.FADE_ENTRY_MARKET=True・下寄りでも建てる・2026-07-28〜）。
# min_entry(前日終値)は「約定判定に使わない参考価格」なので作戦の注文文に寄指とは書かない
# （2026-09-18 本人「売りフェードは寄り指→成り行き」＝作戦タブが寄指と出ていた誤表示の修正）。
# 売り禁(ハイカラ在庫で売る玉)だけ料の帯で注文種別が変わる: 料÷株価 ≤0.45%→成行 / 〜0.66%→当日中指値@前終
# / 〜1.46%→寄付限定@前終 / 超→撃たない（配信 daytrade_paper の💰行と同じ式）。
FADE_CAPITAL = {1: 1_000_000, 2: 500_000}
FADE_EDGE = {"ok": 0.45, "mid": 0.66, "lim": 1.46}


def _fade_rules():
    """daytrade_paper の本物の定数/株数関数を使う（取れなければ上の表示用定数）。"""
    try:
        if str(PARENT) not in sys.path:
            sys.path.insert(0, str(PARENT))
        import daytrade_paper as dp
        return (dp.capital_for_rank, dp._shares_for,
                {"ok": dp.FADE_EDGE_PCT_GAPDN, "mid": dp.FADE_EDGE_PCT_INTRA, "lim": dp.FADE_EDGE_PCT_MAIN},
                bool(dp.FADE_ENTRY_MARKET))
    except Exception:
        def _cap(rk): return FADE_CAPITAL.get(int(rk or 1), FADE_CAPITAL[1])
        def _sh(px, rk=1):
            if not px or px <= 0: return 0
            cap = _cap(rk)
            if int(rk or 1) >= 2 and px * 100 > cap: return 0
            return max(100, int(cap / px / 100) * 100)
        return _cap, _sh, dict(FADE_EDGE), True


def _rng(a: int, b: int) -> str:
    return f"{a}" if a == b else f"{a}〜{b}"


def fade_order_text(prev_close, rank: int, jsf_stop: bool) -> tuple[str, str, int]:
    """(注文文, 売り禁の料の帯/警告, 株数)。配信文(daytrade_paper)と同じ言い方に揃える。"""
    cap_for, shares_for, edge, market = _fade_rules()
    shares = shares_for(prev_close, rank) if prev_close else 0
    cap = cap_for(rank)
    if shares == 0:
        return (f"撃たない（値がさ＝100株が{cap // 10000}万に収まらない）", "", 0)
    if not market:
        order = f"寄指 売り {prev_close:,.0f}円以上 → 大引け成行で買い戻し"
    elif shares > 5_000:
        order = (f"指値 {prev_close - 1:,.0f}円・執行条件は当日中（{shares // 100}単元＝51単元以上は成行の空売り不可）"
                 f" → 大引け成行で買い戻し")
    else:
        order = f"寄り成行 空売り {shares:,}株 → 大引け成行で買い戻し"
    warn = ""
    if jsf_stop:
        ok = int(edge["ok"] / 100 * cap // shares)
        mid = int(edge["mid"] / 100 * cap // shares)
        lim = int(edge["lim"] / 100 * cap // shares)
        yd = f"当日中の指値{prev_close:,.0f}円"
        yb = f"寄付限定の指値{prev_close:,.0f}円"
        if lim < 1:
            band = "1円/株でもエッジ超え → 撃たない"
        elif ok < 1:
            band = f"成行は使わない ／ 〜{lim}円/株→{yb} ／ {lim + 1}円〜→撃たない"
        elif lim > mid > ok:
            band = f"〜{ok}円/株→成行 ／ {_rng(ok + 1, mid)}円→{yd} ／ {_rng(mid + 1, lim)}円→{yb} ／ {lim + 1}円〜→撃たない"
        elif lim > ok:
            band = f"〜{ok}円/株→成行 ／ {_rng(ok + 1, lim)}円→{yb} ／ {lim + 1}円〜→撃たない"
        else:
            band = f"〜{ok}円/株→成行 ／ {ok + 1}円〜→撃たない"
        warn = f"🚫売り禁＝ハイカラ在庫が要る。SBIのプレミアム料(円/株)で注文種別を決める: {band}"
    return order, warn, shares
CRASH_SIZE = "50万"
GOKUJO_SIZE = "200万"   # 2026-09-19 150万→200万（同額乗せで最大400万）
KIWAMI_SELL_SIZE = "100万×最大3"
LADDER = [
    "余力が衝突する日は フェード ＞ 極み売り ＞ 崩壊 の順に残す（崩壊を先に降ろす）",
    "現金余力30万割れ: フェード②ゼロ＋極み売り新規停止／15万割れ: フェード①50万／戻すのは月末",
    "現金余力100万到達: フェード①130万・崩壊100万へ",
    "崩壊は◎(代金20億+)だけ・1日1本・100株が50万を超える高額株は撃たない",
    "極上: 初日の引けが建値-1%以下なら翌朝処分／2日目の終値が建値+1%超なら同額追加（余力200万ある時だけ・本玉と合わせ最大400万）",
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
            prev = (by_code.get(p["code"]) or {}).get("price") or p.get("min_entry")
            # 執行は寄り成行（寄指ではない）。売り禁だけ料の帯で種別が変わる＝配信の💰行と同じ式。
            order, band, shares = fade_order_text(prev, i + 1, bool(p.get("jsf_stop")))
            warns = [w for w in (band, "⚠️明日決算発表" if p["code"] in earn_codes else "") if w]
            orders.append({
                "system": "🩳フェード", "pri": 1, "code": p["code"], "name": p["name"],
                "side": "空売り", "size": FADE_SIZES[i], "shares": shares,
                "order": order,
                "note": " / ".join(x for x in (f"前日+{p.get('gain', 0):.1f}%", f"乖離+{p.get('dev25', 0):.0f}%", f"ATR{p.get('atr_pct', 0):.1f}%", reg) if x),
                "warn": " ／ ".join(warns),
                "iss": p.get("short_mark") or _iss(p["code"]),
                "prev": prev,
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
                "iss": "○", "prev": m.get("price"),
            }
            (orders if m.get("strong") else paper).append(row)
    except Exception as e:
        print(f"[plan] 崩壊節スキップ: {e}")

    # ③ 👑極上（200万・寄指上限）/ 極み買い（紙）/ 🔻極み売り（3×100万）
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
                "iss": _iss(c4), "prev": s.get("prev_close"),
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

    # ⑥ 保有中の玉と出口（実弾の3日持ち系統: 👑極上=shadow_exit_gokujo.json / 🔻極み売り=positions_sell.json）
    holdings: list[dict] = []
    try:
        def _exit_day(entry_date: str) -> str:
            d = datetime.strptime(entry_date, "%Y-%m-%d").date()
            return next_trading_day(next_trading_day(d)).strftime("%Y-%m-%d")   # 3営業日目
        for fname, system, side, size, rule in (
                ("shadow_exit_gokujo.json", "👑極上", "買い", "200万", "3営業日目の大引けで売り／初日の引けが建値-1%以下なら翌朝処分／損切り-3%"),
                ("positions_sell.json", "🔻極み売り", "空売り", "100万", "3営業日目の大引けで買い戻し／損切り+2.5%")):
            j = _load_json(fname) or []
            lst = j.get("positions") if isinstance(j, dict) else j
            for x in lst or []:
                if x.get("status") != "open":
                    continue
                c4 = str(x.get("ticker", "")).replace(".T", "")[:4]
                ed = str(x.get("entry_date") or x.get("signal_date") or "")
                xd = _exit_day(ed) if ed else ""
                # 何日目＝建て日を1日目として target までの営業日数（帳簿の hold_days は前回更新時点の値なので使わない）
                try:
                    _d = datetime.strptime(ed, "%Y-%m-%d").date(); _n = 1
                    while _d < target and _n < 30:
                        _d = next_trading_day(_d); _n += 1
                    day_no = _n
                except Exception:
                    day_no = x.get("hold_days")
                holdings.append({
                    "system": system, "code": c4, "name": x.get("name") or _nm(c4), "side": side, "size": size,
                    "entry_date": ed, "entry_open": x.get("entry_open"), "hold_days": day_no,
                    "exit_date": xd, "exit_today": xd == target_s, "rule": rule,
                    "unrealized": x.get("unrealized_pnl"), "prev_close": x.get("prev_close"),
                    "warn": "⚠️明日決算発表" if c4 in earn_codes else "",
                })
        holdings.sort(key=lambda h: (not h["exit_today"], h["system"]))
    except Exception as e:
        print(f"[plan] 保有節スキップ: {e}")

    # ⑦ 直近の答え合わせ（各系統の帳簿・直近10本＝紙/実弾の別は表示で明記）
    recent: list[dict] = []
    def _rec(system: str, kind: str, items: list[tuple[str, str, float]]):
        items = [x for x in items if x[2] is not None][-10:]
        if not items:
            return
        w = sum(1 for x in items if x[2] > 0)
        recent.append({"system": system, "kind": kind, "n": len(items), "win": w,
                       "avg": round(sum(x[2] for x in items) / len(items), 2),
                       "last": [{"date": x[0], "name": x[1], "pnl": round(x[2], 2)} for x in items[-5:][::-1]]})
    try:
        j = _load_json("positions_day_paper.json") or {}
        _rec("🩳フェード", "紙①100/②50（実弾と同ルール）", [(x.get("entry_session") or x.get("signal_date") or "", x.get("name", ""), x.get("pnl_pct")) for x in (j.get("positions") or []) if x.get("status") == "closed"])
        j = _load_json("crash_short_log.json") or []
        _rec("💥崩壊", "紙30万（見送り除く・◎以外も含む）", [(x.get("exec_date") or x.get("d0") or "", x.get("name", ""), x.get("result_pct")) for x in j if x.get("result_pct") is not None and not x.get("skipped")])
        j = _load_json("shadow_exit_gokujo.json") or []
        lst = j.get("positions") if isinstance(j, dict) else j
        _rec("👑極上", "帳簿200万(9/18まで150万)", [(x.get("exit_date") or x.get("entry_date") or "", x.get("name", ""), x.get("pnl_pct")) for x in (lst or []) if x.get("status") == "closed"])
        j = _load_json("positions_sell.json") or []
        _rec("🔻極み売り", "帳簿100万×3", [(x.get("exit_date") or x.get("entry_date") or "", x.get("name", ""), x.get("pnl_pct")) for x in j if x.get("status") == "closed"])
    except Exception as e:
        print(f"[plan] 答え合わせ節スキップ: {e}")

    print(f"[plan] {target_s}分: 注文{len(orders)}件 紙{len(paper)}件 保有{len(holdings)}件 材料{len(news)}社 明日決算{len(earn_rows)}社")
    return {
        "date": data_date, "target_date": target_s,
        "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "orders": orders, "paper": paper, "holdings": holdings, "recent": recent,
        "news": news[:120], "news_total": len(news), "news_kinds": kinds,
        "earnings_tomorrow": earn_rows[:60], "earnings_tomorrow_total": len(earn_rows),
        "earnings_today_count": len(earn_today),
        "ladder": LADDER,
    }

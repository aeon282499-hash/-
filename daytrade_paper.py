# -*- coding: utf-8 -*-
"""
daytrade_paper.py — デイトレv2 紙トレ台帳（記帳・決済・通算成績＋信用売り可否チェック）
====================================================================================
【役割】既存の main_day.py / screener_day / screener_sell_day には一切手を加えず、
        「発火の答え合わせ」を自動で積み上げて、実弾投入前に通算成績を可視化する層。

【非破壊設計】
  - main_day.main() の成功パス末尾から run(today, signals) を呼ぶ（例外は握りつぶす想定）。
  - 単体実行も可: `python daytrade_paper.py [--dry] [--test]`（day_signals.json から当日発火を読む）。
  - 台帳は positions_day_paper.json（CIでコミットして永続化）。

【決済ロジック（v2は当日完結：寄り→引け）】
  - 記帳時: basis_date = シグナル算出の最終確定足（＝前営業日）。エントリー実セッション = basis_dateの翌取引日。
  - 決済時: そのティッカーの basis_date より後の最初の足を取り、Open/Close で損益確定。
      BUY : 寄り > MAX指値 → 見送り(SKIP) / それ以外 pnl=(引-寄)/寄
      SELL: 寄り < MIN指値 → 見送り(SKIP) / それ以外 pnl=(寄-引)/寄
  - 当日足はまだ無い（寄り前実行）ため、決済は翌営業日以降の実行で自然に確定する。
  - basis_date が14暦日超過しても足が取れない（売買停止/上場廃止）→ expired で台帳から退避。

【信用売り可否】J-Quants /markets/margin-interest の IssType（"2"=貸借銘柄＝空売り可）を利用。
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
BOOK_FILE = "positions_day_paper.json"
DAY_SIGNALS_FILE = "day_signals.json"
CAPITAL_PER_TRADE = 4_000_000   # 紙の1建玉サイズ（円・main_dayの推奨株数と同じ土台）
EXPIRE_DAYS = 14
# 毎日1銘柄（フェード）のGO閾値。ライブ実弾screener_sell_dayは+25%のまま据え置き。
# 検証(全市場10年・往復0.3%後): 毎日トップ株空売り=PF1.33/陽性9年。本体は前日+15%以上帯
# (+15-20%=PF1.50 / +20%超=PF1.35)。+5-15%は薄い(PF≈1.0)→GOは+15%以上のみ。
DAILY_PICK_GAIN_MIN = 15.0


# ------------------------------------------------------------------ util
def _today_jst_date():
    return datetime.now(JST).date()


def is_trading_day(d) -> bool:
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def _code4(ticker: str) -> str:
    return ticker.split(".")[0][:4]


def load_book() -> dict:
    if os.path.exists(BOOK_FILE):
        with open(BOOK_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"positions": [], "expired": [], "last_report_date": None}


def save_book(book: dict) -> None:
    with open(BOOK_FILE, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------ 信用売り可否
def fetch_iss_map(token) -> dict:
    """{code4: IssType} を直近公表週の /markets/margin-interest から。取れなければ {}。"""
    try:
        from screener import _jquants_get
        cur = _today_jst_date() - timedelta(days=4)
        for _ in range(14):
            if cur.weekday() < 5:
                d = _jquants_get("/markets/margin-interest", token,
                                 {"date": cur.strftime("%Y-%m-%d")})
                rows = d.get("data", [])
                if rows:
                    return {str(r.get("Code", ""))[:4]: str(r.get("IssType", "")) for r in rows}
            cur -= timedelta(days=1)
    except Exception as e:
        print(f"[paper] iss_map取得失敗: {e}")
    return {}


def shortability(ticker: str, iss_map: dict) -> dict:
    """信用売り可否の判定を返す。○=貸借銘柄(空売り可) / ×=信用銘柄(制度信用売り不可) / ?=不明。"""
    it = iss_map.get(_code4(ticker))
    if it == "2":
        return {"mark": "○", "iss": it,
                "note": "貸借銘柄＝制度信用で空売り可。ただし増担保・日々公表・逆日歩は当日板で要確認。"}
    if it:
        return {"mark": "×", "iss": it,
                "note": "信用銘柄（貸借でない）＝制度信用の空売り不可。一般信用（売り）在庫があれば可、無ければ見送り。"}
    return {"mark": "?", "iss": None,
            "note": "貸借区分データ無し（新興/新規上場など）。SBIで一般信用売り在庫の有無を要確認。"}


# ------------------------------------------------------------------ データ
def _fetch_all(today):
    """J-Quantsを日付ベースで一括取得（batch_downloadは全銘柄を返す）。決済＋紙SELLスキャン共用。"""
    from screener import batch_download_jquants, _jquants_id_token
    token = _jquants_id_token()
    # 45暦日≒31営業日。当日を除いても20日平均に足る履歴を確保（30日だと不足でスキップ）。
    start = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    return batch_download_jquants(token, start=start, end=end)


# 後方互換（テスト等が参照）: 全銘柄取得に委譲
def _fetch_data(tickers, today):
    return _fetch_all(today)


FADE_CAND_MIN = 5.0        # フェード候補の最低上昇率（これ未満は「急騰なし」）
FADE_TOV_MIN = 3e8         # 流動性フロア（20日代金中央値3億・BTと同一）
STICKY_RANGE_MIN = 0.05    # 張り付き除外: 信号日レンジ(高-安)/終値がこれ以下=ロックS高=踏み上げ危険で除外
                           # 10年BT: 除外でPF1.44→1.62・11年全プラス。7月は-36万→+75万に逆転。
PAPER_MAX_PICKS = 3        # 上位N銘柄まで配信・記帳（10年検証: 5は非効率・3が実質上限）


def daily_top_fades(data: dict, today, iss_map: dict, n: int = PAPER_MAX_PICKS) -> list[dict]:
    """毎日『フェード上位N銘柄』を上昇率降順で返す（各GO/NO-GO判定付き・空なら[]）。
    選定＝貸借○ × 前日+5%以上 × 張り付き除外(信号日レンジ>5%)。
    判定: 前日+15%以上 → GO（撃つ／紙）。+5〜15%は薄い → NO-GO（見送り）。
    10年検証: 張り付き除外でPF1.62・11年全プラス。上位3まで(5は非効率)。"""
    if not data:
        return []
    today_str = today.strftime("%Y-%m-%d")
    cands = []
    for tk, df in data.items():
        if df is None or df.empty:
            continue
        if iss_map.get(_code4(tk)) != "2":                  # 貸借○のみ（売れる玉だけ）
            continue
        d = df[df.index.strftime("%Y-%m-%d") < today_str]   # 前日までの確定足
        if len(d) < 21:
            continue
        c = d["Close"].astype(float)
        v = d["Volume"].astype(float)
        h = d["High"].astype(float)
        lo = d["Low"].astype(float)
        last_c = float(c.iloc[-1]); prev_c = float(c.iloc[-2])
        if last_c < 300 or prev_c <= 0:
            continue
        vol_avg = float(v.iloc[:-1].tail(20).mean())
        if vol_avg < 100_000:
            continue
        tov20 = float((c * v).tail(20).median())
        if tov20 < FADE_TOV_MIN:
            continue
        gain = (last_c - prev_c) / prev_c * 100
        if gain < FADE_CAND_MIN:
            continue
        rng = (float(h.iloc[-1]) - float(lo.iloc[-1])) / last_c
        if rng <= STICKY_RANGE_MIN:                         # 張り付きS高を除外
            continue
        cands.append({
            "ticker": tk, "name": tk, "direction": "SELL",
            "daily_gain": round(gain, 2),
            "prev_close": round(last_c, 1),
            "min_entry_price": round(last_c, 1),
            "vol_ratio": round(float(v.iloc[-1]) / vol_avg if vol_avg > 0 else 0, 1),
            "range_pct": round(rng * 100, 1),
        })
    if not cands:
        return []
    cands.sort(key=lambda x: x["daily_gain"], reverse=True)
    picks = cands[:max(1, n)]
    try:  # 銘柄名補完（上位数件のみ・軽量）
        from screener import fetch_tse_universe
        nm = {t: n2 for t, n2 in fetch_tse_universe()}
        for p in picks:
            p["name"] = nm.get(p["ticker"], p["ticker"])
    except Exception:
        pass
    for i, p in enumerate(picks, 1):
        sh = shortability(p["ticker"], iss_map)
        p["short"] = sh
        p["rank"] = i
        go = p["daily_gain"] >= DAILY_PICK_GAIN_MIN and sh["mark"] == "○"
        p["verdict"] = "GO" if go else "NOGO"
        if not go:
            p["nogo_reason"] = f"前日+{p['daily_gain']:.0f}%<15%＝薄い(コスト後トントン帯)"
    return picks


def _shares_for(limit_price: float) -> int:
    if not limit_price or limit_price <= 0:
        return 0
    return max(100, int(CAPITAL_PER_TRADE / limit_price / 100) * 100)


# ------------------------------------------------------------------ 決済
def settle(book: dict, data: dict, today) -> list[dict]:
    """pending を決済確定。確定した建玉リストを返す。"""
    today_str = today.strftime("%Y-%m-%d")
    just_closed = []
    still_pending = []

    for p in book["positions"]:
        if p.get("status") != "pending":
            still_pending.append(p)
            continue

        df = data.get(p["ticker"])
        basis = p["basis_date"]
        entry_row = None
        if df is not None and not df.empty:
            after = df[df.index.strftime("%Y-%m-%d") > basis]
            if not after.empty:
                entry_row = after.iloc[0]
                entry_date = after.index[0].strftime("%Y-%m-%d")

        # まだエントリーセッションの足が無い / 当日足（未確定）→ pending維持
        if entry_row is None or entry_date >= today_str:
            # 期限切れ（売買停止・上場廃止で永久に取れない）チェック
            basis_dt = datetime.strptime(basis, "%Y-%m-%d").date()
            if (today - basis_dt).days > EXPIRE_DAYS:
                p["status"] = "expired"
                book["expired"].append(p)
            else:
                still_pending.append(p)
            continue

        o = float(entry_row["Open"])
        c = float(entry_row["Close"])
        direction = p["direction"]
        limit = p.get("limit_price")

        if direction == "BUY":
            if limit is not None and o > limit:
                exit_type, pnl = "SKIP", 0.0
            else:
                exit_type, pnl = "CLOSE", (c - o) / o * 100
        else:  # SELL
            if limit is not None and o < limit:
                exit_type, pnl = "SKIP", 0.0
            else:
                exit_type, pnl = "CLOSE", (o - c) / o * 100

        shares = _shares_for(limit or o)
        pnl_yen = int(round(shares * o * pnl / 100)) if exit_type == "CLOSE" else 0

        p.update({
            "status": "closed",
            "entry_session": entry_date,
            "entry_open": round(o, 1),
            "entry_close": round(c, 1),
            "exit_type": exit_type,
            "pnl_pct": round(pnl, 3),
            "pnl_yen": pnl_yen,
            "win": bool(pnl > 0),
        })
        just_closed.append(p)
        still_pending.append(p)

    book["positions"] = still_pending
    return just_closed


# ------------------------------------------------------------------ 記帳
def record(book: dict, signals: list[dict], data: dict, iss_map: dict, today) -> list[dict]:
    """当日発火を pending として記帳（重複は無視）。新規記帳リストを返す。"""
    today_str = today.strftime("%Y-%m-%d")
    existing = {(p["ticker"], p["signal_date"]) for p in book["positions"] + book["expired"]}
    added = []

    for s in signals:
        tk = s["ticker"]
        key = (tk, today_str)
        if key in existing:
            continue

        # basis_date = そのティッカーの当日より前の最終確定足
        df = data.get(tk)
        basis = None
        if df is not None and not df.empty:
            before = df[df.index.strftime("%Y-%m-%d") < today_str]
            if not before.empty:
                basis = before.index[-1].strftime("%Y-%m-%d")
        if basis is None:
            print(f"[paper] {tk} basis足なし → 記帳スキップ")
            continue

        direction = s.get("direction", "BUY")
        limit = s.get("max_entry_price") if direction == "BUY" else s.get("min_entry_price")
        rec = {
            "ticker": tk,
            "name": s.get("name", tk),
            "direction": direction,
            "signal_date": today_str,
            "basis_date": basis,
            "prev_close": s.get("prev_close"),
            "limit_price": limit,
            "status": "pending",
        }
        if direction == "BUY":
            rec["high_20"] = s.get("high_20")
        else:
            rec["daily_gain"] = s.get("daily_gain")
            rec["short"] = shortability(tk, iss_map)
        book["positions"].append(rec)
        added.append(rec)

    return added


# ------------------------------------------------------------------ 通算成績
def cumulative_stats(book: dict) -> dict:
    closed = [p for p in book["positions"] if p.get("status") == "closed"]
    executed = [p for p in closed if p.get("exit_type") == "CLOSE"]
    skipped = [p for p in closed if p.get("exit_type") == "SKIP"]

    def agg(rows):
        if not rows:
            return dict(n=0, win=0.0, avg=0.0, pf=0.0, yen=0)
        wins = sum(1 for r in rows if r["pnl_pct"] > 0)
        gain = sum(r["pnl_pct"] for r in rows if r["pnl_pct"] > 0)
        loss = -sum(r["pnl_pct"] for r in rows if r["pnl_pct"] < 0)
        pf = (gain / loss) if loss > 0 else (float("inf") if gain > 0 else 0.0)
        return dict(n=len(rows), win=wins / len(rows) * 100,
                    avg=sum(r["pnl_pct"] for r in rows) / len(rows),
                    pf=pf, yen=sum(r.get("pnl_yen", 0) for r in rows))

    return {
        "all": agg(executed),
        "buy": agg([r for r in executed if r["direction"] == "BUY"]),
        "sell": agg([r for r in executed if r["direction"] == "SELL"]),
        "skipped": len(skipped),
        "pending": sum(1 for p in book["positions"] if p.get("status") == "pending"),
        "expired": len(book["expired"]),
    }


def _fmt_pf(pf):
    return "∞" if pf == float("inf") else f"{pf:.2f}"


# ------------------------------------------------------------------ Discord
def send_report(just_closed, buy_fires, picks, stats, today, dry=False):
    date_str = today.strftime("%Y年%m月%d日")
    lines = []
    if picks is None:
        picks = []
    go_picks = [p for p in picks if p.get("verdict") == "GO"]

    # ── 🎯 今日のデイトレ 上位N（フェード・毎営業日） ──
    if go_picks:
        lines.append(f"**🎯 今日のデイトレ 上位{len(go_picks)}（フェード＝上がりすぎを空売り・寄指売り→引成買戻し）**")
        for p in go_picks:
            sh = p.get("short") or shortability(p["ticker"], _LAST_ISS)
            lines.append(f"**{p.get('rank', 1)}番** 🔴 **{p.get('name', p['ticker'])}**（{p['ticker']}）"
                         f"前日+{p['daily_gain']:.0f}% ／ 出来高{p.get('vol_ratio', 0):.0f}倍 ／ "
                         f"レンジ{p.get('range_pct', 0):.0f}% ／ 貸借{sh['mark']}")
            lines.append(f"　→ **寄指 売り 指値¥{p['min_entry_price']:,.0f}以上** → 当日引成 買戻し")
        lines.append("　※約定した分だけ・当日決済必須・持ち越し禁止・損切りなし(引けまで保持)")
        lines.append("")
    elif picks:   # GO無し＝薄い候補のみ
        p = picks[0]
        sh = p.get("short") or shortability(p["ticker"], _LAST_ISS)
        lines.append("**🎯 今日のフェード候補（GO無し）**")
        lines.append(f"🔴 {p.get('name', p['ticker'])}（{p['ticker']}）前日+{p['daily_gain']:.0f}% 貸借{sh['mark']}"
                     f" → ⏸️ **見送り**：{p.get('nogo_reason', '薄い')}")
        lines.append("")
    else:
        lines.append("🎯 今日は急騰株ゼロ＝フェード候補なし（見送り）")
        lines.append("")

    # ── 🟢 ライブ買いシグナル（レア） ──
    if buy_fires:
        lines.append(f"**🟢 買いシグナル {len(buy_fires)}件（実弾基準・出来高10倍ブレイク）**")
        for s in buy_fires:
            lines.append(f"・{s.get('name', s['ticker'])}（{s['ticker']}）MAX指値¥{s.get('max_entry_price', 0):,.0f}で寄成買い→引け")
        lines.append("")

    # ── 📓 答え合わせ ──
    if just_closed:
        lines.append("**📓 答え合わせ（前回の当日結果）**")
        for p in just_closed:
            de = "🟢買" if p["direction"] == "BUY" else "🔴売"
            if p["exit_type"] == "SKIP":
                lines.append(f"⏭️{de} {p['name']}（{p['ticker']}）見送り（指値条件外）")
            else:
                mk = "✅" if p["pnl_pct"] > 0 else "❌"
                lines.append(f"{mk}{de} {p['name']}（{p['ticker']}）"
                             f"寄{p['entry_open']:,.0f}→引{p['entry_close']:,.0f} "
                             f"**{p['pnl_pct']:+.2f}%**（{p['pnl_yen']:+,}円）")
        lines.append("")

    a, b, se = stats["all"], stats["buy"], stats["sell"]
    lines.append("**📈 紙トレ通算成績（v2・答え合わせベース）**")
    lines.append(f"執行 **{a['n']}件** / 勝率 **{a['win']:.0f}%** / 平均 **{a['avg']:+.2f}%** / "
                 f"PF **{_fmt_pf(a['pf'])}** / 損益 **{a['yen']:+,}円**")
    if b["n"]:
        lines.append(f"　🟢買 {b['n']}件 勝率{b['win']:.0f}% 平均{b['avg']:+.2f}% PF{_fmt_pf(b['pf'])}")
    if se["n"]:
        lines.append(f"　🔴売 {se['n']}件 勝率{se['win']:.0f}% 平均{se['avg']:+.2f}% PF{_fmt_pf(se['pf'])}")
    tail = f"見送り{stats['skipped']} / 保有中{stats['pending']}"
    if stats["expired"]:
        tail += f" / 失効{stats['expired']}"
    lines.append("　" + tail)

    color = 0x43A047 if a["yen"] > 0 else (0xE53935 if a["yen"] < 0 else 0x757575)
    payload = {"embeds": [{
        "title": f"🧾【デイトレv2 紙トレ】{date_str}",
        "description": "\n".join(lines),
        "color": color,
        "footer": {"text": "実弾ではありません。答え合わせで通算を積み上げ、実弾判断の材料にします。"},
    }]}

    if dry:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    url = (os.getenv("DISCORD_WEBHOOK_DAY_URL") or os.getenv("DISCORD_WEBHOOK_URL_DAY")
           or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
    if not url:
        print("[paper] webhook未設定 → 通知スキップ")
        return
    import requests
    r = requests.post(url, json=payload, timeout=15)
    print(f"[paper] Discord通知 HTTP {r.status_code}")


_LAST_ISS = {}


# ------------------------------------------------------------------ orchestration
def run(today=None, signals=None, dry=False):
    """main_day末尾から呼ぶ想定。毎営業日『フェード上位3』＋ライブBUYを紙で回す。
    紙記帳するのは GO（前日+15%以上×貸借○×張り付き除外）の上位3と、ライブBUY発火のみ。
    signals未指定（単体実行）なら day_signals.json のBUYを読む。"""
    global _LAST_ISS
    if today is None:
        today = _today_jst_date()
    if not is_trading_day(today):
        print("[paper] 休場 → スキップ")
        return
    today_str = today.strftime("%Y-%m-%d")

    # ライブBUYの取り込み元
    if signals is None:
        signals = []
        if os.path.exists(DAY_SIGNALS_FILE):
            with open(DAY_SIGNALS_FILE, "r", encoding="utf-8") as f:
                signals = [s for s in json.load(f) if s.get("signal_date") == today_str]
    buy_fires = [s for s in signals if s.get("direction", "BUY") == "BUY"]

    book = load_book()

    # 決済＆1番選定に全銘柄を一括取得（失敗時は無取得で決済のみ試行）
    try:
        data = _fetch_all(today)
    except Exception as e:
        print(f"[paper] データ取得失敗（{e}）→ 決済のみ試行")
        data = {}

    just_closed = settle(book, data, today)

    _LAST_ISS = fetch_iss_map(_jq_token()) if data else {}
    picks = daily_top_fades(data, today, _LAST_ISS)       # 上位3（各GO/NO-GO）
    go_picks = [p for p in picks if p.get("verdict") == "GO"]

    # 紙記帳＝GOの上位3 ＋ ライブBUY発火のみ（見送りは記帳しない）
    to_record = list(buy_fires) + go_picks
    added = record(book, to_record, data, _LAST_ISS, today)

    stats = cumulative_stats(book)
    print(f"[paper] 決済{len(just_closed)}件 / 記帳{len(added)}件（買{len(buy_fires)}/GO売{len(go_picks)}）/ "
          f"通算執行{stats['all']['n']}件 PF{_fmt_pf(stats['all']['pf'])} 損益{stats['all']['yen']:+,}円")

    # 毎営業日1回だけ配信（上位3まで。二重送信は日付ガード）
    if book.get("last_report_date") != today_str:
        send_report(just_closed, buy_fires, picks, stats, today, dry=dry)
        if not dry:
            book["last_report_date"] = today_str

    if not dry:
        save_book(book)


def _jq_token():
    from screener import _jquants_id_token
    return _jquants_id_token()


def main():
    dry = "--dry" in sys.argv
    if "--test" in sys.argv:
        import _test_daytrade_paper  # noqa
        return
    run(dry=dry)


if __name__ == "__main__":
    main()

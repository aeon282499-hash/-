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
# 紙トレ専用のSELL閾値（ライブ実弾は screener_sell_day の+25%のまま据え置き）。
# 閾値スイープ(全市場10年・往復0.3%後): +20%=年26件/PF1.83/陽性10-11年。+12%で崩壊=底。
PAPER_SELL_GAIN_MIN = 20.0
PAPER_SELL_MAX = 8   # 1日に紙記帳するSELL上限（daily_gain降順）


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
    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    return batch_download_jquants(token, start=start, end=end)


# 後方互換（テスト等が参照）: 全銘柄取得に委譲
def _fetch_data(tickers, today):
    return _fetch_all(today)


def scan_paper_sell(data: dict, today) -> list[dict]:
    """紙トレ専用SELLスキャン（全市場・閾値をPAPER_SELL_GAIN_MINへ一時的に下げてjudgeを再利用）。"""
    if not data:
        return []
    import screener_sell_day as ssd
    today_str = today.strftime("%Y-%m-%d")
    orig = ssd.DAILY_GAIN_MIN
    ssd.DAILY_GAIN_MIN = PAPER_SELL_GAIN_MIN
    hits = []
    try:
        for tk, df in data.items():
            d = df[df.index.strftime("%Y-%m-%d") < today_str]   # 前日までの確定足で判定
            if len(d) < 25:
                continue
            r = ssd.judge_sell_signal_day(tk, tk, d)
            if r:
                hits.append(r)
    finally:
        ssd.DAILY_GAIN_MIN = orig   # 必ず元に戻す（ライブ実弾の+25%を汚さない）
    hits.sort(key=lambda x: x["daily_gain"], reverse=True)
    hits = hits[:PAPER_SELL_MAX]
    # 銘柄名を補完（発火時のみ・軽量）
    if hits:
        try:
            from screener import fetch_tse_universe
            nm = {t: n for t, n in fetch_tse_universe()}
            for h in hits:
                h["name"] = nm.get(h["ticker"], h["ticker"])
        except Exception:
            pass
    return hits


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
def send_report(just_closed, today_fires, stats, today, dry=False):
    date_str = today.strftime("%Y年%m月%d日")
    lines = []

    if just_closed:
        lines.append("**📓 答え合わせ（前回発火の当日結果）**")
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

    sell_fires = [s for s in today_fires if s.get("direction") == "SELL"]
    buy_fires = [s for s in today_fires if s.get("direction", "BUY") == "BUY"]
    if today_fires:
        lines.append(f"**🔔 本日発火（寄り前）: 🟢買{len(buy_fires)}（実弾同基準） / 🔴売{len(sell_fires)}（紙+20%）**")
        if sell_fires:
            lines.append("**🩳 信用売り可否チェック**（○のみ空売り可・実弾は貸借○＋在庫確認が必須）")
            for s in sell_fires:
                sh = shortability(s["ticker"], _LAST_ISS)
                lines.append(f"{sh['mark']} {s.get('name', s['ticker'])}（{s['ticker']}）"
                             f"前日+{s.get('daily_gain', 0):.0f}% … {sh['note']}")
            lines.append("・50単元以下は価格規制の成行制限を受けにくい / 逆日歩は当日実測")
        lines.append("寄りで指値イン→引け成で処分（OCO練習可）。当日決済必須・持ち越し禁止。")
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
    """main_day末尾から呼ぶ想定。
    当日発火 = ライブBUY（signals引数のBUYのみ・実弾screener_dayと同一）＋ 紙SELL（全市場+20%スキャン）。
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

    # 決済＆SELLスキャンに全銘柄を一括取得（失敗時は無取得で決済のみ試行）
    try:
        data = _fetch_all(today)
    except Exception as e:
        print(f"[paper] データ取得失敗（{e}）→ 決済のみ試行")
        data = {}

    just_closed = settle(book, data, today)

    sell_fires = scan_paper_sell(data, today)          # 紙専用 全市場+20%
    fires = buy_fires + sell_fires
    _LAST_ISS = fetch_iss_map(_jq_token()) if sell_fires else {}
    added = record(book, fires, data, _LAST_ISS, today)

    stats = cumulative_stats(book)
    print(f"[paper] 決済{len(just_closed)}件 / 新規記帳{len(added)}件"
          f"（ライブ買{len(buy_fires)}/紙売{len(sell_fires)}）/ "
          f"通算執行{stats['all']['n']}件 PF{_fmt_pf(stats['all']['pf'])} 損益{stats['all']['yen']:+,}円")

    # 通知は「答え合わせ有り or 当日発火有り」かつ本日未送信のときだけ（ハートビート無し）
    should_notify = (just_closed or fires) and book.get("last_report_date") != today_str
    if should_notify:
        send_report(just_closed, fires, stats, today, dry=dry)
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

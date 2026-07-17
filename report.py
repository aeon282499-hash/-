"""
report.py — 夕方15:40 JST に朝のシグナル結果を Discord に送信する
=================================================================
  - 朝の main.py が保存した today_signals.json を読み込む
  - 各銘柄の当日 始値・終値 を J-Quants で取得して損益を計算
  - 結果を trade_history.json に蓄積
  - 本日結果 + 月別累計 + 年間累計 を Discord に送信する
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()
JST              = zoneinfo.ZoneInfo("Asia/Tokyo")
HISTORY_FILE      = "trade_history.json"
SELL_HISTORY_FILE = "trade_history_sell.json"


def _is_trading_day(d) -> bool:
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def _is_week_last_trading_day(d) -> bool:
    """その週の最終営業日か（次の営業日が別の週なら True）。
    通常は金曜・金曜が祝日ならその週の最後の営業日（木曜等）に発火する。"""
    nxt = d + timedelta(days=1)
    while not _is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt.isocalendar()[:2] != d.isocalendar()[:2]

# 階層定義（main.py / close_check.py の TIERS と整合）
TIERS = [
    {
        "key":              "main",
        "label":            "大資金",
        "emoji":            "",
        "size":             1_000_000,
        "buy_sig_file":     "today_signals.json",
        "sell_sig_file":    "today_sell_signals.json",
        "buy_history_file": "trade_history.json",
        "sell_history_file": "trade_history_sell.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
    },
    {
        "key":              "mid",
        "label":            "中資金",
        "emoji":            "🔵",
        "size":             500_000,
        "buy_sig_file":     "today_signals_mid.json",
        "sell_sig_file":    "today_sell_signals_mid.json",
        "buy_history_file": "trade_history_mid.json",
        "sell_history_file": "trade_history_sell_mid.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
    },
    {
        "key":              "small",
        "label":            "小資金",
        "emoji":            "🟢",
        "size":             300_000,
        "buy_sig_file":     "today_signals_small.json",
        "sell_sig_file":    "today_sell_signals_small.json",
        "buy_history_file": "trade_history_small.json",
        "sell_history_file": "trade_history_sell_small.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
    },
]


# ================================================================
# J-Quants（screener.batch_download_jquants と同じ日付ベース全銘柄取得方式）
# Light プランでは個別銘柄取得 (?code=) が制限されるため、close_check.py と
# 同じく当日全銘柄を一括取得→ticker フィルタで処理する。
# ================================================================

def fetch_today_ohlc(tickers: list[str]) -> dict[str, dict]:
    """当日の {ticker: {"open": float, "close": float}} を返す。"""
    if not tickers:
        return {}
    from screener import batch_download_jquants, _jquants_id_token

    today_str = date.today().strftime("%Y-%m-%d")
    print(f"[report] J-Quantsで {today_str} の全銘柄を取得中...")
    token    = _jquants_id_token()
    all_data = batch_download_jquants(token, start=today_str, end=today_str)

    result: dict[str, dict] = {}
    for ticker in tickers:
        df = all_data.get(ticker)
        if df is None or df.empty:
            print(f"  [report] {ticker}: 本日データなし")
            continue
        try:
            o = float(df["Open"].iloc[-1])
            c = float(df["Close"].iloc[-1])
            if o > 0 and c > 0:
                result[ticker] = {"open": o, "close": c}
        except Exception as e:
            print(f"  [report] {ticker} パース失敗: {e}")
    print(f"[report] {len(result)}/{len(tickers)} 銘柄のOHLC取得完了")
    return result


def calc_pnl(direction: str, open_price: float, close_price: float) -> float:
    if direction == "BUY":
        return (close_price - open_price) / open_price * 100
    else:
        return (open_price - close_price) / open_price * 100


# ================================================================
# 履歴管理
# ================================================================

def load_history(path: str = HISTORY_FILE) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("trades", [])


def save_history(trades: list[dict], path: str = HISTORY_FILE) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"trades": trades}, f, ensure_ascii=False, indent=2)


def append_today_results(results: list[dict], trade_date: str,
                         history_path: str = HISTORY_FILE) -> list[dict]:
    """今日の結果を履歴に追加（同日の重複は上書き）。"""
    trades = load_history(history_path)
    trades = [t for t in trades if t["date"] != trade_date]
    for r in results:
        trades.append({
            "date":      trade_date,
            "ticker":    r["ticker"],
            "name":      r["name"],
            "direction": r["direction"],
            "open":      r["open"],
            "close":     r["close"],
            "pnl":       r["pnl"],
            "win":       r["pnl"] > 0,
        })
    save_history(trades, history_path)
    return trades


# ================================================================
# 集計
# ================================================================

def calc_stats(trades: list[dict]) -> dict:
    if not trades:
        return {"count": 0, "wins": 0, "win_rate": 0, "avg_pnl": 0, "pf": 0}
    pnls  = [t["pnl"] for t in trades]
    wins  = sum(1 for p in pnls if p > 0)
    gain  = sum(p for p in pnls if p > 0)
    loss  = abs(sum(p for p in pnls if p < 0))
    return {
        "count":    len(trades),
        "wins":     wins,
        "win_rate": round(wins / len(trades) * 100, 1),
        "avg_pnl":  round(sum(pnls) / len(pnls), 2),
        "pf":       round(gain / loss, 2) if loss > 0 else 999,
    }


# ================================================================
# Discord 送信
# ================================================================

def _post(url: str, payload: dict) -> None:
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[report] Discord 送信失敗: HTTP {resp.status_code}")
    time.sleep(0.5)


def _tier_prefix(tier: dict) -> str:
    if tier["key"] == "main":
        return ""
    return f"{tier['emoji']}【{tier['label']}】"


def send_report(results: list[dict], signal_date: str, all_trades: list[dict],
                tier: dict | None = None) -> None:
    """夕方の1日目スナップショット結果をDiscord送信。"""
    if tier is None:
        tier = TIERS[0]
    url = tier["buy_webhook"]
    if not url:
        print(f"[report-{tier['label']}] buy_webhook未設定 → スキップ")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    today_s  = calc_stats(results)

    lines = []
    for r in results:
        mark      = "✅" if r["pnl"] > 0 else "❌"
        dir_label = "🔴BUY" if r["direction"] == "BUY" else "🔵SELL"
        lines.append(
            f"{mark} **{r['name']}**（{r['ticker']}）{dir_label}\n"
            f"　始値 {r['open']:,.0f}円 → 終値 {r['close']:,.0f}円　**{r['pnl']:+.2f}%**"
        )

    if today_s["count"] > 0:
        summary = (
            f"**{today_s['count']}銘柄** エントリー｜"
            f"勝率 {today_s['wins']}/{today_s['count']}（{today_s['win_rate']}%）｜"
            f"平均 **{today_s['avg_pnl']:+.2f}%**"
        )
        color = 0x43A047 if today_s["avg_pnl"] >= 0 else 0xE53935
    else:
        summary = "本日シグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## {_tier_prefix(tier)}📊 本日のシグナル日中結果｜{date_str}",
        "embeds": [{
            "description": (
                f"※1日目寄付→終値スナップショット（参考値・実現損益とは別）\n\n"
                f"{summary}\n\n" + "\n".join(lines)
            ),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）/ 1件{tier['size']//10000}万円枠"},
        }],
    })
    print(f"[report-{tier['label']}] BUY {today_s['count']}件 送信")


def send_sell_report(results: list[dict], signal_date: str, all_trades: list[dict],
                     tier: dict | None = None) -> None:
    """空売り1日目スナップショット結果。"""
    if tier is None:
        tier = TIERS[0]
    url = tier["sell_webhook"]
    if not url:
        print(f"[report-{tier['label']}] sell_webhook未設定 → SELLスキップ")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    today_s  = calc_stats(results)

    lines = []
    for r in results:
        mark = "✅" if r["pnl"] > 0 else "❌"
        lines.append(
            f"{mark} **{r['name']}**（{r['ticker']}）空売り\n"
            f"　始値 {r['open']:,.0f}円 → 終値 {r['close']:,.0f}円　**{r['pnl']:+.2f}%**"
        )

    if today_s["count"] > 0:
        summary = (
            f"**{today_s['count']}銘柄** エントリー｜"
            f"勝率 {today_s['wins']}/{today_s['count']}（{today_s['win_rate']}%）｜"
            f"平均 **{today_s['avg_pnl']:+.2f}%**"
        )
        color = 0x43A047 if today_s["avg_pnl"] >= 0 else 0xE53935
    else:
        summary = "本日空売りシグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## {_tier_prefix(tier)}📉 本日の空売り日中結果｜{date_str}",
        "embeds": [{
            "description": (
                f"※1日目寄付→終値スナップショット（参考値・実現損益とは別）\n\n"
                f"{summary}\n\n" + "\n".join(lines)
            ),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）/ 1件{tier['size']//10000}万円枠"},
        }],
    })
    print(f"[report-{tier['label']}] SELL {today_s['count']}件 送信")


# ================================================================
# メイン
# ================================================================

def _load_signals_file(filepath: str, today_str: str) -> list[dict] | None:
    """シグナルファイルを読み込んで検証する。Noneは処理スキップ。"""
    if not os.path.exists(filepath):
        print(f"[report] {filepath} が見つかりません → スキップ")
        return None
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)
    signal_date = data.get("date", "")
    if signal_date != today_str:
        print(f"[report] {filepath} 日付 {signal_date} ≠ 今日 {today_str} → スキップ")
        return None
    return data.get("signals", [])


def _process_signals(signals: list[dict], ohlc: dict, signal_date: str,
                     history_path: str) -> tuple[list[dict], list[dict]]:
    """シグナルリストから損益を計算して履歴に保存。"""
    from screener import yose_limit_price
    results = []
    for s in signals:
        t = s["ticker"]
        if t not in ohlc:
            print(f"  [skip] {t}: データなし")
            continue
        o   = ohlc[t]["open"]
        c   = ohlc[t]["close"]
        # 寄指運用(2026-06-11〜): BUYで寄りが指値超え=約定していないのでスナップショット対象外
        # limit_price は main.py が today_signals.json に保存（無い旧形式は prev_close から再計算）
        if s["direction"] == "BUY":
            lp = s.get("limit_price") or yose_limit_price(s.get("prev_close", 0) or 0)
            if lp and o > lp:
                print(f"  [skip] {t}: 寄指不成立(寄り{o:,.0f}円 > 指値{lp:,}円) → 約定なし")
                continue
        pnl = calc_pnl(s["direction"], o, c)
        results.append({
            "ticker":    t,
            "name":      s["name"],
            "direction": s["direction"],
            "open":      o,
            "close":     c,
            "pnl":       round(pnl, 2),
        })
    all_trades = append_today_results(results, signal_date, history_path)
    return results, all_trades


def main() -> None:
    today_jst = datetime.now(JST).date()
    today_str = today_jst.strftime("%Y-%m-%d")

    # 全階層のsignalsをまずロードしてticker集合を作る
    tier_signals = {}
    union_tickers = set()
    for tier in TIERS:
        if not tier["buy_webhook"] and tier["key"] != "main":
            continue
        buy_sigs  = _load_signals_file(tier["buy_sig_file"],  today_str)
        sell_sigs = _load_signals_file(tier["sell_sig_file"], today_str)
        tier_signals[tier["key"]] = (buy_sigs, sell_sigs)
        for sigs in (buy_sigs or [], sell_sigs or []):
            union_tickers.update(s["ticker"] for s in sigs)

    if not union_tickers:
        print("[report] 本日シグナル0件 → 取得スキップ")
        ohlc = {}
    else:
        ohlc = fetch_today_ohlc(sorted(union_tickers))

    for tier in TIERS:
        key = tier["key"]
        if key not in tier_signals:
            continue
        buy_sigs, sell_sigs = tier_signals[key]
        print(f"\n[report-{tier['label']}] 処理開始")

        # 夕方の「日中結果｜1日目スナップショット（参考値）」Discord配信は
        # ユーザー指示(2026-06-26)で完全廃止。trade_history.json への日次蓄積は
        # _process_signals 内で継続（後参照・バックアップ用）＝send_report系は呼ばない。
        if buy_sigs is not None:
            _process_signals(buy_sigs, ohlc, today_str, tier["buy_history_file"])

        if sell_sigs is not None:
            _process_signals(sell_sigs, ohlc, today_str, tier["sell_history_file"])

    # 週次レポート（金曜＝その週の最終営業日の引け後だけ・2026-06-26追加）。
    # 今週の確定損益(BUY/SELL)＋保有中持ち越しを全決済の金額明細つきで1通配信。
    # 実現損益は positions_*.json から集計（trade_historyは初日スナップなので使わない）。
    if _is_week_last_trading_day(today_jst):
        _send_weekly_reports(today_jst)


def _send_weekly_reports(today_jst: date) -> None:
    """全階層の週次レポートを送信する。

    帳簿(positions_*.json)は翌営業日朝の main.py が確定するため、金曜15:40時点では
    当日決済分(MAXHOLD/RSI/OCO)が open のまま・当日エントリーが pending のまま残る。
    そのままだと「処分済みなのに持ち越し」表示になる（2026-07-03 日本化薬の誤表示）ので、
    コピーに対して update_positions を当日の引けまでドライランしてから集計する。
    ここではファイル保存しない＝帳簿の確定は従来どおり翌朝の main.py が行う。
    """
    import copy
    from tracker import update_positions
    from notifier import send_weekly_report
    from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS

    # 引け後なので「明日」を基準にすると update_positions が当日バーまで処理する
    virtual_today = today_jst + timedelta(days=1)

    tier_pos: list[tuple[dict, list, list]] = []
    has_active = False
    for tier in TIERS:
        if not tier["buy_webhook"] and tier["key"] != "main":
            continue
        key = tier["key"]
        pos_file      = "positions.json"      if key == "main" else f"positions_{key}.json"
        sell_pos_file = "positions_sell.json" if key == "main" else f"positions_sell_{key}.json"
        buy_pos  = json.load(open(pos_file, encoding="utf-8"))      if os.path.exists(pos_file)      else []
        sell_pos = json.load(open(sell_pos_file, encoding="utf-8")) if os.path.exists(sell_pos_file) else []
        tier_pos.append((tier, buy_pos, sell_pos))
        has_active = has_active or any(p.get("status") in ("pending", "open")
                                       for p in buy_pos + sell_pos)

    # 全階層で使い回す価格データを1回だけ取得（窓は tracker と同じ120日。
    # 旧30日窓は起点が朝runと1日ズレるだけでRSI50境界の判定が割れ、2026-07-17に
    # 保有中のビックカメラを「RSI回復決済」と誤表示した）
    all_data = None
    if has_active:
        start = (virtual_today - timedelta(days=RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
        end   = virtual_today.strftime("%Y-%m-%d")
        print(f"[report] 週次ドライラン用の価格データ取得中（{start}〜{end}）...")
        token    = _jquants_id_token()
        all_data = batch_download_jquants(token, start=start, end=end)

    for tier, buy_pos, sell_pos in tier_pos:
        sim_buy  = update_positions(copy.deepcopy(buy_pos),  virtual_today, all_data=all_data)[0]
        sim_sell = update_positions(copy.deepcopy(sell_pos), virtual_today, all_data=all_data)[0]
        send_weekly_report(sim_buy, sim_sell, today_jst, tier=tier)


if __name__ == "__main__":
    main()

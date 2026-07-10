# -*- coding: utf-8 -*-
"""main_earnings_hold.py — 決算持ち越しシグナル（2026-07-10新設・BT根拠は _bt_earnings_hold*.py）。

毎営業日14:55 JST（schedule_close.yml相乗り）:
  1. JPX決算発表予定を更新（失敗時は手元のJSONで続行）＋発表時刻履歴の鮮度維持
  2. 大100万/中50万/小30万の3階層それぞれ（2026-07-10分割・各8枠・専用Webhook）:
     - 昨日エントリー分を決済記帳（entry=シグナル日終値[J-Quants公式]/exit=翌営業日寄り）
     - 本日発表予定 × ルールA（RSI≤45・5日騰落<-3%・売買代金20日中央値≥10億・株価≤階層上限）
       → RSI昇順に最大8枠 → Discord配信「大引け成行買いリスト」
  3. positions_earnings*.json 保存（翌日の決済記帳用）

出口は無条件で翌寄り成行売り（1晩・オーバーナイトはSTOP無効）。
BT(4.5年・8枠): 大PF1.34/+540万・中PF1.39/+272万・小PF1.51/+159万・全階層とも年利≈15%/全5年プラス。

実行: python main_earnings_hold.py [--force](時間ガード無視) [--dry](配信/保存なし)
      [--test](フォーマット確認用のサンプル配信のみ)
"""
from __future__ import annotations

import json
import os
import sys
import zoneinfo
from datetime import date, datetime, timedelta

import jpholiday
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
SCHEDULE_FILE = "jpx_earnings_schedule.json"
TIMES_FILE = "earnings_times.json"

# ── ルールA（BT確定値・むやみに変えない） ──
SLOTS = 8
RSI_MAX = 45.0
RUNUP_MAX = -3.0      # 直前5営業日騰落% がこれ未満
TOV_MIN = 1e9         # 20日中央値売買代金
HIST_DAYS = 60        # J-Quants取得窓（暦日）

# 3階層（2026-07-10分割・スイングと同じ価格帯構造・各8枠）
TIERS = [
    {"label": "大資金", "size": 1_000_000, "price_cap": 10000,
     "webhook_env": "DISCORD_WEBHOOK_EARNINGS_URL",
     "positions_file": "positions_earnings.json"},
    {"label": "中資金", "size": 500_000, "price_cap": 5000,
     "webhook_env": "DISCORD_WEBHOOK_EARNINGS_MID_URL",
     "positions_file": "positions_earnings_mid.json"},
    {"label": "小資金", "size": 300_000, "price_cap": 3000,
     "webhook_env": "DISCORD_WEBHOOK_EARNINGS_SMALL_URL",
     "positions_file": "positions_earnings_small.json"},
]
MAX_PRICE_CAP = max(t["price_cap"] for t in TIERS)


# ================================================================
# 共通ユーティリティ
# ================================================================

def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    if d.month == 12 and d.day == 31:
        return False
    if d.month == 1 and d.day <= 3:
        return False
    return True


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while not is_trading_day(n):
        n += timedelta(days=1)
    return n


def time_bucket(t: str | None) -> str:
    """DiscTime('15:30:00')→時間帯。場中組は買う時点で発表済みの可能性が高い。"""
    if not t:
        return "履歴なし"
    try:
        hm = int(t[:2]) * 60 + int(t[3:5])
    except Exception:
        return "履歴なし"
    if hm < 9 * 60:
        return "寄り前"
    if hm < 15 * 60:
        return "場中"
    return "引け後"


def load_times() -> dict:
    if os.path.exists(TIMES_FILE):
        with open(TIMES_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def last_disc_time(times: dict, ticker: str, before: str) -> str | None:
    """beforeより前の直近の発表時刻。時刻表示用（除外には使わない=BTで逆効果と確定）。"""
    dm = times.get(ticker, {})
    prev = [d for d in dm if d < before]
    return dm[max(prev)] if prev else None


def refresh_earnings_times(today: date, days: int = 10) -> None:
    """直近N営業日の/fins/summaryを取得しearnings_times.jsonへマージ（時刻の鮮度維持）。"""
    import time as _time

    import urllib3
    urllib3.disable_warnings()
    key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not key:
        print("[times] JQUANTS_API_KEY なし → スキップ")
        return
    times = load_times()
    d = today
    fetched = 0
    while fetched < days:
        d -= timedelta(days=1)
        if not is_trading_day(d):
            continue
        fetched += 1
        ds = d.strftime("%Y-%m-%d")
        try:
            r = requests.get("https://api.jquants.com/v2/fins/summary",
                             headers={"x-api-key": key}, params={"date": ds},
                             timeout=60, verify=False)
            if r.status_code != 200:
                continue
            for x in r.json().get("data", []):
                code = str(x.get("Code", ""))[:4]
                dd, tt = x.get("DiscDate"), x.get("DiscTime")
                if code and dd and tt:
                    times.setdefault(code + ".T", {})[dd] = tt
        except Exception as e:
            print(f"[times] {ds} 取得失敗: {e}")
        _time.sleep(1.1)
    with open(TIMES_FILE, "w", encoding="utf-8") as f:
        json.dump(times, f, ensure_ascii=False)
    print(f"[times] 直近{days}営業日をマージ（銘柄{len(times)}）")


def rule_pass(rsi: float | None, runup5: float | None,
              tov20: float | None, price: float | None,
              price_cap: float = MAX_PRICE_CAP) -> bool:
    if rsi is None or runup5 is None or tov20 is None or price is None:
        return False
    if any(pd.isna(x) for x in (rsi, runup5, tov20, price)):
        return False
    return (rsi <= RSI_MAX and runup5 < RUNUP_MAX
            and tov20 >= TOV_MIN and price <= price_cap)


def calc_shares(price: float, size: int) -> int:
    return max(100, int(size / price / 100) * 100)


def load_positions(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"last_signal_date": None, "positions": []}


def save_positions(store: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=1)


def send_discord(embeds: list[dict], webhook: str, label: str,
                 dry: bool = False) -> None:
    if dry:
        print(f"[dry-{label}] Discord送信スキップ:")
        print(json.dumps(embeds, ensure_ascii=False, indent=1)[:1500])
        return
    if not webhook:
        print(f"[warn-{label}] Webhook未設定 → 送信不可")
        return
    for attempt in range(3):
        try:
            r = requests.post(webhook, json={"embeds": embeds}, timeout=15)
            if r.status_code in (200, 204):
                print(f"[discord-{label}] 送信OK ({len(embeds)} embeds)")
                return
            print(f"[discord-{label}] HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[discord-{label}] attempt{attempt + 1} 失敗: {e}")
    print(f"[discord-{label}] 3回失敗 → 断念")


# ================================================================
# 決済記帳（昨日エントリー分）
# ================================================================

def settle_pendings(store: dict, today: date, all_data: dict,
                    size: int) -> list[dict]:
    """pendingを決済記帳。entry=シグナル日の終値(J-Quants)・exit=翌営業日寄り。
    戻り値: 今回closedにした明細（Discord表示用）。"""
    import yfinance as yf
    settled = []
    for pos in store["positions"]:
        if pos.get("status") != "pending":
            continue
        tk = pos["ticker"]
        sig_d = datetime.strptime(pos["date"], "%Y-%m-%d").date()
        exit_d = next_trading_day(sig_d)
        if exit_d > today:
            continue  # まだ決済日が来ていない（通常ありえないが安全側）

        entry = exit_px = None
        df = all_data.get(tk)
        if df is not None and len(df):
            idx = df.index.strftime("%Y-%m-%d")
            m_in = idx == pos["date"]
            if m_in.any():
                entry = float(df["Close"][m_in].iloc[0])
            m_out = idx == exit_d.strftime("%Y-%m-%d")
            if m_out.any():
                v = float(df["Open"][m_out].iloc[0])
                if v > 0:
                    exit_px = v
        if entry is None or exit_px is None:
            try:
                h = yf.Ticker(tk).history(period="1mo")
                hi = h.index.strftime("%Y-%m-%d")
                if entry is None:
                    m = hi == pos["date"]
                    if m.any():
                        entry = float(h["Close"][m][0])
                if exit_px is None:
                    m = hi == exit_d.strftime("%Y-%m-%d")
                    if m.any():
                        exit_px = float(h["Open"][m][0])
            except Exception as e:
                print(f"  [settle] {tk} yfinance失敗: {e}")
        if entry is None or exit_px is None:
            pos["note"] = f"決済価格未取得(entry={entry}, exit={exit_px})・翌日再試行"
            print(f"  [settle] {tk} 価格未取得 → pending維持")
            continue

        shares = pos.get("shares") or calc_shares(entry, size)
        pos.update({
            "status": "closed", "entry": round(entry, 1),
            "exit_date": exit_d.strftime("%Y-%m-%d"), "exit": round(exit_px, 1),
            "pnl_pct": round((exit_px - entry) / entry * 100, 2),
            "pnl_yen": int((exit_px - entry) * shares),
        })
        pos.pop("note", None)
        settled.append(pos)
        print(f"  [settle] {tk} {pos['name']} {entry:,.0f}→{exit_px:,.0f} "
              f"{pos['pnl_pct']:+.2f}% ({pos['pnl_yen']:+,}円)")
    return settled


# ================================================================
# 当日シグナル（候補生成は全階層共用・価格帯カットは階層側）
# ================================================================

def build_candidates(codes: list[dict], all_data: dict,
                     times: dict | None = None) -> list[dict]:
    """発表予定銘柄にルールA(価格帯は最大キャップ)を適用。現在値はyfinance(15分遅延≒14:40)。"""
    import yfinance as yf
    from screener import calc_rsi

    times = times or {}
    today_str = date.today().strftime("%Y-%m-%d")
    out = []
    for x in codes:
        tk = x["code"] + ".T"
        df = all_data.get(tk)
        if df is None or len(df) < 30:
            continue
        closes = df["Close"].dropna()
        vols = df["Volume"].reindex(closes.index).fillna(0)
        tov20 = float((closes * vols).tail(20).median())
        if tov20 < TOV_MIN:      # 現在値を取りに行く前に足切り（yfinanceコール節約）
            continue
        try:
            intraday = yf.Ticker(tk).history(period="1d", interval="5m")
            price = float(intraday["Close"].iloc[-1]) if not intraday.empty else None
        except Exception as e:
            print(f"  [yfinance] {tk} 現在値失敗: {e}")
            price = None
        if price is None or price <= 0:
            continue
        seq = closes.tolist() + [price]
        rsi = calc_rsi(pd.Series(seq))
        runup5 = (price - seq[-6]) / seq[-6] * 100 if len(seq) >= 6 and seq[-6] > 0 else None
        if rsi is None or runup5 is None:
            continue
        if rule_pass(rsi, runup5, tov20, price, MAX_PRICE_CAP):
            lt = last_disc_time(times, tk, today_str)
            out.append({"ticker": tk, "code": x["code"], "name": x["name"],
                        "type": x.get("type", ""), "price": price,
                        "rsi": round(float(rsi), 1), "runup5": round(runup5, 1),
                        "tov20": tov20,
                        "last_time": lt[:5] if lt else None,
                        "last_bucket": time_bucket(lt)})
    return sorted(out, key=lambda r: r["rsi"])


# ================================================================
# Discord embeds
# ================================================================

def embed_results(settled: list[dict], tier: dict) -> dict:
    total = sum(p["pnl_yen"] for p in settled)
    lines = []
    for p in settled:
        lines.append(f"{'🟢' if p['pnl_yen'] > 0 else '🔴'} **{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" 買{p['entry']:,.0f}円→売{p['exit']:,.0f}円｜"
                     f"{p['pnl_pct']:+.2f}%｜**{p['pnl_yen']:+,}円**")
    wins = sum(1 for p in settled if p["pnl_yen"] > 0)
    return {
        "title": f"✅ 決算持ち越し【{tier['label']}】｜昨日分の決済結果（{wins}勝{len(settled) - wins}敗）",
        "description": "\n".join(lines) + f"\n\n**合計 {total:+,}円**",
        "color": 0x2ECC71 if total >= 0 else 0xE74C3C,
    }


def embed_signals(picks: list[dict], n_scheduled: int, today: date, tier: dict,
                  note: str | None = None) -> dict:
    md = today.strftime("%m/%d")
    if not picks:
        return {
            "title": f"📊 決算持ち越し【{tier['label']}】｜対象なし（{md}）",
            "description": f"本日の発表予定 {n_scheduled}件 → 売られすぎ条件の該当なし。\n"
                           "（決算シーズン外はゼロが続くのが正常です）"
                           + (f"\n⚠️ {note}" if note else ""),
            "color": 0x95A5A6,
        }
    lines = []
    for i, r in enumerate(picks, 1):
        shares = calc_shares(r["price"], tier["size"])
        amount = shares * r["price"] / 10000
        lb, lt = r.get("last_bucket", "履歴なし"), r.get("last_time")
        if lb == "場中":
            t_note = f"⚠️ 前回発表 {lt}（場中型＝本日すでに発表済みの可能性）"
        elif lt:
            t_note = f"前回発表 {lt}（今夜型）"
        else:
            t_note = "前回発表時刻 不明"
        lines.append(
            f"**{i}. {r['name']}**（{r['code']}）{r['type']}\n"
            f"　現在値 {r['price']:,.0f}円 → **大引け成行 {shares:,}株**（約{amount:,.0f}万円）\n"
            f"　RSI {r['rsi']} / 5日騰落 {r['runup5']:+.1f}% / {t_note}")
    return {
        "title": f"📊 決算持ち越し【{tier['label']}】｜本日の買いリスト {len(picks)}件（{md}）",
        "description": "\n".join(lines) + (f"\n⚠️ {note}" if note else ""),
        "color": 0x3498DB,
        "footer": {"text": "今夜決算発表→明日寄り成行で売り（夜のうちに売り予約推奨）｜"
                           f"{SLOTS}枠×{tier['size'] // 10000}万｜オーバーナイトはSTOP無効・最悪-25%級あり"},
    }


# ================================================================
# main
# ================================================================

def main() -> None:
    force = "--force" in sys.argv
    dry = "--dry" in sys.argv

    if "--test" in sys.argv:
        for tier in TIERS:
            picks = [{"ticker": "0000.T", "code": "0000", "name": "テスト銘柄",
                      "type": "第１四半期", "price": 2340.0, "rsi": 32.5,
                      "runup5": -6.2, "tov20": 2.5e9,
                      "last_time": "15:30", "last_bucket": "引け後"}]
            e = embed_signals(picks, 42, date.today(), tier)
            e["title"] = "🧪【テスト配信】" + e["title"]
            send_discord([e], os.getenv(tier["webhook_env"], ""),
                         tier["label"], dry=dry)
        return

    now = datetime.now(JST)
    today = now.date()
    print(f"[earnings_hold] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")

    if not force:
        if not is_trading_day(today):
            print("[earnings_hold] 休場日 → スキップ")
            return
        hm = now.hour * 60 + now.minute
        if not (14 * 60 + 30 <= hm <= 15 * 60 + 18):
            print(f"[earnings_hold] 時間外({now.strftime('%H:%M')}) → スキップ"
                  "（発注が間に合う14:30-15:18のみ配信）")
            return

    # 二重発火ガード（先頭階層の記録で代表判定）
    head_store = load_positions(TIERS[0]["positions_file"])
    if head_store.get("last_signal_date") == today.strftime("%Y-%m-%d") and not force:
        print("[earnings_hold] 本日配信済み → スキップ（二重発火ガード）")
        return

    # ── 1. JPX予定表更新（失敗しても手元JSONで続行） ──
    note = None
    try:
        import fetch_jpx_earnings_schedule as fjx
        fjx.main()
    except Exception as e:
        print(f"[earnings_hold] JPX更新失敗: {e} → 手元のJSONで続行")
        note = "JPX予定表の更新に失敗（前回取得分で判定）"
    if not os.path.exists(SCHEDULE_FILE):
        print("[earnings_hold] 予定表なし → 中止")
        return
    sched = json.load(open(SCHEDULE_FILE, encoding="utf-8"))
    fetched = sched.get("fetched", "?")
    todays = sched.get("schedule", {}).get(today.strftime("%Y-%m-%d"), [])
    print(f"[earnings_hold] 予定表({fetched}取得) 本日{len(todays)}件")

    # ── 1b. 発表時刻履歴の鮮度維持（表示用・失敗しても続行） ──
    try:
        refresh_earnings_times(today)
    except Exception as e:
        print(f"[earnings_hold] 時刻履歴の更新失敗: {e} → 手元データで続行")
    times = load_times()

    # ── 2. J-Quants履歴（昨日まで・決済記帳と判定に全階層共用） ──
    from screener import _jquants_id_token, batch_download_jquants
    start = (today - timedelta(days=HIST_DAYS)).strftime("%Y-%m-%d")
    end = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    token = _jquants_id_token()
    all_data = batch_download_jquants(token, start=start, end=end)
    print(f"[earnings_hold] J-Quants {start}〜{end}: {len(all_data)}銘柄")

    # ── 3. 候補生成（yfinanceは1回だけ・価格帯カットは階層側） ──
    cands_all = build_candidates(todays, all_data, times)
    print(f"[earnings_hold] 全候補 {len(cands_all)}件（価格帯カット前）")

    # ── 4. 階層ループ: 決済記帳 → 当日シグナル → 配信 → 保存 ──
    for tier in TIERS:
        label = tier["label"]
        store = load_positions(tier["positions_file"])
        embeds = []

        settled = settle_pendings(store, today, all_data, tier["size"])
        if settled:
            embeds.append(embed_results(settled, tier))

        pending_now = {p["ticker"] for p in store["positions"]
                       if p.get("status") == "pending"}
        free_slots = max(0, SLOTS - len(pending_now))
        picks = [c for c in cands_all
                 if c["price"] <= tier["price_cap"]
                 and c["ticker"] not in pending_now][:free_slots]
        print(f"[earnings_hold-{label}] 配信{len(picks)}件（空き枠{free_slots}）")
        embeds.append(embed_signals(picks, len(todays), today, tier, note=note))

        send_discord(embeds, os.getenv(tier["webhook_env"], ""), label, dry=dry)

        if not dry:
            for p in picks:
                store["positions"].append({
                    "ticker": p["ticker"], "name": p["name"], "type": p["type"],
                    "date": today.strftime("%Y-%m-%d"),
                    "signal_price": round(p["price"], 1),
                    "shares": calc_shares(p["price"], tier["size"]),
                    "rsi": p["rsi"], "runup5": p["runup5"],
                    "status": "pending",
                })
            store["last_signal_date"] = today.strftime("%Y-%m-%d")
            save_positions(store, tier["positions_file"])
            print(f"[earnings_hold-{label}] {tier['positions_file']} 保存"
                  f"（pending {len(picks)}件追加）")


if __name__ == "__main__":
    main()

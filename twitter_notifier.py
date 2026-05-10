"""
twitter_notifier.py — X(Twitter) 自動投稿モジュール
=====================================================

スイング・デイトレシグナルをX(Twitter)に自動投稿する。

方針: 銘柄名は出さず「件数・成績のみ」を流す。
銘柄詳細は note メンバーシップ（NOTE_URL）に誘導する。
"""

import os
import json
import tweepy
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

NOTE_URL = os.getenv("NOTE_URL", "").strip()
DISCLAIMER = "※個人の売買記録の共有です。投資は自己責任で。"


def _get_client() -> tweepy.Client | None:
    """Tweepy クライアントを返す。キー未設定時は None。"""
    api_key     = os.getenv("TWITTER_API_KEY", "").strip()
    api_secret  = os.getenv("TWITTER_API_SECRET", "").strip()
    access_token  = os.getenv("TWITTER_ACCESS_TOKEN", "").strip()
    access_secret = os.getenv("TWITTER_ACCESS_SECRET", "").strip()

    if not all([api_key, api_secret, access_token, access_secret]):
        print("[twitter] APIキー未設定 → スキップ")
        return None

    return tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_secret,
    )


def _post(text: str) -> bool:
    """ツイートを投稿する。成功でTrue。"""
    client = _get_client()
    if client is None:
        return False
    try:
        client.create_tweet(text=text)
        # Windows cp932コンソールで絵文字を直接printすると例外で握りつぶされるため安全に出力
        try:
            print(f"[twitter] posted ({len(text)} chars): {text[:30]}...")
        except UnicodeEncodeError:
            print(f"[twitter] posted ({len(text)} chars)")
        return True
    except Exception as e:
        try:
            print(f"[twitter] failed: {e}")
        except UnicodeEncodeError:
            print(f"[twitter] failed (encode error in message)")
        return False


def _cta() -> str:
    """有料配信への誘導文（NOTE_URL 設定時のみ）"""
    if NOTE_URL:
        return f"\n\n📝 銘柄詳細・買い理由\n{NOTE_URL}"
    return ""


def post_swing_signals(
    buy_signals: list[dict],
    today: date,
    macro: dict,
    sell_signals: list[dict] | None = None,
) -> None:
    """
    スイングシグナルの『件数 + チラ見せ1銘柄ずつ』をツイートする。
    チラ見せは screener の勝ちやすさスコア1位（先頭要素）。銘柄名+コードのみ。
    買値・STOP・TP・買い理由は note メンバーシップ限定。
    """
    sell_signals = sell_signals or []
    if not buy_signals and not sell_signals:
        return

    date_str = today.strftime("%m/%d")
    lines = [f"📊【スイング】{date_str} シグナル"]

    if buy_signals:
        lines.append(f"🔴 BUY {len(buy_signals)}件")
    if sell_signals:
        lines.append(f"🔵 SELL {len(sell_signals)}件")

    # チラ見せ：各方向のスコア1位を銘柄名+コードだけ公開
    teaser_lines = []
    if buy_signals:
        top = buy_signals[0]
        code = top["ticker"].replace(".T", "")
        teaser_lines.append(f"  {code} {top['name']}（BUY）")
    if sell_signals:
        top = sell_signals[0]
        code = top["ticker"].replace(".T", "")
        teaser_lines.append(f"  {code} {top['name']}（SELL）")
    if teaser_lines:
        lines.append("")
        lines.append("▶ 本日のチラ見せ（スコア1位）")
        lines.extend(teaser_lines)

    sp  = macro.get("sp500")
    nas = macro.get("nasdaq")
    if sp is not None and nas is not None:
        lines.append("")
        lines.append(f"米市況: S&P500 {sp:+.2f}% / NASDAQ {nas:+.2f}%")

    lines.append("#日本株 #スイングトレード")
    _post("\n".join(lines) + _cta())


def post_day_signals(signals: list[dict], today: date) -> None:
    """デイトレシグナルの『件数のみ』をツイートする。"""
    if not signals:
        return

    date_str = today.strftime("%m/%d")
    buy = sum(1 for s in signals if s["direction"] == "BUY")
    sell = sum(1 for s in signals if s["direction"] == "SELL")

    lines = [f"⚡【デイトレ】{date_str} シグナル"]
    if buy:
        lines.append(f"🔴 BUY {buy}件")
    if sell:
        lines.append(f"🔵 SELL {sell}件")
    lines.append("#日本株 #デイトレ")
    _post("\n".join(lines) + _cta())


def post_swing_results(results: list[dict], today: date) -> None:
    """スイング決済結果（合計のみ）をツイートする。"""
    if not results:
        return

    date_str = today.strftime("%m/%d")
    wins   = sum(1 for r in results if r.get("pnl_pct", 0) > 0)
    total  = len(results)
    total_pnl = sum(r.get("pnl_pct", 0) for r in results)

    lines = [f"📈【スイング決済】{date_str}"]
    lines.append(f"{total}件 ({wins}勝{total - wins}敗)")
    lines.append(f"合計: {total_pnl:+.2f}%")
    lines.append("#日本株 #トレード結果")
    _post("\n".join(lines) + _cta())


def _load_trade_history() -> list[dict]:
    """trade_history.json を読み込む。失敗時は空リスト。"""
    p = Path("trade_history.json")
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[twitter] trade_history.json 読み込み失敗: {e}")
        return []


def post_monthly_summary(today: date) -> None:
    """
    月初（毎月1日）に前月の月次サマリーを投稿する。
    trade_history.json から計算。
    """
    history = _load_trade_history()
    if not history:
        print("[twitter] trade_history.json が空 → 月次投稿スキップ")
        return

    # 前月の年月を算出
    if today.month == 1:
        target_year, target_month = today.year - 1, 12
    else:
        target_year, target_month = today.year, today.month - 1

    target_str = f"{target_year}-{target_month:02d}"
    monthly = [
        h for h in history
        if str(h.get("close_date", "")).startswith(target_str)
    ]
    if not monthly:
        print(f"[twitter] {target_str} の決済データなし → 月次投稿スキップ")
        return

    total = len(monthly)
    wins = sum(1 for h in monthly if h.get("pnl_pct", 0) > 0)
    total_pnl = sum(h.get("pnl_pct", 0) for h in monthly)
    win_rate = wins / total * 100 if total else 0
    # 月利 = 合計% × (100万 / 500万) ※notifier.send_monthly_report と整合（資金500万・MAX5並列）
    monthly_return = total_pnl * (100 / 500)

    lines = [f"📅 {target_year}年{target_month}月 スイング月次レポート"]
    lines.append(f"決済: {total}件 / 勝率: {win_rate:.1f}%")
    lines.append(f"合計損益: {total_pnl:+.2f}%")
    lines.append(f"月利換算: {monthly_return:+.2f}%（資金500万・1件100万・MAX5並列基準）")
    lines.append("#日本株 #スイング #トレード成績")
    _post("\n".join(lines) + _cta())

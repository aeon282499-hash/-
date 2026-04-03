"""
twitter_notifier.py — X(Twitter) 自動投稿モジュール
=====================================================

スイング・デイトレシグナルをX(Twitter)に自動投稿する。
"""

import os
import tweepy
from datetime import date
from dotenv import load_dotenv

load_dotenv()


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
        print(f"[twitter] 投稿完了: {text[:30]}...")
        return True
    except Exception as e:
        print(f"[twitter] 投稿失敗: {e}")
        return False


def post_swing_signals(signals: list[dict], today: date, macro: dict) -> None:
    """スイングシグナルをツイートする。"""
    if not signals:
        return

    date_str = today.strftime("%m/%d")
    lines = [f"📊【スイング】{date_str} シグナル"]

    for sig in signals[:3]:  # 最大3件（文字数制限）
        direction = "🔴買い" if sig["direction"] == "BUY" else "🔵空売り"
        lines.append(f"▶ {sig['name']} {direction}")

    if len(signals) > 3:
        lines.append(f"他{len(signals)-3}件")

    # マクロ
    dow = macro.get("dow")
    nas = macro.get("nasdaq")
    if dow is not None and nas is not None:
        lines.append(f"S&P500 {dow:+.1f}% / QQQ {nas:+.1f}%")

    lines.append("#日本株 #株式投資 #売買シグナル")
    _post("\n".join(lines))


def post_day_signals(signals: list[dict], today: date) -> None:
    """デイトレシグナルをツイートする。"""
    if not signals:
        return

    date_str = today.strftime("%m/%d")
    lines = [f"⚡【デイトレ】{date_str} シグナル"]

    for sig in signals[:3]:
        direction = "🔴買い" if sig["direction"] == "BUY" else "🔵空売り"
        prev = sig.get("prev_return", 0)
        lines.append(f"▶ {sig['name']} {direction}（前日{prev:+.1f}%）")

    if len(signals) > 3:
        lines.append(f"他{len(signals)-3}件")

    lines.append("#日本株 #デイトレ #売買シグナル")
    _post("\n".join(lines))


def post_swing_results(results: list[dict], today: date) -> None:
    """スイング結果をツイートする。"""
    if not results:
        return

    date_str = today.strftime("%m/%d")
    wins   = sum(1 for r in results if r.get("pnl_pct", 0) > 0)
    total  = len(results)
    total_pnl = sum(r.get("pnl_pct", 0) for r in results)

    lines = [f"📈【スイング結果】{date_str}"]
    lines.append(f"決済: {total}件 {wins}勝{total-wins}敗")

    for r in results[:3]:
        pnl = r.get("pnl_pct", 0)
        mark = "✅" if pnl > 0 else "❌"
        lines.append(f"{mark} {r.get('name','')} {pnl:+.2f}%")

    lines.append(f"合計: {total_pnl:+.2f}%")
    lines.append("#日本株 #株式投資")
    _post("\n".join(lines))

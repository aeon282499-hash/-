"""
test_twitter.py — X(Twitter) 投稿テストスクリプト

使い方:
    python test_twitter.py            # 各種ダミーデータで投稿テスト
    python test_twitter.py --signals  # シグナル投稿のみテスト
    python test_twitter.py --results  # 決済結果投稿のみテスト
    python test_twitter.py --monthly  # 月次サマリー投稿のみテスト

事前に .env または環境変数で以下を設定:
    TWITTER_API_KEY
    TWITTER_API_SECRET
    TWITTER_ACCESS_TOKEN
    TWITTER_ACCESS_SECRET
    NOTE_URL  (任意・空ならCTA行は出ない)
"""

import sys
from datetime import date

from twitter_notifier import (
    post_swing_signals,
    post_swing_results,
    post_monthly_summary,
)


def test_signals():
    dummy_buy = [
        {"ticker": "7203.T", "name": "トヨタ", "direction": "BUY"},
        {"ticker": "9984.T", "name": "SBG", "direction": "BUY"},
        {"ticker": "6758.T", "name": "ソニー", "direction": "BUY"},
    ]
    dummy_sell = [
        {"ticker": "9433.T", "name": "KDDI", "direction": "SELL"},
    ]
    macro = {"sp500": 0.32, "nasdaq": -0.18}
    post_swing_signals(dummy_buy, date.today(), macro, sell_signals=dummy_sell)


def test_results():
    dummy_results = [
        {"name": "A社", "pnl_pct": 4.8},
        {"name": "B社", "pnl_pct": -2.9},
        {"name": "C社", "pnl_pct": 1.2},
    ]
    post_swing_results(dummy_results, date.today())


def test_monthly():
    post_monthly_summary(date.today())


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--signals" in args:
        test_signals()
    elif "--results" in args:
        test_results()
    elif "--monthly" in args:
        test_monthly()
    else:
        print(">> 1) シグナル投稿テスト")
        test_signals()
        print(">> 2) 決済結果投稿テスト")
        test_results()
        print(">> 3) 月次サマリー投稿テスト")
        test_monthly()

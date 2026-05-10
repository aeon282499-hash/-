"""
test_discord_public.py — 公開Discord（株AI スイングシグナル）への並行送信テスト

使い方:
    python -X utf8 test_discord_public.py            # 全種類テスト
    python -X utf8 test_discord_public.py --buy      # buy のみ
    python -X utf8 test_discord_public.py --sell     # sell のみ
    python -X utf8 test_discord_public.py --close    # close のみ
    python -X utf8 test_discord_public.py --monthly  # monthly のみ

各チャンネルにテスト embed が届くか確認する。
"""

import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()  # .env を読み込んでから notifier をインポート

import notifier
import importlib
importlib.reload(notifier)  # PUBLIC_* 定数が .env 後に再評価されるように


def test_buy():
    """買いシグナルテスト → #buy-signals"""
    dummy = [
        {
            "ticker": "7203.T", "name": "トヨタ自動車", "direction": "BUY",
            "prev_close": 2500, "rsi": 38.2, "deviation": -2.5, "vol_ratio": 2.3, "turnover": 35e8,
        },
        {
            "ticker": "9984.T", "name": "ソフトバンクグループ", "direction": "BUY",
            "prev_close": 8500, "rsi": 41.0, "deviation": -1.8, "vol_ratio": 1.5, "turnover": 50e8,
        },
    ]
    notifier.send_signals(dummy, date.today(), {"sp500": 0.32, "nasdaq": -0.18, "bias": "neutral"})


def test_sell():
    """空売りシグナルテスト → #sell-signals"""
    dummy = [
        {
            "ticker": "9433.T", "name": "KDDI", "direction": "SELL",
            "prev_close": 4500, "rsi": 65.0, "deviation": 4.5, "day_change": 3.5,
            "vol_ratio": 2.0, "turnover": 28e8,
        },
    ]
    notifier.send_sell_signals(dummy, date.today())


def test_close():
    """大引け処分通知テスト → #close-signals"""
    dummy = [
        {
            "ticker": "6758.T", "name": "ソニーグループ", "direction": "BUY",
            "reason_type": "RSI", "reason": "RSI回復", "today_hold": 2,
            "rsi_now": 52.3, "current_price": 13800, "entry_open": 13500,
        },
        {
            "ticker": "7741.T", "name": "HOYA", "direction": "BUY",
            "reason_type": "MAXHOLD", "reason": "3日目強制処分", "today_hold": 3,
            "rsi_now": 48.0, "current_price": 27500, "entry_open": 27830,
        },
    ]
    notifier.send_close_signals(dummy, date.today())


def test_monthly():
    """月次レポートテスト → #monthly-report"""
    dummy_positions = [
        {"status": "closed", "pnl_pct": 4.8, "exit_date": "2026-04-15", "name": "A", "ticker": "0001"},
        {"status": "closed", "pnl_pct": -2.9, "exit_date": "2026-04-20", "name": "B", "ticker": "0002"},
        {"status": "closed", "pnl_pct": 1.2, "exit_date": "2026-04-25", "name": "C", "ticker": "0003"},
        {"status": "closed", "pnl_pct": 3.5, "exit_date": "2026-05-02", "name": "D", "ticker": "0004"},
        {"status": "closed", "pnl_pct": -1.0, "exit_date": "2026-05-08", "name": "E", "ticker": "0005"},
    ]
    notifier.send_monthly_report(dummy_positions, date.today())


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--buy" in args:
        test_buy()
    elif "--sell" in args:
        test_sell()
    elif "--close" in args:
        test_close()
    elif "--monthly" in args:
        test_monthly()
    else:
        print(">> 1) 買いシグナル")
        test_buy()
        print(">> 2) 空売りシグナル")
        test_sell()
        print(">> 3) 大引け処分")
        test_close()
        print(">> 4) 月次レポート")
        test_monthly()

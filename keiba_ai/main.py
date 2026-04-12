#!/usr/bin/env python3
"""
競馬AI メインCLI

使い方:
  # データ収集
  python -m keiba_ai.main collect --start 2024-01-01 --end 2024-12-31

  # モデル学習
  python -m keiba_ai.main train --data keiba_ai/data/races.csv

  # バックテスト
  python -m keiba_ai.main backtest --data keiba_ai/data/races.csv --model keiba_ai/models/model.pkl

  # 当日予測
  python -m keiba_ai.main predict --date 2026-04-12
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("keiba_ai/logs/keiba_ai.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def cmd_collect(args):
    """データ収集コマンド。"""
    from keiba_ai.scraper import collect_race_results
    from keiba_ai.config import DATA_DIR

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    save_path = Path(args.output) if args.output else DATA_DIR / "races.csv"

    logger.info(f"データ収集: {start} ～ {end} → {save_path}")
    df = collect_race_results(
        start_date=start,
        end_date=end,
        save_path=save_path,
        jra_only=not args.include_nar,
    )
    logger.info(f"収集完了: {len(df)} 行")


def cmd_train(args):
    """モデル学習コマンド。"""
    from keiba_ai.model import train_from_csv
    from keiba_ai.config import MODEL_DIR

    model_output = args.model or str(MODEL_DIR / "model.pkl")
    logger.info(f"学習開始: {args.data} → {model_output}")
    model = train_from_csv(
        csv_path=args.data,
        model_output=model_output,
        history_dir=args.history_dir,
    )
    logger.info("学習完了")


def cmd_backtest(args):
    """バックテストコマンド。"""
    import pandas as pd
    from keiba_ai.model import KeibaPredictor
    from keiba_ai.betting import KellyBettingStrategy
    from keiba_ai.backtester import Backtester, plot_bankroll_curve
    from keiba_ai.config import INITIAL_BANKROLL, BET_TYPE

    logger.info(f"バックテスト: {args.data}")
    df = pd.read_csv(args.data, encoding="utf-8-sig")

    model = KeibaPredictor.load(args.model)
    strategy = KellyBettingStrategy(
        kelly_fraction=float(args.kelly_fraction),
        min_ev=float(args.min_ev),
    )
    bankroll = float(args.bankroll or INITIAL_BANKROLL)

    bt = Backtester(model=model, strategy=strategy, initial_bankroll=bankroll, bet_type=BET_TYPE)
    result = bt.run(
        race_df=df,
        start_date=args.start,
        end_date=args.end,
    )

    result.print_summary()

    # 保存
    from keiba_ai.config import DATA_DIR
    result.save_csv(DATA_DIR / "bet_history.csv")
    result.save_monthly_report(DATA_DIR / "monthly_report.csv")

    if args.plot:
        plot_bankroll_curve(result, save_path=DATA_DIR / "bankroll.png")


def cmd_predict(args):
    """当日予測コマンド。"""
    from keiba_ai.runner import run_today
    from keiba_ai.config import INITIAL_BANKROLL, BET_TYPE, MODEL_DIR

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    model_path = args.model or MODEL_DIR / "model.pkl"
    bankroll = float(args.bankroll or INITIAL_BANKROLL)

    run_today(
        target_date=target_date,
        model_path=model_path,
        bankroll=bankroll,
        bet_type=args.bet_type or BET_TYPE,
        jra_only=not args.include_nar,
        output_json=args.output,
    )


def main():
    parser = argparse.ArgumentParser(
        description="競馬AI - Kelly基準ベッティング",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # collect
    p_collect = sub.add_parser("collect", help="レースデータを収集する")
    p_collect.add_argument("--start", required=True, help="開始日 YYYY-MM-DD")
    p_collect.add_argument("--end", required=True, help="終了日 YYYY-MM-DD")
    p_collect.add_argument("--output", help="保存先CSVパス")
    p_collect.add_argument("--include-nar", action="store_true", help="地方競馬も含める")

    # train
    p_train = sub.add_parser("train", help="モデルを学習する")
    p_train.add_argument("--data", required=True, help="学習データCSVパス")
    p_train.add_argument("--model", help="モデル保存先")
    p_train.add_argument("--history-dir", help="馬の過去成績ディレクトリ")

    # backtest
    p_bt = sub.add_parser("backtest", help="バックテストを実行する")
    p_bt.add_argument("--data", required=True, help="レースデータCSVパス")
    p_bt.add_argument("--model", required=True, help="学習済みモデルパス")
    p_bt.add_argument("--start", help="開始日 YYYY-MM-DD")
    p_bt.add_argument("--end", help="終了日 YYYY-MM-DD")
    p_bt.add_argument("--bankroll", type=float, help="初期資金 (円)")
    p_bt.add_argument("--kelly-fraction", default=0.25, type=float, help="Kelly分数 (default: 0.25)")
    p_bt.add_argument("--min-ev", default=1.10, type=float, help="最低期待値 (default: 1.10)")
    p_bt.add_argument("--plot", action="store_true", help="資金推移グラフを保存")

    # predict
    p_pred = sub.add_parser("predict", help="当日の予測を実行する")
    p_pred.add_argument("--date", help="予測日 YYYY-MM-DD (default: 今日)")
    p_pred.add_argument("--model", help="学習済みモデルパス")
    p_pred.add_argument("--bankroll", type=float, help="現在の資金 (円)")
    p_pred.add_argument("--bet-type", choices=["win", "place"], help="賭け式")
    p_pred.add_argument("--include-nar", action="store_true", help="地方競馬も含める")
    p_pred.add_argument("--output", help="結果をJSONに保存")

    args = parser.parse_args()

    dispatch = {
        "collect": cmd_collect,
        "train": cmd_train,
        "backtest": cmd_backtest,
        "predict": cmd_predict,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()

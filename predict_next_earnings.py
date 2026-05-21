"""predict_next_earnings.py
earnings_calendar.json の過去 DiscDate から、次の2回分の決算予想日を推定し、
earnings_calendar.json を上書き更新する。

使い方:
  python predict_next_earnings.py

挙動:
  - 各銘柄の過去 DiscDate を年間隔と四半期間隔で解析
  - 直近4回の平均間隔（だいたい90日前後）を使って次の2回分を推定
  - JSON は「過去実績 + 予測」を統合した形で保存（区別なし・本番で同じ扱い）
  - 推定日が今日から180日以上先のものは含めない（精度低下回避）
"""
import json
import os
from datetime import datetime, date, timedelta

PATH = "earnings_calendar.json"
HORIZON_DAYS = 180  # 今日から180日先までの予測のみ含める
MIN_HISTORY = 4  # 過去4回以上のDiscDateがある銘柄のみ予測


def predict_next_disc_dates(past_dates: list[str], max_predictions: int = 2) -> list[str]:
    if len(past_dates) < MIN_HISTORY:
        return []
    try:
        sorted_dates = sorted(
            datetime.strptime(d, "%Y-%m-%d").date() for d in past_dates
        )
    except Exception:
        return []
    # 直近4回の間隔を平均
    recent = sorted_dates[-MIN_HISTORY:]
    intervals = [(recent[i + 1] - recent[i]).days for i in range(len(recent) - 1)]
    if not intervals:
        return []
    avg_interval = sum(intervals) / len(intervals)
    # 異常値除外（30日未満 or 180日超は平均歪んでる）
    if not (60 <= avg_interval <= 130):
        return []
    last = sorted_dates[-1]
    today = date.today()
    predicted: list[str] = []
    for n in range(1, max_predictions + 1):
        nxt = last + timedelta(days=int(round(avg_interval * n)))
        # 今日から180日以内のみ
        if today <= nxt <= today + timedelta(days=HORIZON_DAYS):
            predicted.append(nxt.strftime("%Y-%m-%d"))
        elif nxt > today + timedelta(days=HORIZON_DAYS):
            break
    return predicted


def main() -> None:
    if not os.path.exists(PATH):
        raise SystemExit(f"{PATH} が見つかりません")

    with open(PATH, "r", encoding="utf-8") as f:
        cal = json.load(f)

    total_added = 0
    tickers_with_predict = 0
    for ticker, dates in cal.items():
        preds = predict_next_disc_dates(list(dates), max_predictions=2)
        if preds:
            tickers_with_predict += 1
            for p in preds:
                if p not in dates:
                    dates.append(p)
                    total_added += 1
            cal[ticker] = sorted(set(dates))

    with open(PATH, "w", encoding="utf-8") as f:
        json.dump(cal, f, ensure_ascii=False, indent=2)

    print(f"予測決算日を追加: {total_added}件 / {tickers_with_predict}銘柄")
    print(f"earnings_calendar.json 更新完了（総銘柄: {len(cal)}）")


if __name__ == "__main__":
    main()

"""build_earnings_calendar.py
J-Quants V2 /fins/summary から指定期間の決算開示日を取得して JSON 保存。

使い方:
  python build_earnings_calendar.py 2022-01-01 2026-05-20

出力ファイル: earnings_calendar.json
  形式: { "1234.T": ["2022-05-15", "2022-08-15", ...], ... }
"""
import ssl, urllib3
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings()

import json
import os
import sys
import time
import requests
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()
import jpholiday

API_KEY = os.getenv("JQUANTS_API_KEY", "").strip()
BASE = "https://api.jquants.com/v2"


def fetch_summary_for_date(date_str: str) -> list[dict]:
    items: list[dict] = []
    pagination_key = None
    while True:
        params: dict = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key
        try:
            r = requests.get(
                f"{BASE}/fins/summary",
                headers={"x-api-key": API_KEY},
                params=params,
                timeout=60,
                verify=False,
            )
        except Exception as e:
            print(f"  [{date_str}] ERR {e}")
            return items
        if r.status_code == 429:
            print(f"  [{date_str}] 429 → 60秒待機")
            time.sleep(60)
            continue
        if r.status_code != 200:
            return items
        data = r.json()
        items.extend(data.get("data", []))
        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break
        time.sleep(1.1)
    return items


def main():
    start = sys.argv[1] if len(sys.argv) > 1 else "2022-01-01"
    end = sys.argv[2] if len(sys.argv) > 2 else (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    output = "earnings_calendar.json"

    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date()
    trading_days: list[str] = []
    while cur <= end_d:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            trading_days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    print(f"対象営業日: {len(trading_days)}日 ({start} ~ {end})")
    print(f"想定時間: 約{len(trading_days) * 1.2 / 60:.0f}分（Light 60req/min基準）")

    earnings_dates: dict[str, list[str]] = {}
    total = 0
    t0 = time.time()
    for i, d in enumerate(trading_days):
        items = fetch_summary_for_date(d)
        for item in items:
            code = str(item.get("Code", ""))[:4]
            disc = item.get("DiscDate", "")
            if code and disc:
                tk = code + ".T"
                earnings_dates.setdefault(tk, []).append(disc)
                total += 1
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (len(trading_days) - i - 1) / 60
            print(f"  {i+1}/{len(trading_days)} 完了 / 累計{total}レコード / "
                  f"銘柄{len(earnings_dates)}件 / ETA約{eta:.0f}分")
        time.sleep(1.1)

    for k in earnings_dates:
        earnings_dates[k] = sorted(set(earnings_dates[k]))

    with open(output, "w", encoding="utf-8") as f:
        json.dump(earnings_dates, f, ensure_ascii=False, indent=2)

    print(f"\n保存完了: {output}")
    print(f"  銘柄数: {len(earnings_dates)}")
    print(f"  総決算レコード数: {total}")
    print(f"  経過時間: {(time.time() - t0) / 60:.1f}分")


if __name__ == "__main__":
    main()

"""
sector_filter.py — 東証33業種セクター流入フィルタ

Phase 1 セクター絞り込み用モジュール。
- ticker → S33Nm 対応を /equities/master から取得（JSONキャッシュ）
- all_data から各日のセクター別平均日次リターン → 5日移動平均でランキング
- 各日「上位 1/3 セクター集合」を返す

look-ahead防止: ある trade_date の判定は trade_date より前 (前営業日まで) のリターンで行う。
"""
import json
import os
import time
from pathlib import Path

import warnings

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")
load_dotenv()

JQ_BASE = "https://api.jquants.com/v2"
SECTOR_CACHE = Path("sector33_map.json")


def _api_key() -> str:
    k = (os.getenv("JQUANTS_API_KEY", "") or os.getenv("JQUANTS_REFRESH_TOKEN", "")).strip()
    if not k:
        raise ValueError("JQUANTS_API_KEY 未設定")
    return k


def fetch_sector33_map(force_refresh: bool = False) -> dict[str, str]:
    """ticker (例 '7203.T') -> S33Nm。プライム/スタンダード/グロースのみ。"""
    if SECTOR_CACHE.exists() and not force_refresh:
        with open(SECTOR_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    print("[sector_filter] /equities/master 取得中...")
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{JQ_BASE}/equities/master",
                headers={"x-api-key": _api_key()},
                timeout=60,
                verify=False,
            )
            if resp.status_code == 429:
                print("  [sector_filter] 429 → 60秒待機")
                time.sleep(60)
                continue
            resp.raise_for_status()
            items = resp.json().get("data", [])
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f"  [sector_filter] retry {attempt+1}: {e}")
            time.sleep(10)
    target = {"プライム", "スタンダード", "グロース"}
    out: dict[str, str] = {}
    for it in items:
        if it.get("MktNm") not in target:
            continue
        code = str(it.get("Code", ""))[:4]
        s33 = (it.get("S33Nm") or "").strip()
        if code and s33:
            out[code + ".T"] = s33
    with open(SECTOR_CACHE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[sector_filter] {len(out)} 銘柄 → {SECTOR_CACHE}")
    return out


def build_sector_ranking(
    all_data: dict[str, pd.DataFrame],
    sector_map: dict[str, str],
    window: int = 5,
    top_frac: float = 1 / 3,
) -> tuple[dict[str, set[str]], pd.DataFrame]:
    """
    各日の「上位 top_frac セクター集合」を構築する。

    Returns:
      ranking: {date_str 'YYYY-MM-DD' -> set of S33Nm}
      sector_window_df: 参考用 (rows=date, cols=S33Nm, values=5日平均日次リターン)
    """
    print(f"[sector_filter] 銘柄リターン集計中 ({len(all_data)} 銘柄, 窓={window}日)...")

    # 銘柄ごとに sector を付与して daily return 計算
    blocks = []
    for ticker, df in all_data.items():
        sector = sector_map.get(ticker)
        if not sector or df is None or len(df) < window + 2:
            continue
        ret = df["Close"].pct_change()
        sub = pd.DataFrame({"ret": ret})
        sub["sector"] = sector
        blocks.append(sub)
    if not blocks:
        raise RuntimeError("セクター付き銘柄データが0件です")
    long_df = pd.concat(blocks, axis=0).dropna(subset=["ret"])
    long_df = long_df.reset_index().rename(columns={"index": "date", "Date": "date"})
    if "date" not in long_df.columns:
        long_df = long_df.rename(columns={long_df.columns[0]: "date"})

    # date × sector のピボット → 各セルは銘柄平均日次リターン
    daily = long_df.groupby(["date", "sector"])["ret"].mean().unstack(fill_value=0.0)
    daily = daily.sort_index()

    # 5日移動平均
    sw = daily.rolling(window=window, min_periods=window).mean()

    n_sectors = sw.shape[1]
    top_n = max(1, int(round(n_sectors * top_frac)))
    print(f"[sector_filter] セクター数={n_sectors} / 上位{top_n}を採用 (top_frac={top_frac:.3f})")

    ranking: dict[str, set[str]] = {}
    for d, row in sw.iterrows():
        valid = row.dropna()
        if valid.empty:
            continue
        top = set(valid.sort_values(ascending=False).head(top_n).index)
        ranking[pd.Timestamp(d).strftime("%Y-%m-%d")] = top
    print(f"[sector_filter] ranking 構築完了 ({len(ranking)} 日分)")
    return ranking, sw


def is_ticker_in_top_sector(
    ticker: str,
    trade_date: str,
    sector_map: dict[str, str],
    ranking: dict[str, set[str]],
    all_trading_days: list[str],
) -> bool:
    """
    trade_date の前営業日時点のランキングで、ticker のセクターが上位に入っているか。
    セクター不明 or ランキング欠損 → False（排除）
    """
    sector = sector_map.get(ticker)
    if not sector:
        return False
    try:
        idx = all_trading_days.index(trade_date)
    except ValueError:
        return False
    if idx == 0:
        return False
    prev_day = all_trading_days[idx - 1]
    top = ranking.get(prev_day)
    if top is None:
        # フォールバック: 直近で利用可能な日
        prev_avail = [d for d in ranking.keys() if d <= prev_day]
        if not prev_avail:
            return False
        top = ranking[max(prev_avail)]
    return sector in top


if __name__ == "__main__":
    smap = fetch_sector33_map()
    sectors_count: dict[str, int] = {}
    for s in smap.values():
        sectors_count[s] = sectors_count.get(s, 0) + 1
    print(f"\nセクター内訳 ({len(sectors_count)} 種):")
    for s, n in sorted(sectors_count.items(), key=lambda x: -x[1]):
        print(f"  {s:<20} {n:>4} 銘柄")

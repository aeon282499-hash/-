"""
screener_sector_theme.py — セクター×テーマ シグナルスクリーナー

既存 screener.judge_signal_pre による BUY 判定をベースに、
「セクター上位 (S33・20日平均・上位50%) OR テーマ語ヒット銘柄」フィルタを乗せる。

Phase 1-3 BT 結論 (2022-01-01 〜 2026-05-02):
  OFF        : PF1.25 / +654% / MaxDD-68.0%
  Sec_OR_Theme: PF1.31 / +459% / MaxDD-50.5%  ← 採用
  2025年: PF1.37 / 2026年: PF1.48 (素のPF1.33/0.99 → 直近劇的改善)

SELLは対象外 (BTで Sec_OR_Theme を BUY のみで検証したため)。
"""
import json
import math
import time
import warnings
from datetime import date as _date, datetime, timedelta
from pathlib import Path

import jpholiday
import pandas as pd
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")
load_dotenv()

from screener import (
    judge_signal_pre, judge_sell_signal_pre, fetch_tse_universe, batch_download_jquants,
    _jquants_id_token, _load_earnings_calendar, _is_near_earnings,
    fetch_macro, LOOKBACK_DAYS, MAX_SIGNALS,
)
from sector_filter import (
    fetch_sector33_map, build_sector_ranking, is_ticker_in_top_sector,
)

# Phase 1-3 BT で確定したパラメータ (BUY)
SECTOR_WINDOW = 20
SECTOR_TOP_FRAC = 0.50

# SELL (空売り) BT で確定したパラメータ
#   bt_sector_sell_refine.py: 最弱セクター下位33%・window20 で
#   PF1.37 / +45.9% / MaxDD-13.8% / 全5年プラス / 最弱年PF1.03 (素のSELLはPF0.98で負け)
SECTOR_SELL_WINDOW = 20
SECTOR_SELL_TOP_FRAC = 0.33

THEME_KEYWORDS_PATH = Path("theme_keywords.json")


def load_theme_universe(name_map: dict[str, str]) -> set[str]:
    """name_map (ticker -> name) からテーマ語ヒット銘柄を抽出。"""
    with open(THEME_KEYWORDS_PATH, "r", encoding="utf-8") as f:
        themes = json.load(f)["themes"]
    keywords: list[str] = []
    for kw_list in themes.values():
        keywords.extend(kw_list)
    keywords = list({k for k in keywords if len(k) >= 2})
    out: set[str] = set()
    for ticker, name in name_map.items():
        for kw in keywords:
            if kw in name:
                out.add(ticker)
                break
    return out


def _build_live_sector_ranking(
    all_data: dict[str, pd.DataFrame],
    sector_map: dict[str, str],
) -> tuple[dict[str, set[str]], list[str]]:
    """ライブ用ランキング構築。全all_dataから ranking と trading days を返す。"""
    ranking, _ = build_sector_ranking(
        all_data, sector_map, window=SECTOR_WINDOW, top_frac=SECTOR_TOP_FRAC
    )
    all_trading_days = sorted({
        d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")
    })
    return ranking, all_trading_days


def _winprob_score(c: dict) -> float:
    rsi = c["rsi"]; dev = c["deviation"]; turn = c["turnover"]
    rsi_score = 1.0 / (1.0 + ((rsi - 38.0) / 8.0) ** 2)
    dev_score = 1.0 / (1.0 + ((dev + 3.0) / 2.0) ** 2)
    turn_score = math.log10(max(turn, 1) / 1e9 + 1.0) / 3.0
    return rsi_score * 0.30 + dev_score * 0.30 + turn_score * 0.40


def run_sector_theme_screener() -> tuple[list[dict], list[dict], list[dict], dict, dict]:
    """
    本スクリーニング。

    戻り値:
      signals       — BUY: Sec_OR_Theme 通過後の top MAX_SIGNALS (positions除外後)
      sell_signals  — SELL: 最弱セクター(下位33%)の急騰後反落 top MAX_SIGNALS (positions除外後)
      all_filtered  — BUY の Sec_OR_Theme 通過した全候補 (スコア順)
      macro         — マクロ環境情報
      diag          — 診断情報 (フィルタヒット内訳)
    """
    macro = fetch_macro()
    universe = fetch_tse_universe()
    name_map = {t: n for t, n in universe}
    print(f"[screener_st] ユニバース: {len(name_map)} 銘柄")

    sector_map = fetch_sector33_map()
    theme_universe = load_theme_universe(name_map)
    print(f"[screener_st] テーマ語ヒット銘柄: {len(theme_universe)}")

    token = _jquants_id_token()
    # LOOKBACK_DAYS は 30〜60 営業日くらいの想定。Sectorランキングは20営業日窓必要なので余裕あり
    data = batch_download_jquants(token, lookback_trading_days=LOOKBACK_DAYS)
    if not data:
        print("[screener_st] J-Quants データ取得失敗")
        return [], [], macro, {}

    sector_ranking, all_trading_days = _build_live_sector_ranking(data, sector_map)

    # SELL用: 最弱セクター下位33%ランキング (空売り = 資金流出セクターの急騰を売る)
    sell_sector_ranking, _ = build_sector_ranking(
        data, sector_map, window=SECTOR_SELL_WINDOW,
        top_frac=SECTOR_SELL_TOP_FRAC, end="bottom",
    )

    # 日経 25MA 状態 (参考表示用)
    nk_above_ma25 = None
    try:
        nk_df = data.get("1321.T")
        if nk_df is not None and len(nk_df) >= 25:
            nk_close = float(nk_df["Close"].iloc[-1])
            nk_ma25 = float(nk_df["Close"].rolling(25).mean().iloc[-1])
            nk_above_ma25 = (nk_close >= nk_ma25)
            print(f"[screener_st] 日経1321.T: {nk_close:,.0f}円 / 25MA: {nk_ma25:,.0f}円 → "
                  f"{'25MA以上↑' if nk_above_ma25 else '25MA以下↓'}")
    except Exception as e:
        print(f"[screener_st] 日経判定失敗: {e}")

    _load_earnings_calendar()
    today_str = _date.today().strftime("%Y-%m-%d")

    # 判定 + Sec_OR_Theme フィルタ
    raw_buy: list[dict] = []
    raw_sell: list[dict] = []
    diag = {"raw_buy": 0, "earnings_skip": 0, "sector_pass": 0, "theme_pass": 0,
            "or_pass": 0, "both_pass": 0,
            "raw_sell": 0, "sell_earnings_skip": 0, "sell_sector_pass": 0}

    for ticker, df in data.items():
        if ticker not in name_map:
            continue
        result = judge_signal_pre(ticker, name_map[ticker], df)
        if not result:
            continue
        diag["raw_buy"] += 1
        if _is_near_earnings(ticker, today_str):
            diag["earnings_skip"] += 1
            continue
        # Sec_OR_Theme: sector top OR theme universe
        in_sector = is_ticker_in_top_sector(
            ticker, today_str, sector_map, sector_ranking, all_trading_days
        )
        in_theme = ticker in theme_universe
        if in_sector: diag["sector_pass"] += 1
        if in_theme:  diag["theme_pass"] += 1
        if not (in_sector or in_theme):
            continue
        diag["or_pass"] += 1
        if in_sector and in_theme:
            diag["both_pass"] += 1

        # 候補情報にフィルタ通過状況を添える
        result["sector"] = sector_map.get(ticker, "")
        result["in_sector_top"] = in_sector
        result["in_theme"] = in_theme
        raw_buy.append(result)

    raw_buy.sort(key=_winprob_score, reverse=True)

    print(f"[screener_st] 診断: 素のBUY={diag['raw_buy']} / 決算除外={diag['earnings_skip']} / "
          f"sector通過={diag['sector_pass']} / theme通過={diag['theme_pass']} / "
          f"OR通過(最終)={diag['or_pass']} / 両方={diag['both_pass']}")

    # SELL: 急騰後反落を「最弱セクター(下位33%)」のみで空売り
    for ticker, df in data.items():
        if ticker not in name_map:
            continue
        sresult = judge_sell_signal_pre(ticker, name_map[ticker], df)
        if not sresult:
            continue
        diag["raw_sell"] += 1
        if _is_near_earnings(ticker, today_str):
            diag["sell_earnings_skip"] += 1
            continue
        in_bottom = is_ticker_in_top_sector(
            ticker, today_str, sector_map, sell_sector_ranking, all_trading_days
        )
        if not in_bottom:
            continue
        diag["sell_sector_pass"] += 1
        sresult["sector"] = sector_map.get(ticker, "")
        sresult["in_sector_bottom"] = True
        raw_sell.append(sresult)

    # 空売りは BT 同様、売買代金(流動性)上位から採用
    raw_sell.sort(key=lambda c: c.get("turnover", 0), reverse=True)
    print(f"[screener_st] SELL診断: 素のSELL={diag['raw_sell']} / 決算除外={diag['sell_earnings_skip']} / "
          f"最弱セクター通過(最終)={diag['sell_sector_pass']}")

    # positions_sector_theme.json で既存ポジ除外 (BUY/SELL 共通)
    open_tickers = set()
    try:
        with open("positions_sector_theme.json", "r", encoding="utf-8") as f:
            ps = json.load(f)
        open_tickers = {p["ticker"] for p in ps if p.get("status") in ("pending", "open")}
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[screener_st] positions 読込警告: {e}")

    candidates = [c for c in raw_buy if c["ticker"] not in open_tickers]
    signals = candidates[:MAX_SIGNALS]
    print(f"[screener_st] BUY: OR通過 {len(raw_buy)} → ポジション除外後 {len(candidates)} "
          f"→ top{len(signals)} シグナル")

    sell_candidates = [c for c in raw_sell if c["ticker"] not in open_tickers]
    sell_signals = sell_candidates[:MAX_SIGNALS]
    print(f"[screener_st] SELL: 最弱通過 {len(raw_sell)} → ポジション除外後 {len(sell_candidates)} "
          f"→ top{len(sell_signals)} シグナル")

    return signals, sell_signals, raw_buy, macro, diag


if __name__ == "__main__":
    sigs, sell_sigs, all_pass, macro, diag = run_sector_theme_screener()
    print()
    print("=" * 72)
    print("  本日のBUYシグナル (sector_theme)")
    print("=" * 72)
    for i, s in enumerate(sigs, 1):
        flags = []
        if s.get("in_sector_top"): flags.append(f"sec[{s.get('sector','?')}]")
        if s.get("in_theme"): flags.append("THEME")
        print(f"  {i}. [{s['ticker']}] {s['name']} "
              f"RSI={s['rsi']} dev={s['deviation']:+.1f}% "
              f"代金={s['turnover']/1e8:.0f}億 / {'+'.join(flags)}")
    print("=" * 72)
    print("  本日のSELLシグナル (最弱セクター急騰後反落)")
    print("=" * 72)
    for i, s in enumerate(sell_sigs, 1):
        print(f"  {i}. [{s['ticker']}] {s['name']} "
              f"RSI={s['rsi']} dev={s['deviation']:+.1f}% 前日比={s.get('day_change',0):+.1f}% "
              f"代金={s['turnover']/1e8:.0f}億 / sec[{s.get('sector','?')}]")
    print("=" * 72)

"""
build_dashboard.py — ダッシュボード用スナップショット書き出し

J-Quants を1回だけ取得して以下を一括算出し、dashboard_data.json に保存する:
  - テーマ熱ランキング(theme_tracker)
  - 米株前夜の追い風(us_overnight)
  - 銘柄 S/A/B/C スコア(ranker)

cron(GitHub Actions)で日次生成して commit → Streamlit ダッシュボードはこの JSON を
読むだけ(ロード毎に J-Quants を叩かない)。

実行: python build_dashboard.py
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from dotenv import load_dotenv

load_dotenv()

from theme_tracker import run_theme_tracker, load_theme_members
from us_overnight import tailwind_by_theme
from ranker import rank_stocks, tier_summary

OUT_PATH = Path("dashboard_data.json")


def _theme_public(tr: dict, us_tw: float | None) -> dict:
    """テーマ行から members(重い)を落とし、米前夜追い風を付ける。"""
    return {
        "theme": tr["theme"],
        "heat": tr["heat"],
        "n": tr["n"],
        "n_total": tr.get("n_total"),
        "avg_r1": tr["avg_r1"],
        "avg_r5": tr["avg_r5"],
        "avg_r20": tr["avg_r20"],
        "pct_above_ma25": tr["pct_above_ma25"],
        "breakout": tr["breakout"],
        "us_drivers": tr.get("us_drivers", []),
        "policy": tr.get("policy", ""),
        "us_tailwind": us_tw,
    }


def build() -> dict:
    print("[build] テーマトラッカー実行(J-Quants取得)...")
    ranked, _hot = run_theme_tracker()
    if not ranked:
        raise RuntimeError("テーマランキング0件: J-Quants取得失敗の可能性")

    print("[build] 米株前夜レイヤー取得(yfinance)...")
    themes = load_theme_members()
    tw, driver_returns = tailwind_by_theme(themes)

    print("[build] 銘柄 S/A/B/C スコアリング...")
    stocks = rank_stocks(ranked, tw)
    summ = tier_summary(stocks)

    from screener import _today_jst, _JST   # JST基準（UTCランナーの朝は1日古い表示になる）
    snapshot = {
        "generated_at": datetime.now(_JST).strftime("%Y-%m-%d %H:%M:%S"),
        "date": _today_jst().strftime("%Y-%m-%d"),
        "tier_summary": summ,
        "driver_returns": driver_returns,
        "themes": [_theme_public(tr, tw.get(tr["theme"])) for tr in ranked],
        "stocks": stocks,
    }
    return snapshot


def main():
    snap = build()
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

    s = snap["tier_summary"]
    print(f"\n[build] {OUT_PATH} 保存完了")
    print(f"[build] {snap['date']}  銘柄 {len(snap['stocks'])} 件  "
          f"S={s['S']} A={s['A']} B={s['B']} C={s['C']}  "
          f"テーマ {len(snap['themes'])} 件")
    # 上位プレビュー
    print("\n[build] 上位10銘柄プレビュー:")
    for r in snap["stocks"][:10]:
        drv = "/".join(r["us_drivers"][:2]) if r["us_drivers"] else "国内発"
        print(f"  [{r['tier']}] {r['score']:5.1f}  [{r['ticker']}] {r['name']}  "
              f"〔{r['theme']} heat{r['theme_heat']:.0f} {drv}〕")


if __name__ == "__main__":
    main()

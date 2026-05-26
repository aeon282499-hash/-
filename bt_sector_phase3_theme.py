"""
bt_sector_phase3_theme.py — Phase 3 セクター×テーマ語辞書 BT

CoName にテーマ語が含まれる銘柄を「テーマ銘柄」とし、Phase 1 ベスト
(window=20, top_frac=0.5) と組み合わせて効果検証する。

注意:
  CoName マッチングは粗い (例: '東京エレクトロン' は半導体だがキーワード未ヒット)。
  Phase 2 でホワイトリストが逆効果だったので、テーマ語も逆効果の可能性が高い前提で実行。
"""
import json
import pickle
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from sector_filter import fetch_sector33_map, build_sector_ranking
from bt_sector_phase2 import simulate_mode
from bt_sector_filter import (
    build_long_df, summarize, yearly_breakdown,
    CACHE_FILE, START, END,
)

WINDOW = 20
TOP_FRAC = 0.50
THEME_PATH = Path("theme_keywords.json")


def build_theme_universe(name_map: dict[str, str]) -> set[str]:
    """name_map (ticker -> name) からテーマ語ヒット銘柄を抽出。"""
    with open(THEME_PATH, "r", encoding="utf-8") as f:
        themes = json.load(f)["themes"]
    keywords: list[str] = []
    for kw_list in themes.values():
        keywords.extend(kw_list)
    keywords = list({k for k in keywords if len(k) >= 2})  # 1文字は除外

    out: set[str] = set()
    for ticker, name in name_map.items():
        for kw in keywords:
            if kw in name:
                out.add(ticker)
                break
    return out


def main():
    if not CACHE_FILE.exists():
        print(f"[ph3] {CACHE_FILE} がありません"); return

    print("[ph3] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    name_map = cache["name_map"]
    print(f"[ph3] all_data: {len(all_data)} 銘柄 / name_map: {len(name_map)}  ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()
    theme_universe = build_theme_universe(name_map)
    print(f"[ph3] テーマ語ヒット銘柄: {len(theme_universe)} (例: {list(theme_universe)[:5]})")

    print(f"[ph3] セクターランキング構築 (window={WINDOW}, frac={TOP_FRAC})...")
    sector_ranking, _ = build_sector_ranking(
        all_data, sector_map, window=WINDOW, top_frac=TOP_FRAC
    )

    print("[ph3] 指標事前計算...")
    t_pre = time.time()
    df_long = build_long_df(all_data)
    print(f"[ph3] long DF: {len(df_long):,} 行 ({time.time()-t_pre:.0f}s)")

    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    # simulate_mode を再利用 (whitelist 引数にテーマ universe を渡す)
    modes = ["OFF", "Sector", "Sec_OR_White", "Sec_AND_White", "White_only"]
    labels_remap = {
        "OFF": "OFF",
        "Sector": "Sector",
        "Sec_OR_White": "Sec_OR_Theme",
        "Sec_AND_White": "Sec_AND_Theme",
        "White_only": "Theme_only",
    }

    results = []
    yearly_rows = []
    for m in modes:
        disp = labels_remap[m]
        print(f"\n[ph3] ===== {disp} =====")
        t_g = time.time()
        trades = simulate_mode(
            df_long, all_data, all_trading_days,
            mode=m,
            sector_map=sector_map,
            sector_ranking=sector_ranking,
            whitelist=theme_universe,   # ← テーマ universe を whitelist パラメータに流用
        )
        s = summarize(trades, disp)
        s["elapsed"] = time.time() - t_g
        print(f"[ph3] {disp}: {s['count']}件 / PF{s['pf']:.2f} / 累積{s['cum']:+.1f}% / MaxDD{s['maxdd']:+.1f}% ({s['elapsed']:.0f}s)")
        results.append(s)
        yb = yearly_breakdown(trades); yb["mode"] = disp; yearly_rows.append(yb)

    print(f"\n{'='*82}")
    print(f"  Phase 3 セクター×テーマ語 ({START} 〜 {END})")
    print(f"  Phase1 best: window={WINDOW} / top_frac={TOP_FRAC} / テーマ universe={len(theme_universe)}銘柄")
    print(f"{'='*82}")
    print(f"  {'mode':<16} {'件数':>6} {'勝率':>7} {'PF':>6} {'累積%':>9} {'平均':>9} {'MaxDD':>9}")
    print(f"  {'-'*80}")
    for s in results:
        print(
            f"  {s['label']:<16} {s['count']:>6} {s['wr']:>6.1f}% {s['pf']:>6.2f} "
            f"{s['cum']:>+8.1f}% {s['avg']:>+7.3f}% {s['maxdd']:>+8.1f}%"
        )
    print(f"{'='*82}\n")

    print(f"{'='*82}")
    print(f"  年別 PF / 累積% / 件数")
    print(f"{'='*82}")
    all_y = pd.concat(yearly_rows, ignore_index=True)
    pivot_pf = all_y.pivot_table(index="year", columns="mode", values="pf", aggfunc="first")
    pivot_cum = all_y.pivot_table(index="year", columns="mode", values="cum_pct", aggfunc="first")
    pivot_n = all_y.pivot_table(index="year", columns="mode", values="count", aggfunc="first")
    cols = [labels_remap[m] for m in modes]
    cols = [c for c in cols if c in pivot_pf.columns]
    pivot_pf = pivot_pf[cols]; pivot_cum = pivot_cum[cols]; pivot_n = pivot_n[cols]
    print("\n[件数]");   print(pivot_n.to_string(float_format=lambda x: f"{x:.0f}"))
    print("\n[PF]");     print(pivot_pf.to_string(float_format=lambda x: f"{x:.2f}"))
    print("\n[累積%]");  print(pivot_cum.to_string(float_format=lambda x: f"{x:+.1f}"))

    pd.DataFrame(results).to_csv("bt_sector_phase3_summary.csv", index=False, encoding="utf-8-sig")
    all_y.to_csv("bt_sector_phase3_yearly.csv", index=False, encoding="utf-8-sig")
    print(f"\n[ph3] 出力: bt_sector_phase3_summary.csv / bt_sector_phase3_yearly.csv")
    print(f"[ph3] 総経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()

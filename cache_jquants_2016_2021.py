# -*- coding: utf-8 -*-
"""スタンダードプラン開通(2026-07-18)後の10年BT用キャッシュ拡張。

既存 jquants_cache.pkl は 2021-10-03〜 なので、その前を埋める
2016-07-19（プラン最古）〜 2021-10-02 を四半期チャンクで取得して
jquants_cache_2016_2021.pkl に保存する（形式は cache_jquants.py と同一）。
チャンクごとに _cache_chunks/ へ中間保存＝中断しても再実行で続きから。
"""
import pickle
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from screener import batch_download_jquants, fetch_tse_universe, _jquants_id_token

OUT_FILE  = Path("jquants_cache_2016_2021.pkl")
CHUNK_DIR = Path("_cache_chunks")
START = date(2016, 7, 19)   # スタンダード最古（10年ローリング）
END   = date(2021, 10, 2)   # 既存キャッシュの開始前日


def quarters(start: date, end: date):
    """(chunk_start, chunk_end) を四半期区切りで返す"""
    cur = start
    while cur <= end:
        q_end = date(cur.year + (1 if cur.month > 9 else 0),
                     ((cur.month - 1) // 3 * 3 + 4 - 1) % 12 + 1, 1) - timedelta(days=1)
        yield cur, min(q_end, end)
        cur = q_end + timedelta(days=1)


def main():
    t0 = time.time()
    CHUNK_DIR.mkdir(exist_ok=True)
    token = _jquants_id_token()

    chunks = list(quarters(START, END))
    print(f"[cache10y] {START} 〜 {END} を {len(chunks)} チャンクで取得", flush=True)

    for cs, ce in chunks:
        tag = f"{cs.strftime('%Y%m%d')}_{ce.strftime('%Y%m%d')}"
        fp = CHUNK_DIR / f"chunk_{tag}.pkl"
        if fp.exists():
            print(f"[cache10y] {tag} 済み → スキップ", flush=True)
            continue
        print(f"[cache10y] {tag} 取得中...", flush=True)
        data = batch_download_jquants(token, start=cs.strftime("%Y-%m-%d"),
                                      end=ce.strftime("%Y-%m-%d"))
        tmp = fp.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.rename(fp)
        print(f"[cache10y] {tag} 保存 ({len(data)}銘柄) 経過{(time.time()-t0)/60:.1f}分", flush=True)

    # ── マージ ────────────────────────────────
    print("[cache10y] 全チャンク取得完了 → マージ", flush=True)
    import pandas as pd
    merged: dict = {}
    for cs, ce in chunks:
        tag = f"{cs.strftime('%Y%m%d')}_{ce.strftime('%Y%m%d')}"
        with open(CHUNK_DIR / f"chunk_{tag}.pkl", "rb") as f:
            data = pickle.load(f)
        for tkr, df in data.items():
            merged.setdefault(tkr, []).append(df)
    all_data = {}
    for tkr, dfs in merged.items():
        df = pd.concat(dfs).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        all_data[tkr] = df

    universe = fetch_tse_universe(token)
    payload = {
        "all_data": all_data,
        "name_map": {t: n for t, n in universe},
        "universe": universe,
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "start": START.strftime("%Y-%m-%d"),
        "end": END.strftime("%Y-%m-%d"),
    }
    with open(OUT_FILE, "wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[cache10y] 完了: {OUT_FILE} ({OUT_FILE.stat().st_size/1e6:.0f}MB) "
          f"{len(all_data)}銘柄 / 総経過{(time.time()-t0)/60:.1f}分", flush=True)


if __name__ == "__main__":
    main()

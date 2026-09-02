# -*- coding: utf-8 -*-
"""立花 e支店API の蓄積情報（最大約20年の日足）を全銘柄分取得して pkl 化する。

出力 tachibana_history.pkl は jquants_cache.pkl と同じ形:
    {"all_data": {"1301.T": DataFrame[Open,High,Low,Close,Volume] (Date index, 分割調整済み)},
     "name_map": {...}, "universe": [(ticker, name), ...], "fetched_at": ..., "source": "tachibana"}
→ 既存BTの all_data をこれに差し替えれば、J-Quantsの10年ローリング窓の外（2006〜2016）まで検証を延伸できる。

使い方:
    python tachibana_fetch_history.py --universe            # jquants_cache.pkl の universe 全銘柄（約3,750）
    python tachibana_fetch_history.py --codes 7203,6501     # 指定銘柄だけ
    python tachibana_fetch_history.py --universe --limit 50 # 試し
    再実行は自動で続き（--fresh で最初から）。100銘柄ごとに保存。

注意:
- 共用システムのため要求間隔 --sleep（既定0.5秒）を守る。3,750銘柄で1〜2時間。
- AM0:00-0:59は前日分の反映処理時間帯なので避ける（API側の案内）。
- 仮想URL(1日券)は夜間閉局で失効。翌日は再ログイン（電話認証→3分以内）が必要。
"""
from __future__ import annotations

import argparse
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path

from tachibana import TachibanaClient, TachibanaError, TachibanaSessionError, TachibanaProtocolError, history_to_cache_frame

OUT = Path("tachibana_history.pkl")
UNIVERSE_SRC = Path("jquants_cache.pkl")


def load_universe() -> tuple[list[tuple[str, str]], dict[str, str]]:
    d = pickle.load(open(UNIVERSE_SRC, "rb"))
    return list(d["universe"]), dict(d["name_map"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", action="store_true")
    ap.add_argument("--codes", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--fresh", action="store_true", help="既存pklを無視して最初から")
    ap.add_argument("--demo", action="store_true")
    a = ap.parse_args()

    out = Path(a.out)
    payload = {"all_data": {}, "name_map": {}, "universe": [], "fetched_at": None, "source": "tachibana", "errors": {}}
    if out.exists() and not a.fresh:
        payload = pickle.load(open(out, "rb"))
        payload.setdefault("errors", {})
        print(f"[resume] {out} 既存 {len(payload['all_data'])} 銘柄")

    if a.codes:
        tickers = [(c.strip().upper().removesuffix(".T") + ".T", "") for c in a.codes.split(",") if c.strip()]
        name_map = {}
    elif a.universe:
        tickers, name_map = load_universe()
    else:
        ap.error("--universe か --codes を指定")
    if a.limit:
        tickers = tickers[: a.limit]
    payload["universe"] = tickers
    payload["name_map"].update(name_map)

    todo = [t for t, _ in tickers if t not in payload["all_data"]]
    print(f"対象 {len(tickers)} / 未取得 {len(todo)}")
    if not todo:
        return 0

    tc = TachibanaClient(demo=a.demo)
    tc.MIN_INTERVAL = max(tc.MIN_INTERVAL, a.sleep)
    tc.ensure_session()
    t0 = time.time()
    n_ok = n_err = 0
    for i, ticker in enumerate(todo, 1):
        code = ticker.removesuffix(".T")
        try:
            df = history_to_cache_frame(tc.price_history(code))
            if df.empty:
                payload["errors"][ticker] = "empty"
                n_err += 1
            else:
                payload["all_data"][ticker] = df
                payload["errors"].pop(ticker, None)
                n_ok += 1
            if i % 25 == 0 or i == len(todo):
                el = time.time() - t0
                print(f"  {i}/{len(todo)} ok={n_ok} err={n_err} 経過{el/60:.1f}分 残り推定{el/i*(len(todo)-i)/60:.1f}分  最新 {ticker} {len(df)}行")
        except TachibanaSessionError as e:
            print(f"[停止] 仮想URLが無効になりました（閉局？）: {e}。保存して終了。再実行で続きから。")
            break
        except TachibanaProtocolError as e:
            if str(e.p_errno) == "9":
                print(f"[停止] サービス停止中(p_errno=9)。保存して終了。")
                break
            payload["errors"][ticker] = str(e)
            n_err += 1
        except TachibanaError as e:
            payload["errors"][ticker] = str(e)
            n_err += 1
        except KeyboardInterrupt:
            print("\n[中断] 保存して終了")
            break
        if i % 100 == 0:
            payload["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pickle.dump(payload, open(out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)

    payload["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pickle.dump(payload, open(out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    spans = [(t, df.index.min().date(), df.index.max().date(), len(df)) for t, df in payload["all_data"].items()]
    if spans:
        oldest = min(spans, key=lambda x: x[1])
        print(f"保存: {out} ({out.stat().st_size/1e6:.1f} MB) 銘柄 {len(spans)} / 最古 {oldest[0]} {oldest[1]} / エラー {len(payload['errors'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

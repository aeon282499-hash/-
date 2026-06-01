"""
screener_volume_theory.py — 立花型出来高理論スクリーナー (BT v6本番版)

ロジック:
  凪(ATR<4%, 20Dret∈[-10,+15]%)
  + vol蓄積(直近5日合計/過去15日(直近5除く)平均×5 ∈ [1.3, 3.0])
  + 20日高値ブレイク陽線
  + 売買代金≥5000万, 株価≥300円

最新営業日のシグナルを抽出して返す。
BT実績: PF1.10 / 4年累積+133%
"""
import os
import pickle
import time
import warnings
from datetime import date, timedelta, datetime
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import urllib3
import jpholiday
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")
load_dotenv()

BASE = "https://api.jquants.com/v2"
KEY  = (os.getenv("JQUANTS_API_KEY","") or os.getenv("JQUANTS_REFRESH_TOKEN","")).strip()
CACHE = Path("jquants_cache.pkl")

# v6パラメータ
ATR_MAX      = 4.0
RET20_MIN    = -0.10
RET20_MAX    = 0.15
VOL_RATIO_MIN = 1.3
VOL_RATIO_MAX = 3.0
MIN_VALUE    = 50_000_000
MIN_PRICE    = 300
MAX_GAP      = 0.03
MAX_SIGNALS  = 3   # 同日通知最大数


def jget(path: str, params: dict | None = None) -> dict:
    for _ in range(3):
        r = requests.get(f"{BASE}{path}", headers={"x-api-key": KEY},
                         params=params or {}, timeout=60, verify=False)
        if r.status_code == 429:
            time.sleep(60); continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"3回失敗: {path}")


def get_last_trading_day() -> str:
    """直近の営業日(今日含まず前日まで)を返す YYYY-MM-DD。"""
    cur = date.today() - timedelta(days=1)
    while cur.weekday() >= 5 or jpholiday.is_holiday(cur):
        cur -= timedelta(days=1)
    return cur.strftime("%Y-%m-%d")


def trading_days_back(n: int) -> list[str]:
    cur = date.today() - timedelta(days=1)
    out = []
    while len(out) < n:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            out.append(cur.strftime("%Y-%m-%d"))
        cur -= timedelta(days=1)
    out.reverse()
    return out


def fetch_sector_map() -> dict[str, dict]:
    """Code4桁 → {name, s17nm, s33nm, mkt}"""
    items = jget("/equities/master").get("data", [])
    out = {}
    target = {"プライム","スタンダード","グロース"}
    for it in items:
        if it.get("MktNm","") not in target:
            continue
        code = str(it.get("Code",""))[:4]
        out[code] = {
            "name":  it.get("CoName", code),
            "s17":   it.get("S17", ""),
            "s17nm": it.get("S17Nm", ""),
            "s33nm": it.get("S33Nm", ""),
            "mkt":   it.get("MktNm", ""),
        }
    return out


def fetch_missing_dates(cache_data: dict, target_dates: list[str]) -> dict:
    """キャッシュに無い日付分を fetch して all_data に統合。"""
    # キャッシュの最新日付を取得
    sample = next(iter(cache_data.values()))
    last_cached = sample.index.max().strftime("%Y-%m-%d")
    print(f"[cache] 最新日: {last_cached}")
    need = [d for d in target_dates if d > last_cached]
    if not need:
        print(f"[cache] 差分fetch不要")
        return cache_data

    print(f"[fetch] {len(need)} 日分追加取得: {need[0]} 〜 {need[-1]}")
    rec_by_ticker: dict[str, list] = {}
    for i, d in enumerate(need, 1):
        pkey = None
        while True:
            params = {"date": d}
            if pkey: params["pagination_key"] = pkey
            data = jget("/equities/bars/daily", params)
            for x in data.get("data", []):
                code = str(x.get("Code",""))[:4] + ".T"
                rec_by_ticker.setdefault(code, []).append({
                    "Date": pd.to_datetime(d),
                    "Open": pd.to_numeric(x.get("AdjO"), errors="coerce"),
                    "High": pd.to_numeric(x.get("AdjH"), errors="coerce"),
                    "Low":  pd.to_numeric(x.get("AdjL"), errors="coerce"),
                    "Close":pd.to_numeric(x.get("AdjC"), errors="coerce"),
                    "Volume":pd.to_numeric(x.get("AdjVo"), errors="coerce"),
                })
            pkey = data.get("pagination_key")
            if not pkey: break
            time.sleep(1.2)
        if i % 5 == 0:
            print(f"  [fetch] {i}/{len(need)} 日")
        time.sleep(1.2)

    # 統合
    merged = {}
    for t, df in cache_data.items():
        merged[t] = df.copy()
    for t, recs in rec_by_ticker.items():
        new_df = pd.DataFrame(recs).set_index("Date").sort_index().dropna(subset=["Close"])
        if t in merged:
            combined = pd.concat([merged[t], new_df])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
            merged[t] = combined
        else:
            merged[t] = new_df
    print(f"[merge] 完了 {len(merged)} 銘柄")
    return merged


def detect_signals_on_date(all_data: dict, smap: dict, target_date: str) -> list[dict]:
    """target_date(YYYY-MM-DD)時点の v6 シグナルを抽出。"""
    td = pd.to_datetime(target_date)
    sigs = []
    for ticker, df in all_data.items():
        code4 = ticker.split(".")[0]
        if code4 not in smap:
            continue  # プライム/スタンダード/グロース外は除外
        if td not in df.index:
            continue
        df = df.sort_index()
        # target_date 以前の40日が要る
        window = df.loc[:td].tail(40)
        if len(window) < 40:
            continue
        c, h, l, o, v = window["Close"], window["High"], window["Low"], window["Open"], window["Volume"]
        prev_c = c.shift(1)
        tr = pd.concat([h-l, (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
        atr_pct = (tr.rolling(20).mean()/c).iloc[-1]*100
        ret20 = c.iloc[-1]/c.iloc[-21]-1 if len(c) >= 21 else None
        vol_recent5 = v.iloc[-5:].sum()
        vol_base15  = v.iloc[-20:-5].mean()*5 if len(v) >= 20 else None
        if vol_base15 is None or vol_base15 == 0: continue
        vol_ratio = vol_recent5 / vol_base15
        high20_prev = h.iloc[-21:-1].max() if len(h) >= 21 else None
        breakout = c.iloc[-1] > high20_prev
        bullish = c.iloc[-1] > o.iloc[-1]
        value = c.iloc[-1] * v.iloc[-1]

        if not (atr_pct < ATR_MAX): continue
        if ret20 is None or not (RET20_MIN <= ret20 <= RET20_MAX): continue
        if not (VOL_RATIO_MIN <= vol_ratio <= VOL_RATIO_MAX): continue
        if not breakout: continue
        if not bullish: continue
        if value < MIN_VALUE: continue
        if c.iloc[-1] < MIN_PRICE: continue

        info = smap[code4]
        sigs.append({
            "ticker": ticker,
            "code4": code4,
            "name": info["name"],
            "s17nm": info["s17nm"],
            "s33nm": info["s33nm"],
            "mkt": info["mkt"],
            "close": float(c.iloc[-1]),
            "atr_pct": float(atr_pct),
            "ret20": float(ret20),
            "vol_ratio": float(vol_ratio),
            "value_oku": float(value/1e8),
            "high20_prev": float(high20_prev),
        })
    return sigs


def run_screener() -> tuple[str, list[dict], list[dict], list[dict]]:
    """最新営業日のシグナル＋テーマ熱を返す。
    Returns (target_date, signals[], ranked_themes[], hot_themes[])
    """
    target = get_last_trading_day()
    print(f"[run] 対象営業日: {target}")
    # キャッシュ読み
    print(f"[run] キャッシュ読み込み...")
    with open(CACHE,"rb") as f:
        cache = pickle.load(f)
    # 必要な日付範囲 = target含む直近30営業日
    need_days = trading_days_back(30)
    # 不足分fetch
    all_data = fetch_missing_dates(cache["all_data"], need_days)
    # セクターマスタ
    print(f"[run] セクターマスタ取得...")
    smap = fetch_sector_map()
    # シグナル抽出
    print(f"[run] シグナル抽出...")
    sigs = detect_signals_on_date(all_data, smap, target)

    # --- テーマ熱連動(同じ all_data で算出・再DLなし)。失敗してもv6コアは壊さない ---
    ranked: list[dict] = []
    hot: list[dict] = []
    try:
        from theme_tailwind import (compute_theme_heat, build_reverse_map,
                                    attach_tailwind, rerank)
        heat_map, ranked, hot = compute_theme_heat(all_data)
        rev_map = build_reverse_map()
        sigs = attach_tailwind(sigs, heat_map, rev_map)   # 全候補にタグ付け(切り詰め前)
        sigs = rerank(sigs, MAX_SIGNALS)                  # ホットテーマ優先→vol_ratio
        print(f"[run] テーマ熱連動OK: ホットテーマ {len(hot)}件 / ランク {len(ranked)}件")
    except Exception as e:
        print(f"[run] テーマ熱連動スキップ({e}) → vol_ratio順にフォールバック")
        sigs.sort(key=lambda x: x["vol_ratio"], reverse=True)
        sigs = sigs[:MAX_SIGNALS]

    print(f"[run] シグナル: {len(sigs)} 件")
    return target, sigs, ranked, hot


if __name__ == "__main__":
    target, sigs, ranked, hot = run_screener()
    print()
    if ranked:
        print(f"=== 🔥今ホットなテーマ TOP8 ===")
        for i, r in enumerate(ranked[:8], 1):
            print(f"  {i:2d}. {r['theme']} heat{r['heat']:.0f} "
                  f"5d{r['avg_r5']:+.1f}% 25MA上{r['pct_above_ma25']*100:.0f}%")
        print()
    print(f"=== {target} のシグナル ({len(sigs)}件) ===")
    for s in sigs:
        tw = ""
        if s.get("theme"):
            tw = f" [{'🔥' if s.get('theme_hot') else ''}{s['theme']} heat{s.get('theme_heat')}]"
        print(f"  {s['ticker']} {s['name']} ({s['mkt']}/{s['s17nm']}){tw}")
        print(f"    終値:{s['close']:,.0f} ATR{s['atr_pct']:.2f}% 20D{s['ret20']*100:+.1f}% "
              f"vol_r{s['vol_ratio']:.2f}x 売買代金{s['value_oku']:.1f}億 "
              f"20日高値{s['high20_prev']:,.0f}円突破")

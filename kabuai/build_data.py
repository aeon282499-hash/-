"""
build_data.py — KabuAI クローン フェーズ1 ドライバ

SPEC.md §6 の (1)データ取得層 + (2)指標計算 + (3)静的JSON生成 の最小版。
既定データソースは親プロジェクトの jquants_cache.pkl を再利用（API 不要・高速）。

  python build_data.py            # 既定ソースで latest.json + data/<日付>.json を生成
  KABUAI_DATA_SOURCE=jquants_api  # （将来）都度取得

出力は「計算済み指標のみ」。生 OHLCV は JSON に載せない（SPEC §1 ライセンス方針）。
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)
load_dotenv(HERE / ".env")
load_dotenv(PARENT / ".env")  # 親のキーも拾う

from momentum import indicators, chart_series, SR_COEF, SR_WINDOW, GRADE_BANDS  # noqa: E402
import signals as sig  # noqa: E402
import ai_summary  # noqa: E402

# ── 仮パラメータ ────────────────────────────────────────────
MIN_TURNOVER = 1e8     # 平均売買代金（円/日）下限＝流動性フィルタ（仮）
TOP_N = 50             # ランキング掲載件数
EXPORT_TOP = 120       # 詳細チャートJSONを書き出す上位件数（＋シグナル点灯銘柄）
CHART_DAYS = 60        # 詳細チャートの営業日数（≒3ヶ月）
SOURCE = os.getenv("KABUAI_DATA_SOURCE", "jquants_cache")

# ── J-Quants V2 直接取得（CI 用。親 jquants_cache.pkl は 217MB/gitignore のため CI からは読めない） ──
JQUANTS_BASE = "https://api.jquants.com/v2"
API_LOOKBACK_DAYS = int(os.getenv("KABUAI_API_LOOKBACK", "140"))  # 60日チャートに指数ラインを満たすには≈120営業日必要


def load_jquants_cache() -> tuple[dict, dict, str]:
    """親 my-first-project/jquants_cache.pkl を再利用。"""
    import pickle

    path = PARENT / "jquants_cache.pkl"
    if not path.exists():
        raise FileNotFoundError(f"{path} が無い。親プロジェクトで cache 生成済みのはず。")
    with open(path, "rb") as f:
        c = pickle.load(f)
    data = c["all_data"]
    name_map = c.get("name_map", {}) or {}
    data_date = str(c.get("end", "") or "")
    if not data_date:
        mx = max(df.index.max() for df in data.values() if df is not None and len(df))
        data_date = mx.strftime("%Y-%m-%d")
    return data, name_map, data_date


def _jquants_key() -> str:
    key = (os.getenv("JQUANTS_API_KEY", "") or os.getenv("JQUANTS_REFRESH_TOKEN", "")).strip()
    if not key:
        raise ValueError("JQUANTS_API_KEY が未設定（CI Secrets / .env を確認）")
    return key


def _jquants_get(path: str, token: str, params: dict | None = None) -> dict:
    import requests
    verify = os.getenv("JQUANTS_VERIFY_SSL", "true").lower() not in ("0", "false", "no")
    resp = requests.get(f"{JQUANTS_BASE}{path}", headers={"x-api-key": token},
                        params=params or {}, timeout=60, verify=verify)
    resp.raise_for_status()
    return resp.json()


def _recent_trading_days(n: int) -> list[str]:
    import jpholiday
    from datetime import date, timedelta
    days: list[str] = []
    cur = date.today() - timedelta(days=1)
    while len(days) < n:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur -= timedelta(days=1)
    days.reverse()
    return days


def load_jquants_api() -> tuple[dict, dict, str]:
    """J-Quants V2 から日足を直接取得（screener.py と同方式）。CI 専用パス。
    返り値は load_jquants_cache と同形 (data, name_map, data_date)。"""
    token = _jquants_key()

    name_map: dict = {}
    try:
        master = _jquants_get("/equities/master", token)
        for item in master.get("data", []):
            mkt = item.get("MktNm", "")
            if any(k in mkt for k in ("プライム", "スタンダード", "グロース")):
                code = str(item.get("Code", ""))[:4]
                name_map[code] = item.get("CoName", code)
        print(f"[jquants_api] master: {len(name_map)} 銘柄名ロード")
    except Exception as e:
        print(f"[jquants_api] master 取得失敗: {e}（銘柄名なしで継続）")

    tdays = _recent_trading_days(API_LOOKBACK_DAYS)
    print(f"[jquants_api] {tdays[0]} 〜 {tdays[-1]}（{len(tdays)} 営業日）取得開始 …")
    records: list[dict] = []
    for i, ds in enumerate(tdays):
        pk = None
        while True:
            params = {"date": ds}
            if pk:
                params["pagination_key"] = pk
            try:
                d = _jquants_get("/equities/bars/daily", token, params)
                records.extend(d.get("data", []))
                pk = d.get("pagination_key")
                if not pk:
                    break
                time.sleep(1.2)
            except Exception as e:
                if "429" in str(e):
                    print("  [jquants_api] レート制限 → 60秒待機してリトライ")
                    time.sleep(60)
                    continue
                print(f"  [jquants_api] {ds} 取得失敗: {e}")
                break
        if (i + 1) % 10 == 0:
            print(f"  [jquants_api] {i + 1}/{len(tdays)} 日完了 …")
        time.sleep(1.2)  # 60req/分制限

    if not records:
        raise RuntimeError("J-Quants から 0 レコード。APIキー・契約プラン・日付を確認。")

    df_all = pd.DataFrame(records)
    required = {"Code", "Date", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"}
    if not required.issubset(df_all.columns):
        raise RuntimeError(f"J-Quants カラム不足: {required - set(df_all.columns)}")
    df_all["Date"] = pd.to_datetime(df_all["Date"])
    df_all = df_all.rename(columns={"AdjO": "Open", "AdjH": "High", "AdjL": "Low",
                                    "AdjC": "Close", "AdjVo": "Volume"})
    data: dict = {}
    for code, grp in df_all.groupby("Code"):
        ticker = str(code)[:4] + ".T"
        sub = grp[["Date", "Open", "High", "Low", "Close", "Volume"]].set_index("Date").sort_index()
        sub = sub.apply(pd.to_numeric, errors="coerce").dropna(subset=["Close"])
        if not sub.empty:
            data[ticker] = sub
    data_date = max(df.index.max() for df in data.values()).strftime("%Y-%m-%d")
    print(f"[jquants_api] {len(data)} 銘柄・最新日 {data_date}")
    return data, name_map, data_date


def _name(name_map: dict, ticker: str) -> str:
    for k in (ticker, ticker.replace(".T", ""), ticker[:4]):
        if k in name_map:
            return str(name_map[k])
    return ticker


def _grade_of(m: float) -> str:
    for thr, g in GRADE_BANDS:
        if m >= thr:
            return g
    return "D"


def export_stocks(data: dict, rows: list[dict], data_date: str, disclaimer: str) -> int:
    """ランキング上位＋シグナル点灯銘柄について、詳細チャートJSONを個別ファイルで書き出す。
    生OHLCVの一括配信ではなく『表示用に窓を限定した加工済みデータ』(SPEC §1)。"""
    sdir = DATA_DIR / "stocks"
    sdir.mkdir(exist_ok=True)
    targets = [r for r in rows if r["rank"] <= EXPORT_TOP or r.get("signals")]
    keep = {"price", "momentum", "grade", "sr", "power", "rsi", "stab", "r1", "r5", "r20", "vr"}
    written = 0
    for r in targets:
        df = data.get(r["code"] + ".T")
        if df is None:
            continue
        payload = {
            "code": r["code"], "name": r["name"], "data_date": data_date, "rank": r["rank"],
            "indicators": {k: r[k] for k in keep},
            "signals": r.get("signals", []),
            "ai": r.get("ai"),
            "chart": chart_series(df, days=CHART_DAYS),
            "disclaimer": disclaimer,
            "chart_note": "日足は約3ヶ月・表示用に加工した参考データ（生データ配信ではありません）",
        }
        with open(sdir / f"{r['code']}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        written += 1
    return written


def build_market(data: dict, scored_tickers: list[str], rows: list[dict]) -> dict:
    """流動性銘柄の等加重プロキシで市場の地合いを 0〜100 + ランクで出す。
    SPEC は日経/TOPIX/グロースの3指数を想定するが、当キャッシュに指数データが
    無いため『市場全体（等加重プロキシ）』1本で算出（画面でその旨を明示）。"""
    closes = []
    for t in scored_tickers:
        df = data.get(t)
        if df is None or len(df) < SR_WINDOW + 2:
            continue
        closes.append(df["Close"].astype(float).tail(70).rename(t))
    if not closes:
        return {"available": False}
    mat = pd.concat(closes, axis=1, sort=True)
    daily = mat.pct_change().mean(axis=1)            # 等加重・日次リターン
    proxy = (1 + daily.fillna(0)).cumprod()
    win = daily.tail(SR_WINDOW).dropna()
    sd = float(win.std())
    if sd <= 0 or np.isnan(sd):
        return {"available": False}
    ma25 = float(proxy.tail(25).mean())
    ma_dev = float(proxy.iloc[-1] / ma25 - 1.0)                                   # 25日線乖離
    trail20 = float(proxy.iloc[-1] / proxy.iloc[-21] - 1.0) if len(proxy) > 21 else 0.0
    breadth = float(np.mean([1.0 if r["r5"] > 0 else 0.0 for r in rows])) if rows else 0.5

    # 地合いは 50 を中立とする回帰ゲージ（個別株の SR×18 とは別スケール）
    score = 50.0 + ma_dev * 100 * 1.5 + trail20 * 100 * 1.0 + (breadth - 0.5) * 100 * 0.6
    score = max(0.0, min(100.0, score))
    if proxy.iloc[-1] >= ma25 and trail20 >= 0:
        regime = "上げ"
    elif proxy.iloc[-1] < ma25 and trail20 < -0.03:
        regime = "強い下げ"
    else:
        regime = "下げ／もみ合い"
    sr_proxy = float((proxy.iloc[-1] / proxy.iloc[-min(SR_WINDOW, len(proxy) - 1)] - 1.0)
                     / (sd * np.sqrt(min(SR_WINDOW, len(win)))))
    return {
        "available": True,
        "label": "市場全体（東証・等加重プロキシ）",
        "score": round(score, 1),
        "grade": _grade_of(score),
        "regime": regime,
        "breadth_pct": round(breadth * 100, 1),
        "ma_dev_pct": round(ma_dev * 100, 2),
        "trail20_pct": round(trail20 * 100, 2),
        "sr": round(sr_proxy, 2),
        "note": "日経225 / TOPIX / グロースの個別指数は指数データ取得後に追加予定",
    }


def build() -> dict:
    if SOURCE == "jquants_cache":
        data, name_map, data_date = load_jquants_cache()
    elif SOURCE == "jquants_api":
        data, name_map, data_date = load_jquants_api()
    else:
        raise NotImplementedError(f"KABUAI_DATA_SOURCE={SOURCE} は未実装（jquants_cache / jquants_api）")

    t0 = time.time()
    rows: list[dict] = []
    scored_tickers: list[str] = []
    n_skip = 0
    for ticker, df in data.items():
        ind = indicators(df)
        if ind is None:
            n_skip += 1
            continue
        if ind["turnover"] < MIN_TURNOVER:
            continue
        scored_tickers.append(ticker)
        rows.append({
            "code": ticker.replace(".T", ""),
            "name": _name(name_map, ticker),
            "price": ind["price"],
            "momentum": ind["momentum"],
            "grade": ind["grade"],
            "sr": ind["sr"],
            "power": ind["power"],
            "rsi": ind["rsi"],
            "stab": ind["stab"],
            "r1": ind["r1"],
            "r5": ind["r5"],
            "r20": ind["r20"],
            "vr": ind["vr"],
            "mom_hist": ind["mom_hist"],
        })

    rows.sort(key=lambda x: x["momentum"], reverse=True)
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    grade_counts: dict[str, int] = {}
    for r in rows:
        grade_counts[r["grade"]] = grade_counts.get(r["grade"], 0) + 1

    # ── フェーズ3: シグナル判定（全 scored を走査） ──
    signals = sig.detect(rows)
    sig_counts = {k: signals["groups"][k]["count"] for k in signals["order"]}

    # ── フェーズ3: 地合い ──
    market = build_market(data, scored_tickers, rows)

    # ── フェーズ6: AI要約（警戒メモ）。export対象（上位＋シグナル点灯）のみ付与 ──
    ai_targets = [r for r in rows if r["rank"] <= EXPORT_TOP or r.get("signals")]
    ai_meta = ai_summary.annotate(ai_targets)

    disclaimer = "価格・指標はAI等で整理・加工した参考表示です。リアルタイム配信ではありません。投資判断は自己責任。"
    # ── フェーズ5: 詳細チャートJSON書き出し ──
    n_charts = export_stocks(data, rows, data_date, disclaimer)

    try:
        data_lag_days = (datetime.now().date() - datetime.strptime(data_date, "%Y-%m-%d").date()).days
    except Exception:
        data_lag_days = None

    top = rows[:TOP_N]
    out = {
        "schema": "kabuai-phase8",
        "data_date": data_date,
        "data_lag_days": data_lag_days,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": SOURCE,
        "disclaimer": disclaimer,
        "universe_total": len(data),
        "universe_scored": len(rows),
        "min_turnover_yen": MIN_TURNOVER,
        "grade_counts": grade_counts,
        "stock_charts": n_charts,
        "ai": ai_meta,
        "market": market,
        "signals": signals,
        "ranking": top,
    }
    print(f"[build] scored {len(rows)} / {len(data)} 銘柄 "
          f"(skip {n_skip} 履歴不足ほか) / {time.time()-t0:.1f}s")
    print(f"[build] grade分布: {grade_counts}")
    print(f"[build] 地合い: {market.get('label','-')} score={market.get('score')} "
          f"[{market.get('grade')}] {market.get('regime')} breadth={market.get('breadth_pct')}%")
    print(f"[build] シグナル点灯数: {sig_counts}")
    print(f"[build] AI要約: provider={ai_meta['provider']} "
          f"LLM={ai_meta['llm_notes']} / rule={ai_meta['rule_notes']} / 計{ai_meta['total']}")
    print(f"[build] 詳細チャートJSON: {n_charts} 銘柄 → data/stocks/")
    return out


def main():
    out = build()
    dated = DATA_DIR / f"{out['data_date'].replace('-', '')}.json"
    latest = DATA_DIR / "latest.json"
    for p in (dated, latest):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[build] 出力: {dated.name} / {latest.name}  (データ日付 {out['data_date']})")
    print("[build] TOP5 プレビュー:")
    for r in out["ranking"][:5]:
        print(f"   #{r['rank']:<2} {r['name']:<14} {r['code']} ¥{r['price']:<8} "
              f"指数{r['momentum']:>5} [{r['grade']}] SR{r['sr']:>5} POW{r['power']:>5} "
              f"5日{r['r5']:>+6}% RSI{r['rsi']:>5}")


if __name__ == "__main__":
    main()

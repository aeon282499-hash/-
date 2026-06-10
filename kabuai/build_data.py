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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

JST = timezone(timedelta(hours=9))  # runner は UTC のため、取得対象日は JST 基準で決める

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)
load_dotenv(HERE / ".env")
load_dotenv(PARENT / ".env")  # 親のキーも拾う

from momentum import indicators, chart_series, SR_COEF, SR_WINDOW, GRADE_BANDS  # noqa: E402
import signals as sig  # noqa: E402
import signal_track  # noqa: E402
import ai_summary  # noqa: E402

# ── 仮パラメータ ────────────────────────────────────────────
MIN_TURNOVER = 1e8     # 平均売買代金（円/日）下限＝流動性フィルタ（仮）
TOP_N = 50             # ランキング掲載件数
EXPORT_TOP = 120       # 詳細チャートJSONを書き出す上位件数（＋シグナル点灯銘柄）
CHART_DAYS = 60        # 詳細チャートの営業日数（≒3ヶ月）
SEG_MIN_NAMES = 8      # 区分別地合いプロキシを出す最低銘柄数（これ未満の区分は省略）
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
    # キャッシュには市場区分が無いので best-effort で master から補完（キー無し/オフライン時は空）
    seg_map: dict = {}
    try:
        seg_map = _segment_map_from_master(_jquants_get("/equities/master", _jquants_key()))
    except Exception as e:
        print(f"[jquants_cache] 区分マップ取得スキップ（地合いは全体プロキシのみ）: {e}")
    return data, name_map, data_date, seg_map


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


def _segment_map_from_master(master: dict) -> dict:
    """master(/equities/master) から code(4桁) → 市場区分(プライム/スタンダード/グロース)。"""
    seg: dict = {}
    for item in master.get("data", []):
        mkt = item.get("MktNm", "")
        code = str(item.get("Code", ""))[:4]
        if "プライム" in mkt:
            seg[code] = "プライム"
        elif "スタンダード" in mkt:
            seg[code] = "スタンダード"
        elif "グロース" in mkt:
            seg[code] = "グロース"
    return seg


def _recent_trading_days(n: int) -> list[str]:
    import jpholiday
    days: list[str] = []
    # JST 基準の当日を起点に含める。当日 EOD が未公開なら J-Quants が 0 件を返すだけで、
    # data_date は実際にレコードが返った最新日（= 前営業日）に自動で下がる（無害）。
    # 公開され次第その日を拾えるので、朝/夕どちらのビルドでも最新を取りこぼさない。
    cur = datetime.now(JST).date()
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
    seg_map: dict = {}
    try:
        master = _jquants_get("/equities/master", token)
        seg_map = _segment_map_from_master(master)
        for item in master.get("data", []):
            mkt = item.get("MktNm", "")
            if any(k in mkt for k in ("プライム", "スタンダード", "グロース")):
                code = str(item.get("Code", ""))[:4]
                name_map[code] = item.get("CoName", code)
        print(f"[jquants_api] master: {len(name_map)} 銘柄名ロード"
              f"（区分 P{sum(v=='プライム' for v in seg_map.values())}"
              f"/S{sum(v=='スタンダード' for v in seg_map.values())}"
              f"/G{sum(v=='グロース' for v in seg_map.values())}）")
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
    # 株式ユニバース（プライム/スタンダード/グロース）に限定。bars/daily は ETF・REIT 等も
    # 返すため、master に無い銘柄を除外しないと指数や派生ファンドが上位に紛れ込む。
    allowed = set(name_map.keys())
    data: dict = {}
    n_drop_nonequity = 0
    for code, grp in df_all.groupby("Code"):
        c4 = str(code)[:4]
        if allowed and c4 not in allowed:   # master 取得失敗時(allowed空)はフィルタしない
            n_drop_nonequity += 1
            continue
        ticker = c4 + ".T"
        sub = grp[["Date", "Open", "High", "Low", "Close", "Volume"]].set_index("Date").sort_index()
        sub = sub.apply(pd.to_numeric, errors="coerce").dropna(subset=["Close"])
        if not sub.empty:
            data[ticker] = sub
    data_date = max(df.index.max() for df in data.values()).strftime("%Y-%m-%d")
    print(f"[jquants_api] {len(data)} 銘柄・最新日 {data_date}"
          f"（非株式 {n_drop_nonequity} 件を除外）")
    return data, name_map, data_date, seg_map


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
    keep = {"price", "momentum", "grade", "sr", "power", "rsi", "stab",
            "r1", "r5", "r10", "r20", "vr"}
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


def export_search_index(rows: list[dict], data_date: str) -> int:
    """検索用の軽量インデックスを全 scored 銘柄ぶん書き出す（latest.json はTOP50のみのため）。
    気になる銘柄をコード/名前で引いて、シグナルが点灯しているかを確認できるようにする。
    生OHLCVは載せず計算済み指標だけ（SPEC §1）。フロントが遅延fetchする独立ファイル。"""
    keep = ("code", "name", "price", "momentum", "grade", "sr", "power", "rsi",
            "stab", "r1", "r5", "r10", "r20", "rank")
    stocks = []
    for r in rows:
        o = {k: r[k] for k in keep}
        if r.get("signals"):          # 非点灯は省略（[]）してサイズ削減。フロントは signals||[] で受ける
            o["signals"] = r["signals"]
        stocks.append(o)
    payload = {"schema": "kabuai-search-1", "data_date": data_date,
               "count": len(stocks), "stocks": stocks}
    with open(DATA_DIR / "search_index.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    return len(stocks)


def _gauge_from_closes(closes: list, rows_for_breadth: list[dict], label: str, note: str) -> dict:
    """等加重プロキシ系列から地合いゲージ（0〜100＋ランク＋レジーム）を計算する純関数。"""
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
    breadth = float(np.mean([1.0 if r["r5"] > 0 else 0.0 for r in rows_for_breadth])) if rows_for_breadth else 0.5

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
        "label": label,
        "score": round(score, 1),
        "grade": _grade_of(score),
        "regime": regime,
        "breadth_pct": round(breadth * 100, 1),
        "ma_dev_pct": round(ma_dev * 100, 2),
        "trail20_pct": round(trail20 * 100, 2),
        "sr": round(sr_proxy, 2),
        "count": len(closes),
        "note": note,
    }


def _seg_closes(data: dict, rows_subset: list[dict]) -> list:
    out = []
    for r in rows_subset:
        df = data.get(r["code"] + ".T")
        if df is None or len(df) < SR_WINDOW + 2:
            continue
        out.append(df["Close"].astype(float).tail(70).rename(r["code"]))
    return out


def build_market(data: dict, scored_tickers: list[str], rows: list[dict],
                 seg_map: dict | None = None) -> dict:
    """流動性銘柄の等加重プロキシで地合いを 0〜100 + ランクで出す。
    公式指数（日経225/TOPIX等）は API プラン対象外のため使えない。代わりに保有株価から
    『市場全体』＋『プライム/スタンダード/グロース 区分別』の等加重プロキシを自前算出する
    （画面で“等加重プロキシ”である旨を明示）。日経225は J-Quants に存在しないため不可。"""
    overall = _gauge_from_closes(
        _seg_closes(data, rows), rows, "市場全体（東証・等加重プロキシ）",
        "公式指数（日経225/TOPIX等）はAPIプラン対象外。全銘柄の等加重プロキシで代替")
    if not overall.get("available"):
        return overall

    # 区分別（master の市場区分が取れているときのみ）
    if seg_map:
        segments = {}
        for key, jp in (("prime", "プライム"), ("standard", "スタンダード"), ("growth", "グロース")):
            sub = [r for r in rows if seg_map.get(r["code"]) == jp]
            cl = _seg_closes(data, sub)
            if len(cl) < SEG_MIN_NAMES:
                continue
            g = _gauge_from_closes(cl, sub, f"{jp}市場（等加重プロキシ）", "")
            if g.get("available"):
                segments[key] = g
        if segments:
            overall["segments"] = segments
            overall["segments_note"] = "東証3市場の区分別・等加重プロキシ（公式の市場指数ではありません）"
    return overall


def build() -> dict:
    if SOURCE == "jquants_cache":
        data, name_map, data_date, seg_map = load_jquants_cache()
    elif SOURCE == "jquants_api":
        data, name_map, data_date, seg_map = load_jquants_api()
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
            "r10": ind["r10"],
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

    # ── フェーズ11: 全面リバウンド判定（強反転の「広がり」ゲート） ──
    # bt_best_pick.py (2026-06-10・2021-2026・4636銘柄54万発動・翌朝寄りentry・出口8日/-12%・
    # 流動性1億円) で確定: 強反転のエッジは「その日の強反転点灯数」に強く依存する。
    #   点灯10件以上の日(全面リバウンド・年45-55日): 勝率62.5% 平均+2.16%/件 中央+2.00% 陽性年5/5
    #   点灯1-9件の日(散発・銘柄固有の下落):          勝率42.2% 平均-0.61%/件 2022-2025全year負け
    # 閾値感度は単調(5件+1.64%→10件+2.16%→20件+2.65%→30件+3.00%)でナイフエッジなし。
    # 広がり日は他の買い候補も持ち上げる(反転+0.90% vs 通常日+0.06% / accum系+0.71% vs +0.09%)。
    # 広がり日内ではK絞り(本命1点)より全件分散が有利(K=3で+1.33% vs 全件+2.16%)。
    REBOUND_B = 10
    sr_count = sig_counts.get("strong_reversal", 0)
    rebound = {
        "sr_count": sr_count,
        "threshold": REBOUND_B,
        "mode": "broad" if sr_count >= REBOUND_B else ("sparse" if sr_count >= 1 else "none"),
        "stats": {  # 上記BTの実測値(理論値・手数料/スリッページ未考慮)
            "broad": {"win": 62.5, "avg": 2.16, "med": 2.00, "n": 7737, "pos_years": "5/5"},
            "sparse": {"win": 42.2, "avg": -0.61, "n": 2707},
            "lift": {"reversal": [0.90, 0.06], "accum": [0.71, 0.09]},  # [広がり日, 通常日] 平均%
        },
        "note": ("強反転は『市場全体が深い下落から反発する日』にだけ強いエッジを持つ。"
                 "点灯が散発の日は銘柄固有の下落で、過去実績では強反転でも平均マイナス。"),
    }

    # ── フェーズ3/9: 地合い（全体＋区分別プロキシ） ──
    market = build_market(data, scored_tickers, rows, seg_map)

    # ── フェーズ10: シグナル別トラックレコード（過去シミュレーション） ──
    t_trk = time.time()
    try:
        track = signal_track.build_track(data, scored_tickers, horizons=(5, 10, 20))
    except Exception as e:
        print(f"[build] トラックレコード生成スキップ: {e}")
        track = {"available": False}

    # 長期版（手元pklで事前計算しコミットした track_longterm.json）があれば優先採用。
    # CIは直近140営業日しか取得できず実績窓が薄いので、存在すれば数年窓に差し替える。
    lt_path = HERE / "track_longterm.json"
    if lt_path.exists():
        try:
            with open(lt_path, encoding="utf-8") as f:
                lt = json.load(f)
            if lt.get("available"):
                track = lt
                print(f"[build] 長期トラックレコードを採用: "
                      f"{lt['period']['from']}〜{lt['period']['to']} ({lt.get('window','')})")
        except Exception as e:
            print(f"[build] 長期トラックレコード読込スキップ（短期版を使用）: {e}")

    # ── フェーズ6: AI要約（警戒メモ）。export対象（上位＋シグナル点灯）のみ付与 ──
    ai_targets = [r for r in rows if r["rank"] <= EXPORT_TOP or r.get("signals")]
    ai_meta = ai_summary.annotate(ai_targets)

    disclaimer = "価格・指標はAI等で整理・加工した参考表示です。リアルタイム配信ではありません。投資判断は自己責任。"
    # ── フェーズ5: 詳細チャートJSON書き出し ──
    n_charts = export_stocks(data, rows, data_date, disclaimer)
    # ── 銘柄検索インデックス（全scored・遅延fetch用） ──
    n_search = export_search_index(rows, data_date)

    try:
        data_lag_days = (datetime.now(JST).date() - datetime.strptime(data_date, "%Y-%m-%d").date()).days
    except Exception:
        data_lag_days = None

    top = rows[:TOP_N]
    out = {
        "schema": "kabuai-phase11",
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
        "search_index": n_search,
        "ai": ai_meta,
        "market": market,
        "rebound": rebound,
        "signals": signals,
        "signal_track": track,
        "ranking": top,
    }
    print(f"[build] scored {len(rows)} / {len(data)} 銘柄 "
          f"(skip {n_skip} 履歴不足ほか) / {time.time()-t0:.1f}s")
    print(f"[build] grade分布: {grade_counts}")
    print(f"[build] リバウンド広がり: 強反転{sr_count}件 → {rebound['mode']} (しきい値{REBOUND_B})")
    print(f"[build] 地合い: {market.get('label','-')} score={market.get('score')} "
          f"[{market.get('grade')}] {market.get('regime')} breadth={market.get('breadth_pct')}%")
    if market.get("segments"):
        segline = " / ".join(f"{g['label'].split('市場')[0]} {g['score']}[{g['grade']}]{g['regime']}({g['count']})"
                             for g in market["segments"].values())
        print(f"[build] 区分別地合い: {segline}")
    print(f"[build] シグナル点灯数: {sig_counts}")
    if track.get("available"):
        pl = track["period"]
        parts = []
        for k in track["order"]:
            g = track["groups"][k]
            if g["n"] <= 0:
                continue
            h5 = g["h"].get("5") or {}
            parts.append(f"{g['label']} n{g['n']}(5d勝{h5.get('win','-')}%/平均{h5.get('avg','-')}%)")
        print(f"[build] トラックレコード {pl['from']}〜{pl['to']} {pl['names']}銘柄 "
              f"/ {time.time()-t_trk:.1f}s: " + " / ".join(parts))
    print(f"[build] AI要約: provider={ai_meta['provider']} "
          f"LLM={ai_meta['llm_notes']} / rule={ai_meta['rule_notes']} / 計{ai_meta['total']}")
    print(f"[build] 詳細チャートJSON: {n_charts} 銘柄 → data/stocks/")
    print(f"[build] 検索インデックス: {n_search} 銘柄 → data/search_index.json")
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

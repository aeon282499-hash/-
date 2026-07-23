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
import correlation  # noqa: E402  先物連動度タグ（yfinance は使用時にのみ import）

# ── 仮パラメータ ────────────────────────────────────────────
MIN_TURNOVER = 1e8     # 採点・シグナル検出・リバウンド判定の土台（全銘柄・不変）
# v3(2026-07-07): ✅買い候補として「表示」する床＝10億。板の厚い銘柄だけ推奨する。
# 採点/シグナル検出/全面リバウンド日の判定は MIN_TURNOVER(1億)の全銘柄のまま＝decouple。
# 根拠: _edge_by_turnover.py で反発・買い集めの勝率/PFは10億がピーク(小型偏重でない)、
#       _turnover_check.py で10億でも候補68件/日・毎日≥5件と枯れない。フロントが
#       各候補の turnover_oku で ≥ pick_min_oku を絞る。
PICK_MIN_TURNOVER = 1e9
# 値動き天井(2026-07-09): _edge_by_volatility.py(2023以降・買い系4.8万件・出口8日/-12%)で
# 20日平均日中値幅 3〜4%がPF1.60と最良、≥6%は勝率34%・2023-2026全年PF≤1.05の宝くじゾーン
# (勝てば+19%だが-12%損切り連発で期待値ゼロ)。ピック床と同じくフロントの「表示」だけ絞る。
PICK_MAX_RNG = 6.0
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
    targets = [r for r in rows if r["rank"] <= EXPORT_TOP or r.get("signals") or r.get("sell_end") or r.get("fade_pick")]
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
            "futures": ({"corr": r.get("futures_corr"), "tag": r["futures_tag"]}
                        if r.get("futures_tag") else None),
            "ai": r.get("ai"),
            "chart": chart_series(df, days=CHART_DAYS),
            "disclaimer": disclaimer,
            "chart_note": "日足は約3ヶ月・表示用に加工した参考データ（生データ配信ではありません）",
        }
        with open(sdir / f"{r['code']}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        written += 1
    return written


def export_search_index(rows: list[dict], data_date: str, rows_illiq: list[dict] | None = None) -> int:
    """検索用の軽量インデックスを書き出す（latest.json はTOP50のみのため）。
    気になる銘柄をコード/名前で引いて、シグナルが点灯しているかを確認できるようにする。
    生OHLCVは載せず計算済み指標だけ（SPEC §1）。フロントが遅延fetchする独立ファイル。
    2026-06-12: 低流動銘柄(売買代金1億円未満)も illiq=1 フラグつきで収録（検索/詳細は全銘柄
    対応・ランキング/シグナルは従来どおり流動性フィルタ後のみ＝フロント側でilliqを除外）。"""
    keep = ("code", "name", "price", "momentum", "grade", "sr", "power", "rsi",
            "stab", "r1", "r5", "r10", "r20", "rank", "turnover_oku", "rng20", "sector")
    stocks = []
    for r in rows:
        o = {k: r[k] for k in keep}
        if r.get("signals"):          # 非点灯は省略（[]）してサイズ削減。フロントは signals||[] で受ける
            o["signals"] = r["signals"]
        if r.get("futures_tag"):      # 先物連動タグ（付与済み銘柄のみ・データ不足は省略=非表示）
            o["futures_corr"] = r.get("futures_corr")
            o["futures_tag"] = r["futures_tag"]
        stocks.append(o)
    for r in (rows_illiq or []):
        o = {k: r.get(k) for k in keep}
        o["illiq"] = 1                # 低流動＝採点・シグナル対象外（執行困難の正直表示）
        stocks.append(o)
    payload = {"schema": "kabuai-search-1", "data_date": data_date,
               "count": len(stocks), "stocks": stocks}
    with open(DATA_DIR / "search_index.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
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


def _edge_signal_keys(track: dict) -> set:
    """フロントの sigHasEdge と同じゲート＝出口エッジのある買いシグナル
    （いずれかの保有期間で勝率52%以上＆平均プラス）。業種の✅買い候補数の集計に使う。"""
    keys: set = set()
    if not track or not track.get("available"):
        return keys
    hs = track.get("horizons", [5, 10, 20])
    for k, g in (track.get("groups") or {}).items():
        best = None
        for h in hs:
            x = (g.get("h") or {}).get(str(h))
            if x and x.get("n"):
                if best is None or x["win"] > best["win"]:
                    best = x
        if best and best["win"] >= 52 and best["avg"] > 0:
            keys.add(k)
    return keys


def build_sector_heat(rows: list[dict], track: dict,
                      min_members: int = 5, top_n: int = 12) -> dict | None:
    """33業種ごとに 熱(=所属銘柄の指数の中央値)・広がり(=1ヶ月プラス率)・✅買い候補数 を集計し、
    熱の高い順にランキング。各業種に上位 top_n 銘柄を同梱（タップで展開＝ドリルダウン用）。
    熱い=今その業種に資金が来ている/過熱の記述子であって買い推奨ではない（買いは✅印を参照）。"""
    try:
        with open(PARENT / "sector33_map.json", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"[build] 業種マップ読込スキップ（業種熱なし）: {e}")
        return None
    sec_of = {str(k)[:4]: v for k, v in raw.items()}
    edge = _edge_signal_keys(track)
    from collections import defaultdict
    buckets: dict[str, list] = defaultdict(list)
    for r in rows:                       # rows=流動性ユニバース（採点・シグナル対象）
        sec = sec_of.get(r["code"])
        if sec:
            buckets[sec].append(r)
    sectors = []
    for sec, mem in buckets.items():
        if len(mem) < min_members:
            continue
        heat = sum(x["momentum"] for x in mem) / len(mem)   # 平均指数（業種の勢い）
        strong = sum(1 for x in mem if x["grade"] in ("S", "A"))  # 強い銘柄(指数60↑)
        breadth = round(100.0 * sum(1 for x in mem if x["r20"] > 0) / len(mem))
        buycand = sum(1 for x in mem if any(s in edge for s in (x.get("signals") or [])))
        mem.sort(key=lambda x: x["momentum"], reverse=True)
        top = [{"code": x["code"], "name": x["name"], "momentum": x["momentum"],
                "grade": x["grade"], "price": x["price"], "r1": x["r1"], "r5": x["r5"],
                "signals": x.get("signals") or []} for x in mem[:top_n]]
        sectors.append({"sector": sec, "n": len(mem), "heat": round(heat, 1),
                        "strong": strong, "breadth": breadth, "buycand": buycand, "top": top})
    # ✅検証済み買い候補の多い順（同数は平均指数=勢いで）。33業種は大箱で平均指数は団子に
    # なりがちなので、アプリのエッジ優先思想に合わせ「今いちばん買える業種」を上に出す。
    sectors.sort(key=lambda s: (s["buycand"], s["heat"]), reverse=True)
    return {
        "sectors": sectors,
        "top_n": top_n,
        "note": ("各業種の『熱（勢い）』＝所属銘柄の指数（勢いスコア）の平均。広がり＝1ヶ月プラスの銘柄割合。"
                 "✅＝検証済み買い候補の数。熱い業種＝いまその業種に資金が来ている/過熱の目安であって"
                 "買い推奨ではありません（買いは各銘柄の✅検証済み買い候補を参照）。"),
    }


def build() -> dict:
    if SOURCE == "jquants_cache":
        data, name_map, data_date, seg_map = load_jquants_cache()
    elif SOURCE == "jquants_api":
        data, name_map, data_date, seg_map = load_jquants_api()
    else:
        raise NotImplementedError(f"KABUAI_DATA_SOURCE={SOURCE} は未実装（jquants_cache / jquants_api）")

    t0 = time.time()
    # 業種マップ（33業種）を先読み。買い候補の分散キャップ＋「なぜ買い」表示用に各rowへ付与。
    try:
        with open(PARENT / "sector33_map.json", encoding="utf-8") as _sf:
            _sec_raw = json.load(_sf)
        SEC_OF = {str(k)[:4]: v for k, v in _sec_raw.items()}
    except Exception as _e:
        print(f"[build] 業種マップ先読みスキップ（分散キャップは無効化）: {_e}")
        SEC_OF = {}
    rows: list[dict] = []
    rows_illiq: list[dict] = []       # 低流動（検索/詳細にのみ載せる・採点/シグナル対象外）
    scored_tickers: list[str] = []
    n_skip = 0
    for ticker, df in data.items():
        ind = indicators(df)
        if ind is None:
            n_skip += 1
            continue
        target = rows
        if ind["turnover"] < MIN_TURNOVER:
            target = rows_illiq
        else:
            scored_tickers.append(ticker)
        target.append({
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
            "turnover_oku": round(ind["turnover"] / 1e8, 1),  # 売買代金(億円/日)。ピック床の絞りに使う
            "rng20": ind["rng20"],                            # 値動き%/日。ピック天井(宝くじ株除外)に使う
            "sector": SEC_OF.get(ticker.replace(".T", "")),   # 33業種（買い候補の分散キャップ用）
            "mom_hist": ind["mom_hist"],
            # 🔻売り（モメンタム終了）判定用（v4・2026-07-18）
            "ma5_dev": ind["ma5_dev"],
            "below5": ind["below5"],
            "down_candle": ind["down_candle"],
            "vol_x": ind["vol_x"],
            "off_peak20": ind["off_peak20"],
            "runup20": ind["runup20"],
        })

    rows.sort(key=lambda x: x["momentum"], reverse=True)
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    for r in rows_illiq:
        r["rank"] = None              # 低流動はランキング対象外（順位なし）

    grade_counts: dict[str, int] = {}
    for r in rows:
        grade_counts[r["grade"]] = grade_counts.get(r["grade"], 0) + 1

    # ── フェーズ3: シグナル判定（全 scored を走査） ──
    signals = sig.detect(rows)
    sig_counts = {k: signals["groups"][k]["count"] for k in signals["order"]}

    # ── 先物連動度タグ（2026-07-02 追加仕様・correlation.py） ──
    # 買い系シグナル点灯銘柄に、先物(既定=日経レバ 1570.T)との5分足リターン相関を付与。
    # 高連動=先物依存(押し目→反発の型が壊れやすい・注意) / 自力=自分の需給で動く(型が
    # 出やすい・狙い目)。yfinance が落ちていてもビルドは止めない（タグ無し=フロント非表示）。
    FUTURES_SIGNALS = ("strong_accum", "accum", "strong_reversal", "reversal")
    futures_meta = None
    try:
        f_codes = [r["code"] for r in rows
                   if any(k in FUTURES_SIGNALS for k in (r.get("signals") or []))]
        f_tags = correlation.tag_codes(f_codes) if f_codes else {}
        for r in rows:
            t = f_tags.get(r["code"])
            if t:
                r["futures_corr"] = t["futures_corr"]
                r["futures_tag"] = t["futures_tag"]
        for g in signals["groups"].values():   # members は detect 時のコピーなので別途反映
            for m in g["members"]:
                t = f_tags.get(m["code"])
                if t:
                    m["futures_corr"] = t["futures_corr"]
                    m["futures_tag"] = t["futures_tag"]
        futures_meta = {"bench": correlation.BENCHMARK_CODE,
                        "th_high": correlation.TH_HIGH, "th_mid": correlation.TH_MID,
                        "window": correlation.WINDOW,
                        "targets": len(f_codes), "tagged": len(f_tags)}
        print(f"[build] 先物連動タグ: 対象{len(f_codes)}銘柄 → 付与{len(f_tags)} "
              f"(bench={correlation.BENCHMARK_CODE})")
    except Exception as e:
        print(f"[build] 先物連動タグはスキップ（非致命）: {e}")

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
    # 広がり日の実績ヒストリー（make_rebound_history.py で手元pklから月1事前計算・コミット）。
    # 無ければ単に出ない（CI単独でも壊れない・track_longterm と同じ運用）。
    rh_path = HERE / "rebound_history.json"
    if rh_path.exists():
        try:
            with open(rh_path, encoding="utf-8") as f:
                rh = json.load(f)
            if rh.get("threshold") == REBOUND_B and rh.get("recent"):
                rebound["history"] = rh
                print(f"[build] リバウンド履歴を注入: 広がり日{rh['summary']['days']}日 ({rh['generated']}時点)")
        except Exception as e:
            print(f"[build] リバウンド履歴読込スキップ: {e}")

    # ── フェーズ12: テーマ爆益（blast）セクション ──
    # 親リポのテーマトラッカーが毎朝コミットする dashboard_data.json から blast スコアを
    # 取り込む。検証(bt_exit/bt_exit_regime 2026-06-01): blast>=70 ×「翌寄り買い→8営業日
    # 保有・利確なし・SL-12%」= +2.36%/件・PF1.62(損切りなし理論値 +3.03%/PF1.84・n1481)。
    # バーベル型(勝率53.7%)・検証期間2025-04〜2026-05(1.1年)・下げ相場ではシグナル自然沈黙。
    # 朝ビルド(7時)はダッシュボード生成(8時台)より早いため、テーマ日付は1営業日古いことが
    # ある=セクションに日付を明示して正直に出す。無ければ非表示(非致命)。
    theme_blast = None
    tb_path = HERE.parent / "dashboard_data.json"
    if tb_path.exists():
        try:
            with open(tb_path, encoding="utf-8") as f:
                tb = json.load(f)
            members = []
            for s in tb.get("stocks", []):
                bl = s.get("blast")
                if bl is None or bl < 50:        # 50未満は載せない(本命70+/参考50-69)
                    continue
                members.append({
                    "code": str(s.get("ticker", "")).replace(".T", ""),
                    "name": s.get("name", ""),
                    "theme": s.get("theme", ""),
                    "blast": round(float(bl), 1),
                    "heat": s.get("theme_heat"),
                    "dev": s.get("dev"),
                    "r20": round(float(s.get("r20", 0)) * 100, 1) if s.get("r20") is not None else None,
                    "vr": s.get("vr"),
                    "price": s.get("close"),
                    "overext": bool(s.get("overextended")),
                })
            members.sort(key=lambda x: x["blast"], reverse=True)
            theme_blast = {
                "date": tb.get("date"),
                "threshold": 70,
                "plan": {"hold": 8, "sl": 12, "tp": None,
                         "text": "翌寄り買い → 約8営業日保有・利確なし(勝ち馬は走らせる)・損切り-12%(保険)"},
                "stats": {"win": 53.7, "avg": 2.36, "pf": 1.62, "stop_rate": 22,
                          "avg_nostop": 3.03, "pf_nostop": 1.84, "n": 1481,
                          "period": "2025-04〜2026-05"},
                "members": members[:24],
                "n_hot": sum(1 for m in members if m["blast"] >= 70),
                "note": ("テーマ銘柄(約140)のうち「もう走っている×超ホットなテーマ×大商い」の継続型スコア。"
                         "勝率53%で裾の大化けを取りに行くバーベル型＝損切り-12%の厳守が前提。"
                         "下げ相場では条件が同時成立しにくくシグナルが自然に減る(内蔵プロテクション)。"),
            }
            print(f"[build] テーマ爆益: {tb.get('date')} 時点 blast50+={len(members)} / 70+={theme_blast['n_hot']}")
        except Exception as e:
            print(f"[build] テーマ爆益読込スキップ: {e}")

    # ── フェーズ13: 今日のセクターローテ（当日寄りで執行するシグナルの取り込み） ──
    # 親リポの main_sector_theme.py が毎朝8:25頃にコミットする today_signals_sector_theme.json
    # を表示する（Discord配信は停止済み=アプリ集約）。朝8:40の追加ビルドで当日分を寄り前に公開。
    # 7時ビルド時点では前営業日の断面になるため is_today フラグで正直に区別する。
    # BT実績: BUY=スイング条件×(セクター上位50% OR テーマ語) PF1.31/+459%、
    #         SELL=最弱セクター急騰売り PF1.37/+45.9%/全5年プラス(1日1件上限)。
    sector_today = None
    st_path = HERE.parent / "today_signals_sector_theme.json"
    if st_path.exists():
        try:
            with open(st_path, encoding="utf-8") as f:
                stj = json.load(f)
            today_jst_str = datetime.now(JST).strftime("%Y-%m-%d")
            def _st_row(s, sell=False):
                r = {"code": str(s.get("ticker", "")).replace(".T", ""),
                     "name": s.get("name", ""), "rsi": s.get("rsi"),
                     "dev": s.get("deviation"), "price": s.get("prev_close"),
                     "turnover_oku": round(float(s.get("turnover_oku", 0)), 0),
                     "sector": s.get("sector", "")}
                if sell:
                    r["day_change"] = s.get("day_change")
                else:
                    r["in_theme"] = bool(s.get("in_theme"))
                return r
            sector_today = {
                "date": stj.get("date"),
                "is_today": stj.get("date") == today_jst_str,
                "buy": [_st_row(s) for s in stj.get("signals", [])],
                "sell": [_st_row(s, sell=True) for s in stj.get("sell_signals", [])],
                "plan_buy": "当日9:00 寄り成行で買い → 損切り-3%/利確+5%のOCO・RSI50回復か3日目の大引けで手仕舞い",
                "plan_sell": "当日9:00 寄り成行で空売り → 損切り+3%/利確-5%・RSI50か3日目大引けで買い戻し",
                "stats": {"buy": {"pf": 1.31, "cum": 459}, "sell": {"pf": 1.37, "cum": 45.9, "pos_years": "5/5"}},
            }
            print(f"[build] セクターローテ: {stj.get('date')} 分 BUY{len(sector_today['buy'])}/SELL{len(sector_today['sell'])} "
                  f"(is_today={sector_today['is_today']})")
        except Exception as e:
            print(f"[build] セクターローテ読込スキップ: {e}")

    # ── v4(2026-07-18): 🔻売り・モメンタム終了検出 ──
    # 「直近1ヶ月走った銘柄の上昇が終わった」をEODで検出する情報タブ（空売り推奨ではない・
    # デイトレ化しない=本人確定指示）。実データ検証: 高速7504/ベクトル6058は7/16夕方時点で
    # 検出→翌7/17も続落。条件: runup20≥+15%（モメンタム銘柄だった）× 終値5MA割れ ×
    # （陰線 or 前日比-2%以下）× 出来高が20日平均の1.3倍以上（=下げで商い増・資金抜け）。
    SELL_RUNUP_MIN = 15.0
    SELL_VOLX_MIN = 1.3
    SELL_TURNOVER_MIN_OKU = 5.0   # 売買代金5億以上（板の薄い銘柄のノイズ排除）
    sell_members = []
    for r in rows:
        try:
            if (r.get("runup20") is not None and r["runup20"] >= SELL_RUNUP_MIN
                    and r.get("ma5_dev") is not None and r["ma5_dev"] < 0
                    and (r.get("down_candle") or (r.get("r1") is not None and r["r1"] <= -2.0))
                    and r.get("vol_x") is not None and r["vol_x"] >= SELL_VOLX_MIN
                    and (r.get("turnover_oku") or 0) >= SELL_TURNOVER_MIN_OKU
                    and (r.get("price") or 0) >= 100):
                sell_members.append({
                    "code": r["code"], "name": r["name"], "price": r["price"],
                    "r1": r["r1"], "off_peak20": r["off_peak20"], "ma5_dev": r["ma5_dev"],
                    "vol_x": r["vol_x"], "below5": r["below5"], "runup20": r["runup20"],
                    "turnover_oku": r["turnover_oku"], "sector": r.get("sector"),
                })
        except Exception:
            continue
    # 初日(below5==1)を先頭に、次いで出来高倍率順＝「今日崩れた」が一番上
    sell_members.sort(key=lambda m: (0 if m["below5"] <= 1 else 1, -(m["vol_x"] or 0)))
    sell_members = sell_members[:30]
    # 掲載銘柄は詳細チャート(日足+5MA)を必ず見られるようにエクスポート対象へフラグ
    # （崩れ銘柄はランキング上位でもシグナル点灯でもないことが多く、素通しだとチャートなしになる）
    _sell_codes = {m["code"] for m in sell_members}
    for r in rows:
        if r["code"] in _sell_codes:
            r["sell_end"] = True

    # ── 信用データバッジ（スタンダードプラン2026-07-18開通・非致命） ──
    # 買残急増=イナゴ玉滞留・日々公表指定=規制近接は、どちらもモメンタム終焉の
    # 定番サイン。掲載30銘柄だけAPIで引く(≤60コール)。失敗/プラン外は無言スキップ。
    try:
        _tok = _jquants_key()
        _m_from = (datetime.now(JST) - timedelta(days=45)).strftime("%Y-%m-%d")
        _a_from = (datetime.now(JST) - timedelta(days=10)).strftime("%Y-%m-%d")
        for m in sell_members:
            try:
                mi = _jquants_get("/markets/margin-interest", _tok,
                                  {"code": m["code"], "from": _m_from}).get("data", [])
                if len(mi) >= 2:
                    mi = sorted(mi, key=lambda r: r["Date"])
                    last, prev = mi[-1], mi[0]
                    lv, pv = float(last.get("LongVol") or 0), float(prev.get("LongVol") or 0)
                    if pv > 0:
                        m["margin_chg"] = round((lv / pv - 1) * 100, 1)   # 買残の約4-6週変化率%
                al = _jquants_get("/markets/margin-alert", _tok,
                                  {"code": m["code"], "from": _a_from}).get("data", [])
                m["margin_alert"] = bool(al)                              # 日々公表銘柄に指定中か
                time.sleep(0.15)
            except Exception:
                continue
        n_badge = sum(1 for m in sell_members if m.get("margin_chg") is not None or m.get("margin_alert"))
        print(f"[build] 信用バッジ: {n_badge}/{len(sell_members)}銘柄に付与")
    except Exception as _e:
        print(f"[build] 信用バッジはスキップ（非致命）: {_e}")

    # ── 🪶信用軽バッジ（🐵ランキング・2026-07-20・_bt_margin_momentum.py 全銘柄10年BT） ──
    # 実証された構造は「買残が重いと悪い」ではなく「信用買残がほぼ無い(買残回転<0.25日)
    # 銘柄だけが上昇継続しやすい」: 両期間(2017-21/2022-26)×代金3分位の全6セルで軽い側が
    # プラス差・2019年以降は8年中7年プラス。重い側への警告は符号不安定=根拠なしなので
    # バッジは軽い側にだけ付ける。週次残高スナップショット1本で全銘柄分を引ける・非致命。
    DC_LIGHT_MAX = 0.25
    try:
        _tok = _jquants_key()
        _snap = None
        for _back in range(3, 16):     # 直近の週次残高(金曜キー・翌週火曜公表)を遡って探す
            _ds = (datetime.now(JST) - timedelta(days=_back)).strftime("%Y-%m-%d")
            _mi = _jquants_get("/markets/margin-interest", _tok, {"date": _ds}).get("data", [])
            if _mi:
                _snap = {str(_r.get("Code", ""))[:4]: _r for _r in _mi}
                print(f"[build] 🪶信用軽: 週次残高 {_ds} ({len(_snap)}銘柄)")
                break
            time.sleep(0.2)
        if _snap:
            _n_light = 0
            for r in rows[:TOP_N]:
                if (r.get("momentum") or 0) < 60:      # BTの実証範囲=モメンタム点灯(S/A)のみ
                    continue
                _rec = _snap.get(r["code"])
                if _rec is None:
                    continue                           # 残高データなし→バッジ判定しない(fail-safe)
                _lv = _rec.get("LongVol")
                _adv = (r.get("turnover_oku") or 0) * 1e8
                _px = r.get("price") or 0
                if _lv is None or _adv <= 0 or _px <= 0:
                    continue
                _dc = float(_lv) * _px / _adv
                r["days_cover"] = round(_dc, 2)
                if _dc < DC_LIGHT_MAX:
                    _n_light += 1
            print(f"[build] 🪶信用軽: ランキング{TOP_N}銘柄中 {_n_light}件 (回転<{DC_LIGHT_MAX}日)")
    except Exception as _e:
        print(f"[build] 🪶信用軽はスキップ（非致命）: {_e}")
    sell_watch = {
        "date": data_date,
        "members": sell_members,
        "count": len(sell_members),
        "cond": {"runup20": SELL_RUNUP_MIN, "vol_x": SELL_VOLX_MIN,
                 "turnover_oku": SELL_TURNOVER_MIN_OKU},
        "note": ("直近1ヶ月で+15%以上走った銘柄が「終値5MA割れ×陰線×出来高増」で崩れた日を検出。"
                 "上昇モメンタムの終わりサイン＝保有者の出口検討・高値づかみ回避の参考情報。"
                 "空売りの推奨ではありません。"),
    }
    print(f"[build] 🔻売り(モメンタム終了): {len(sell_members)}件 (掲載{min(len(sell_members),30)})")

    # ── 🩳 デイトレ売り（フェード）・2026-07-23 本人指示でアプリ掲載 ──
    # Discord配信(daytrade_paper.daily_top_fades)と同一ロジックを直接import＝判定のズレなし。
    # 貸借○ × 張り付き除外 × 前日+5%↑、GO=+12%↑（BT: 2022-2026 632件 勝率54.3%
    # 平均+0.71%/件 PF1.35・寄指不成立/年別貸借まで再現・fade_ladder_bt.py）。
    # 売り禁(margin-alert)は除外せず🚫バッジ=同日本人指示「ハイカラで売れた」(制度✕でも
    # SBI一日信用HYPER/一般信用は別枠在庫)。翌営業日「寄り売り→引け買戻し」の当日完結。非致命。
    fade: dict = {"date": data_date, "picks": [], "banned": 0, "go": 0}
    try:
        if str(PARENT) not in sys.path:
            sys.path.insert(0, str(PARENT))
        import daytrade_paper as _dp
        _tok2 = _jquants_key()
        _iss = _dp.fetch_iss_map(_tok2)
        _ratio = _dp.fetch_ratio_map(_tok2)
        _alert = _dp.fetch_alert_map(_tok2)
        _banned: list = []
        # data_date終値を「信号日」にする（<比較なので+1日。朝build=今日の寄り用/夜build=翌営業日用）
        _fade_today = (datetime.strptime(data_date, "%Y-%m-%d") + timedelta(days=1)).date()
        _picks = (_dp.daily_top_fades(data, _fade_today, _iss, ratio_map=_ratio,
                                      alert_map=_alert, excluded_out=_banned) if _iss else [])
        for p in _picks:
            _c4 = str(p["ticker"]).replace(".T", "")[:4]
            _nm = name_map.get(p["ticker"]) or name_map.get(_c4) or p.get("name") or _c4
            fade["picks"].append({
                "code": _c4, "name": _nm, "gain": p.get("daily_gain"),
                "vol_ratio": p.get("vol_ratio"), "range_pct": p.get("range_pct"),
                "min_entry": p.get("min_entry_price"),
                "short_mark": (p.get("short") or {}).get("mark", "?"),
                "borrow": p.get("borrow", ""), "reg_note": p.get("reg_note", ""),
                "jsf_stop": bool(p.get("jsf_stop")),
                "verdict": p.get("verdict"), "nogo_reason": p.get("nogo_reason", ""),
            })
            for r in rows:                      # 詳細チャートを必ず出せるよう export 対象化
                if r["code"] == _c4:
                    r["fade_pick"] = True
                    break
        fade["banned"] = len(_banned)
        fade["go"] = sum(1 for p in fade["picks"] if p.get("verdict") == "GO")
        fade["go_min"] = _dp.DAILY_PICK_GAIN_MIN
        fade["stats"] = {"n": 632, "win": 54.3, "avg": 0.71, "pf": 1.35,
                         "period": "2022-2026/07", "y2026_avg": 1.59}
        print(f"[build] 🩳デイトレ売り(フェード): picks{len(fade['picks'])} "
              f"GO{fade['go']} 売り禁🚫{sum(1 for p in fade['picks'] if p.get('jsf_stop'))}")
    except Exception as _e:
        print(f"[build] 🩳フェードはスキップ（非致命）: {_e}")
        fade = {"date": data_date, "picks": [], "banned": 0, "go": 0, "error": True}
    sell_watch["fade"] = fade

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
    ai_targets = [r for r in rows if r["rank"] <= EXPORT_TOP or r.get("signals") or r.get("sell_end") or r.get("fade_pick")]
    ai_meta = ai_summary.annotate(ai_targets)

    # ── 銘柄探検（探索用スクリーナー・2026-07-02追加） ──
    # ストップ高/初動/上昇中/押し目/短期反発をカテゴリ化して data/explorer.json に出力。
    # ロジックは explorer_signals.py 1ファイル・閾値は explorer_config.json。
    # ✅買い候補（BT検証済み）とは独立の探索機能なので、失敗してもビルドは止めない。
    try:
        import explorer_signals
        exp = explorer_signals.build_explorer(data, name_map, data_date)
        with open(DATA_DIR / "explorer.json", "w", encoding="utf-8") as f:
            json.dump(exp, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        print(f"[build] 銘柄探検: counts={exp['counts']} / ランキング{len(exp['ranking']['items'])}件 "
              f"/ 長期イベント={'あり' if exp['ranking']['longterm']['available'] else 'なし'}")
    except Exception as e:
        print(f"[build] 銘柄探検はスキップ（非致命）: {e}")

    disclaimer = "価格・指標はAI等で整理・加工した参考表示です。リアルタイム配信ではありません。投資判断は自己責任。"
    # ── フェーズ5: 詳細チャートJSON書き出し ──
    n_charts = export_stocks(data, rows, data_date, disclaimer)
    # ── 銘柄検索インデックス（全scored・遅延fetch用） ──
    n_search = export_search_index(rows, data_date, rows_illiq)
    print(f"[build] 検索インデックス: 採点{len(rows)} + 低流動{len(rows_illiq)} 銘柄（低流動は検索/詳細のみ）")

    try:
        data_lag_days = (datetime.now(JST).date() - datetime.strptime(data_date, "%Y-%m-%d").date()).days
    except Exception:
        data_lag_days = None

    # ── 業種別の熱（33業種ブラウズ: 熱ランキング→タップで銘柄ドリルダウン） ──
    sector_heat = build_sector_heat(rows, track)
    if sector_heat:
        print(f"[build] 業種熱: {len(sector_heat['sectors'])}業種 "
              f"(1位 {sector_heat['sectors'][0]['sector']} 熱{sector_heat['sectors'][0]['heat']})")

    # ④ 買い候補の「最近の調子」メーター。CIで毎日 fresh 計算する（毎日更新＝冷え/回復を
    # リアルタイム反映）。load_jquants_api は master でETF除外済みなので skip=None でよい。
    # 失敗時はコミット済 picks_scoreboard.json（make_picks_scoreboard.pyの手元長期版）へフォールバック。
    picks_scoreboard = None
    try:
        import picks_scoreboard as _psb
        picks_scoreboard = _psb.compute(data, name_map, SEC_OF, data_date)
        if picks_scoreboard:
            rc = picks_scoreboard.get("recent", {})
            print(f"[build] 調子メーター(毎日fresh計算): regime={picks_scoreboard.get('regime')} "
                  f"直近{picks_scoreboard.get('recent_weeks')}週 勝率{rc.get('win')}%/平均{rc.get('avg')}%")
    except Exception as e:
        print(f"[build] 調子メーターfresh計算スキップ→コミット済JSONへ: {e}")
    if not picks_scoreboard:
        ps_path = HERE / "picks_scoreboard.json"
        if ps_path.exists():
            try:
                with open(ps_path, encoding="utf-8") as f:
                    picks_scoreboard = json.load(f)
                print(f"[build] 調子メーター(コミット済JSON fallback): regime={picks_scoreboard.get('regime')}")
            except Exception as e:
                print(f"[build] スコアボードJSON読込も失敗: {e}")

    top = rows[:TOP_N]
    out = {
        "schema": "kabuai-phase14",  # phase14 = 先物連動タグ + v2フロント（2026-07-02）
        "data_date": data_date,
        "data_lag_days": data_lag_days,
        "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),   # JST表示（runnerはUTC）
        "source": SOURCE,
        "disclaimer": disclaimer,
        "universe_total": len(data),
        "universe_scored": len(rows),
        "min_turnover_yen": MIN_TURNOVER,
        "pick_min_oku": round(PICK_MIN_TURNOVER / 1e8, 1),  # v3: ✅買い候補の表示床（10億）
        "pick_max_rng": PICK_MAX_RNG,  # 値動き天井（%/日）: これ以上動く宝くじ株は買い候補に出さない

        "grade_counts": grade_counts,
        "stock_charts": n_charts,
        "search_index": n_search,
        "ai": ai_meta,
        "market": market,
        "futures": futures_meta,
        "rebound": rebound,
        "sell_watch": sell_watch,
        "picks_scoreboard": picks_scoreboard,   # ④ 直近の調子メーター
        "theme_blast": theme_blast,
        "sector_today": sector_today,
        "sector_heat": sector_heat,
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
            json.dump(out, f, ensure_ascii=False, indent=2, allow_nan=False)
    print(f"[build] 出力: {dated.name} / {latest.name}  (データ日付 {out['data_date']})")
    print("[build] TOP5 プレビュー:")
    for r in out["ranking"][:5]:
        print(f"   #{r['rank']:<2} {r['name']:<14} {r['code']} ¥{r['price']:<8} "
              f"指数{r['momentum']:>5} [{r['grade']}] SR{r['sr']:>5} POW{r['power']:>5} "
              f"5日{r['r5']:>+6}% RSI{r['rsi']:>5}")


if __name__ == "__main__":
    main()

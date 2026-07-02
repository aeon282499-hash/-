"""
explorer_signals.py — 銘柄探検スクリーナーの検知ロジック（1ファイル集約・2026-07-02）

ユーザー要望: 「シグナル条件のロジックは1ファイルにまとめて、後から閾値を
検証・調整しやすく」。閾値はすべて explorer_config.json に外出し。

機能:
  Phase1: ストップ高検知（値幅制限テーブルで当日S高/張り付きを検出・日付付き履歴）
  Phase2: 初動検知（75日線上抜け×出来高急増×高値ブレイク×長期低迷レジーム）
          ＋状態遷移（初動→上昇中→押し目）＋初動待ち＋上昇ランキング
  Phase3: 短期反発候補（急騰のフィボナッチ押し38.2-50%ゾーン・S高/下げ止まり/N字タグ）

正直性の注意（UIにも明示）:
  - ここは「探索用スクリーナー」。✅今日の買い候補（BT検証済み）とは別物で、
    各カテゴリの優位性は未検証。勝率等の数字は出さない。
  - 株価は調整後(Adj)のため、直近に分割があった銘柄はストップ高判定が
    ズレることがある（値幅制限は生値基準のため）＝近似検出。
  - データはEOD（1日遅れ）。「当日」とはデータ日付のこと。
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent

# ── 東証の値幅制限テーブル（基準値段=前日終値 → 制限値幅） ──
PRICE_LIMITS = [
    (100, 30), (200, 50), (500, 80), (700, 100), (1000, 150),
    (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
    (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000),
    (70000, 10000), (100000, 15000), (150000, 30000), (200000, 40000),
    (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000),
    (1500000, 300000), (2000000, 400000), (3000000, 500000), (5000000, 700000),
    (7000000, 1000000), (10000000, 1500000), (15000000, 3000000),
    (20000000, 4000000), (30000000, 5000000), (50000000, 7000000),
    (float("inf"), 10000000),
]


def load_config() -> dict:
    with open(HERE / "explorer_config.json", encoding="utf-8") as f:
        return json.load(f)


def limit_width(prev_close: float) -> float:
    """前日終値から当日の制限値幅（ストップ高までの幅）を返す。"""
    if prev_close is None or not np.isfinite(prev_close) or prev_close <= 0:
        return np.nan
    for band, width in PRICE_LIMITS:
        if prev_close < band:
            return float(width)
    return float(PRICE_LIMITS[-1][1])


# ═══ Phase1: ストップ高検知 ═══════════════════════════════════

def stop_high_flags(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """各日について ストップ高到達(reached) / 張り付き(stuck=終値も上限) を判定。
    調整後株価のため上限に tol_pct% の許容を持たせた近似検出。"""
    tol = float(cfg["stop_high"]["tol_pct"]) / 100.0
    prev = df["Close"].shift(1)
    width = prev.map(limit_width)
    limit = prev + width
    thresh = limit * (1 - tol)
    reached = (df["High"] >= thresh) & prev.notna() & (df["High"] > prev)
    stuck = reached & (df["Close"] >= thresh)
    return pd.DataFrame({"reached": reached, "stuck": stuck}, index=df.index)


# ═══ Phase2: 初動検知・状態遷移・ランキング ═══════════════════

def _shodo_frame(df: pd.DataFrame, cfg: dict) -> pd.DataFrame | None:
    """初動判定に使う中間系列をまとめて返す（イベント走査と初動待ちで共用）。"""
    s = cfg["shodo"]
    if len(df) < s["ma_long"] + s["regime_lookback"] + 2:
        return None
    c, h, v = df["Close"], df["High"], df["Volume"]
    ma = c.rolling(s["ma_long"]).mean()
    above = c > ma
    volr = v / v.shift(1).rolling(s["vol_avg_days"]).mean()          # 出来高/過去20日平均(当日除く)
    hh = h.shift(1).rolling(s["high_break_days"]).max()              # 直近高値(当日除く)
    below_frac = (~above).shift(1).rolling(s["regime_lookback"]).mean()  # 直前に線の下にいた割合
    return pd.DataFrame({
        "close": c, "ma": ma, "above": above,
        "cross": above & (~above.shift(1).fillna(False)),
        "vol_ok": volr >= s["vol_ratio_min"], "volr": volr,
        "brk": h > hh, "hh": hh,
        "regime": below_frac >= s["regime_below_ma_min"],
    }, index=df.index)


def shodo_events(df: pd.DataFrame, cfg: dict) -> list[dict]:
    """初動イベント（date/price=当日終値）を全履歴から検出。"""
    f = _shodo_frame(df, cfg)
    if f is None:
        return []
    hit = f["cross"] & f["vol_ok"] & f["brk"] & f["regime"]
    out = []
    for ts in f.index[hit.fillna(False)]:
        out.append({"date": ts.strftime("%Y-%m-%d"),
                    "price": round(float(f.at[ts, "close"]), 1),
                    "volr": round(float(f.at[ts, "volr"]), 1)})
    return out


def shodo_wait_today(df: pd.DataFrame, cfg: dict) -> dict | None:
    """初動待ち＝条件にあと一歩（75日線の下だが -ma_near_pct% 以内、他条件を
    min_conditions 個以上満たす）。最新日で判定。"""
    f = _shodo_frame(df, cfg)
    if f is None or len(f) == 0:
        return None
    w = cfg["shodo_wait"]
    last = f.iloc[-1]
    if not np.isfinite(last["ma"]) or bool(last["above"]):
        return None
    gap = float(last["close"] / last["ma"] - 1) * 100          # 負の値（線の下）
    if gap < -float(w["ma_near_pct"]):
        return None
    conds = [bool(np.isfinite(last["volr"]) and last["volr"] >= w["vol_ratio_min"]),
             bool(np.isfinite(last["hh"]) and df["High"].iloc[-1] >= last["hh"] * 0.97),
             bool(last["regime"])]
    if sum(conds) < int(w["min_conditions"]):
        return None
    return {"ma_gap_pct": round(gap, 1), "conds": sum(conds)}


def classify_state(df: pd.DataFrame, event: dict, cfg: dict) -> dict | None:
    """直近の初動イベント後の状態。None=状態終了（カテゴリ外）。
    上昇中: 高値からの押しが浅い（トレンド継続中）
    押し目: 初動株価→高値のフィボ押し fib_lo〜fib_hi% の範囲で調整中"""
    st, rb = cfg["state"], cfg["rebound"]
    ed = pd.Timestamp(event["date"])
    seg = df[df.index >= ed]
    if len(seg) == 0:
        return None
    close_now = float(df["Close"].iloc[-1])
    price0 = float(event["price"])
    ma_long = df["Close"].rolling(cfg["shodo"]["ma_long"]).mean().iloc[-1]
    if close_now < price0 * (1 - float(st["fail_below_shodo_pct"]) / 100):
        return None                                    # 初動が否定された
    if np.isfinite(ma_long) and close_now < float(ma_long):
        return None                                    # 長期線割れ＝状態終了
    hi = float(seg["High"].max())
    denom = hi - price0
    pull = (hi - close_now) / denom * 100 if denom > 0 else 0.0
    if pull < float(st["rising_pullback_max"]):
        state = "rising"
    elif pull <= float(rb["fib_hi"]):
        state = "oshime"
    else:
        return None
    return {"state": state, "pullback_pct": round(pull, 1), "hi": round(hi, 1),
            "days": int(len(seg))}


def _spark(df: pd.DataFrame, since: pd.Timestamp | None, points: int) -> list:
    """ミニチャート用の終値列（間引き・丸め）。sinceの5本前から現在まで。"""
    c = df["Close"]
    if since is not None:
        pos = c.index.searchsorted(since)
        c = c.iloc[max(0, pos - 5):]
    c = c.dropna()
    if len(c) > points:
        idx = np.linspace(0, len(c) - 1, points).round().astype(int)
        c = c.iloc[idx]
    return [round(float(x), 1) for x in c]


# ═══ Phase3: 短期反発候補（フィボナッチ押し目） ═══════════════

def rebound_today(df: pd.DataFrame, cfg: dict, sh: pd.DataFrame | None) -> dict | None:
    """直近の急騰（起点安値L→高値H が surge_min_gain_pct%以上・直近high_within_days日
    以内に高値）が、フィボ押し fib_lo〜fib_hi% で押し目形成中かを最新日で判定。"""
    rb = cfg["rebound"]
    need = rb["surge_lookback_days"] + rb["high_within_days"] + 5
    if len(df) < need:
        return None
    tail = df.iloc[-(rb["surge_lookback_days"] + rb["high_within_days"]):]
    # 高値H: 直近 high_within_days 日以内の最高値
    recent = tail.iloc[-rb["high_within_days"]:]
    hi_ts = recent["High"].idxmax()
    H = float(recent["High"].max())
    if hi_ts == tail.index[-1]:
        return None                                    # 今日が高値＝まだ押していない
    # 起点L: 高値以前 surge_lookback_days 日の最安値
    before = tail[tail.index <= hi_ts]
    L = float(before["Low"].min())
    lo_ts = before["Low"].idxmin()
    if not (L > 0 and H > L):
        return None
    gain = (H / L - 1) * 100
    if gain < float(rb["surge_min_gain_pct"]):
        return None
    close_now = float(df["Close"].iloc[-1])
    r = (H - close_now) / (H - L) * 100                # フィボ押し率（0=高値・100=起点）
    if not (float(rb["fib_lo"]) <= r <= float(rb["fib_hi"])):
        return None
    after = df[df.index > hi_ts]
    pb_low = float(after["Low"].min()) if len(after) else close_now
    pb_low_ts = after["Low"].idxmin() if len(after) else df.index[-1]

    tags = []
    # S高: 急騰の起点〜現在にストップ高あり
    if sh is not None:
        seg = sh[(sh.index >= lo_ts)]
        if bool(seg["reached"].any()):
            tags.append("S高")
    # 下げ止まり: 陽線反転 or 下ヒゲ + 出来高減少
    o, hgh, lw, cl, vol = (float(df[k].iloc[-1]) for k in ("Open", "High", "Low", "Close", "Volume"))
    rng = max(hgh - lw, 1e-9)
    wick = (min(o, cl) - lw) / rng
    vol_ma5 = float(df["Volume"].shift(1).rolling(5).mean().iloc[-1])
    vol_calm = np.isfinite(vol_ma5) and vol_ma5 > 0 and vol <= vol_ma5 * float(rb["sagedomari_vol_ratio_max"])
    yousen_at_low = cl > o and lw <= pb_low * 1.005
    if vol_calm and (yousen_at_low or wick >= float(rb["sagedomari_wick_min"])):
        tags.append("下げ止まり")
    # N字: 押し安値が2日以上前で、そこから反発再開（ただし高値未満）
    if (pb_low_ts < df.index[-1] and pb_low > 0
            and close_now >= pb_low * (1 + float(rb["nji_rise_from_low_pct"]) / 100)
            and close_now < H):
        tags.append("N字")

    zone = float(rb["zone_lo"]) <= r <= float(rb["zone_hi"])
    return {"fib_pct": round(r, 1), "in_zone": bool(zone), "tags": tags,
            "surge_gain_pct": round(gain, 1),
            "surge_from": lo_ts.strftime("%Y-%m-%d"), "surge_low": round(L, 1),
            "high_date": hi_ts.strftime("%Y-%m-%d"), "surge_high": round(H, 1)}


# ═══ 全体ビルド（毎日引け後バッチ＝build_data.py から呼ばれる） ═══

def _turnover_ok(df: pd.DataFrame, min_turnover: float) -> bool:
    t = (df["Close"] * df["Volume"]).tail(20).mean()
    return bool(np.isfinite(t) and t >= min_turnover)


def _r1(df: pd.DataFrame) -> float | None:
    c = df["Close"]
    if len(c) < 2 or c.iloc[-2] <= 0:
        return None
    return round(float(c.iloc[-1] / c.iloc[-2] - 1) * 100, 2)


def load_longterm() -> dict | None:
    """make_explorer_longterm.py が手元pklから事前計算・コミットした1年分の
    初動イベント（月1回更新・track_longterm と同じ運用）。無ければ非致命。"""
    p = HERE / "explorer_longterm.json"
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        return j if j.get("events") else None
    except Exception:
        return None


def build_explorer(data: dict, name_map: dict, data_date: str, cfg: dict | None = None) -> dict:
    """全銘柄を走査して探検カテゴリ＋ランキング＋S高履歴を生成する。"""
    cfg = cfg or load_config()
    sh_cfg, rk = cfg["stop_high"], cfg["ranking"]
    ref = pd.Timestamp(data_date)          # 基準日（この直近に足が無い銘柄=非アクティブは除外）

    # 長期初動イベント（手元pkl由来・月1コミット）。CI窓(200日)では初動判定に135本の
    # 履歴が要るため直近約65日しか検出できない＝65〜90日前のイベントの状態遷移
    # (上昇中/押し目)が消える。長期イベントを状態判定にもマージして取りこぼしを防ぐ。
    lt = load_longterm()
    lt_latest: dict[str, dict] = {}
    if lt:
        for e in lt["events"]:
            cur = lt_latest.get(e["code"])
            if cur is None or e["date"] > cur["date"]:
                lt_latest[e["code"]] = e

    def name_of(code: str) -> str:
        for k in (code, code + ".T"):
            if k in name_map:
                return str(name_map[k])
        return code

    cats = {k: [] for k in ("shodo", "shodo_wait", "rising", "oshime", "rebound", "stop_high")}
    sh_hist: dict[str, dict] = {}
    fresh_events: list[dict] = []          # CI窓で検出した初動（ランキング用）
    latest_event_by_code: dict[str, dict] = {}

    for ticker, df in data.items():
        if df is None or len(df) < 2:
            continue
        code = ticker.replace(".T", "")
        ts_last = df.index[-1]
        if (ref - ts_last).days > 5:
            continue                       # 上場廃止・売買停止などで足が止まっている銘柄
        price = float(df["Close"].iloc[-1])
        base = {"code": code, "name": name_of(code), "price": round(price, 1), "r1": _r1(df)}

        # ── ストップ高（全銘柄対象・直近 history_days 日の履歴も集計） ──
        sh = None
        try:
            tail = df.tail(int(sh_cfg["history_days"]) + 2)
            sh = stop_high_flags(tail, cfg)
            hit_days = sh[sh["reached"]]
            for ts, row in hit_days.iterrows():
                d = ts.strftime("%Y-%m-%d")
                rec = sh_hist.setdefault(d, {"date": d, "count": 0, "stuck": 0, "codes": []})
                rec["count"] += 1
                rec["stuck"] += int(bool(row["stuck"]))
                if len(rec["codes"]) < 60:
                    rec["codes"].append(code)
            recent = sh.iloc[-int(sh_cfg["recent_days"]):]
            if bool(recent["reached"].any()):
                ts_hit = recent[recent["reached"]].index[-1]
                cats["stop_high"].append({**base,
                    "date": ts_hit.strftime("%Y-%m-%d"),
                    "stuck": bool(sh.at[ts_hit, "stuck"]),
                    "today": bool(ts_hit == ts_last)})
        except Exception:
            pass

        # ── 初動系（流動性フィルタあり） ──
        try:
            if _turnover_ok(df, float(cfg["shodo"]["min_turnover"])):
                events = shodo_events(df, cfg)
                if events:
                    fresh_events.extend([{**e, "code": code, "name": base["name"]} for e in events])
                # 直近イベント＝CI窓の検出と長期イベントの新しい方（同日ならCI優先）
                last_e = events[-1] if events else None
                lte = lt_latest.get(code)
                if lte and (last_e is None or lte["date"] > last_e["date"]):
                    last_e = {"date": lte["date"], "price": lte["price"], "volr": lte.get("volr")}
                if last_e:
                    latest_event_by_code[code] = last_e
                    days_ago = int((ts_last - pd.Timestamp(last_e["date"])).days)
                    if days_ago <= int(cfg["shodo"]["fresh_days"]) + 2:
                        cats["shodo"].append({**base, "shodo_date": last_e["date"],
                                              "shodo_price": last_e["price"], "volr": last_e.get("volr")})
                    elif days_ago <= int(cfg["state"]["horizon_days"]):
                        stt = classify_state(df, last_e, cfg)
                        if stt:
                            cats["rising" if stt["state"] == "rising" else "oshime"].append(
                                {**base, "shodo_date": last_e["date"], "shodo_price": last_e["price"],
                                 "hi": stt["hi"], "pullback_pct": stt["pullback_pct"]})
                else:
                    w = shodo_wait_today(df, cfg)
                    if w:
                        cats["shodo_wait"].append({**base, **w})

            # ── 短期反発候補（Phase3） ──
            if _turnover_ok(df, float(cfg["rebound"]["min_turnover"])):
                rbi = rebound_today(df, cfg, sh)
                if rbi:
                    cats["rebound"].append({**base, **rbi,
                        "spark": _spark(df, pd.Timestamp(rbi["surge_from"]), int(rk["spark_points"]))})
        except Exception:
            continue

    # ── 上昇ランキング（CI窓の新鮮イベント＋手元pkl由来の長期イベントをマージ） ──
    merged: dict[tuple, dict] = {}
    for e in fresh_events:
        merged[(e["code"], e["date"])] = {"code": e["code"], "name": e["name"],
                                          "date": e["date"], "price": e["price"], "lt_max": None}
    lt_since = None
    if lt:
        lt_since = lt.get("since")
        for e in lt["events"]:
            key = (e["code"], e["date"])
            if key not in merged:
                merged[key] = {"code": e["code"], "name": e.get("name") or name_of(e["code"]),
                               "date": e["date"], "price": e["price"], "lt_max": e.get("max_high")}
    items = []
    horizon = pd.Timestamp(data_date) - pd.Timedelta(days=int(rk["windows_days"]["1y"]))
    for (code, date), e in merged.items():
        if pd.Timestamp(date) < horizon:
            continue
        df = data.get(code + ".T")
        if df is None or len(df) < 2 or (ref - df.index[-1]).days > 5:
            continue
        seg = df[df.index >= pd.Timestamp(date)]
        seg_max = float(seg["High"].max()) if len(seg) else None
        mx = max(x for x in (seg_max, e["lt_max"]) if x is not None) if (seg_max or e["lt_max"]) else None
        price0 = float(e["price"])
        if not mx or price0 <= 0:
            continue
        close_now = float(df["Close"].iloc[-1])
        items.append({"code": code, "name": e["name"], "shodo_date": date,
                      "shodo_price": round(price0, 1), "max_high": round(mx, 1),
                      "max_gain_pct": round((mx / price0 - 1) * 100, 1),
                      "cur_gain_pct": round((close_now / price0 - 1) * 100, 1),
                      "price": round(close_now, 1),
                      "spark": _spark(df, pd.Timestamp(date), int(rk["spark_points"]))})
    # 同一銘柄は最大上昇のイベント1件に集約 → 最大上昇率で降順。
    # 1年窓の上位だけだと直近3ヶ月のイベントがほぼ弾かれる（古参の大化けが上位を
    # 独占する）ため、3ヶ月窓の上位も別枠でマージしてフロントの期間トグルを成立させる。
    best: dict[str, dict] = {}
    for it in items:
        if it["code"] not in best or it["max_gain_pct"] > best[it["code"]]["max_gain_pct"]:
            best[it["code"]] = it
    allitems = sorted(best.values(), key=lambda x: x["max_gain_pct"], reverse=True)
    cut3m = pd.Timestamp(data_date) - pd.Timedelta(days=int(rk["windows_days"]["3m"]))
    top1y = allitems[:int(rk["max_items"])]
    top3m = [x for x in allitems if pd.Timestamp(x["shodo_date"]) >= cut3m][:max(80, int(rk["max_items"]) // 2)]
    seen_codes: set = set()
    ranking = []
    for it in top1y + top3m:
        if it["code"] in seen_codes:
            continue
        seen_codes.add(it["code"])
        ranking.append(it)
    ranking.sort(key=lambda x: x["max_gain_pct"], reverse=True)

    # ── 並び・上限 ──
    cats["stop_high"].sort(key=lambda x: (x["date"], x["stuck"], x.get("r1") or 0), reverse=True)
    cats["shodo"].sort(key=lambda x: x["shodo_date"], reverse=True)
    cats["shodo_wait"].sort(key=lambda x: x["ma_gap_pct"], reverse=True)
    cats["rising"].sort(key=lambda x: x["pullback_pct"])
    cats["oshime"].sort(key=lambda x: x["pullback_pct"])
    cats["rebound"].sort(key=lambda x: (0 if x["in_zone"] else 1, x["fib_pct"]))
    CAP = 120
    counts = {k: len(v) for k, v in cats.items()}

    return {
        "schema": "kabuai-explorer-1",
        "data_date": data_date,
        "note": ("銘柄探検は探索用スクリーナーです。✅今日の買い候補（過去検証済み）とは別物で、"
                 "各カテゴリの優位性は未検証。ストップ高は調整後株価による近似検出です。"),
        "config_echo": {"shodo": cfg["shodo"], "rebound": {k: v for k, v in cfg["rebound"].items() if not k.startswith("_")}},
        "counts": counts,
        "categories": {k: v[:CAP] for k, v in cats.items()},
        "stop_high_history": sorted(sh_hist.values(), key=lambda x: x["date"], reverse=True)[:int(sh_cfg["history_days"])],
        "ranking": {"items": ranking, "windows_days": rk["windows_days"],
                    "longterm": {"available": bool(lt), "since": lt_since,
                                 "generated": (lt or {}).get("generated")}},
    }

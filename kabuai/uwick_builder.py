# -*- coding: utf-8 -*-
"""uwick_builder.py — 🎯前夜の核「上髭逆指値で使えそうな明日の銘柄」（2026-10-13・チンパン v7 再設計・本人決定）。

■ 何を出すか
  当日の日足に「上髭」が出た銘柄（上髭 ≥3%・髭がレンジの半分以上・20日平均代金 ≥3億・株価300〜10,000円）を並べ、
  1銘柄ごとに 翌日の逆指値の水準（買い＝髭の高値＋1呼値／売り＝安値−1呼値）・ATR・代金・貸借/売り禁 を付ける。
  売り側は 貸借銘柄で日証金の申込停止(売り禁)でないものだけ。

■ 言わないこと（検証の結果・_bt_uwick_stop_jp_1012.py）
  日足20年: 上髭→翌日高値抜け買い(大引け決済)は 2005-21 +0.24〜0.45%/回 だが 2022-26 は +0.01%＝エッジは消えている。
  鏡像の 上髭→翌日安値割れ売り は 2022-26 +0.15%。髭条件なしの高値抜け買いは −0.10%。日足では損切りの効き方は測れない。
  5分足70日(_bt_uwick_5m_1013.py・06-22〜10-01・1,590銘柄): 買い −0.61%/回(SL1〜3%で −0.7〜−1.0%・SL発動50〜85%)・売り −0.25%(TP1.5%で±0)・髭なし対照±0 ＝ 直近は負け。
  → 「候補と水準を並べる道具」。上がる/下がる・勝てるとは書かない。

■ データ
  build_data.py が持つ J-Quants 日足 data({ticker: DataFrame[Open,High,Low,Close,Volume]})・name_map・業種・
  iss(貸借区分 /markets/margin-interest IssType "2"=貸借)・alert(/markets/margin-alert RestrictedByJSF=売り禁)。
  呼値は TOPIX500(Core30/Large70/Mid400) と一般で刻みが違う → scale_of(ScaleCategory) があれば使う・無ければ一般の刻み(粗い側＝保守的)。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd

JST = timezone(timedelta(hours=9))
UW_MIN_PCT = 3.0        # 上髭(高値−実体上端)/終値 ≥3%
UW_RANGE_RATIO = 0.5    # 髭 ≥ 当日レンジ×0.5
TOV20_MIN = 3e8         # 20日平均代金 ≥3億
PX_LO, PX_HI = 300, 10000
TOP_N = 20
TP_PCT, SL_PCT, LOSS_YEN = 1.5, 3.0, 20000   # 🧮電卓と同じ既定（利確+1.5%・損切り−3%・1回の損失2万円）

RULES = [
    f"上髭 ≥{UW_MIN_PCT:.0f}%（高値−実体の上端を終値で割る）かつ 髭 ≥ 当日レンジ×{UW_RANGE_RATIO}",
    f"20日平均代金 ≥{TOV20_MIN/1e8:.0f}億・株価{PX_LO}〜{PX_HI:,}円・当日の足がある銘柄だけ",
    "買い＝髭の高値＋1呼値に買い逆指値（寄りがその上なら寄り値で約定＝覚悟する）・大引けまで",
    "売り＝当日安値−1呼値に売り逆指値（貸借○・売り禁でない銘柄だけ）・大引けまで",
    "並び＝20日平均代金の順（人と注文が集まる順）。上がる/下がるは付けない",
]
STATS = {
    "period": "日足20年（立花3,696銘柄・現存のみ）",
    "buy": "上髭3%→翌日高値抜け買い・大引け決済: 05-08 +0.45% / 09-16 +0.34% / 17-21 +0.24% / 22-26 +0.01%（1回あたり・往復0.10%込み）",
    "sell": "上髭3%→翌日安値割れ売り・大引け買戻し: 22-26 +0.15%（買いの鏡像・貸借/売り禁は未反映）",
    "control": "髭条件なしの前日高値抜け買い: −0.10%（髭そのものには意味があるが、直近は消えている）",
    "intraday": "5分足70日(2026-06〜10・1,590銘柄・_bt_uwick_5m_1013.py): 買い 1回−0.61%(損切り1〜3%を付けると−0.7〜−1.0%・損切り発動50〜85%)／売り −0.25%(利確1.5%付きで±0)／髭なし対照 ±0＝直近はマイナス",
    "caveat": "日足20年でもエッジは2022年以降消え、5分足70日では損切り付きで負け。『勝てる』とは言わない。候補と水準を並べるだけ。",
}


def tick_of(px: float, t500: bool) -> float:
    """JPX 呼値。TOPIX500 構成銘柄は細かい刻み。"""
    table = ((1000, 0.1), (3000, 0.5), (10000, 1), (30000, 5), (100000, 10)) if t500 else \
            ((3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100))
    for lim, t in table:
        if px <= lim:
            return t
    return 50 if t500 else 1000


def _rnd(v: float) -> float:
    return round(v * 10) / 10


def _is_trading_day(d: date) -> bool:
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return False
    except Exception:
        pass
    if d.weekday() >= 5:
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def next_trading_day(d: date) -> date:
    cur = d + timedelta(days=1)
    while not _is_trading_day(cur):
        cur += timedelta(days=1)
    return cur


def build_uwick(data: dict, name_map: dict, data_date: str, iss: dict | None, alert: dict | None,
                sec_of: dict | None = None, scale_of: dict | None = None, today: date | None = None) -> dict:
    iss = iss or {}; alert = alert or {}; sec_of = sec_of or {}; scale_of = scale_of or {}
    dd = pd.Timestamp(data_date)
    rows = []
    n_day = 0
    for tk, df in data.items():
        if df is None or len(df) < 21:
            continue
        if df.index[-1] != dd:
            continue   # 当日の足が無い（出来ず/未公開）
        n_day += 1
        try:
            o = float(df["Open"].iloc[-1]); h = float(df["High"].iloc[-1]); l = float(df["Low"].iloc[-1])
            c = float(df["Close"].iloc[-1]); v = float(df["Volume"].iloc[-1]); pc = float(df["Close"].iloc[-2])
        except Exception:
            continue
        if not (c > 0 and pc > 0 and h >= max(o, c) and l <= min(o, c) and v > 0) or not (PX_LO <= c <= PX_HI):
            continue
        cv = (df["Close"].astype(float) * df["Volume"].astype(float))
        tov20 = float(cv.iloc[-20:].mean())
        if not (tov20 >= TOV20_MIN):
            continue
        uw = (h - max(o, c)) / c * 100
        rng = (h - l) / c * 100
        if not (uw >= UW_MIN_PCT and rng > 0 and uw >= UW_RANGE_RATIO * rng):
            continue
        hh = df["High"].astype(float); ll = df["Low"].astype(float); cc = df["Close"].astype(float)
        tr = pd.concat([hh - ll, (hh - cc.shift(1)).abs(), (ll - cc.shift(1)).abs()], axis=1).max(axis=1)
        atr_pct = float(tr.iloc[-14:].mean()) / c * 100
        tov = c * v
        tov_med = float(cv.iloc[-21:-1].median()) if len(cv) >= 21 else float("nan")
        tov_ratio = tov / tov_med if tov_med and tov_med > 0 else float("nan")
        v20 = float(df["Volume"].astype(float).iloc[-21:-1].mean())
        vr = v / v20 if v20 > 0 else float("nan")
        code4 = str(tk).replace(".T", "")[:4]
        name = name_map.get(code4) or name_map.get(tk) or code4
        sc = str(scale_of.get(code4, ""))
        t500 = any(k in sc for k in ("Core30", "Large70", "Mid400"))
        buy_level = _rnd(h + tick_of(h, t500))
        sell_level = _rnd(l - tick_of(l, t500))
        sh = str(iss.get(code4, ""))
        al = alert.get(code4) or {}
        rows.append({
            "code": code4, "name": name, "sector": sec_of.get(code4, ""),
            "close": c, "open": o, "high": h, "low": l, "chg": round((c / pc - 1) * 100, 2),
            "uw_pct": round(uw, 2), "rng_pct": round(rng, 2), "uw_ratio": round(uw / rng, 2),
            "atr_pct": round(atr_pct, 2), "turnover_oku": round(tov / 1e8, 1), "tov20_oku": round(tov20 / 1e8, 1),
            "tov_ratio": round(tov_ratio, 2) if tov_ratio == tov_ratio else None,
            "vol_ratio": round(vr, 2) if vr == vr else None,
            "body_up": c >= o,
            "t500": t500, "tick": tick_of(c, t500),
            "buy_level": buy_level, "sell_level": sell_level,
            "shortable": "○" if sh == "2" else ("×" if sh == "1" else "?"),
            "jsf_stop": bool(al.get("jsf_stop")), "jsf_warn": bool(al.get("jsf_warn")), "tse_reg": bool(al.get("tse_reg")),
        })
    rows.sort(key=lambda r: -r["tov20_oku"])
    buy = rows[:TOP_N]
    sell = [r for r in rows if r["shortable"] == "○" and not r["jsf_stop"]][:TOP_N]
    today = today or datetime.now(JST).date()
    d0 = dd.date()
    target = next_trading_day(d0)
    return {
        "date": data_date, "target_date": target.strftime("%Y-%m-%d"),
        "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "fresh": target >= today,
        "n_day": n_day, "matched": len(rows),
        "buy": buy, "sell": sell,
        "n_sell_blocked": sum(1 for r in rows if not (r["shortable"] == "○" and not r["jsf_stop"])),
        "iss_known": bool(iss), "alert_known": bool(alert), "scale_known": bool(scale_of),
        "calc": {"tp": TP_PCT, "sl": SL_PCT, "loss_yen": LOSS_YEN},
        "rules": RULES, "stats": STATS,
    }


if __name__ == "__main__":   # ローカル確認: 親の jquants_cache.pkl で直近日の候補を出す
    import json
    import pickle
    import sys
    from pathlib import Path
    sys.stdout.reconfigure(encoding="utf-8")
    root = Path(__file__).resolve().parent.parent
    c = pickle.load(open(root / "jquants_cache.pkl", "rb"))
    data = c["all_data"]; nm = c.get("name_map", {}) or {}
    dd = max(df.index.max() for df in data.values() if df is not None and len(df)).strftime("%Y-%m-%d")
    try:
        sec = {str(k)[:4]: v for k, v in json.load(open(root / "sector33_map.json", encoding="utf-8")).items()}
    except Exception:
        sec = {}
    out = build_uwick(data, nm, dd, {}, {}, sec)
    print(f"{dd}: 当日足{out['n_day']} 該当{out['matched']} 買い{len(out['buy'])} 売り{len(out['sell'])}(貸借不明なので0でよい) target={out['target_date']} fresh={out['fresh']}")
    for r in out["buy"][:8]:
        print(f"  {r['code']} {r['name'][:10]:10s} 終{r['close']:>8,.0f} 高{r['high']:>8,.0f} 上髭{r['uw_pct']:4.1f}% 髭/幅{r['uw_ratio']:.2f} ATR{r['atr_pct']:4.1f}% 20日代金{r['tov20_oku']:6.1f}億 買逆指{r['buy_level']:>8,.1f} 売逆指{r['sell_level']:>8,.1f}")

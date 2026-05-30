"""
us_overnight.py — 米株前夜レイヤー

各テーマの us_drivers(米震源: NVDA/SOX/ハイパースケーラー等)の「前夜の騰落率」を取得し、
テーマごとの追い風スコア(平均前夜騰落%)を返す。

東証はEODに構造的に1日遅れる。米震源が前夜に大きく動けば翌日その日本バスケットへ波及する
ので、「前夜の米国の動き」を当日朝のシグナルに先回りで織り込む層。

データ源: yfinance(curl_cffi の chrome なりすましでYahooのbotブロックをかわしてバッチDL)。
  ※Stooqの米国日足は2026にAPIキー(captcha)必須化したため不可。
  ※Alpha Vantageは無料25req/日で~30銘柄/日に不足するため指数欠損時の保険のみ。
Twitterは使わない: 価格・出来高が資金流入のグラウンドトゥルース。
"""
from __future__ import annotations

import warnings

import pandas as pd

warnings.filterwarnings("ignore")

# us_drivers トークン → yfinance シンボルの特例マップ。
# 既定はトークンそのまま(NVDA等)。指数や別表記だけ明示する。
_YF_SYMBOL = {
    "SOX": "^SOX",     # PHLX半導体指数
    "ABBN": "ABB",     # ABB(米ADR)
}


def _yf_symbol(token: str) -> str:
    return _YF_SYMBOL.get(token, token)


def _make_session():
    """Yahooのbotブロックをかわすためのcurl_cffiセッション。無ければNone。"""
    try:
        from curl_cffi import requests as cfr
        return cfr.Session(impersonate="chrome", verify=False)
    except Exception:
        return None


def fetch_driver_returns(tokens: list[str]) -> dict[str, float]:
    """ユニークな us_drivers トークン群の前夜騰落率を yfinance で一括取得。{token: pct}。"""
    if not tokens:
        return {}
    sym_by_token = {tok: _yf_symbol(tok) for tok in tokens}
    symbols = sorted(set(sym_by_token.values()))

    import yfinance as yf
    sess = _make_session()
    kwargs = dict(period="5d", interval="1d", auto_adjust=True,
                  progress=False, group_by="ticker")
    if sess is not None:
        kwargs["session"] = sess
    try:
        raw = yf.download(symbols, **kwargs)
    except Exception as e:
        print(f"[us_overnight] yfinance 取得失敗: {e}")
        return {}

    def _overnight(sym: str) -> float | None:
        try:
            sub = raw[sym] if len(symbols) > 1 else raw
            close = sub["Close"].dropna()
            if len(close) < 2:
                return None
            last, prev = float(close.iloc[-1]), float(close.iloc[-2])
            return round((last - prev) / prev * 100, 2) if prev else None
        except Exception:
            return None

    ret_by_sym = {s: _overnight(s) for s in symbols}
    out: dict[str, float] = {}
    for tok, sym in sym_by_token.items():
        v = ret_by_sym.get(sym)
        if v is not None:
            out[tok] = v
    missing = [t for t in tokens if t not in out]
    if missing:
        print(f"[us_overnight] 前夜騰落 取得不可: {missing}")
    return out


def tailwind_by_theme(
    themes: dict,
    driver_returns: dict[str, float] | None = None,
) -> tuple[dict[str, float | None], dict[str, float]]:
    """
    テーマごとの米前夜追い風(us_drivers の平均前夜騰落%)を返す。
    国内発テーマ(us_drivers空)は None=中立。

    戻り値: (tailwind_by_theme{theme: pct|None}, driver_returns{token: pct})
    themes は theme_members.json の "themes" 辞書(各 value に us_drivers[])。
    driver_returns 未指定なら全テーマの us_drivers をユニーク収集して自動取得。
    """
    if driver_returns is None:
        tokens = sorted({d for t in themes.values() for d in t.get("us_drivers", [])})
        driver_returns = fetch_driver_returns(tokens) if tokens else {}

    tw: dict[str, float | None] = {}
    for tname, tinfo in themes.items():
        drivers = tinfo.get("us_drivers", [])
        vals = [driver_returns[d] for d in drivers if d in driver_returns]
        tw[tname] = round(sum(vals) / len(vals), 2) if vals else None
    return tw, driver_returns


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    from pathlib import Path

    themes = json.load(open(Path("theme_members.json"), encoding="utf-8"))["themes"]
    tw, drv = tailwind_by_theme(themes)

    print()
    print("=" * 70)
    print("  米震源(us_drivers)の前夜騰落率")
    print("=" * 70)
    for tok, ret in sorted(drv.items(), key=lambda kv: kv[1], reverse=True):
        print(f"  {tok:<6} {ret:+6.2f}%")

    print()
    print("=" * 70)
    print("  テーマ別 米前夜追い風 (us_drivers 平均)")
    print("=" * 70)
    rows = [(t, v) for t, v in tw.items() if v is not None]
    rows.sort(key=lambda kv: kv[1], reverse=True)
    for t, v in rows:
        print(f"  {t:<22} {v:+6.2f}%")
    n_dom = sum(1 for v in tw.values() if v is None)
    print(f"  (国内発テーマ {n_dom} 件は中立)")
    print("=" * 70)

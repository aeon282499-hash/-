"""
correlation.py — 先物連動度タグ（2026-07-02 追加仕様）

各銘柄が「先物（既定=日経レバ 1570.T）にどれだけ連動して動いているか」を
5分足リターンの相関係数で数値化し、買い候補にタグとして持たせる。

狙い:
  指数寄与の大きい値嵩株は先物のアービトラージで機械的に振られ、
  押し目→反発の「型」が壊れやすい。
    高連動 = 先物依存（型が壊れやすい・注意）
    自力   = 自分の需給で動く（型が出やすい・狙い目）

- ベンチは BENCHMARK_CODE で外出し（TOPIX型を見たいなら 1306.T に差し替え可）
- 閾値 TH_HIGH / TH_MID も定数1か所（運用しながら体感に合わせて調整する前提）
- β値（変動の大きさ）は出さない。方向の連動（相関）のみ。リアルタイムでもない
  （yfinance の遅延5分足・直近2日 → 共通足の直近 WINDOW 本で判定）。
- 朝ビルド(7時/8:40)時点では当日足がまだ無いため、実質「前営業日の連動」になる。
  夕ビルド(18:30)は当日ザラ場の連動を反映する。
"""
from __future__ import annotations

import numpy as np

BENCHMARK_CODE = "1570.T"   # 日経レバ。TOPIX型の連動を見たいなら "1306.T"
TH_HIGH = 0.70              # これ以上 = 高連動（先物依存・注意）
TH_MID = 0.40               # これ以上 = 中連動 / 未満 = 自力
WINDOW = 48                 # 相関を取る共通足の本数（5分×48 ≒ 直近4時間）
MIN_BARS = 20               # 最低これだけ共通足が無いと判定しない（リターン19点）
MAX_CODES = 250             # 一括タグ付けの上限（暴走ガード）

TAG_HIGH = "高連動"
TAG_MID = "中連動"
TAG_SELF = "自力"
TAG_NA = "データ不足"


def futures_correlation(stock_series, bench_series, window=None):
    """
    stock_series, bench_series:
        {datetime: close} の dict、または (datetime, close) のリスト。
        必ず「同じ時刻の足どうし」で相関を取るため、時刻で inner join する。
    bench_series は 1570.T（日経レバ）の5分足を想定。
    返り値: (corr, tag)
        corr: -1.0〜1.0（None ならデータ不足）
        tag : '高連動' / '中連動' / '自力' / 'データ不足'
    """
    # --- 時刻で揃える（欠損足があってもズレないように必ずjoin） ---
    s = dict(stock_series)
    b = dict(bench_series)
    common = sorted(set(s.keys()) & set(b.keys()))
    if window:
        common = common[-window:]
    if len(common) < MIN_BARS:      # 最低20本（=リターン19点）ないと判定しない
        return None, TAG_NA

    sc = np.array([s[t] for t in common], dtype=float)
    bc = np.array([b[t] for t in common], dtype=float)

    # 前足比リターンで相関を取る（価格そのものだと右肩上がり同士で
    # 過大に相関が出るため、必ずリターンに変換する）
    sr = np.diff(sc) / sc[:-1]
    br = np.diff(bc) / bc[:-1]
    if sr.std() == 0 or br.std() == 0:
        return None, TAG_NA

    corr = float(np.corrcoef(sr, br)[0, 1])
    if np.isnan(corr):
        return None, TAG_NA

    if corr >= TH_HIGH:
        tag = TAG_HIGH   # 先物依存・型が壊れやすい → 下にソート／注意色
    elif corr >= TH_MID:
        tag = TAG_MID
    else:
        tag = TAG_SELF   # 自力で動く・型が出やすい → 狙い目
    return round(corr, 2), tag


def _closes_from_download(df, ticker: str) -> dict:
    """yf.download の結果から ticker の {timestamp: close} を取り出す（欠損は落とす）。"""
    try:
        if hasattr(df.columns, "levels"):            # 複数銘柄: MultiIndex (ticker, field)
            if ticker not in df.columns.get_level_values(0):
                return {}
            close = df[ticker]["Close"]
        else:                                        # 単一銘柄: フラット列
            close = df["Close"]
        close = close.dropna()
        return {ts: float(v) for ts, v in close.items()}
    except Exception:
        return {}


def fetch_5m(tickers: list[str]):
    """yfinance で 5分足（直近2日）を一括取得 → {ticker: {timestamp: close}}。
    ベンチも同じバッチに入れて1回で取る（各銘柄で使い回すため）。"""
    import yfinance as yf

    df = yf.download(
        tickers=tickers, period="2d", interval="5m",
        group_by="ticker", auto_adjust=False, progress=False, threads=True,
    )
    if df is None or len(df) == 0:
        return {}
    if len(tickers) == 1 and not hasattr(df.columns, "levels"):
        return {tickers[0]: _closes_from_download(df, tickers[0])}
    return {t: _closes_from_download(df, t) for t in tickers}


def tag_codes(codes: list[str], window: int = WINDOW) -> dict:
    """4桁コードのリストに先物連動タグを一括付与する。
    返り値: {code: {"futures_corr": float|None, "futures_tag": str}}
    yfinance が落ちていても呼び出し側を巻き込まない（例外は上に投げる→build 側で非致命扱い）。"""
    codes = list(dict.fromkeys(codes))[:MAX_CODES]
    if not codes:
        return {}
    tickers = [c + ".T" for c in codes]
    series = fetch_5m(tickers + [BENCHMARK_CODE])
    bench = series.get(BENCHMARK_CODE) or {}
    out: dict = {}
    if len(bench) < MIN_BARS:
        return out                      # ベンチが取れない日は全銘柄タグなし（表示なし）
    for c, t in zip(codes, tickers):
        corr, tag = futures_correlation(series.get(t) or {}, bench, window=window)
        if tag == TAG_NA:
            continue                    # データ不足は載せない（フロントは非表示）
        out[c] = {"futures_corr": corr, "futures_tag": tag}
    return out


if __name__ == "__main__":
    # 簡易ライブ確認: python correlation.py 7203 6857
    import sys
    cs = sys.argv[1:] or ["7203", "6857", "9984"]
    for code, v in tag_codes(cs).items():
        print(f"{code}: r={v['futures_corr']} {v['futures_tag']}")

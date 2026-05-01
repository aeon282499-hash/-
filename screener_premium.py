"""
screener_premium.py — 至高版・厳選買いシグナル
====================================================

【コンセプト】
  少数精鋭・大ロット運用。シグナル頻度は2日に1〜2件目安。
  既存スイング(PF1.27)を上回る期待値を狙う精度特化型モデル。

【BUYシグナル判定フロー（judge_signal_premium）】
  ① RSI(14)        ≦ 25      （極端な売られすぎ）
  ② 25MA乖離率     ≦ -4.0%   （明確な下方乖離）
  ③ ボラ/出来高    値幅≧ATR×1.5 OR 出来高≧平均×2.0
  ④ 流動性         売買代金 ≧ 30億円（大型優先）
  ⑤ 高ボラ除外     ATR/終値 ≦ 2.0%
  ⑥ 決算除外       直近2営業日以内に決算発表の銘柄はスキップ
  ⑦ 地合い拒否     日経225が直近20日σの-2倍超下落 → ゼロ件配信
  ⑧ 多因子スコア   RSI深さ・乖離深さ・出来高比・流動性の加重和で順位付
  ⑨ 最終選定       上位 MAX_SIGNALS_PRM (=2) 銘柄

【ファイル独立性】
  本モジュールは screener.py の共通関数（インジケータ・データ取得）を import するが、
  positions / 通知 / バックテストは全て *_premium 専用ファイルで完結する。
"""

import math
import json as _json

from screener import (
    _jquants_id_token,
    fetch_tse_prime_universe,
    batch_download_jquants,
    fetch_earnings_tickers,
    fetch_macro,
    calc_rsi,
    calc_ma_deviation,
    calc_range_ratio,
    calc_volume_ratio,
    calc_turnover,
    calc_atr,
    LOOKBACK_DAYS,
    MA_DEV_PERIOD,
    ATR_PERIOD,
    RSI_PERIOD,
)

# ================================================================
# === 至高版 閾値設定 ===
# ================================================================

RSI_BUY_MAX_PRM     = 35       # RSIがこの値以下 → 至高買い候補（v2: 25→35）
DEV_BUY_MAX_PRM     = -2.5     # 25MA乖離率がこの値(%)以下 → 至高買い候補（v2: -4→-2.5）

RANGE_MULT_PRM      = 1.5
VOL_MULT_PRM        = 2.0
TURNOVER_MIN_PRM    = 2_500_000_000     # 25億円（v2: 30億→25億）
ATR_VOL_CAP_PRM     = 2.5               # ATR/終値(%) (v2: 2.0→2.5)

NK_PANIC_SIGMA      = 2.0      # 日経-2σ超下落の日は配信停止

MAX_SIGNALS_PRM     = 3        # 最大3銘柄（v2: 2→3）
POSITION_BUDGET_JPY = 1_500_000  # 1件あたり投入額（v2: 250万→150万・3並列＝450万運用）


# ================================================================
# 多因子スコアリング
# ================================================================

def premium_score(c: dict) -> float:
    """至高版・勝ちやすさスコア。値が大きいほど期待値が高い。

    要素:
      RSI深さ      （RSI=15→1.0, 25→0.33, 30→0）
      25MA乖離深さ （-4%→0, -7%→1.0, -10%→1.5）
      出来高比     （3x→1.0, 5x→1.5）
      流動性       （10億→1.0, 30億→1.5）
    """
    rsi  = c["rsi"]
    dev  = c["deviation"]
    vol  = c.get("vol_ratio") or 1.0
    turn = c["turnover"]

    rsi_score  = max(0.0, (30.0 - rsi) / 15.0)
    dev_score  = max(0.0, min((-dev - 4.0) / 3.0, 1.5))
    vol_score  = min(vol / 3.0, 1.5)
    turn_score = min(math.log10(max(turn, 1) / 1e9), 1.5) if turn > 1e9 else 0.0

    return rsi_score * 0.30 + dev_score * 0.30 + vol_score * 0.15 + turn_score * 0.25


# ================================================================
# ETF/ETN/REIT 判定（個別株のみ採用するため除外）
# ================================================================

# 名称ベースのETFキーワード（強い特徴語のみ・誤検出を避ける）
_ETF_NAME_KEYWORDS = (
    "ETF", "ETN", "REIT",
    "インバース", "レバレッジ", "ベア", "ブル",
    "上場投信", "上場インデックス", "上場ベア", "上場ブル",
    "iシェアーズ", "MAXIS", "NEXT FUNDS",
    "ダイワ上場投信", "野村NF",
    "指数連動", "純金信託", "金価格連動",
)

# ETF/ETN/REITの代表的コード帯（補助的フォールバック・名称取得失敗時）
_ETF_CODE_PREFIXES = ("13", "14", "15", "16", "17", "18", "21", "22", "23", "25", "26")

# 上記コード帯でも個別株として除外したくない例外（必要に応じて追加）
_ETF_CODE_EXCEPTIONS = {
    # 例外があれば追加
}


def is_etf_ticker(ticker: str, name: str | None = None) -> bool:
    """ETF/ETN/REITか判定。名称優先・コード帯フォールバック。"""
    if name:
        for kw in _ETF_NAME_KEYWORDS:
            if kw in name:
                return True
        # 名称があり、ETFキーワードを含まない → 個別株とみなす
        return False
    code = ticker.replace(".T", "")
    if code in _ETF_CODE_EXCEPTIONS:
        return False
    return any(code.startswith(p) for p in _ETF_CODE_PREFIXES)


# ================================================================
# シグナル判定（至高版）
# ================================================================

def judge_signal_premium(ticker: str, name: str, df) -> dict | None:
    """至高版・厳選買い判定（BUYのみ・ETF除外）。"""
    # ── ETF/ETN/REIT 除外（個別株のみ採用） ────────────
    if is_etf_ticker(ticker, name):
        return None

    close = df["Close"].dropna()
    if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD, 20) + 5:
        return None

    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)
    turnover    = calc_turnover(df)
    if any(v is None for v in [rsi, deviation, turnover]):
        return None

    last_close = float(close.iloc[-1])

    # ── 高ボラ除外 ────────────────────────────────────
    atr = calc_atr(df)
    if atr is None or last_close <= 0:
        return None
    if (atr / last_close * 100) > ATR_VOL_CAP_PRM:
        return None

    # ── 至高条件: 極端売られすぎ + 深押し ──────────────
    if not (rsi <= RSI_BUY_MAX_PRM and deviation <= DEV_BUY_MAX_PRM):
        return None

    # ── ボラ OR 出来高（パニック気味の動きが必要）───────
    range_ok = (range_ratio is not None) and (range_ratio >= RANGE_MULT_PRM)
    vol_ok   = (vol_ratio   is not None) and (vol_ratio   >= VOL_MULT_PRM)
    if not (range_ok or vol_ok):
        return None

    # ── 流動性 ────────────────────────────────────────
    if turnover < TURNOVER_MIN_PRM:
        return None

    cond_vol = []
    if range_ok: cond_vol.append(f"値幅/ATR={range_ratio:.1f}（≧{RANGE_MULT_PRM}）")
    if vol_ok:   cond_vol.append(f"出来高比={vol_ratio:.1f}（≧{VOL_MULT_PRM}）")

    reason = [
        f"RSI({RSI_PERIOD}) = {rsi}（≦{RSI_BUY_MAX_PRM}：極端な売られすぎ）",
        f"25MA乖離率 = {deviation:+.1f}%（≦{DEV_BUY_MAX_PRM}%：明確な下方乖離）",
        "③ " + " / ".join(cond_vol),
        f"売買代金 = {turnover/1e8:.0f}億円（≧{TURNOVER_MIN_PRM/1e8:.0f}億・大型優先）",
        f"ATR/終値 = {atr/last_close*100:.2f}%（≦{ATR_VOL_CAP_PRM}%・低ボラ）",
    ]

    return {
        "ticker":      ticker,
        "name":        name,
        "direction":   "BUY",
        "rsi":         rsi,
        "deviation":   deviation,
        "range_ratio": range_ratio,
        "vol_ratio":   vol_ratio,
        "turnover":    turnover,
        "atr":         atr,
        "atr_pct":     round(atr / last_close * 100, 3),
        "prev_close":  last_close,
        "reason":      reason,
    }


# ================================================================
# 地合いフィルタ（パニック日拒否）
# ================================================================

def is_nikkei_panic(nk_df, sigma: float = NK_PANIC_SIGMA) -> bool | None:
    """日経225ETF(1321.T)の前日リターンが直近20日σの -sigma 倍を下回るか判定。

    Returns: True=パニック日（配信停止）, False=通常, None=データ不足で判定不能
    """
    if nk_df is None or len(nk_df) < 22:
        return None
    close = nk_df["Close"].dropna()
    if len(close) < 22:
        return None
    rets = close.pct_change() * 100.0
    yest = float(rets.iloc[-1])
    std  = float(rets.iloc[-21:-1].std())
    if std <= 0:
        return None
    return yest < -sigma * std


# ================================================================
# メイン
# ================================================================

def run_screener_premium() -> tuple[list[dict], dict]:
    """
    至高版・毎朝のスクリーニング本体。
    戻り値: (BUYシグナルリスト, マクロ情報dict)
    """

    macro = fetch_macro()

    universe = fetch_tse_prime_universe()
    name_map = {t: n for t, n in universe}
    tickers  = [t for t, _ in universe]
    print(f"[premium] ユニバース: {len(tickers)} 銘柄")

    token = _jquants_id_token()
    data  = batch_download_jquants(token, lookback_trading_days=LOOKBACK_DAYS)
    if not data:
        print("[premium] J-Quantsデータ取得失敗 → シグナルなし")
        return [], macro

    earnings_exclude = fetch_earnings_tickers(days=2)

    # ── 地合い: 日経パニック判定（-2σ超下落日は停止）──
    nk_df = data.get("1321.T")
    panic = is_nikkei_panic(nk_df)
    if panic is True:
        print("[premium] 日経225 -2σ超下落（パニック日）→ 至高版は配信停止")
        return [], macro
    if panic is False:
        print("[premium] 日経225 通常レンジ → 配信継続")
    else:
        print("[premium] 日経パニック判定不能（データ不足）→ 配信継続")

    # ── シグナル判定 ──────────────────────────────
    candidates: list[dict] = []
    for ticker, df in data.items():
        close = df["Close"].dropna()
        if len(close) < max(MA_DEV_PERIOD, ATR_PERIOD) + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_premium(ticker, name, df)
        if not result:
            continue
        if ticker in earnings_exclude:
            print(f"  [SKIP] {ticker} 直近決算発表のため除外")
            continue
        candidates.append(result)
        print(f"  [PREMIUM HIT] [{ticker}] {name} "
              f"RSI={result['rsi']} dev={result['deviation']:+.1f}% "
              f"turn={result['turnover']/1e8:.0f}oku "
              f"score={premium_score(result):.3f}")

    # ── 既存ポジションの銘柄を除外（至高版専用ファイル）──
    try:
        with open("positions_premium.json", encoding="utf-8") as f:
            existing = _json.load(f)
        open_tickers = {p["ticker"] for p in existing if p.get("status") in ("pending", "open")}
        if open_tickers:
            before = len(candidates)
            candidates = [c for c in candidates if c["ticker"] not in open_tickers]
            print(f"[premium] 保有中銘柄を除外: {before - len(candidates)}件 {open_tickers}")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[premium] positions_premium.json 読込失敗: {e}")

    # ── 多因子スコア降順 → 上位 MAX_SIGNALS_PRM 銘柄 ──
    candidates.sort(key=premium_score, reverse=True)
    signals = candidates[:MAX_SIGNALS_PRM]

    print(f"[premium] BUY候補{len(candidates)}銘柄 → {len(signals)}銘柄を採用")
    return signals, macro

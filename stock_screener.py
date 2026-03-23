"""
stock_screener.py
=================
銘柄選定・売買シグナル生成モジュール

【ロジック概要】寄り引けデイトレード向けスコアリング
  以下の3つのテクニカル指標を組み合わせてスコアを算出し、
  買いシグナル / 売りシグナルが最も強い銘柄を1〜3つ抽出する。

  1. RSI (14日) ─ 過熱・売られすぎ判定
       RSI < 35 → 買いスコア +2
       RSI > 65 → 売りスコア +2

  2. 25日移動平均乖離率 ─ 平均回帰の狙い目
       乖離率 < -4% → 買いスコア +2（売られ過ぎで戻りを狙う）
       乖離率 > +4% → 売りスコア +2（買われ過ぎで押しを狙う）

  3. 前日ボラティリティブレイクアウト ─ 勢いの強さ確認
       前日の値幅が20日ATRの1.5倍以上 → |スコア| +1（動きが大きい日の翌日は注目）

  ※ 指標の閾値やスコア加算値はこのファイル冒頭の定数で一元管理しています。
     自分のロジックに合わせて自由に変更してください。

カスタマイズポイント:
  - UNIVERSE    : 対象銘柄リストを差し替えるだけで別の銘柄群に変更可
  - RSI_BUY_TH  : RSI買いシグナルの閾値
  - RSI_SELL_TH : RSI売りシグナルの閾値
  - DEV_BUY_TH  : 乖離率買いシグナルの閾値(%)
  - DEV_SELL_TH : 乖離率売りシグナルの閾値(%)
  - VOL_MULT    : ボラティリティ倍率の閾値
  - TOP_N       : 最大何銘柄まで通知するか
"""

import yfinance as yf
import pandas as pd
import numpy as np

# ─────────────────────────────────────
# 対象銘柄ユニバース（日経225採用銘柄から流動性の高い代表銘柄）
# 銘柄コードの末尾に ".T" を付けると yfinance が東証銘柄として認識する
# ─────────────────────────────────────
UNIVERSE = [
    ("7203.T", "トヨタ自動車"),
    ("9984.T", "ソフトバンクグループ"),
    ("6758.T", "ソニーグループ"),
    ("9983.T", "ファーストリテイリング"),
    ("6861.T", "キーエンス"),
    ("6098.T", "リクルートホールディングス"),
    ("4063.T", "信越化学工業"),
    ("8035.T", "東京エレクトロン"),
    ("9433.T", "KDDI"),
    ("8306.T", "三菱UFJフィナンシャル・グループ"),
    ("6954.T", "ファナック"),
    ("6367.T", "ダイキン工業"),
    ("7974.T", "任天堂"),
    ("8316.T", "三井住友フィナンシャルグループ"),
    ("4568.T", "第一三共"),
    ("4519.T", "中外製薬"),
    ("6902.T", "デンソー"),
    ("7267.T", "本田技研工業"),
    ("6981.T", "村田製作所"),
    ("9020.T", "東日本旅客鉄道"),
]

# ─────────────────────────────────────
# シグナル判定閾値（ここを変えるだけでロジックが変わる）
# ─────────────────────────────────────
RSI_PERIOD   = 14     # RSI算出期間
MA_PERIOD    = 25     # 移動平均期間
ATR_PERIOD   = 20     # ATR算出期間（ボラティリティ計算用）
RSI_BUY_TH   = 35    # RSIがこの値以下 → 買いシグナル
RSI_SELL_TH  = 65    # RSIがこの値以上 → 売りシグナル
DEV_BUY_TH   = -4.0  # 乖離率がこの値(%)以下 → 買いシグナル
DEV_SELL_TH  = +4.0  # 乖離率がこの値(%)以上 → 売りシグナル
VOL_MULT     = 1.5   # 前日値幅がATRの何倍以上 → ボラ加点
TOP_N        = 3     # 最大抽出銘柄数


# ─────────────────────────────────────
# テクニカル指標計算関数
# ─────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> float:
    """
    RSI (Relative Strength Index) を計算して最新値を返す。
    - close: 終値のSeriesデータ
    - period: 計算期間（デフォルト14日）
    戻り値: 0〜100 の float（データ不足時は None）
    """
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)


def calc_ma_deviation(close: pd.Series, period: int = MA_PERIOD) -> float:
    """
    移動平均乖離率(%) を計算して最新値を返す。
    乖離率 = (現在値 - MA) / MA × 100
    - 正の値 → MA上方乖離（買われ過ぎ傾向）
    - 負の値 → MA下方乖離（売られ過ぎ傾向）
    戻り値: float（データ不足時は None）
    """
    if len(close) < period:
        return None
    ma = close.rolling(period).mean().iloc[-1]
    latest = close.iloc[-1]
    deviation = (latest - ma) / ma * 100
    return round(deviation, 2)


def calc_prev_day_range_ratio(df: pd.DataFrame, atr_period: int = ATR_PERIOD) -> float:
    """
    前日の値幅 / ATR の比率を計算する（ボラティリティブレイクアウト指標）。
    比率が大きいほど前日の動きが通常より大きかったことを示す。
    戻り値: float（データ不足時は None）
    """
    if len(df) < atr_period + 2:
        return None
    # True Range の計算
    high = df["High"]
    low  = df["Low"]
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(atr_period).mean().iloc[-2]   # 前日時点のATR
    prev_range = high.iloc[-2] - low.iloc[-2]       # 前日の値幅
    if atr == 0:
        return None
    return round(prev_range / atr, 2)


# ─────────────────────────────────────
# シグナル判定関数（カスタマイズのメイン）
# ─────────────────────────────────────

def judge_signal(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """
    1銘柄分のシグナルスコアを計算して結果dictを返す。
    シグナルなし（スコア0）の場合は None を返す。

    戻り値 dict のキー:
      ticker    : 銘柄コード
      name      : 銘柄名
      direction : "BUY" or "SELL"
      score     : 絶対スコア（大きいほど強いシグナル）
      rsi       : RSI値
      deviation : 乖離率(%)
      vol_ratio : ボラティリティ比率
      reason    : 選定理由の文字列リスト

    ─── カスタマイズ方法 ───────────────────────────────
    このリストに独自の指標を追加するだけで複合シグナルに拡張できる。
    例）前日ギャップ率、MACD、ボリンジャーバンド等

    buy_score  が正 → 買いシグナルとして抽出
    sell_score が正 → 売りシグナルとして抽出
    より強いほうを採用し、両方0なら対象外とする。
    ────────────────────────────────────────────────────
    """
    close = df["Close"].dropna()
    if len(close) < MA_PERIOD + 5:
        return None

    rsi       = calc_rsi(close)
    deviation = calc_ma_deviation(close)
    vol_ratio = calc_prev_day_range_ratio(df)

    if rsi is None or deviation is None:
        return None

    buy_score  = 0
    sell_score = 0
    reason_buy  = []
    reason_sell = []

    # ── 指標①: RSI判定 ──────────────────────────────
    if rsi < RSI_BUY_TH:
        buy_score += 2
        reason_buy.append(f"RSI {rsi} < {RSI_BUY_TH}（売られ過ぎ）")
    elif rsi > RSI_SELL_TH:
        sell_score += 2
        reason_sell.append(f"RSI {rsi} > {RSI_SELL_TH}（買われ過ぎ）")

    # ── 指標②: 移動平均乖離率判定 ────────────────────
    if deviation < DEV_BUY_TH:
        buy_score += 2
        reason_buy.append(f"{MA_PERIOD}MA乖離率 {deviation:+.1f}%（下方乖離）")
    elif deviation > DEV_SELL_TH:
        sell_score += 2
        reason_sell.append(f"{MA_PERIOD}MA乖離率 {deviation:+.1f}%（上方乖離）")

    # ── 指標③: ボラティリティブレイクアウト ──────────
    if vol_ratio is not None and vol_ratio >= VOL_MULT:
        # 買い・売りの強いほうにボーナス加算
        if buy_score >= sell_score and buy_score > 0:
            buy_score += 1
            reason_buy.append(f"前日値幅/ATR = {vol_ratio}（高ボラ、勢い強）")
        elif sell_score > buy_score:
            sell_score += 1
            reason_sell.append(f"前日値幅/ATR = {vol_ratio}（高ボラ、勢い強）")

    # ── 最終判定 ─────────────────────────────────────
    if buy_score == 0 and sell_score == 0:
        return None   # シグナルなし

    if buy_score >= sell_score:
        direction = "BUY"
        score     = buy_score
        reason    = reason_buy
    else:
        direction = "SELL"
        score     = sell_score
        reason    = reason_sell

    return {
        "ticker":    ticker,
        "name":      name,
        "direction": direction,
        "score":     score,
        "rsi":       rsi,
        "deviation": deviation,
        "vol_ratio": vol_ratio,
        "reason":    reason,
    }


# ─────────────────────────────────────
# メインスクリーニング関数
# ─────────────────────────────────────

def run_screener() -> list[dict]:
    """
    ユニバース全銘柄をスクリーニングし、
    シグナルスコアが高い順に最大 TOP_N 件のリストを返す。

    戻り値: judge_signal() が返す dict のリスト（シグナルなしの場合は空リスト）
    """
    print(f"[screener] {len(UNIVERSE)}銘柄のデータを取得中...")
    results = []

    for ticker, name in UNIVERSE:
        try:
            # 過去60営業日分の日足データを取得
            df = yf.download(ticker, period="60d", interval="1d",
                             auto_adjust=True, progress=False)
            if df.empty or len(df) < MA_PERIOD + 5:
                print(f"  [{ticker}] データ不足のためスキップ")
                continue

            signal = judge_signal(ticker, name, df)
            if signal:
                results.append(signal)
                print(f"  [{ticker}] {name} → {signal['direction']} score={signal['score']}")
            else:
                print(f"  [{ticker}] {name} → シグナルなし")

        except Exception as e:
            print(f"  [{ticker}] エラー: {e}")

    # スコア降順でソートして上位 TOP_N 件を返す
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:TOP_N]

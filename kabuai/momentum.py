"""
momentum.py — KabuAI クローン フェーズ1 指標エンジン

SPEC.md §4 のモメンタム指数を忠実に実装する:

    base       = SR × 18
    power_adj  = clamp(出来高込みの押し上げ力, -3, +8)
    momentum   = clamp(base + power_adj, 0, 100)

SR / POWER / RSI / STAB / ランク(S/A/B/C/D) を 1 銘柄の日足 OHLCV から計算する。
窓・係数・閾値はすべて「仮（未最適化）」。本家も「最適化で決める前提」と明記しており、
このプロジェクトの流儀どおり後でバックテストして確定 → ここの定数を差し替える。

純関数 indicators(df) を提供。テスト・バッチ・将来の BT から再利用できる。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── 仮パラメータ（未最適化・後で BT 確定） ─────────────────────────────
SR_WINDOW = 40      # シャープ計算窓（営業日）SPEC: 20〜60 の範囲で要調整
VOL_FAST = 5        # 出来高 直近窓
VOL_SLOW = 25       # 出来高 基準窓
RANGE_WINDOW = 20   # 値幅の基準（中央値）窓
RSI_WINDOW = 14     # 標準
R20_WINDOW = 20     # 「1ヶ月」近似
MIN_HISTORY = 60    # これ未満の銘柄は対象外

SR_COEF = 18.0           # base = SR × 18
POWER_VR_COEF = 3.0      # 出来高比の寄与
POWER_RANGE_COEF = 2.0   # 値幅拡大の寄与
POWER_CLAMP = (-3.0, 8.0)
STAB_VOL_COEF = 20.0     # STAB = 100 - 日次ボラ% × 係数

GRADE_BANDS = [(80, "S"), (60, "A"), (40, "B"), (20, "C"), (-1e9, "D")]


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(min(hi, max(lo, x)))


def _rsi(close: pd.Series, n: int = RSI_WINDOW) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    ag = gain.ewm(alpha=1.0 / n, adjust=False).mean().iloc[-1]
    al = loss.ewm(alpha=1.0 / n, adjust=False).mean().iloc[-1]
    if al == 0:
        return 100.0 if ag > 0 else 50.0
    rs = ag / al
    return float(100.0 - 100.0 / (1.0 + rs))


def _grade(momentum: float) -> str:
    for thr, g in GRADE_BANDS:
        if momentum >= thr:
            return g
    return "D"


def indicators(df: pd.DataFrame) -> dict | None:
    """
    df: index=Date 昇順, columns=[Open,High,Low,Close,Volume]
    戻り値: 指標 dict、履歴不足や欠損なら None。
    """
    if df is None or len(df) < MIN_HISTORY:
        return None
    df = df.sort_index()
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    vol = df["Volume"].astype(float)
    if close.iloc[-1] <= 0 or close.isna().iloc[-1]:
        return None

    ret = close.pct_change()

    # ── SR：期間リターン ÷ 期間ボラ（窓 SR_WINDOW） ─────────────
    win = ret.tail(SR_WINDOW).dropna()
    daily_std = float(win.std())
    if daily_std <= 0 or np.isnan(daily_std):
        return None
    period_ret = float(close.iloc[-1] / close.iloc[-min(SR_WINDOW, len(close) - 1)] - 1.0)
    period_std = daily_std * np.sqrt(min(SR_WINDOW, len(win)))
    sr = period_ret / period_std if period_std > 0 else 0.0
    base = sr * SR_COEF

    # ── POWER：出来高比 × 値幅拡大 × 方向（[-3,+8] にクランプ） ──
    vr = float(vol.tail(VOL_FAST).mean() / (vol.tail(VOL_SLOW).mean() + 1e-9))
    rng = (high - low) / close
    range_med = float(rng.tail(RANGE_WINDOW).median())
    range_now = float(rng.tail(VOL_FAST).mean())
    range_z = range_now / (range_med + 1e-9)
    r5 = float(close.iloc[-1] / close.iloc[-6] - 1.0) if len(close) > 6 else 0.0
    direction = 1.0 if r5 >= 0 else -1.0
    raw_power = direction * ((vr - 1.0) * POWER_VR_COEF + (range_z - 1.0) * POWER_RANGE_COEF)
    power = _clamp(raw_power, *POWER_CLAMP)

    momentum = _clamp(base + power, 0.0, 100.0)

    # ── 補助指標 ─────────────────────────────────────────────
    rsi = _rsi(close)
    stab = _clamp(100.0 - daily_std * 100.0 * STAB_VOL_COEF, 0.0, 100.0)
    r1 = float(close.iloc[-1] / close.iloc[-2] - 1.0)
    r10 = float(close.iloc[-1] / close.iloc[-11] - 1.0) if len(close) > 10 else r5
    r20 = float(close.iloc[-1] / close.iloc[-(R20_WINDOW + 1)] - 1.0) if len(close) > R20_WINDOW else r5
    # 直近5営業日のモメンタム推移（簡易: 各日終点で base+power を粗く再計算せず、指数の近似履歴）
    mom_hist = _momentum_history(close, ret, vol, high, low, days=5)
    turnover = float((close.iloc[-1] * vol.tail(VOL_SLOW).mean()))

    # 直近の出来高が欠損(NaN)だと vr / turnover が NaN になり、二重事故になる:
    #  ① JSON 出力に NaN が載り、ブラウザの JSON.parse が全体で落ちる（アプリ白画面）
    #  ② `turnover < MIN_TURNOVER` が NaN 比較で False になり流動性フィルタをすり抜ける
    # daily_std ガード（Close 由来）は出来高欠損を捕まえないため、ここで明示的に除外する。
    if not np.isfinite(vr) or not np.isfinite(turnover):
        return None

    return {
        "price": round(float(close.iloc[-1]), 1),
        "momentum": round(momentum, 1),
        "grade": _grade(momentum),
        "sr": round(float(sr), 2),
        "power": round(float(power), 2),
        "rsi": round(rsi, 1),
        "stab": round(stab, 1),
        "r1": round(r1 * 100, 2),
        "r5": round(r5 * 100, 2),
        "r10": round(r10 * 100, 2),
        "r20": round(r20 * 100, 2),
        "vr": round(vr, 2),
        "turnover": turnover,
        "mom_hist": mom_hist,
    }


def _mom_at(close, ret, vol, high, low, end: int):
    """スライス [:end]（end 本目まで）の終点でのモメンタム指数。不足なら None。"""
    if end < MIN_HISTORY:
        return None
    c = close.iloc[:end]
    r = ret.iloc[:end]
    v = vol.iloc[:end]
    h = high.iloc[:end]
    lo = low.iloc[:end]
    win = r.tail(SR_WINDOW).dropna()
    sd = float(win.std())
    if sd <= 0 or np.isnan(sd):
        return None
    pr = float(c.iloc[-1] / c.iloc[-min(SR_WINDOW, len(c) - 1)] - 1.0)
    ps = sd * np.sqrt(min(SR_WINDOW, len(win)))
    sr = pr / ps if ps > 0 else 0.0
    vr = float(v.tail(VOL_FAST).mean() / (v.tail(VOL_SLOW).mean() + 1e-9))
    rng = (h - lo) / c
    rz = float(rng.tail(VOL_FAST).mean()) / (float(rng.tail(RANGE_WINDOW).median()) + 1e-9)
    r5 = float(c.iloc[-1] / c.iloc[-6] - 1.0) if len(c) > 6 else 0.0
    d = 1.0 if r5 >= 0 else -1.0
    p = _clamp(d * ((vr - 1.0) * POWER_VR_COEF + (rz - 1.0) * POWER_RANGE_COEF), *POWER_CLAMP)
    return round(_clamp(sr * SR_COEF + p, 0.0, 100.0), 1)


def _momentum_history(close, ret, vol, high, low, days: int = 5) -> list[float]:
    """過去 days 営業日それぞれの終点で base+power を再計算した推移。"""
    n = len(close)
    return [_mom_at(close, ret, vol, high, low, n - back) for back in range(days - 1, -1, -1)]


def chart_series(df: pd.DataFrame, days: int = 60) -> dict:
    """直近 days 営業日の OHLCV ＋ モメンタム指数推移（詳細チャート用・加工済みデータ）。
    生データの一括配信ではなく、表示用に窓を限定した参考データ。"""
    df = df.sort_index()
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    openp = df["Open"].astype(float)
    vol = df["Volume"].astype(float)
    ret = close.pct_change()
    n = len(close)
    days = min(days, n)
    rng = range(n - days, n)
    return {
        "dates": [d.strftime("%Y-%m-%d") for d in df.index[n - days:n]],
        "o": [round(float(openp.iloc[e]), 1) for e in rng],
        "h": [round(float(high.iloc[e]), 1) for e in rng],
        "l": [round(float(low.iloc[e]), 1) for e in rng],
        "c": [round(float(close.iloc[e]), 1) for e in rng],
        "v": [int(vol.iloc[e]) if not np.isnan(vol.iloc[e]) else 0 for e in rng],
        "momentum": [_mom_at(close, ret, vol, high, low, e + 1) for e in rng],
    }

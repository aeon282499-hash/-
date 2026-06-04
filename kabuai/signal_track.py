"""
signal_track.py — KabuAI クローン シグナル別ヒストリカル実績（トラックレコード）

各銘柄の日足から momentum.py と同一式でローリング指標系列を作り、signals.SIGNAL_DEFS と
同じ条件を過去の各営業日に当てて「発動」を検出。発動日の終値を基準に horizons 営業日後の
終値までの順方向リターンを集計する。

これは固定ルールの過去シミュレーション（理論値・手数料/スリッページ未考慮）であって、
実トレード結果ではない。画面でもその旨を明示する（SPEC §1 正直表示）。

build() のホットパスを汚さないよう純関数 build_track(...) を提供し、呼び出し側は
try/except で囲む（生成失敗しても日次ビルドは止めない）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import signals as sig
from momentum import (
    SR_WINDOW, VOL_FAST, VOL_SLOW, RANGE_WINDOW, RSI_WINDOW, R20_WINDOW,
    MIN_HISTORY, SR_COEF, POWER_VR_COEF, POWER_RANGE_COEF, POWER_CLAMP,
)

TRACK_LOOKBACK = 250  # 評価する直近営業日数の上限（検証窓を一定に保つ）

# 出口の目安（bt_signal_exit.py で確定: 買い系3シグナル60万発動・2021-2026）。
# 発動日終値でエントリー → 保有EXIT_T営業日の時間決済 ×（翌日以降の安値が
# entry×(1-EXIT_SL%) に触れたら損切り）・利確なし＝利を引っ張る。勝率がほぼ最高の
# 帯で、テーマトラッカーBTの先験(約8日/SL-12%)とも一致・全買い系で陽性年5/6。
EXIT_T = 8
EXIT_SL = 12


def _rolling(df: pd.DataFrame) -> pd.DataFrame:
    """momentum.indicators() を各営業日に対してベクトル化したローリング指標系列。
    最新日の各値は indicators(df) の出力と一致するよう、窓・丸めを厳密に揃える。"""
    df = df.sort_index()
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    vol = df["Volume"].astype(float)
    ret = close.pct_change()

    # ── SR：iloc[-1]/iloc[-SR_WINDOW] = shift(SR_WINDOW-1)（indicators と同じ参照位置） ──
    period_ret = close / close.shift(SR_WINDOW - 1) - 1.0
    daily_std = ret.rolling(SR_WINDOW).std()
    period_std = daily_std * np.sqrt(SR_WINDOW)
    sr = period_ret / period_std
    base = sr * SR_COEF

    # ── POWER ──
    vr = vol.rolling(VOL_FAST).mean() / (vol.rolling(VOL_SLOW).mean() + 1e-9)
    rng = (high - low) / close
    range_med = rng.rolling(RANGE_WINDOW).median()
    range_now = rng.rolling(VOL_FAST).mean()
    range_z = range_now / (range_med + 1e-9)
    r5 = close / close.shift(5) - 1.0
    direction = np.where(r5 >= 0, 1.0, -1.0)
    raw_power = direction * ((vr - 1.0) * POWER_VR_COEF + (range_z - 1.0) * POWER_RANGE_COEF)
    power = raw_power.clip(POWER_CLAMP[0], POWER_CLAMP[1])

    momentum = (base + power).clip(0.0, 100.0)

    # ── RSI（ewm adjust=False は因果的＝各日の値が indicators の iloc[-1] と一致） ──
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    ag = gain.ewm(alpha=1.0 / RSI_WINDOW, adjust=False).mean()
    al = loss.ewm(alpha=1.0 / RSI_WINDOW, adjust=False).mean()
    rsi = 100.0 - 100.0 / (1.0 + ag / al)
    rsi = rsi.where(al != 0, pd.Series(np.where(ag > 0, 100.0, 50.0), index=close.index))

    r1 = close / close.shift(1) - 1.0
    r20 = close / close.shift(R20_WINDOW) - 1.0

    # signals が参照する丸めを production と一致させる（境界での発動差を防ぐ）
    out = pd.DataFrame({
        "close": close,
        "momentum": momentum.round(1),
        "vr": vr.round(2),
        "power": power.round(2),
        "r1": (r1 * 100).round(2),
        "r5": (r5 * 100).round(2),
        "r20": (r20 * 100).round(2),
        "rsi": rsi.round(1),
    })
    return out


def build_track(data: dict, tickers: list[str], horizons=(5, 10, 20),
                lookback: int = TRACK_LOOKBACK, min_history: int = MIN_HISTORY,
                since: str | None = None) -> dict:
    defs = sig.SIGNAL_DEFS
    hs = sorted(int(h) for h in horizons)
    hmax = hs[-1]
    since_ts = pd.Timestamp(since) if since else None

    agg = {k: {h: [] for h in hs} for (k, *_rest) in defs}
    exit_agg = {k: [] for (k, *_rest) in defs}   # 出口ポリシー(EXIT_T×EXIT_SL)の実現リターン
    counts = {k: 0 for (k, *_rest) in defs}
    dmin = None
    dmax = None
    n_names = 0

    for tk in tickers:
        df = data.get(tk)
        if df is None or len(df) < min_history + hmax + 1:
            continue
        R = _rolling(df)
        idx = R.index
        close = R["close"].to_numpy(dtype=float)
        low = df.sort_index()["Low"].astype(float).to_numpy()   # R と同じ昇順インデックス
        mom = R["momentum"].to_numpy(dtype=float)
        vr = R["vr"].to_numpy(dtype=float)
        power = R["power"].to_numpy(dtype=float)
        r1 = R["r1"].to_numpy(dtype=float)
        r5 = R["r5"].to_numpy(dtype=float)
        r20 = R["r20"].to_numpy(dtype=float)
        rsi = R["rsi"].to_numpy(dtype=float)
        n = len(close)

        p_end = n - 1 - hmax          # ここまでなら全 horizon の順方向リターンが取れる
        p_start = max(min_history - 1, 4, p_end - lookback + 1)
        if since_ts is not None:      # 暦日カットオフ（長期窓を一定の開始日に揃える）
            p_start = max(p_start, int(idx.searchsorted(since_ts)))
        if p_end < p_start:
            continue
        n_names += 1

        for p in range(p_start, p_end + 1):
            if (np.isnan(mom[p]) or np.isnan(vr[p]) or np.isnan(power[p])
                    or np.isnan(rsi[p]) or np.isnan(r5[p]) or np.isnan(r1[p]) or np.isnan(r20[p])):
                continue
            ep = close[p]
            if ep <= 0 or np.isnan(ep):
                continue
            row = {
                "momentum": float(mom[p]), "vr": float(vr[p]), "power": float(power[p]),
                "r1": float(r1[p]), "r5": float(r5[p]), "r20": float(r20[p]),
                "rsi": float(rsi[p]), "mom_hist": mom[p - 4:p + 1].tolist(),
            }
            fired = False
            for (k, _lb, _em, _ds, _st, fn) in defs:
                try:
                    if fn(row):
                        counts[k] += 1
                        fired = True
                        for h in hs:
                            xp = close[p + h]
                            if not np.isnan(xp):
                                agg[k][h].append(xp / ep - 1.0)
                        if p + EXIT_T < n:   # 出口ポリシーの実現リターン
                            stop = ep * (1.0 - EXIT_SL / 100.0)
                            if (low[p + 1: p + 1 + EXIT_T] <= stop).any():
                                exit_agg[k].append(-EXIT_SL / 100.0)
                            else:
                                xc = close[p + EXIT_T]
                                if not np.isnan(xc):
                                    exit_agg[k].append(xc / ep - 1.0)
                except Exception:
                    pass
            if fired:
                d = idx[p]
                dmin = d if (dmin is None or d < dmin) else dmin
                dmax = d if (dmax is None or d > dmax) else dmax

    groups: dict = {}
    order: list[str] = []
    for (k, lb, em, _ds, st, _fn) in defs:
        order.append(k)
        hh: dict = {}
        for h in hs:
            arr = np.asarray(agg[k][h], dtype=float)
            if arr.size == 0:
                hh[str(h)] = None
            else:
                hh[str(h)] = {
                    "win": round(float((arr > 0).mean() * 100), 1),
                    "avg": round(float(arr.mean() * 100), 2),
                    "med": round(float(np.median(arr) * 100), 2),
                    "n": int(arr.size),
                }
        ea = np.asarray(exit_agg[k], dtype=float)
        if ea.size == 0:
            ex = None
        else:
            gw = ea[ea > 0]
            gl = ea[ea < 0]
            avgwin = float(gw.mean() * 100) if gw.size else 0.0
            avgloss = float(gl.mean() * 100) if gl.size else 0.0
            ex = {
                "t": EXIT_T, "sl": EXIT_SL,
                "win": round(float((ea > 0).mean() * 100), 1),
                "avg": round(float(ea.mean() * 100), 2),
                "avgwin": round(avgwin, 2),
                "avgloss": round(avgloss, 2),
                "rr": (round(avgwin / abs(avgloss), 2) if avgloss < 0 else None),
                "n": int(ea.size),
            }
        groups[k] = {"label": lb, "emoji": em, "stance": st, "n": counts[k], "h": hh, "exit": ex}

    return {
        "available": any(counts[k] > 0 for k in counts),
        "horizons": list(hs),
        "period": {
            "from": dmin.strftime("%Y-%m-%d") if dmin is not None else None,
            "to": dmax.strftime("%Y-%m-%d") if dmax is not None else None,
            "names": n_names,
        },
        "universe": len(tickers),
        "note": ("固定ルールの過去シミュレーション。発動日の終値を基準に各営業日後の終値までの"
                 "騰落率を集計（理論値・手数料/スリッページ・約定ずれ未考慮。実トレード結果ではありません）。"),
        "groups": groups,
        "order": order,
    }

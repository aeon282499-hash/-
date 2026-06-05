"""bt_momentum_core.py — コア・モメンタム指数が将来リターンを当てているか検証する。

指数 momentum = clamp(SR×18 + clamp(power,-3,+8), 0, 100) は窓も係数も全部
「仮・未最適化」。本当に意味があるのは「ある営業日に高指数の銘柄が、低指数の銘柄より
その後よく上がるか(=クロスセクションで単調に効くか)」。これを **日次の分位スプレッド**
で測る(プールした相関は地合いベータで水増しされるので使わない＝各営業日の中で
指数を5分位に分け、最上位Q5の順方向リターン − 最下位Q1 を全営業日平均する)。

さらに SR_WINDOW / power の有無 / 0-100クランプ の是非をスイープし、置き換えで
明確かつ年別に頑健な改善が出るかを見る(出なければ正直に「現行据え置き」と記す)。
出口は確定ポリシー(保有8日/SL-12%/利確なし)でも評価。

実行: python bt_momentum_core.py

── 結果(2026-06-05・432万サンプル2021-2026): 順方向エッジ無し＝最適化対象なし ──
分位別fwd10平均は Q1+0.42→Q3+0.65(ピーク)→Q5+0.47 の逆U字で Q5-Q1≒+0.05%≒ゼロ。
グレード別はもっと露骨に逆相関: S-2.65%(勝率39.7%)/A-0.06/B+0.24/C+0.38/D+0.58(51.4%)
＝「強い」と出すSが将来最弱(ブローオフ反落)、最弱Dが最良。SR窓20/30/40/60は全て
Q5-Q1≒-0.09〜+0.07%・powerは予測寄与ゼロ・クランプも順位に無影響。短期(5-20日)は
反転が支配し、トレーリングなSRは順方向を当てない。→ 係数再最適化はノイズへのカーブ
フィットになるだけなので不採用。指数は「現在の過熱度の記述子」であって買いランキング
ではない。買いエッジは検証済みシグナル+出口にあり指数グレードには無い。
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from momentum import (
    SR_WINDOW, VOL_FAST, VOL_SLOW, RANGE_WINDOW, MIN_HISTORY,
    SR_COEF, POWER_VR_COEF, POWER_RANGE_COEF, POWER_CLAMP, GRADE_BANDS,
)
from signal_track import EXIT_T, EXIT_SL

TMAX = 20
SR_WINS = (20, 30, 40, 60)   # SR窓スイープ候補（現行=40）
HS = (5, 10, 20)


def _sr_series(close: pd.Series, ret: pd.Series, w: int) -> np.ndarray:
    period_ret = close / close.shift(w - 1) - 1.0
    period_std = ret.rolling(w).std() * np.sqrt(w)
    return (period_ret / period_std).to_numpy(dtype=float)


def _power_series(close, high, low, vol) -> np.ndarray:
    vr = vol.rolling(VOL_FAST).mean() / (vol.rolling(VOL_SLOW).mean() + 1e-9)
    rng = (high - low) / close
    range_z = rng.rolling(VOL_FAST).mean() / (rng.rolling(RANGE_WINDOW).median() + 1e-9)
    r5 = close / close.shift(5) - 1.0
    direction = np.where(r5 >= 0, 1.0, -1.0)
    raw = direction * ((vr - 1.0) * POWER_VR_COEF + (range_z - 1.0) * POWER_RANGE_COEF)
    return raw.clip(POWER_CLAMP[0], POWER_CLAMP[1]).to_numpy(dtype=float)


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    cols = {k: [] for k in ("date", "year", "f5", "f10", "f20", "exit",
                            "sr20", "sr30", "sr40", "sr60", "power")}
    nn = 0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + TMAX + 1:
            continue
        df = df.sort_index()
        close = df["Close"].astype(float)
        high = df["High"].astype(float)
        low = df["Low"].astype(float)
        vol = df["Volume"].astype(float)
        ret = close.pct_change()
        cv = close.to_numpy(dtype=float)
        lowv = low.to_numpy(dtype=float)
        srs = {w: _sr_series(close, ret, w) for w in SR_WINS}
        powv = _power_series(close, high, low, vol)
        dord = df.index.values.astype("datetime64[D]").astype(np.int32)
        yrs = df.index.year.to_numpy()
        n = len(cv)
        nn += 1
        p_end = n - 1 - TMAX
        for p in range(MIN_HISTORY - 1, p_end + 1):
            ep = cv[p]
            if ep <= 0 or np.isnan(ep) or np.isnan(powv[p]):
                continue
            if any(np.isnan(srs[w][p]) for w in SR_WINS):
                continue
            f5 = cv[p + 5] / ep - 1.0
            f10 = cv[p + 10] / ep - 1.0
            f20 = cv[p + 20] / ep - 1.0
            if np.isnan(f5) or np.isnan(f10) or np.isnan(f20):
                continue
            stop = ep * (1.0 - EXIT_SL / 100.0)
            if (lowv[p + 1: p + 1 + EXIT_T] <= stop).any():
                ex = -EXIT_SL / 100.0
            else:
                ex = cv[p + EXIT_T] / ep - 1.0
            cols["date"].append(dord[p]); cols["year"].append(yrs[p])
            cols["f5"].append(f5); cols["f10"].append(f10); cols["f20"].append(f20)
            cols["exit"].append(ex)
            cols["sr20"].append(srs[20][p]); cols["sr30"].append(srs[30][p])
            cols["sr40"].append(srs[40][p]); cols["sr60"].append(srs[60][p])
            cols["power"].append(powv[p])
    df = pd.DataFrame({
        "date": np.asarray(cols["date"], np.int32),
        "year": np.asarray(cols["year"], np.int16),
        "f5": np.asarray(cols["f5"], np.float32),
        "f10": np.asarray(cols["f10"], np.float32),
        "f20": np.asarray(cols["f20"], np.float32),
        "exit": np.asarray(cols["exit"], np.float32),
        "sr20": np.asarray(cols["sr20"], np.float32),
        "sr30": np.asarray(cols["sr30"], np.float32),
        "sr40": np.asarray(cols["sr40"], np.float32),
        "sr60": np.asarray(cols["sr60"], np.float32),
        "power": np.asarray(cols["power"], np.float32),
    })
    # 現行指数（SR40×18 + power をクランプ）
    df["mom"] = np.clip(df["sr40"] * SR_COEF + df["power"], 0.0, 100.0).round(1)
    print(f"集約: {nn}銘柄  サンプル {len(df):,}  営業日 {df['date'].nunique():,}")
    return df


def _grade(m):
    for thr, g in GRADE_BANDS:
        if m >= thr:
            return g
    return "D"


def _day_quantile(df, scorecol, q=5):
    """各営業日の中で score を q分位に割り当てる（クロスセクション）。"""
    r = df.groupby("date")[scorecol].rank(method="first", pct=True).to_numpy()
    return np.clip((r * q).astype(int), 0, q - 1)


def quintile_table(df, scorecol, retcol, q=5, min_per_day=30):
    """各日 q分位 → 分位別の順方向リターン平均/勝率（日数でフィルタ）。"""
    # 1日あたりの銘柄数が薄い日は除外（クロスセクションが成立しないため）
    cnt = df.groupby("date")[scorecol].transform("size").to_numpy()
    m = cnt >= min_per_day
    sub = df[m]
    qq = _day_quantile(sub, scorecol, q)
    ret = sub[retcol].to_numpy(dtype=float)
    rows = []
    for b in range(q):
        sel = qq == b
        rr = ret[sel]
        rows.append((b, rr.size, rr.mean() * 100, (rr > 0).mean() * 100))
    return rows, sub, qq, ret


def spread_by_year(sub, qq, ret, year, q=5):
    """Q(top) − Q(bottom) の順方向リターン差を年別に。"""
    out = {}
    for y in sorted(set(year.tolist())):
        ym = year == y
        if ym.sum() < 500:
            continue
        top = ret[ym & (qq == q - 1)]
        bot = ret[ym & (qq == 0)]
        if top.size < 50 or bot.size < 50:
            continue
        out[y] = (top.mean() - bot.mean()) * 100
    return out


def show_quintile(tag, df, scorecol, retcol):
    rows, sub, qq, ret = quintile_table(df, scorecol, retcol)
    q = len(rows)
    spread = rows[-1][2] - rows[0][2]
    mono = all(rows[i][2] <= rows[i + 1][2] for i in range(q - 1))
    print(f"  [{tag}] {retcol}:")
    for b, n, avg, win in rows:
        bar = "Q%d" % (b + 1)
        print(f"     {bar}  n{n:>8,d}  平均{avg:+6.2f}%  勝率{win:4.1f}%")
    yr = spread_by_year(sub, qq, ret, sub["year"].to_numpy(), q)
    pos = sum(1 for v in yr.values() if v > 0)
    ycells = " ".join(f"{y}:{v:+.1f}" for y, v in yr.items())
    print(f"     => Q{q}-Q1 スプレッド {spread:+.2f}%  単調={'○' if mono else '×'}  "
          f"陽性年{pos}/{len(yr)}  [{ycells}]")
    return spread, mono, pos, len(yr)


def main():
    df = collect()

    print(f"\n========== 1) 現行指数は将来リターンを当てているか（日次クロスセクション5分位） ==========")
    for rc in ("f5", "f10", "f20", "exit"):
        show_quintile("現行 momentum(SR40×18+pow,clamp)", df, "mom", rc)

    print(f"\n========== 2) ユーザが見るグレード S/A/B/C/D 別の実績（h=10 と 出口） ==========")
    df["grade"] = df["mom"].map(_grade)
    for rc in ("f10", "exit"):
        print(f"  -- {rc} --")
        for g in ("S", "A", "B", "C", "D"):
            sub = df[df["grade"] == g]
            if len(sub) < 100:
                print(f"     {g}  標本不足({len(sub)})"); continue
            r = sub[rc].to_numpy(dtype=float)
            print(f"     {g}  n{len(sub):>8,d}  平均{r.mean()*100:+6.2f}%  勝率{(r>0).mean()*100:4.1f}%")

    print(f"\n========== 3) SR_WINDOW スイープ（h=10 の Q5-Q1・現行=40） ==========")
    for w in SR_WINS:
        sc = f"sr{w}"
        show_quintile(f"SR{w} 単独ランク", df, sc, "f10")

    print(f"\n========== 4) power は予測に効くか（SRのみ vs SR+power・h=10） ==========")
    df["base40"] = df["sr40"] * SR_COEF
    df["raw40"] = df["sr40"] * SR_COEF + df["power"]
    show_quintile("SR40のみ(base)", df, "base40", "f10")
    show_quintile("SR40+power(raw・未クランプ)", df, "raw40", "f10")
    show_quintile("powerのみ", df, "power", "f10")

    print(f"\n========== 5) 0-100クランプは順位情報を捨てているか（h=10） ==========")
    show_quintile("クランプ後 momentum", df, "mom", "f10")
    show_quintile("未クランプ raw40", df, "raw40", "f10")

    print("\n判定: 採用は『Q5-Q1スプレッドが明確プラス & 単調 & 陽性年=ほぼ全年』。"
          "現行40を置き換えるのは、別窓がh=10とexitの両方でスプレッド/頑健性を明確改善した時のみ。")


if __name__ == "__main__":
    main()

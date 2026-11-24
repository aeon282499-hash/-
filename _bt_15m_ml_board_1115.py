# -*- coding: utf-8 -*-
"""_bt_15m_ml_board_1115.py — 次の15分足の向きを「特徴量を広げた機械学習＋板」で当てられるか（2026-11-15）。

本人「15個の特徴量の問題じゃない？」「板も入れて作って」。
過去の LightGBM 失敗（_BRIEF_for_fable5.md）は「決算持ち越しの日足順位予測・15特徴量」で、15分足の向きとは別問題。
ここで 15分足専用に作り直し、ウォークフォワード（学習に使っていない日だけで評価）で測る。

■ Model P（値段・出来高だけ・全銘柄・全時間帯）  データ: _intraday_cache_5m.pkl → 15分足
■ Model B（板あり）  データ: board_ticks/*.pkl（tachibana_board_recorder.py・10秒ごとの板10本/成行/現在値/出来高）
    記録窓が am 08:55〜10:05 / pm 12:25〜13:05 なので予測できるのは:
      寄り前(08:55〜09:00の気配) → 9:00〜9:15 の足 ／ 9:00→9:15, 9:15→9:30, 9:30→9:45 ／ 12:30→12:45
    同じサンプルで「板の特徴量を抜いたモデル」とも比べる＝板の上乗せだけを取り出す。
■ 評価（すべて学習に使っていない日）
    的中率（多数派クラスを当て続けた場合との差）・AUC
    売買: 予測確率 p≥閾値で買い / p≤1−閾値で売り（空売り）、足の始値→終値で1本だけ。コスト COST_PCT 往復。
    比べる相手: 同じ件数をランダムな向きで張った場合。
    ウォークフォワード: 日付順に並べ、最初の MIN_TRAIN_DAYS 日で学習→次の STEP 日を予測→学習期間を伸ばして繰り返す。
■ モデル: lightgbm があれば LGBMClassifier、無ければ sklearn の HistGradientBoostingClassifier。

実行: python -X utf8 _bt_15m_ml_board_1115.py > _log_15m_ml_board_1115.txt
      （板だけ: --only-board ／ 5分足だけ: --only-price）
"""
from __future__ import annotations

import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CACHE = "_intraday_cache_5m.pkl"
BOARD_DIR = Path("board_ticks")
COST_PCT = 0.10
MIN_TOV_PREV = 3e8
MIN_TRAIN_DAYS_P, MIN_TRAIN_DAYS_B, STEP = 20, 15, 5
THRESHOLDS = (0.52, 0.55, 0.58, 0.60)
SEED = 0


# ───────────────────────── モデル ─────────────────────────
def make_model():
    try:
        import lightgbm as lgb
        return lgb.LGBMClassifier(n_estimators=300, learning_rate=0.03, num_leaves=15, min_child_samples=200,
                                  subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
                                  random_state=SEED, verbose=-1)
    except ImportError:
        from sklearn.ensemble import HistGradientBoostingClassifier
        return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                              min_samples_leaf=200, l2_regularization=1.0, random_state=SEED)


def auc(y: np.ndarray, p: np.ndarray) -> float:
    o = np.argsort(p); r = np.empty(len(p)); r[o] = np.arange(1, len(p) + 1)
    n1 = y.sum(); n0 = len(y) - n1
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)) if n1 and n0 else np.nan


def walk_forward(D: pd.DataFrame, feats: list[str], min_train: int) -> pd.Series:
    days = sorted(D.day.unique())
    p = pd.Series(np.nan, index=D.index)
    i = min_train
    while i < len(days):
        tr = D.day.isin(days[:i]); te = D.day.isin(days[i:i + STEP])
        m = make_model()
        m.fit(D.loc[tr, feats].to_numpy(float), D.loc[tr, "y"].to_numpy(int))
        p[te] = m.predict_proba(D.loc[te, feats].to_numpy(float))[:, 1]
        i += STEP
    return p


def importance(D: pd.DataFrame, feats: list[str]) -> list[tuple[str, float]]:
    m = make_model(); m.fit(D[feats].to_numpy(float), D.y.to_numpy(int))
    imp = getattr(m, "feature_importances_", None)
    if imp is None:
        return [("（lightgbm が無いので重要度は省略・pip install lightgbm で出る）", 0.0)]
    imp = np.asarray(imp, float); imp = imp / (imp.sum() + 1e-12)
    return sorted(zip(feats, imp), key=lambda x: -x[1])[:12]


def report(title: str, D: pd.DataFrame, p: pd.Series) -> None:
    m = p.notna()
    d, pp = D[m], p[m].to_numpy()
    y = d.y.to_numpy(int); r = d.ret.to_numpy(float)
    rng = np.random.default_rng(SEED)
    base = max(y.mean(), 1 - y.mean()) * 100
    acc = ((pp >= 0.5) == (y == 1)).mean() * 100
    print(f"\n--- {title} ---")
    print(f"  評価サンプル {len(d):,}（{d.day.nunique()}日）  的中率 {acc:.1f}% ／ 多数派を当て続けた場合 {base:.1f}% ／ AUC {auc(y, pp):.3f}")
    for th in THRESHOLDS:
        side = np.where(pp >= th, 1, np.where(pp <= 1 - th, -1, 0))
        k = side != 0
        if k.sum() < 30:
            print(f"  閾値{th:.2f}: 売買{k.sum()}件（少なすぎ）"); continue
        net = side[k] * r[k] - COST_PCT
        rnd = rng.choice([-1, 1], size=k.sum()) * r[k] - COST_PCT
        gp, gl = net[net > 0].sum(), -net[net <= 0].sum()
        hit = (np.sign(r[k]) == side[k]).mean() * 100
        days_k = d.day.to_numpy()[k]
        half = np.array([x >= sorted(set(days_k))[len(set(days_k)) // 2] for x in days_k])
        print(f"  閾値{th:.2f}: 売買{k.sum():>6,}件（買{(side==1).sum():,}/売{(side==-1).sum():,}）"
              f" 方向的中{hit:>5.1f}% 平均{net.mean()*100:>+6.1f}bp PF{(gp/gl if gl else np.inf):>5.2f}"
              f" ／ ランダム{rnd.mean()*100:>+6.1f}bp ／ 前半{net[~half].mean()*100:>+6.1f}・後半{net[half].mean()*100:>+6.1f}bp")


# ───────────────────────── Model P（5分足→15分足） ─────────────────────────
def to_15m(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["dt"])
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
    d = df.assign(dt=dt).sort_values("dt")
    d = d[(d.v > 0) & (d.c > 0)]
    hm = d.dt.dt.hour * 60 + d.dt.dt.minute
    d = d[((hm >= 540) & (hm < 690)) | ((hm >= 750) & (hm < 930))].copy()
    hm = d.dt.dt.hour * 60 + d.dt.dt.minute
    base = np.where(hm < 690, 540, 750)
    sm = np.minimum(base + ((hm - base) // 15) * 15, 915)
    d["day"] = d.dt.dt.date; d["sm"] = sm; d["pv"] = d.c * d.v
    g = d.groupby(["day", "sm"], sort=True)
    return pd.DataFrame({"o": g.o.first(), "h": g.h.max(), "l": g.l.min(), "c": g.c.last(),
                         "v": g.v.sum(), "pv": g.pv.sum()}).reset_index()


def price_feats(g: pd.DataFrame, prev_close: float) -> pd.DataFrame:
    """1銘柄1日の15分足 g（o,h,l,c,v,pv,sm）→ 足kの終値時点で分かる特徴量 と 次足の答え。"""
    o0 = g.o.iloc[0]
    c, h, l, v = g.c.to_numpy(float), g.h.to_numpy(float), g.l.to_numpy(float), g.v.to_numpy(float)
    ret = (c / g.o.to_numpy(float) - 1) * 100
    rng = (h / l - 1) * 100
    cumh, cuml = np.maximum.accumulate(h), np.minimum.accumulate(l)
    vwap = g.pv.cumsum().to_numpy() / np.maximum(g.v.cumsum().to_numpy(), 1)
    vmean = np.cumsum(v) / np.arange(1, len(v) + 1)
    f = pd.DataFrame({
        "slot": g.sm.to_numpy(), "r0": ret, "r1": np.r_[np.nan, ret[:-1]], "r2": np.r_[np.nan, np.nan, ret[:-2]],
        "rng_rel": rng / (np.cumsum(rng) / np.arange(1, len(rng) + 1) + 1e-9),
        "pos": (c - l) / np.maximum(h - l, 1e-9), "vol_rel": v / np.maximum(vmean, 1),
        "vwap_dev": (c / vwap - 1) * 100, "day_ret": (c / o0 - 1) * 100, "gap": (o0 / prev_close - 1) * 100,
        "from_hi": (c / cumh - 1) * 100, "from_lo": (c / cuml - 1) * 100, "first_r": ret[0],
    })
    s = np.sign(ret); streak = np.zeros(len(s))
    for i in range(len(s)):
        streak[i] = s[i] + (streak[i - 1] if i and np.sign(streak[i - 1]) == s[i] else 0) if s[i] else 0
    f["streak"] = streak
    nr = np.r_[(c[1:] / g.o.to_numpy(float)[1:] - 1) * 100, np.nan]
    f["ret"] = nr; f["y"] = (nr > 0).astype(float); f.loc[~(nr != 0) | np.isnan(nr), "y"] = np.nan
    return f.iloc[:-1]


def build_price() -> pd.DataFrame:
    C = pickle.load(open(CACHE, "rb"))
    out = []
    for tk, df in C.items():
        if df is None or len(df) < 100 or not {"dt", "o", "h", "l", "c", "v"}.issubset(df.columns):
            continue
        b = to_15m(df)
        prev_close, prev_tov = np.nan, np.nan
        for day, g in b.groupby("day", sort=True):
            g = g.reset_index(drop=True)
            if len(g) >= 8 and prev_tov >= MIN_TOV_PREV and prev_close > 0:
                f = price_feats(g, prev_close); f["tk"] = tk; f["day"] = day
                f["prev_day_ret"] = np.nan
                out.append(f)
            prev_close, prev_tov = float(g.c.iloc[-1]), float(g.pv.sum())
    D = pd.concat(out, ignore_index=True)
    # 市場（全銘柄の同じ日・同じ足の平均）と、その銘柄の市場との差
    mk = D.groupby(["day", "slot"]).r0.transform("mean")
    D["mkt_r0"] = mk; D["rel_r0"] = D.r0 - mk
    D["mkt_day"] = D.groupby(["day", "slot"]).day_ret.transform("mean")
    D = D.dropna(subset=["y"]).reset_index(drop=True)
    D["y"] = D.y.astype(int)
    return D


P_FEATS = ["slot", "r0", "r1", "r2", "rng_rel", "pos", "vol_rel", "vwap_dev", "day_ret", "gap", "from_hi", "from_lo",
           "first_r", "streak", "mkt_r0", "rel_r0", "mkt_day"]


# ───────────────────────── Model B（板） ─────────────────────────
def load_board() -> pd.DataFrame:
    frames = []
    for p in sorted(BOARD_DIR.glob("*.pkl")):
        try:
            d = pickle.load(open(p, "rb"))
        except Exception:
            continue
        df = d["rows"].copy(); df["date"] = d["date"]; df["slot_name"] = d["slot"]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    B = pd.concat(frames, ignore_index=True)
    B["t"] = pd.to_datetime(B.date + " " + B.ts.astype(str))
    return B.sort_values(["code", "t"]).reset_index(drop=True)


def snap_feats(s: pd.Series) -> dict:
    """板1枚 → 特徴量。数量が無い段は0扱い。"""
    a = [s.get(f"a{i}q", np.nan) for i in range(1, 11)]; b = [s.get(f"b{i}q", np.nan) for i in range(1, 11)]
    a = np.nan_to_num(np.asarray(a, float)); b = np.nan_to_num(np.asarray(b, float))
    imb = lambda x, y: (y - x) / (x + y) if (x + y) > 0 else 0.0
    ap, bp = s.get("a1p", np.nan), s.get("b1p", np.nan)
    mid = (ap + bp) / 2 if ap > 0 and bp > 0 else s.get("last", np.nan)
    return {
        "imb1": imb(a[0], b[0]), "imb3": imb(a[:3].sum(), b[:3].sum()), "imb10": imb(a.sum(), b.sum()),
        "imb_ou": imb(np.nan_to_num(s.get("over", 0)), np.nan_to_num(s.get("under", 0))),
        "mkt_imb": imb(np.nan_to_num(s.get("mkt_sell_qty", 0)), np.nan_to_num(s.get("mkt_buy_qty", 0))),
        "spread_bp": (ap / bp - 1) * 1e4 if ap > 0 and bp > 0 else np.nan,
        "depth_rel": (a.sum() + b.sum()), "mid": mid,
    }


def build_board() -> pd.DataFrame:
    B = load_board()
    if B.empty:
        return B
    rows = []
    # 足の境界（その時刻の直前の板で判定）: 寄り前→9:00足、9:00/9:15/9:30/9:45、12:30/12:45
    pairs = [("08:59:59", "09:00", "09:15", "寄り前→9:00足"), ("09:14:59", "09:15", "09:30", "9:00足→9:15足"),
             ("09:29:59", "09:30", "09:45", "9:15足→9:30足"), ("09:44:59", "09:45", "10:00", "9:30足→9:45足"),
             ("12:44:59", "12:45", "13:00", "12:30足→12:45足")]
    for (code, date), g in B.groupby(["code", "date"], sort=False):
        g = g.set_index("t").sort_index()
        last, vol = g["last"].astype(float), g["volume"].astype(float)
        dv = vol.diff().clip(lower=0).fillna(0)
        # 歩み値の近似: 出来高の増分を「値が上がった/仲値より上で約定＝買い」「下＝売り」で符号付け（tick rule）
        sgn = np.sign(last.diff()).replace(0, np.nan).ffill().fillna(0)
        sv = dv * sgn
        prev_close = float(g["prev_close"].dropna().iloc[0]) if g["prev_close"].notna().any() else np.nan
        for t_feat, t_in, t_out, lab in pairs:
            tf, ti, to = (pd.Timestamp(f"{date} {x}") for x in (t_feat, t_in + ":00", t_out + ":00"))
            win_end = g.loc[:tf]
            if win_end.empty or (tf - win_end.index[-1]).total_seconds() > 60:
                continue
            s = win_end.iloc[-1]
            # 答え: エントリー=境界直後の最初の現在値（寄り前→9:00足は始値）、イグジット=次の境界直前の現在値
            if lab.startswith("寄り前"):
                e = s.get("open", np.nan) if s.get("open", 0) > 0 else np.nan
                after = g.loc[ti:ti + pd.Timedelta(seconds=60)]
                if not (e > 0) and not after.empty:
                    e = after["open"].dropna().iloc[0] if after["open"].notna().any() else np.nan
            else:
                after = g.loc[ti:ti + pd.Timedelta(seconds=30)]
                e = float(after["last"].iloc[0]) if not after.empty else float(s["last"])
            ex_w = g.loc[:to - pd.Timedelta(seconds=1)]
            if ex_w.empty or (to - ex_w.index[-1]).total_seconds() > 60 or not (e > 0):
                continue
            x = float(ex_w["last"].iloc[-1])
            r = (x / e - 1) * 100
            if r == 0 or not np.isfinite(r):
                continue
            f = snap_feats(s)
            bar = g.loc[tf - pd.Timedelta(minutes=15):tf]
            imb_series = bar.apply(lambda q: snap_feats(q)["imb3"], axis=1) if len(bar) > 1 else pd.Series([f["imb3"]])
            f.update({
                "imb3_mean": float(imb_series.mean()), "imb3_chg": float(imb_series.iloc[-1] - imb_series.iloc[0]),
                "flow_bar": float(sv.loc[bar.index].sum() / max(dv.loc[bar.index].sum(), 1)),
                "flow_5m": float(sv.loc[tf - pd.Timedelta(minutes=5):tf].sum() / max(dv.loc[tf - pd.Timedelta(minutes=5):tf].sum(), 1)),
                "depth_rel": f["depth_rel"] / max(float(dv.loc[bar.index].sum()), 1),
            })
            px = float(s["last"]) if s["last"] > 0 else f["mid"]
            o_day = float(s.get("open", np.nan))
            f.update({
                "k": lab, "code": code, "day": pd.Timestamp(date).date(),
                "b_r0": (px / float(bar["last"].iloc[0]) - 1) * 100 if len(bar) and bar["last"].iloc[0] > 0 else 0.0,
                "b_day_ret": (px / o_day - 1) * 100 if o_day > 0 else 0.0,
                "b_gap": ((o_day if o_day > 0 else px) / prev_close - 1) * 100 if prev_close > 0 else np.nan,
                "b_vwap_dev": (px / float(s["vwap"]) - 1) * 100 if s.get("vwap", 0) > 0 else 0.0,
                "b_hi": (px / float(s["high"]) - 1) * 100 if s.get("high", 0) > 0 else 0.0,
                "b_lo": (px / float(s["low"]) - 1) * 100 if s.get("low", 0) > 0 else 0.0,
                "ret": r, "y": int(r > 0),
            })
            rows.append(f)
    D = pd.DataFrame(rows)
    if D.empty:
        return D
    D["slot_i"] = D.k.map({p[3]: i for i, p in enumerate(pairs)})
    D["mkt_r0"] = D.groupby(["day", "k"]).b_r0.transform("mean")
    D["mkt_imb3"] = D.groupby(["day", "k"]).imb3.transform("mean")
    return D.reset_index(drop=True)


B_PRICE = ["slot_i", "b_r0", "b_day_ret", "b_gap", "b_vwap_dev", "b_hi", "b_lo", "mkt_r0"]
B_BOARD = ["imb1", "imb3", "imb10", "imb_ou", "mkt_imb", "spread_bp", "depth_rel", "imb3_mean", "imb3_chg",
           "flow_bar", "flow_5m", "mkt_imb3"]


# ───────────────────────── 実行 ─────────────────────────
def main() -> None:
    only_b, only_p = "--only-board" in sys.argv, "--only-price" in sys.argv
    print(f"[ml15] コスト {COST_PCT}%往復 / ウォークフォワード STEP={STEP}日 / 閾値 {THRESHOLDS}")
    if not only_b:
        if Path(CACHE).exists():
            D = build_price()
            print(f"\n================ Model P（値段・出来高のみ・全銘柄・全時間帯）================")
            print(f"[P] サンプル {len(D):,} / 銘柄 {D.tk.nunique():,} / 日 {D.day.nunique()}（{min(D.day)}〜{max(D.day)}）"
                  f" / 陽線率 {D.y.mean()*100:.1f}%")
            if D.day.nunique() > MIN_TRAIN_DAYS_P:
                p = walk_forward(D, P_FEATS, MIN_TRAIN_DAYS_P)
                report("Model P（特徴量17個）", D, p)
                for s in sorted(D.slot.unique()):
                    m = (D.slot == s) & p.notna()
                    if m.sum() > 500:
                        acc = ((p[m] >= 0.5) == (D.y[m] == 1)).mean() * 100
                        print(f"    時間帯 {s//60:02d}:{s%60:02d}の足→次足  的中 {acc:.1f}%（n={m.sum():,}）")
                print("  重要度（全期間で学習・参考）: " + ", ".join(f"{k} {v:.2f}" for k, v in importance(D, P_FEATS)))
            else:
                print("  日数が足りない（ウォークフォワード不可）")
        else:
            print(f"[P] {CACHE} が無い → スキップ")
    if not only_p:
        Db = build_board()
        print(f"\n================ Model B（板あり・記録した銘柄と時間帯だけ）================")
        if Db.empty:
            print(f"[B] {BOARD_DIR}/ に板の記録が無い → スキップ"); return
        print(f"[B] サンプル {len(Db):,} / 銘柄 {Db.code.nunique():,} / 日 {Db.day.nunique()}（{min(Db.day)}〜{max(Db.day)}）"
              f" / 陽線率 {Db.y.mean()*100:.1f}%")
        print("    場面別の件数: " + ", ".join(f"{k} {n:,}" for k, n in Db.k.value_counts().items()))
        if Db.day.nunique() <= MIN_TRAIN_DAYS_B:
            print(f"  日数 {Db.day.nunique()} ≤ {MIN_TRAIN_DAYS_B}（ウォークフォワード不可）。記録が貯まってから再実行。"); return
        pb = walk_forward(Db, B_PRICE + B_BOARD, MIN_TRAIN_DAYS_B)
        p0 = walk_forward(Db, B_PRICE, MIN_TRAIN_DAYS_B)
        report("Model B 板あり（値段8＋板12）", Db, pb)
        report("同じサンプルで板なし（値段8のみ）＝板の上乗せの比較相手", Db, p0)
        for k in Db.k.unique():
            m = (Db.k == k) & pb.notna()
            if m.sum() > 200:
                a1 = ((pb[m] >= 0.5) == (Db.y[m] == 1)).mean() * 100
                a0 = ((p0[m] >= 0.5) == (Db.y[m] == 1)).mean() * 100
                print(f"    {k:<14} 板あり 的中{a1:.1f}% / 板なし {a0:.1f}%（n={m.sum():,}）")
        print("  重要度（板あり・全期間で学習・参考）: " + ", ".join(f"{k} {v:.2f}" for k, v in importance(Db, B_PRICE + B_BOARD)))
        # 単純な板ルール（モデルなし）: imb3 の符号に張る
        m = Db.imb3.abs() >= 0.3
        if m.sum() > 100:
            side = np.sign(Db.imb3[m]); net = side * Db.ret[m] - COST_PCT
            print(f"\n  参考: モデル無しの単純ルール「上位3本の板の偏り|imb3|≥0.3 の向きに張る」 {m.sum():,}件"
                  f" 方向的中{(np.sign(Db.ret[m]) == side).mean()*100:.1f}% 平均{net.mean()*100:+.1f}bp（コスト後）")

    print("\n読み方: 的中率は多数派を当て続けた場合を、売買の平均はランダムを、それぞれ上回って初めて意味がある。"
          "\n        前半・後半の両方でコスト後プラスでなければ偶然。板の効果は「板あり − 板なし」で見る。"
          "\n        板の記録は土俵リスト（前夜に選んだ注目銘柄）だけなので、全銘柄に当てはまるとは限らない。")


if __name__ == "__main__":
    main()

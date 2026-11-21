# -*- coding: utf-8 -*-
"""_bt_15m_next_candle_1115.py — 「次の15分足が陽線か陰線か」を過去から当てられるか（2026-11-15）。

本人「君なら過去から次の15分足陽線か陰線予想できるはず」→ 方向そのものはほぼ五分五分の見立て。
ここでは「偏りがあるかもしれない場面」に絞って、①陽線になる確率 ②次の足を寄りで入って引けで出た損益（コスト後）
を測り、③前半の日付で見つかった偏りが後半の日付でも残るか（＝後付けでないか）を確かめる。

データ: _intraday_cache_5m.pkl（yfinance 5分足・{ticker: DataFrame[dt,o,h,l,c,v]}・足ラベル=開始時刻・約60営業日ローリング）
  - 9:00 の足が出来高0の気配足になる日がある → 出来高0の足は捨てる
  - 15分足は 9:00/9:15/…/11:15（前場10本）・12:30/…/15:15（後場12本、15:25〜15:30の引け板寄せは15:15の足に入る）
流動性: 前日の売買代金（5分足の c×v 合計）≥ 3億 の銘柄日だけ。
売買の想定: 足kの終値を見て、足k+1の始値で入り足k+1の終値で出る（成行・1本だけ持つ）。
コスト: COST_PCT（往復・既定0.10%＝呼値1つ分のスリッページ程度。手数料無料の口座想定）。
判定: 条件ごとに n・陽線率・次足の平均リターン(bp)・コスト後・前半/後半。
      「使える」の目安 = 前半と後半で同じ向き かつ 両方でコスト後プラス かつ n≥300。

実行: python -X utf8 _bt_15m_next_candle_1115.py > _log_15m_next_candle_1115.txt
"""
from __future__ import annotations

import pickle
import sys

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CACHE = "_intraday_cache_5m.pkl"
COST_PCT = 0.10
MIN_TOV_PREV = 3e8
MIN_N = 300


def to_15m(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["dt"])
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
    d = df.assign(dt=dt).sort_values("dt")
    d = d[(d.v > 0) & (d.c > 0)]
    hm = d.dt.dt.hour * 60 + d.dt.dt.minute
    am = (hm >= 540) & (hm < 690)                     # 9:00〜11:30
    pm = (hm >= 750) & (hm < 930)                     # 12:30〜15:30
    d = d[am | pm].copy()
    hm = d.dt.dt.hour * 60 + d.dt.dt.minute
    base = np.where(hm < 690, 540, 750)
    slot_min = base + ((hm - base) // 15) * 15
    slot_min = np.where(slot_min >= 915, 915, slot_min)   # 15:15 の足に引けまで入れる
    d["day"] = d.dt.dt.date
    d["slot"] = [f"{m // 60:02d}:{m % 60:02d}" for m in slot_min]
    d["pv"] = d.c * d.v
    g = d.groupby(["day", "slot"], sort=True)
    out = pd.DataFrame({"o": g.o.first(), "h": g.h.max(), "l": g.l.min(), "c": g.c.last(),
                        "v": g.v.sum(), "pv": g.pv.sum()}).reset_index()
    return out


def build() -> pd.DataFrame:
    C = pickle.load(open(CACHE, "rb"))
    rows = []
    for tk, df in C.items():
        if df is None or len(df) < 100 or not {"dt", "o", "h", "l", "c", "v"}.issubset(df.columns):
            continue
        b = to_15m(df)
        if b.empty:
            continue
        days = sorted(b.day.unique())
        prev_close, prev_tov = np.nan, np.nan
        for day in days:
            g = b[b.day == day].reset_index(drop=True)
            tov_today = float(g.pv.sum())
            if len(g) >= 8 and prev_tov >= MIN_TOV_PREV and prev_close > 0:
                o0 = float(g.o.iloc[0])
                cum_pv, cum_v = g.pv.cumsum().to_numpy(), g.v.cumsum().to_numpy()
                body = ((g.c / g.o - 1) * 100).to_numpy()
                rng = ((g.h / g.l - 1) * 100).to_numpy()
                for k in range(len(g) - 1):
                    nxt = g.iloc[k + 1]
                    if not (nxt.o > 0):
                        continue
                    vwap = cum_pv[k] / cum_v[k] if cum_v[k] > 0 else np.nan
                    streak = 0
                    for j in range(k, -1, -1):
                        s = np.sign(body[j])
                        if s == 0 or (streak and np.sign(streak) != s):
                            break
                        streak += int(s)
                    rows.append({
                        "tk": tk, "day": day, "slot_next": nxt.slot, "slot_k": g.slot.iloc[k],
                        "body": body[k], "rng_rel": rng[k] / (np.nanmean(rng[:k + 1]) + 1e-9),
                        "streak": streak, "above_vwap": bool(g.c.iloc[k] > vwap) if vwap == vwap else np.nan,
                        "day_ret": (g.c.iloc[k] / o0 - 1) * 100, "gap": (o0 / prev_close - 1) * 100,
                        "first_dir": np.sign(body[0]),
                        "y_up": int(nxt.c > nxt.o), "y_dn": int(nxt.c < nxt.o),
                        "ret": (nxt.c / nxt.o - 1) * 100,
                    })
            prev_close, prev_tov = float(g.c.iloc[-1]), tov_today
    D = pd.DataFrame(rows)
    days_sorted = sorted(D.day.unique())
    cut = days_sorted[len(days_sorted) // 2]
    D["half"] = np.where(D.day < cut, "前半", "後半")
    return D


def line(lab: str, d: pd.DataFrame) -> tuple[str, bool]:
    n = len(d)
    if n == 0:
        return f"  {lab:<34} n=0", False
    up = d.y_up.mean() * 100
    m = d.ret.mean()
    side = 1 if m >= 0 else -1                         # 平均の向きに張る（売りは空売り想定）
    net = side * m - COST_PCT
    h = {k: g.ret.mean() for k, g in d.groupby("half")}
    h1, h2 = h.get("前半", np.nan), h.get("後半", np.nan)
    ok = n >= MIN_N and np.sign(h1) == np.sign(h2) == side and side * h1 - COST_PCT > 0 and side * h2 - COST_PCT > 0
    s = (f"  {lab:<34} n={n:>7,} 陽線率{up:>5.1f}% 平均{m*100:>+6.1f}bp "
         f"{'買' if side > 0 else '売'}コスト後{net*100:>+6.1f}bp 前半{h1*100:>+6.1f}/後半{h2*100:>+6.1f}bp"
         + ("  ★" if ok else ""))
    return s, ok


def main() -> None:
    D = build()
    print(f"[15m] 次足サンプル {len(D):,} / 銘柄 {D.tk.nunique():,} / 日 {D.day.nunique()}"
          f"（{min(D.day)}〜{max(D.day)}・前半/後半で分割）/ コスト {COST_PCT}%往復")
    hits = []

    def show(title, groups):
        print(f"\n=== {title} ===")
        for lab, d in groups:
            s, ok = line(lab, d)
            print(s)
            if ok:
                hits.append(f"{title}｜{lab}")

    show("全体（何も条件をつけない）", [("全部", D)])

    print("\n=== 単純な予想の的中率（陽/陰が決まった足だけ）===")
    dd = D[(D.body != 0) & (D.y_up + D.y_dn > 0)]
    same = ((np.sign(dd.body) > 0) == (dd.y_up == 1)).mean() * 100
    print(f"  前の足と同じ向き（順張り） 的中{same:.1f}% / 逆（逆張り） 的中{100-same:.1f}%  n={len(dd):,}")
    for h, g in dd.groupby("half"):
        s2 = ((np.sign(g.body) > 0) == (g.y_up == 1)).mean() * 100
        print(f"    {h}: 順張り的中 {s2:.1f}%")

    show("次の足の時間帯", [(s, g) for s, g in D.groupby("slot_next")])
    show("前の足の実体（%）", [(lab, D[(D.body > lo) & (D.body <= hi)]) for lab, lo, hi in
                         (("大陰線 ≤−1%", -99, -1), ("陰線 −1〜−0.3%", -1, -0.3), ("小さい足 ±0.3%", -0.3, 0.3),
                          ("陽線 +0.3〜+1%", 0.3, 1), ("大陽線 > +1%", 1, 99))])
    show("同じ向きの連続本数", [(f"{'陽' if k > 0 else '陰'}{abs(k)}連続" + ("以上" if abs(k) >= 4 else ""),
                            D[(D.streak == k) if abs(k) < 4 else (np.sign(D.streak) * np.minimum(abs(D.streak), 4) == k)])
                           for k in (-4, -3, -2, -1, 1, 2, 3, 4)])
    show("前の足の値幅（その日の平均比）", [(lab, D[(D.rng_rel > lo) & (D.rng_rel <= hi)]) for lab, lo, hi in
                                (("静か ≤0.7倍", 0, 0.7), ("普通 0.7〜1.5倍", 0.7, 1.5), ("荒い 1.5〜2.5倍", 1.5, 2.5), ("急変 >2.5倍", 2.5, 99))])
    show("VWAPの上か下か", [("VWAPより上", D[D.above_vwap == True]), ("VWAPより下", D[D.above_vwap == False])])
    show("VWAP×前の足", [(f"VWAP{'上' if a else '下'}×前足{'陽' if s > 0 else '陰'}",
                         D[(D.above_vwap == a) & (np.sign(D.body) == s)]) for a in (True, False) for s in (1, -1)])
    show("その日の寄りからの位置", [(lab, D[(D.day_ret > lo) & (D.day_ret <= hi)]) for lab, lo, hi in
                             (("寄りから −3%以下", -99, -3), ("−3〜−1%", -3, -1), ("±1%", -1, 1), ("+1〜+3%", 1, 3), ("+3%以上", 3, 99))])
    show("寄りのギャップ（前日終値比）", [(lab, D[(D.gap > lo) & (D.gap <= hi)]) for lab, lo, hi in
                                (("GD −2%以下", -99, -2), ("−2〜−0.5%", -2, -0.5), ("±0.5%", -0.5, 0.5), ("+0.5〜+2%", 0.5, 2), ("GU +2%以上", 2, 99))])
    show("最初の15分の向き → 次の足（10時台まで）", [
        (f"最初の15分が{'陽' if s > 0 else '陰'}・次足{t}", D[(D.first_dir == s) & (D.slot_next == t)])
        for s in (1, -1) for t in ("09:15", "09:30", "09:45", "10:00")])
    show("引け前（14:45/15:00/15:15の足）× その日の位置", [
        (f"次足{t}・寄りから{'+1%超' if p > 0 else '−1%未満'}", D[(D.slot_next == t) & ((D.day_ret > 1) if p > 0 else (D.day_ret < -1))])
        for t in ("14:45", "15:00", "15:15") for p in (1, -1)])

    print("\n=== まとめ ===")
    if hits:
        print("  前半・後半ともコスト後プラスで向きもそろった条件（★）:")
        for h in hits:
            print(f"   ・{h}")
        print("  ※ 調べた条件の数が多いので、偶然★が付くこともある。★の条件だけで次の60日（新しいキャッシュ）を測り直すまでは本番に使わない。")
    else:
        print("  ★なし＝コストを払って勝てる偏りは見つからなかった（次の15分足の向きは実質五分五分）。")


if __name__ == "__main__":
    main()

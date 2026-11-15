# -*- coding: utf-8 -*-
"""bt_break60_both_recent.py — 初動ブレイク 買い（アプリ同一）＋売り（鏡像）の PF を直近N営業日で測る（2026-11-15）。

本人「PFどうなる」。list_break60_recent.py の一覧（買い17・売り8件）を実際に取引したらどうなるか。
J-Quants V2 直取得（CI で走る・KABUAI_API_LOOKBACK 日）。判定は list_break60_recent.scan と同一の式を全日に適用。
執行: シグナル翌日の寄り。買いは 寄り>前日終値+3% なら見送り（アプリの執行ルール）・S高寄りは買えない。
      売りは鏡像（寄り<前日終値−3% なら見送り・S安寄りは売れない）。貸借/売り禁は判定しない（＝売りは楽観側）。
出口:
  当日: 大引け / 利確x%・損切りy%（日足で同日に両方届いたら損切り先＝保守側）
  保有: 20営業日目の大引け・損切り15%（アプリ BT と同じ形）
コスト 0.2%/往復。PF = 総利益 ÷ 総損失（%ベース・1件同額）。
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

os.environ.setdefault("KABUAI_DATA_SOURCE", "jquants_api")
from build_data import load_jquants_api  # noqa: E402

HB, FRESH, VOLX, TOV = 60, 30, 1.5, 1e8
MAX_CHG, MAX_VOLX, MAX_OVER, GAP_MAX = 20.0, 10.0, 10.0, 3.0
COST, HOLD, SL_HOLD = 0.2, 20, 15.0
TPS, SLS = (1.0, 2.0, 3.0, 5.0), (1.0, 2.0, 3.0)
_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400),
        (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000),
        (30000, 5000), (50000, 7000), (70000, 10000)]


def lim_w(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return w
    return 10000.0


def fresh_mask(ev: np.ndarray) -> np.ndarray:
    out = np.zeros(len(ev), bool)
    last = -10**9
    for i, e in enumerate(ev):
        if e:
            if i - last > FRESH:
                out[i] = True
            last = i
    return out


def events(data: dict) -> pd.DataFrame:
    rows = []
    for tk, df in data.items():
        n = len(df)
        if n < HB + FRESH + 25:
            continue
        o, h, l, c, v = (df[k].astype(float).to_numpy() for k in ("Open", "High", "Low", "Close", "Volume"))
        cs = pd.Series(c)
        hi, lo = cs.rolling(HB).max().shift(1).to_numpy(), cs.rolling(HB).min().shift(1).to_numpy()
        volx = v / (pd.Series(v).rolling(20).mean().shift(1).to_numpy() + 1e-9)
        tov = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        chg = np.r_[np.nan, c[1:] / c[:-1] - 1] * 100
        ok = (tov >= TOV) & (volx >= VOLX) & (volx < MAX_VOLX) & (np.arange(n) + 1 < n)
        with np.errstate(invalid="ignore"):
            b = fresh_mask(np.nan_to_num(c > hi).astype(bool)) & (c > o) & ok & (chg < MAX_CHG) & ((c / hi - 1) * 100 < MAX_OVER)
            s = fresh_mask(np.nan_to_num(c < lo).astype(bool)) & (c < o) & ok & (chg > -MAX_CHG) & ((1 - c / lo) * 100 < MAX_OVER)
        for side, mask in (("買い", b), ("売り", s)):
            sg = 1 if side == "買い" else -1
            for i in np.where(mask)[0]:
                j = i + 1
                e = o[j]
                if not (e > 0):
                    continue
                gap = (e / c[i] - 1) * 100 * sg
                lw = lim_w(c[i])
                if gap > GAP_MAX or (sg == 1 and e >= c[i] + lw - 1e-6) or (sg == -1 and e <= c[i] - lw + 1e-6):
                    continue
                fav = ((h[j] if sg == 1 else l[j]) / e - 1) * 100 * sg       # 有利方向の最大
                adv = ((l[j] if sg == 1 else h[j]) / e - 1) * 100 * sg       # 不利方向の最大（負）
                day = (c[j] / e - 1) * 100 * sg
                # 20日保有・損切り15%
                stop = e * (1 - sg * SL_HOLD / 100)
                end = min(j + HOLD - 1, n - 1)
                hold = None
                if end - j < HOLD - 1:
                    hold = np.nan                                               # まだ20日経っていない
                else:
                    for k in range(j, end + 1):
                        if k > j and (o[k] - stop) * sg <= 0:
                            hold = (o[k] / e - 1) * 100 * sg; break
                        if ((l[k] if sg == 1 else h[k]) - stop) * sg <= 0:
                            hold = -SL_HOLD; break
                    if hold is None:
                        hold = (c[end] / e - 1) * 100 * sg
                rows.append({"side": side, "tk": tk, "d": df.index[j], "y": df.index[j].year,
                             "fav": fav, "adv": adv, "day": day, "hold": hold})
    return pd.DataFrame(rows)


def stat(r: pd.Series, d: pd.DataFrame) -> str:
    r = r.dropna()
    if not len(r):
        return "n=0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    yy = r.groupby(d.loc[r.index, "y"]).mean()
    ys = " ".join(f"{y}:{m:+.2f}" for y, m in yy.items())
    return (f"n={len(r):>5} 平均{r.mean():>+6.2f}% 勝率{(r>0).mean()*100:>4.1f}% "
            f"PF{(gp/gl if gl else np.inf):>5.2f} 中央値{r.median():>+6.2f}% ｜年別平均 {ys}")


def main() -> None:
    data, _, data_date, _ = load_jquants_api()
    D = events(data)
    print(f"\n[bt] データ最新日 {data_date} / イベント {len(D):,}件 "
          f"({D.d.min():%Y-%m-%d}〜{D.d.max():%Y-%m-%d}) 買い{(D.side=='買い').sum()} 売り{(D.side=='売り').sum()}")
    for side in ("買い", "売り"):
        d = D[D.side == side]
        print(f"\n======== {side}（{len(d):,}件・1日平均{len(d)/max(d.d.nunique(),1):.1f}本）========")
        print(f"  {'20日保有・損切り15%':<16} {stat(d.hold - COST, d)}")
        print(f"  {'当日 大引け':<16} {stat(d.day - COST, d)}")
        for tp in TPS:
            r = pd.Series(np.where(d.fav >= tp, tp, d.day), index=d.index) - COST
            print(f"  {'当日 利確+%g%%のみ' % tp:<16} {stat(r, d)}")
        for sl in SLS:
            r = pd.Series(np.where(d.adv <= -sl, -sl, d.day), index=d.index) - COST
            print(f"  {'当日 損切り-%g%%のみ' % sl:<16} {stat(r, d)}")
        for tp in TPS:
            for sl in SLS:
                hit_sl, hit_tp = d.adv <= -sl, d.fav >= tp
                r = pd.Series(np.where(hit_sl, -sl, np.where(hit_tp, tp, d.day)), index=d.index) - COST
                both = int((hit_sl & hit_tp).sum())
                print(f"  {'当日 +%g/-%g' % (tp, sl):<16} {stat(r, d)} ｜両方到達{both}件({both/max(len(d),1)*100:.0f}%)")
    print("\n読み方: 1件同額・コスト0.2%込み。売りは貸借/売り禁を見ていない＝実際は撃てない玉が混ざる（楽観側）。"
          "\n        当日の利確/損切りで同日に両方届いた場合は損切り扱い（保守側）。両方到達が多い行は5分足でないと確定しない。")


if __name__ == "__main__":
    main()

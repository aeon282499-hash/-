# -*- coding: utf-8 -*-
"""bt_vcp_recent.py — VCP（ボラ収縮）をスイングの出口とデイトレの入口に使えるか（2026-11-15）。

本人: J Law の VCP 五歩（X投稿）を見て「それでいい・デイトレで使えるもしくは応用したい」。
J-Quants V2 直取得（CI・KABUAI_API_LOOKBACK 日）。1件同額・コスト0.2%/往復。

■ VCP の形（日足・設定日 t の大引けで判定）
  トレンド: 終値 > 50日線 > 200日線、50日線と200日線が20日前より上、250日高値から15%以内、250日安値の+30%以上
  収縮   : 直近の押しの深さが 60〜30日前 > 30〜10日前 > 直近10日 と浅くなり、直近10日の押しは10%以内
  出来高 : 直近5日平均 < 50日平均×0.8（下げ・保ち合いで出来高が細る）
  流動性 : 20日平均代金 ≥ 1億
  ブレイクポイント(pivot) = 直近10日の高値
■ A. デイトレ（設定日 t → 翌日 t+1 の当日中だけ）
  入口: pivot に買い逆指値。t+1 の高値 ≥ pivot で約定（寄りが上なら寄り値）。寄りが pivot+3% 超は見送り。
  出口: 大引け / 利確+x% / 損切り−y%。
        日足では約定前後の順番が分からない → 損切り判定は「保守=安値が届けば損切り」と「楽観=約定は寄り後と見て、
        寄り>=pivot の時だけ安値で判定・それ以外は大引けで判定」の両方を出す。真実はこの間。
  対照: VCP条件なし・トレンド条件なしの「10日高値に逆指値」（同じ流動性）＝VCPフィルタの上乗せを測る。
■ B. スイング（出口をVCP流に）
  入口: ①VCP pivot 逆指値（上と同じ約定） ②🚩初動ブレイク全件（翌寄り・+3%超見送り） ③うち🆕新高値
  出口: 現行=20日保有・損切り15% / VCP流=損切り5% or 8%・+10%到達で損切りを買値へ・終値が10日線 or 20日線割れで翌寄り決済（最長120日）
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

os.environ.setdefault("KABUAI_DATA_SOURCE", "jquants_api")
from build_data import load_jquants_api  # noqa: E402

COST = 0.2
TOV_MIN = 1e8
CHASE = 3.0
TPS, SLS = (2.0, 3.0, 5.0), (2.0, 3.0, 5.0)
MAXH = 120


def roll_dd(h: np.ndarray, l: np.ndarray, a: int, b: int) -> np.ndarray:
    """各 t について [t-a, t-b) の区間の押しの深さ（区間内の高値からその後の安値までの最大下落%）を近似:
    区間の最高値 → 区間内でそれ以降の最安値。"""
    n = len(h)
    out = np.full(n, np.nan)
    for t in range(a, n):
        hs, ls = h[t - a:t - b + 1] if b > 0 else h[t - a:t + 1], l[t - a:t - b + 1] if b > 0 else l[t - a:t + 1]
        k = int(np.argmax(hs))
        out[t] = (1 - ls[k:].min() / hs[k]) * 100
    return out


def swing_exit(o, h, l, c, ma, j, e, sl_pct, be_at=10.0, ma_exit=True, hold=None, entry_on_j_low=True):
    """j=約定日・e=約定値。損切り sl_pct、+be_at% で損切りを建値へ、終値<maで翌寄り決済。hold 指定時は日数決済のみ。"""
    n = len(c)
    stop = e * (1 - sl_pct / 100)
    end = min(j + (hold - 1 if hold else MAXH), n - 1)
    if hold and j + hold - 1 > n - 1:
        return np.nan
    for k in range(j, end + 1):
        if k > j and o[k] <= stop:
            return (o[k] / e - 1) * 100
        if (k > j or entry_on_j_low) and l[k] <= stop:
            return (stop / e - 1) * 100
        if not hold:
            if h[k] >= e * (1 + be_at / 100):
                stop = max(stop, e)
            if ma_exit and k > j and np.isfinite(ma[k]) and c[k] < ma[k]:
                if k + 1 <= n - 1:
                    return (o[k + 1] / e - 1) * 100
                return np.nan
    if hold:
        return (c[end] / e - 1) * 100
    return np.nan if end == n - 1 else (c[end] / e - 1) * 100


def scan(data: dict):
    day_rows, sw_rows = [], []
    for tk, df in data.items():
        n = len(df)
        if n < 260:
            continue
        o, h, l, c, v = (df[k].astype(float).to_numpy() for k in ("Open", "High", "Low", "Close", "Volume"))
        cs = pd.Series(c)
        ma10, ma20 = cs.rolling(10).mean().to_numpy(), cs.rolling(20).mean().to_numpy()
        ma50, ma200 = cs.rolling(50).mean().to_numpy(), cs.rolling(200).mean().to_numpy()
        hi250, lo250 = cs.rolling(250, min_periods=200).max().to_numpy(), cs.rolling(250, min_periods=200).min().to_numpy()
        tov = pd.Series(c * v).rolling(20).mean().to_numpy()
        v5, v50 = pd.Series(v).rolling(5).mean().to_numpy(), pd.Series(v).rolling(50).mean().to_numpy()
        piv = pd.Series(h).rolling(10).max().to_numpy()
        d1, d2, d3 = roll_dd(h, l, 60, 30), roll_dd(h, l, 30, 10), roll_dd(h, l, 10, 0)
        with np.errstate(invalid="ignore"):
            trend = (c > ma50) & (ma50 > ma200) & (ma50 > np.roll(ma50, 20)) & (ma200 > np.roll(ma200, 20)) \
                & (c >= hi250 * 0.85) & (c >= lo250 * 1.3)
            vcp = trend & (d1 > d2) & (d2 > d3) & (d3 <= 10) & (v5 < v50 * 0.8) & (tov >= TOV_MIN)
            base = (tov >= TOV_MIN) & np.isfinite(piv)
        idx = df.index
        for t in np.where(base[:-1])[0]:
            if t < 250:
                continue
            j = t + 1
            p = piv[t]
            if not (h[j] >= p) or o[j] > p * (1 + CHASE / 100):
                continue
            e = max(o[j], p)
            gap_in = o[j] >= p
            r = {"tk": tk, "d": idx[j], "y": idx[j].year, "vcp": bool(vcp[t]), "gap_in": gap_in,
                 "fav": (h[j] / e - 1) * 100, "adv": (l[j] / e - 1) * 100, "day": (c[j] / e - 1) * 100}
            if vcp[t]:
                day_rows.append(r)
            elif np.random.random() < 0.15:          # 対照は件数が膨大なので15%抽出
                day_rows.append({**r, "vcp": False})
            if vcp[t]:
                for lab, sl, ma, hold in (("現行 20日・損切15%", 15, None, 20), ("VCP流 損切5%・10日線", 5, ma10, None),
                                          ("VCP流 損切5%・20日線", 5, ma20, None), ("VCP流 損切8%・10日線", 8, ma10, None),
                                          ("VCP流 損切8%・20日線", 8, ma20, None)):
                    sw_rows.append({"set": "①VCP pivot逆指値", "ex": lab, "y": idx[j].year,
                                    "r": swing_exit(o, h, l, c, ma, j, e, sl, hold=hold)})
        # ②③ 初動ブレイク（アプリ同一判定）
        hi60 = cs.rolling(60).max().shift(1).to_numpy()
        brk = np.nan_to_num(c > hi60).astype(bool)
        fresh = np.zeros(n, bool); last = -10**9
        for i in range(n):
            if brk[i]:
                if i - last > 30:
                    fresh[i] = True
                last = i
        volx = v / (pd.Series(v).rolling(20).mean().shift(1).to_numpy() + 1e-9)
        tovp = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        chg = np.r_[np.nan, c[1:] / c[:-1] - 1] * 100
        with np.errstate(invalid="ignore"):
            trig = fresh & (c > o) & (volx >= 1.5) & (volx < 10) & (tovp >= TOV_MIN) & (chg < 20) \
                & ((c / hi60 - 1) * 100 < 10) & (np.arange(n) + 1 < n)
        for i in np.where(trig)[0]:
            j = i + 1
            e = o[j]
            if not (e > 0) or e > c[i] * (1 + CHASE / 100):
                continue
            new = bool(np.isfinite(hi250[i - 1]) and c[i] > hi250[i - 1]) if i >= 1 else False
            for lab, sl, ma, hold in (("現行 20日・損切15%", 15, None, 20), ("VCP流 損切5%・10日線", 5, ma10, None),
                                      ("VCP流 損切5%・20日線", 5, ma20, None), ("VCP流 損切8%・10日線", 8, ma10, None),
                                      ("VCP流 損切8%・20日線", 8, ma20, None)):
                rr = swing_exit(o, h, l, c, ma, j, e, sl, hold=hold)
                sw_rows.append({"set": "②初動ブレイク全件", "ex": lab, "y": idx[j].year, "r": rr})
                if new:
                    sw_rows.append({"set": "③うち🆕新高値", "ex": lab, "y": idx[j].year, "r": rr})
    return pd.DataFrame(day_rows), pd.DataFrame(sw_rows)


def stat(r: pd.Series, years: pd.Series) -> str:
    m = r.notna()
    r, years = r[m], years[m]
    if not len(r):
        return "n=0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    yy = r.groupby(years).mean()
    return (f"n={len(r):>6,} 平均{r.mean():>+6.2f}% 勝率{(r>0).mean()*100:>4.1f}% PF{(gp/gl if gl else np.inf):>5.2f} "
            f"中央値{r.median():>+6.2f}% 勝ち平均{r[r>0].mean() if (r>0).any() else 0:>+6.2f}/負け平均{r[r<=0].mean() if (r<=0).any() else 0:>+6.2f} "
            f"｜年別 " + " ".join(f"{y}:{x:+.2f}" for y, x in yy.items()))


def main() -> None:
    np.random.seed(0)
    data, _, data_date, _ = load_jquants_api()
    DY, SW = scan(data)
    print(f"\n[vcp] データ最新日 {data_date} / デイトレ行 {len(DY):,}（VCP {int(DY.vcp.sum()):,}）/ スイング行 {len(SW):,}")

    print("\n================ A. デイトレ（前日VCP → pivot 逆指値 → 当日決済） ================")
    for lab, d in (("VCPあり", DY[DY.vcp]), ("対照: ただの10日高値抜け(15%抽出)", DY[~DY.vcp])):
        nd = d.d.nunique() if len(d) else 1
        print(f"\n--- {lab}（{len(d):,}件・約定のあった日1日平均{len(d)/max(nd,1):.1f}本）---")
        print(f"  {'大引け':<22} {stat(d.day - COST, d.y)}")
        for tp in TPS:
            print(f"  {'利確+%g%%のみ' % tp:<22} {stat(pd.Series(np.where(d.fav >= tp, tp, d.day), index=d.index) - COST, d.y)}")
        for sl in SLS:
            cons = np.where(d.adv <= -sl, -sl, d.day)
            opt = np.where(d.gap_in & (d.adv <= -sl), -sl, d.day)
            print(f"  {'損切り-%g%% 保守' % sl:<22} {stat(pd.Series(cons, index=d.index) - COST, d.y)}")
            print(f"  {'損切り-%g%% 楽観' % sl:<22} {stat(pd.Series(opt, index=d.index) - COST, d.y)}")
        for tp in TPS:
            for sl in SLS:
                cons = np.where(d.adv <= -sl, -sl, np.where(d.fav >= tp, tp, d.day))
                opt = np.where(d.fav >= tp, tp, np.where(d.gap_in & (d.adv <= -sl), -sl, d.day))
                print(f"  {'+%g/-%g 保守' % (tp, sl):<22} {stat(pd.Series(cons, index=d.index) - COST, d.y)}")
                print(f"  {'+%g/-%g 楽観' % (tp, sl):<22} {stat(pd.Series(opt, index=d.index) - COST, d.y)}")

    print("\n================ B. スイング（出口をVCP流に） ================")
    for s in ("①VCP pivot逆指値", "②初動ブレイク全件", "③うち🆕新高値"):
        print(f"\n--- {s} ---")
        for ex in ("現行 20日・損切15%", "VCP流 損切5%・10日線", "VCP流 損切5%・20日線", "VCP流 損切8%・10日線", "VCP流 損切8%・20日線"):
            d = SW[(SW.set == s) & (SW.ex == ex)]
            print(f"  {ex:<18} {stat(d.r - COST, d.y)}")
    print("\n読み方: 1件同額・コスト0.2%込み。保守/楽観の間が真実（日足では約定と安値の前後が分からない）。"
          "\n        スイングのVCP流は +10%到達で損切りを建値へ・終値が移動平均割れで翌寄り決済・最長120日。期間末で未決済の玉は除外。")


if __name__ == "__main__":
    main()

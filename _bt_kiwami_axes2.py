# -*- coding: utf-8 -*-
"""_bt_kiwami_axes2.py — 極み買いの改善候補を実運用構成（3枠×100万・実株数）で総当たり（2026-08-02）。

本人「さらに儲かるようにアップデートして」。対象は7/30に棚上げした2候補＋未検証軸1つ:
  A: 買残回転の除外閾値  0.8(現行) / 1.0 / 1.2 / なし
  B: 株価上限            7,000 / 8,000(7/30候補) / 9,000 / 10,000(現行)
  C: 空売り残高回転(short_dc) ＝ _probe_shortdc_atr.csv に結合済みで**判定記録の無い未検証軸**

エンジンは _bt_tiers_reality.py 方式（全候補から営業日・出口はキャッシュからリプレイ＝
8/1のキャッシュ修復が自動で効く・±6%のカレンダー差異に最も強い）を踏襲。
採用バー: 両期間(17-21/22-26)改善 × 面が高原 × 上位20玉除去でも残る × 機構の説明がつく。
実行: python -X utf8 _bt_kiwami_axes2.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd

MAXH, MAX_SIG, SECTOR_CAP = 3, 5, 3
TP, LIVE_STOP = 5.0, 3.0
YEARS = list(range(2017, 2027))
SIZE, SLOTS = 1_000_000, 3          # 極み実運用 = 3枠×100万

C0 = pd.read_csv("_probe_shortdc_atr.csv", parse_dates=["entry"])
C0 = C0.sort_values(["entry", "score"], ascending=[True, False]).reset_index(drop=True)

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for tk in C0["ticker"].unique():
    dfs = [d for src in (old["all_data"], new["all_data"])
           if (d := src.get(tk)) is not None and len(d)]
    if not dfs:
        continue
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    cl = df["Close"].astype(float)
    dlt = cl.diff()
    ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
    SER[tk] = (df["Open"].astype(float).to_numpy(), df["High"].astype(float).to_numpy(),
               df["Low"].astype(float).to_numpy(), cl.to_numpy(), rsi.to_numpy(),
               {d: i for i, d in enumerate(df.index.strftime("%Y-%m-%d"))}, len(cl))
del old, new
SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))
ALLDAYS = sorted(C0["entry"].unique())
GDI = {d: i for i, d in enumerate(ALLDAYS)}
print(f"[prep] 候補{len(C0):,}件 / {len(SER)}銘柄 / 営業日{len(ALLDAYS)}日", flush=True)
print(f"[short_dc] 有効{C0['short_dc'].notna().mean()*100:.0f}% 分布 "
      f"{C0['short_dc'].describe()[['25%','50%','75%','max']].round(3).to_dict()}")

n = len(C0)
ds = C0["entry"].dt.strftime("%Y-%m-%d").to_numpy()
E = np.full(n, np.nan)
OP = np.full((n, MAXH), np.nan); HI = np.full((n, MAXH), np.nan)
LO = np.full((n, MAXH), np.nan); CL = np.full((n, MAXH), np.nan)
RS = np.full((n, MAXH), np.nan)
for i in range(n):
    a = SER.get(C0["ticker"].iat[i])
    if a is None:
        continue
    o, h, l, c, r, pos, ln = a
    p = pos.get(ds[i])
    if p is None or p + MAXH - 1 >= ln:
        continue
    e = o[p]
    if not (e > 0) or np.isnan(e):
        continue
    E[i] = e
    for k in range(MAXH):
        OP[i, k], HI[i, k], LO[i, k], CL[i, k], RS[i, k] = o[p+k], h[p+k], l[p+k], c[p+k], r[p+k]
by_day: dict = {}
for i in range(n):
    by_day.setdefault(GDI[C0["entry"].iat[i]], []).append(i)
TICK = C0["ticker"].to_numpy(); YEAR = C0["year"].to_numpy()
SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
PRICE = C0["price"].to_numpy(); DC = C0["days_cover"].to_numpy()
SDC = C0["short_dc"].to_numpy()

# 出口リプレイ（_bt_tiers_reality.run_exit と同一）
pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~np.isfinite(E)
sl = E * (1 - LIVE_STOP / 100); tl = E * (1 + TP / 100)
for k in range(MAXH):
    live = ~done
    if not live.any():
        break
    if k > 0:
        op = OP[:, k]
        g = live & (op > 0) & np.isfinite(op) & ((op <= sl) | (op >= tl))
        pnl[g] = (op[g] - E[g]) / E[g] * 100; exo[g] = k; done |= g; live = ~done
    s = live & (LO[:, k] <= sl); pnl[s] = -LIVE_STOP; exo[s] = k; done |= s; live = ~done
    t = live & (HI[:, k] >= tl); pnl[t] = TP; exo[t] = k; done |= t; live = ~done
    r = live & (((RS[:, k] >= 50) & np.isfinite(RS[:, k])) | (k == MAXH - 1))
    pnl[r] = (CL[r, k] - E[r]) / E[r] * 100; exo[r] = k; done |= r


def run(mask: np.ndarray) -> pd.DataFrame:
    """mask を通した候補で選定シム。**参照エンジンと同一の2段構造**
    （_bt_tiers_reality.select → sim の分離。7/29-30の記録済み数字と同じ作法。
    1段目: 1日5件・業種cap3・保有中除外で「配信される玉」を決める
    2段目: 同時3枠・実株数100万/玉で「実際に建てる玉」を決める＝枠溢れはスキップ）。"""
    ok = np.isfinite(pnl) & mask
    ou: dict = {}; os_: dict = {}
    picks = []
    for d in range(len(ALLDAYS)):
        for tk in [t for t, u in ou.items() if u < d]:
            del ou[tk]; del os_[tk]
        sc: dict = {}
        for s in os_.values():
            sc[s] = sc.get(s, 0) + 1
        cnt = 0
        for i in by_day.get(d, []):
            if cnt >= MAX_SIG:
                break
            if not ok[i] or TICK[i] in ou:
                continue
            s = SEC[i]
            if sc.get(s, 0) >= SECTOR_CAP:
                continue
            cnt += 1
            ex = min(d + int(exo[i]), len(ALLDAYS) - 1)
            ou[TICK[i]] = ex; os_[TICK[i]] = s; sc[s] = sc.get(s, 0) + 1
            picks.append((d, ex, int(YEAR[i]), float(pnl[i]), float(E[i]), TICK[i]))
    live: list = []
    rows = []
    for d, ex, y, p, e, tk in picks:
        live = [x for x in live if x >= d]
        if len(live) >= SLOTS:
            continue
        sh = int(SIZE / e / 100) * 100
        if sh <= 0:
            continue
        live.append(ex)
        rows.append({"d": d, "exit": ex, "y": y, "ticker": tk,
                     "pnl": p, "yen": p / 100 * sh * e})
    return pd.DataFrame(rows)


def summarize(R: pd.DataFrame, label: str) -> str:
    yy = R.groupby("y").yen.sum().reindex(YEARS, fill_value=0.0)
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    top20 = R.nlargest(20, "yen").yen.sum()
    return (f"  {label:<26}{len(R):>6}{gp/gl if gl else float('inf'):>6.2f}"
            f"{R.yen.sum():>+13,.0f}{yy[yy.index <= 2021].sum():>+12,.0f}"
            f"{yy[yy.index >= 2022].sum():>+12,.0f}{int((yy > 0).sum()):>4}/10"
            f"{yy.min():>+11,.0f}{R.yen.sum() - top20:>+13,.0f}")


HDR = (f"  {'構成':<26}{'件数':>6}{'PF円':>6}{'10年計':>13}{'前半17-21':>12}"
       f"{'後半22-26':>12}{'勝年':>5}{'最悪年':>11}{'上位20除去':>13}")

print("\n" + "=" * 118)
print("① 買残回転(dc) × 株価上限 の16マス（極み実運用構成）")
print("=" * 118)
print(HDR)
for dc_thr in (0.8, 1.0, 1.2, None):
    for cap in (7000, 8000, 9000, 10000):
        m = (PRICE <= cap) & (~(DC > dc_thr) if dc_thr is not None else np.ones(n, bool))
        lab = f"dc{'∞' if dc_thr is None else dc_thr}×{cap//1000}千円" + \
              ("（現行）" if dc_thr == 0.8 and cap == 10000 else "")
        print(summarize(run(m), lab))

print("\n" + "=" * 118)
print("② 空売り残高回転(short_dc)の層別（現行フィルタ dc0.8×1万円 の上に重ねる）")
print("=" * 118)
base = ~(DC > 0.8) & (PRICE <= 10000)
q = np.nanquantile(SDC, [0.2, 0.4, 0.6, 0.8])
print(f"  short_dc 五分位点: {np.round(q, 3)}")
print(HDR)
print(summarize(run(base), "ベース（現行）"))
# 高い側/低い側の除外を両方向
for lab, m in [
    ("short_dc>q80除外", base & ~(SDC > q[3])),
    ("short_dc>q60除外", base & ~(SDC > q[2])),
    ("short_dc<q20除外", base & ~(SDC < q[0])),
    ("short_dc<q40除外", base & ~(SDC < q[1])),
    ("short_dc欠損も除外(>q80)", base & (SDC <= q[3])),
]:
    print(summarize(run(m), lab))
# 層別の素の成績（選定なし・候補レベル）
print("  --- 候補レベルの帯別（選定なし・等加重%） ---")
okc = np.isfinite(pnl) & base
bins = [-np.inf, q[0], q[1], q[2], q[3], np.inf]
for b in range(5):
    mm = okc & (SDC > bins[b]) & (SDC <= bins[b + 1])
    if mm.sum():
        pp = pnl[mm]
        gp = pp[pp > 0].sum(); gl = -pp[pp <= 0].sum()
        print(f"    帯{b+1} ({bins[b]:.2f}〜{bins[b+1]:.2f}] n={mm.sum():>5} "
              f"平均{pp.mean():+.3f}% PF{gp/gl if gl else float('inf'):.2f}")
mm = okc & ~np.isfinite(SDC)
if mm.sum():
    pp = pnl[mm]
    print(f"    欠損 n={mm.sum()} 平均{pp.mean():+.3f}%")

print("\n" + "=" * 118)
print("③ 感度: 優先株汚染だった6銘柄を除いた場合（①の主要セルのみ）")
print("=" * 118)
BAD = {"2593.T", "5076.T", "7550.T", "9201.T", "9202.T", "9434.T"}
nb = ~np.isin(TICK, list(BAD))
print(HDR)
for dc_thr, cap, lab in [(0.8, 10000, "現行-6銘柄"), (1.2, 10000, "dc1.2-6銘柄"),
                          (0.8, 8000, "8千円-6銘柄"), (1.2, 8000, "dc1.2×8千-6銘柄")]:
    m = (PRICE <= cap) & ~(DC > dc_thr) & nb
    print(summarize(run(m), lab))

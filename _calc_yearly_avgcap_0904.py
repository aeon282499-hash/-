# -*- coding: utf-8 -*-
"""年別: 平均使用資金（営業日ごとの建玉金額の平均）と、それに対する年利。極み買い/極み売り/フェード。2026-09-04。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
# ── 極み買い ──
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
import jpholiday
from datetime import date as _dt, timedelta as _td
def _open(d): return d.weekday() < 5 and not jpholiday.is_holiday(d) and not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))
BD = [pd.Timestamp(d) for d in pd.date_range("2016-09-01", "2026-09-03") if _open(d.date())]
BDI = {d: i for i, d in enumerate(BD)}
def bidx(ts):
    ts = pd.Timestamp(ts)
    return BDI.get(ts, int(np.searchsorted(np.array(BD, dtype="datetime64[ns]"), np.datetime64(ts))))
nd = len(BD); use = np.zeros(nd)
sh = (1_000_000 / E[R0.i.to_numpy()] // 100 * 100); notional = sh * E[R0.i.to_numpy()]
for d, ex, nv in zip(R0.d.to_numpy(), R0["exit"].to_numpy(), notional):
    use[bidx(ALLDAYS[d]):bidx(ALLDAYS[ex]) + 1] += nv
yrs = pd.Series([x.year for x in BD])
avg_buy = pd.Series(use).groupby(yrs).mean(); yen_buy = R0.groupby("y").yen.sum()
# ── 極み売り（sim(3) を exit 付きで再現） ──
src = open("_bt_kiwami_sell_slots.py", encoding="utf-8").read().split('print(f"[候補]')[0]
exec(src)
def sim3(slots=3):
    days = sorted(A["sig"].unique()); di = {d: i for i, d in enumerate(days)}
    live, held, rows = [], {}, []
    for d, g in A.groupby("sig", sort=True):
        i = di[d]; live = [x for x in live if x >= i]; held = {t: x for t, x in held.items() if x >= i}
        used, nn = {}, 0
        for r in g.to_dict("records"):
            if nn >= MAX_PER_DAY or len(live) >= slots: break
            if r["ticker"] in held: continue
            if r["sector"] and used.get(r["sector"], 0) >= SECTOR_CAP: continue
            p, k = replay(r); s_ = int(SIZE / r["entry"] / 100) * 100
            if s_ <= 0: continue
            if r["sector"]: used[r["sector"]] = used.get(r["sector"], 0) + 1
            live.append(i + k); held[r["ticker"]] = i + k; nn += 1
            rows.append({"d": bidx(r["sig"]), "exit": bidx(r["sig"]) + k, "y": r["year"], "yen": p / 100 * s_ * r["entry"], "notional": s_ * r["entry"]})
    return pd.DataFrame(rows), days
Rs, sdays = sim3()
use_s = np.zeros(nd)
for d, ex, nv in zip(Rs.d, Rs["exit"], Rs.notional): use_s[d:min(ex, nd - 1) + 1] += nv
avg_sell = pd.Series(use_s).groupby(yrs).mean(); yen_sell = Rs.groupby("y").yen.sum()
# ── フェード ──
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
d0 = select(P); cap = np.where(d0.rk == 1, S1, S2); shf = (cap / d0.px // 100 * 100).astype(int); shf = np.where((d0.rk == 2) & (d0.px * 100 > S2), 0, shf)
d0["yen"] = d0.pnl / 100 * shf * d0.o1; d0["notional"] = shf * d0.o1
daily = d0.groupby("ent").agg(yen=("yen", "sum"), notional=("notional", "sum")); daily.index = pd.to_datetime(daily.index)
bd = pd.Series(BD); bd = bd[bd >= daily.index.min()]
full = daily.reindex(bd, fill_value=0.0)
avg_fade = full.notional.groupby(full.index.year).mean(); yen_fade = full.yen.groupby(full.index.year).sum()
print(f"{'年':<6}{'買い平均使用':>10}{'買い損益':>9}{'買い年利':>8}{'売り平均使用':>10}{'売り損益':>9}{'売り年利':>8}{'ﾌｪｰﾄﾞ平均使用':>12}{'ﾌｪｰﾄﾞ損益':>10}{'ﾌｪｰﾄﾞ年利':>9}")
for y in range(2021, 2027):
    ab, yb = avg_buy.get(y, np.nan), yen_buy.get(y, 0); as_, ys = avg_sell.get(y, np.nan), yen_sell.get(y, 0); af, yf = avg_fade.get(y, np.nan), yen_fade.get(y, 0)
    print(f"{y:<6}{ab/1e4:>9.0f}万{yb/1e4:>+8.1f}万{yb/ab*100:>+7.1f}%{as_/1e4:>9.0f}万{ys/1e4:>+8.1f}万{ys/as_*100:>+7.1f}%{af/1e4:>11.0f}万{yf/1e4:>+9.1f}万{yf/af*100:>+8.1f}%")
b5 = yen_buy.loc[2021:2025].sum() / avg_buy.loc[2021:2025].mean() / 5 * 100; s5 = yen_sell.loc[2021:2025].sum() / avg_sell.loc[2021:2025].mean() / 5 * 100; f5 = yen_fade.loc[2021:2025].sum() / avg_fade.loc[2021:2025].mean() / 5 * 100
print(f"\n5年平均 年利（平均使用資金ベース）: 買い{b5:+.1f}% / 売り{s5:+.1f}% / フェード{f5:+.1f}%")
print(f"5年の平均使用資金: 買い{avg_buy.loc[2021:2025].mean()/1e4:.0f}万 / 売り{avg_sell.loc[2021:2025].mean()/1e4:.0f}万 / フェード{avg_fade.loc[2021:2025].mean()/1e4:.0f}万")
print("[done]")

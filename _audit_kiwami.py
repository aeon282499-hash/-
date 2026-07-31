# -*- coding: utf-8 -*-
"""_audit_kiwami.py — 極み(スイングBUY)の本番vsBT監査（2026-07-31）。

フェードで6件のズレが出た同じ検査を極みにも通す。

■ 突き合わせ結果（コードを1行ずつ照合）
  RSI / 25MA乖離 / ATR%(高ボラ除外) … 本番もBTも**シグナル日**を見る（一致）
  値幅/ATR・出来高比・売買代金      … 本番もBTも**シグナル日の前日(iloc[-2])**を見る（一致）
  ＝BTは本番を忠実に再現している。**ただし本番の中で日付が揃っていない。**
  デイトレ側は同じ問題を iloc[-2]→iloc[-1] として修正済みだが、スイング側は -2 のまま。

■ ここで測る変種
  A) 現行（-2のまま）＝BTの再現。基準
  B) 出来高比/代金/値幅比を**シグナル日(-1)**に揃える
  C) ETF誤判定の修正（"ブル"/"ベア"/"ダブル"で実在株10銘柄を落としている分を戻す）
  D) B+C

■ 既知のBT側の限界（結果の読み方・bt_10y_robustness.py のヘッダにも記載）
  ・name_mapが現在の上場銘柄由来＝上場廃止銘柄が入らない（生存バイアス）
  ・earnings_calendar.jsonは2021年以前ほぼ未収録＝決算±3日除外が古い年ほど効かない
    （本番は3d38d74でJPX公式予定表に移行済み＝**BTの方が甘い**）

実行: python -X utf8 _audit_kiwami.py
"""
from __future__ import annotations

import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from screener import is_etf_ticker, yose_limit_price

SINCE = "2017-01-01"
TURNOVER_MIN, STOP, TP, MAXH = 2e9, 3.0, 5.0, 3
EARN_WIN, SECTOR_CAP, SLOTS, SIZE = 3, 3, 3, 1_000_000
YEARS = list(range(2017, 2027))
# is_etf_ticker が名称キーワードで誤判定する実在株（2026-07-31の監査で特定）
FALSE_ETF = {"2208.T", "2798.T", "2804.T", "3925.T", "5597.T",
             "6479.T", "6619.T", "7375.T", "7683.T", "9254.T"}


def load_earnings():
    p = Path("earnings_calendar.json")
    if not p.exists():
        return {}
    raw = json.load(open(p, encoding="utf-8"))
    recs = raw.get("records", raw) if isinstance(raw, dict) else raw
    out = {}
    if isinstance(recs, dict):
        for tk, dates in recs.items():
            s = set()
            for ds in dates:
                try:
                    d = datetime.strptime(ds, "%Y-%m-%d").date()
                except Exception:
                    continue
                for off in range(-EARN_WIN, EARN_WIN + 1):
                    s.add((d + timedelta(days=off)).strftime("%Y-%m-%d"))
            out[tk] = s
    return out


def sim(p, on, hn, ln, cn, rn, n):
    """本番の出口（寄りギャップ優先→STOP→TP→RSI50→期限）。bt_sector_cap.sim と同一。"""
    if p + MAXH - 1 >= n:
        return None, None
    e = on[p]
    if not (e > 0) or np.isnan(e):
        return None, None
    stop, tpp = e * (1 - STOP / 100), e * (1 + TP / 100)
    for k in range(MAXH):
        q = p + k
        if k > 0:
            op = on[q]
            if op > 0 and not np.isnan(op):
                if op <= stop:
                    return (op - e) / e * 100, k
                if op >= tpp:
                    return (op - e) / e * 100, k
        if ln[q] <= stop:
            return -STOP, k
        if hn[q] >= tpp:
            return +TP, k
        if (not np.isnan(rn[q]) and rn[q] >= 50) or k == MAXH - 1:
            return (cn[q] - e) / e * 100, k
    return None, None


print("[load] 読込中...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
name_map = dict(old["name_map"]); name_map.update(new["name_map"])
data = {}
for src in (old["all_data"], new["all_data"]):
    for tk, df in src.items():
        data.setdefault(tk, []).append(df)
merged = {}
for tk, dfs in data.items():
    df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index()
    merged[tk] = df[~df.index.duplicated(keep="last")]
secmap = json.load(open("sector33_map.json", encoding="utf-8"))
earn = load_earnings()
since_ts = pd.Timestamp(SINCE)
MG = pickle.load(open("_margin_10y_full.pkl", "rb"))


def collect(shift_fresh: bool, fix_etf: bool):
    """shift_fresh=True で 出来高比/代金/値幅比 をシグナル日(-1)に揃える。"""
    rows = []
    for tk, df in merged.items():
        if df is None or len(df) < 140:
            continue
        name = name_map.get(tk)
        if name is None:
            continue
        if is_etf_ticker(tk, name) and not (fix_etf and tk in FALSE_ETF):
            continue
        o = df["Open"].astype(float); h = df["High"].astype(float)
        l = df["Low"].astype(float); cl = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
        ma25 = cl.rolling(25).mean()
        dev = ((cl - ma25) / ma25 * 100).round(2)
        pc = cl.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        sh = 0 if shift_fresh else 1                 # 0=シグナル日 / 1=その前日
        rr = ((h - l).shift(sh) / atr.shift(sh)).round(2)
        vr = (v.shift(sh) / v.shift(sh + 1).rolling(20).mean()).round(2)
        tov = cl.shift(sh) * v.shift(sh)
        atr_pct = atr / cl * 100
        cand = ((rsi <= 45) & (dev <= -1.5) & ((rr >= 1.5) | (vr >= 2.0))
                & (tov >= TURNOVER_MIN) & (atr_pct <= 3.0) & (cl.index >= since_ts))
        if not cand.fillna(False).any():
            continue
        on, hn, ln = o.to_numpy(), h.to_numpy(), l.to_numpy()
        cn, rn = cl.to_numpy(), rsi.to_numpy()
        idx, n = cl.index, len(cn)
        ewin = earn.get(tk) or earn.get(tk.replace(".T", "")) or set()
        m = MG.get(str(tk)[:4])
        if m is not None and len(m):
            m = m.sort_index()
            midx = np.array([d.strftime("%Y-%m-%d") for d in m.index])
            lv = m["LongVol"].to_numpy(float)
        else:
            midx = None
        tov20 = (cl * v).rolling(20).mean().to_numpy()
        for t in np.where(cand.fillna(False).to_numpy())[0]:
            if t + 2 >= n:
                continue
            ed = idx[t + 1].strftime("%Y-%m-%d")
            if ed < SINCE or ed in ewin:
                continue
            lp = yose_limit_price(cn[t])
            if lp and on[t + 1] > lp:
                continue
            res, exoff = sim(t + 1, on, hn, ln, cn, rn, n)
            if res is None:
                continue
            dc = np.nan
            if midx is not None and tov20[t] > 0:
                p = np.searchsorted(midx, idx[t].strftime("%Y-%m-%d"), side="right") - 1
                if p >= 0:
                    dc = lv[p] * cn[t] / tov20[t]
            rsi_t, dev_t, tov_t = rn[t], dev.iloc[t], tov.iloc[t]
            score = (1 / (1 + ((rsi_t - 38) / 8) ** 2) * 0.30
                     + 1 / (1 + ((dev_t + 3) / 2) ** 2) * 0.30
                     + np.log10(max(tov_t, 1) / 1e9 + 1) / 3 * 0.40)
            rows.append({"entry": ed, "y": idx[t + 1].year, "ticker": tk, "price": cn[t],
                         "score": score, "pnl": res, "exoff": int(exoff), "days_cover": dc,
                         "sector": secmap.get(tk, "")})
    return pd.DataFrame(rows)


def slots(C):
    """3枠・業種cap3・買残回転0.8以下・1玉100万で本番と同じ回し方をする。"""
    C = C[~(C.days_cover > 0.8)].sort_values(["entry", "score"], ascending=[True, False])
    days = sorted(C.entry.unique()); di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in C.groupby("entry", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        used, n = {}, 0
        for r in g.itertuples():
            if n >= SLOTS or len(busy) >= SLOTS:
                break
            if r.ticker in held or not np.isfinite(r.pnl) or r.price * 100 > SIZE:
                continue
            if r.sector and used.get(r.sector, 0) >= SECTOR_CAP:
                continue
            if r.sector:
                used[r.sector] = used.get(r.sector, 0) + 1
            sh = int(SIZE / r.price / 100) * 100
            busy.append(i + int(r.exoff) + 1); held[r.ticker] = i + int(r.exoff) + 1; n += 1
            out.append({"entry": d, "y": r.y, "ticker": r.ticker,
                        "pnl": r.pnl, "yen": r.pnl / 100 * sh * r.price})
    return pd.DataFrame(out)


def st(d):
    yr = d.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    p = d.pnl; loss = -p[p < 0].sum()
    cum = d.sort_values("entry").yen.cumsum()
    return dict(n=len(d), wr=(p > 0).mean() * 100, pf=p[p > 0].sum() / loss,
                tot=d.yen.sum(), avg=d.yen.sum() / 10, win=int((yr > 0).sum()),
                worst=yr.min(), dd=(cum - cum.cummax()).min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


VAR = [("A 現行（BTの再現）", False, False),
       ("B 指標をシグナル日に揃える", True, False),
       ("C ETF誤判定を修正", False, True),
       ("D B+C 両方", True, True)]
print(f"\n  {'変種':<26}{'候補':>7}{'執行':>7}{'勝率':>7}{'PF':>6}{'年平均':>12}{'勝ち':>6}"
      f"{'最悪年':>12}{'最大DD':>12}{'前半':>12}{'後半':>12}")
res = {}
for lab, sf, fe in VAR:
    C = collect(sf, fe)
    S = slots(C)
    s = st(S)
    res[lab] = (C, S, s)
    print(f"  {lab:<26}{len(C):>7}{s['n']:>7}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>4}/10{s['worst']:>+11,.0f}円{s['dd']:>+11,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円")

print("\n■ 年別（円）")
print(f"  {'年':>6}" + "".join(f"{lab[:1]+' '+lab[2:12]:>18}" for lab, _, _ in VAR))
for y in YEARS:
    print(f"  {y:>6}" + "".join(f"{res[lab][1].query('y==@y').yen.sum():>17,.0f}円" for lab, _, _ in VAR))
print(f"  {'計':>6}" + "".join(f"{res[lab][1].yen.sum():>17,.0f}円" for lab, _, _ in VAR))

A, D_ = res["A 現行（BTの再現）"][1], res["D B+C 両方"][1]
print(f"\n■ ETF誤判定10銘柄が実際に選ばれた回数（変種C）")
Cc = res["C ETF誤判定を修正"][1]
f = Cc[Cc.ticker.isin(FALSE_ETF)]
print(f"  {len(f)}回 / 損益{f.yen.sum():+,.0f}円" if len(f) else "  0回（条件を満たす日が無かった）")

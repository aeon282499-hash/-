# -*- coding: utf-8 -*-
"""bt_10y_robustness.py — スタンダードプラン10年データでの現行ロジック頑健性BT（2026-07-18）。

問い: 現行スイングBUY（RSI≤45×乖離≤-1.5×出来高/レンジ×代金20億×ATR3.0・寄指1.01・
      top5選定・業種cap3・STOP3/TP5/RSI50/MAXHOLD3）のエッジは2022-2026の
      地合い固有ではないか？ 2017〜2021（2018年急落・2020コロナ含む）で検証する。

エンジン: bt_sector_cap.py / bt_price_band.py と同一（judge_signal_pre忠実再現＋
実OCO約定＋日次top5/保有中除外＋業種cap3）に、本番仕様の寄指NOFILL
（screener.yose_limit_price・寄り>指値は見送り）を追加。
データ: jquants_cache_2016_2021.pkl + jquants_cache.pkl をマージ（2016-07〜2026-07）。

注意（結果の読み方）:
- name_mapが現在の上場銘柄由来＝10年間の上場廃止銘柄は入らない（生存バイアス・従来BTと同条件）
- earnings_calendar.jsonは2021年以前ほぼ未収録＝決算±3日除外は古い年ほど効かない
実行: python -X utf8 bt_10y_robustness.py
"""
from __future__ import annotations

import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
from screener import is_etf_ticker, yose_limit_price

SINCE = "2017-01-01"
TURNOVER_MIN = 2e9
STOP, TP, MAXH = 3.0, 5.0, 3
EARN_WIN = 3
SECTOR_CAP = 3
TIERS = [("大100万", 10_000, 1_000_000),
         ("中50万", 5_000, 500_000),
         ("小30万", 3_000, 300_000)]
YEARS = list(range(2017, 2027))


def load_earnings() -> dict[str, set]:
    p = Path("earnings_calendar.json")
    if not p.exists():
        return {}
    raw = json.load(open(p, encoding="utf-8"))
    recs = raw.get("records", raw) if isinstance(raw, dict) else raw
    out: dict[str, set] = {}
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
    """実OCO約定（bt_sector_cap.sim と同一）。"""
    if p + MAXH - 1 >= n:
        return None, None
    e = on[p]
    if not (e > 0) or np.isnan(e):
        return None, None
    stop = e * (1 - STOP / 100); tpp = e * (1 + TP / 100)
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


def summary(df: pd.DataFrame) -> dict:
    total = len(df)
    if total == 0:
        return dict(total=0, win=0, avg=0, pf=0, dd=0, cum=0)
    wins = (df["pnl"] > 0).sum()
    cum = df.sort_values("entry")["pnl"].cumsum()
    dd = (cum - cum.cummax()).min()
    lose_sum = df[df["pnl"] <= 0]["pnl"].sum()
    pf = df[df["pnl"] > 0]["pnl"].sum() / abs(lose_sum) if lose_sum != 0 else float("inf")
    return dict(total=total, win=wins / total * 100, avg=df["pnl"].mean(),
                pf=pf, dd=dd, cum=df["pnl"].sum())


def main():
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    name_map = dict(old["name_map"]); name_map.update(new["name_map"])
    data: dict = {}
    for src in (old["all_data"], new["all_data"]):
        for tk, df in src.items():
            data.setdefault(tk, []).append(df)
    merged = {}
    for tk, dfs in data.items():
        df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index()
        merged[tk] = df[~df.index.duplicated(keep="last")]
    print(f"[data] 旧 {old['start']}〜{old['end']} + 新 {new['start']}〜{new['end']} "
          f"→ {len(merged)}銘柄", flush=True)

    secmap = json.load(open("sector33_map.json", encoding="utf-8"))
    earn = load_earnings()
    since_ts = pd.Timestamp(SINCE)

    rows = []
    nn = nofill = 0
    for tk, df in merged.items():
        if df is None or len(df) < 140:
            continue
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):
            continue
        o = df["Open"].astype(float); h = df["High"].astype(float)
        l = df["Low"].astype(float); cl = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        nn += 1
        dlt = cl.diff()
        ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
        rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
        ma25 = cl.rolling(25).mean()
        dev = ((cl - ma25) / ma25 * 100).round(2)
        pc = cl.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        rr = ((h - l).shift(1) / atr.shift(1)).round(2)
        vr = (v.shift(1) / v.shift(2).rolling(20).mean()).round(2)
        tov = cl.shift(1) * v.shift(1)
        atr_pct = atr / cl * 100
        cand = ((rsi <= 45) & (dev <= -1.5) & ((rr >= 1.5) | (vr >= 2.0))
                & (tov >= TURNOVER_MIN) & (atr_pct <= 3.0))
        cand &= cl.index >= since_ts
        if not cand.fillna(False).any():
            continue
        on = o.to_numpy(); hn = h.to_numpy(); ln = l.to_numpy()
        cn = cl.to_numpy(); rn = rsi.to_numpy()
        idx = cl.index; n = len(cn)
        ewin = earn.get(tk) or earn.get(tk.replace(".T", "")) or set()
        for t in np.where(cand.fillna(False).to_numpy())[0]:
            if t + 2 >= n:
                continue
            entry_day = idx[t + 1].strftime("%Y-%m-%d")
            if entry_day < SINCE or entry_day in ewin:
                continue
            # ── 寄指NOFILL（本番2026-06-11以降の仕様）──
            lp = yose_limit_price(cn[t])
            if lp and on[t + 1] > lp:
                nofill += 1
                continue
            res, exoff = sim(t + 1, on, hn, ln, cn, rn, n)
            if res is None:
                continue
            rsi_t, dev_t, tov_t = rn[t], dev.iloc[t], tov.iloc[t]
            score = (1 / (1 + ((rsi_t - 38) / 8) ** 2) * 0.30
                     + 1 / (1 + ((dev_t + 3) / 2) ** 2) * 0.30
                     + np.log10(max(tov_t, 1) / 1e9 + 1) / 3 * 0.40)
            rows.append({"entry": entry_day, "year": idx[t + 1].year, "ticker": tk,
                         "price": cn[t], "score": score, "pnl": res, "exoff": int(exoff)})
    D0 = pd.DataFrame(rows)
    D0.to_csv("_bt10y_candidates.csv", index=False)
    print(f"[collect] {nn}銘柄走査 / 採用候補 {len(D0):,}件 / 寄指NOFILL {nofill:,}件（{SINCE}〜）",
          flush=True)

    def select(D0: pd.DataFrame, price_cap: int):
        Dt = D0[D0["price"] <= price_cap].sort_values(["entry", "score"], ascending=[True, False])
        days = sorted(Dt["entry"].unique())
        di = {d: i for i, d in enumerate(days)}
        open_until: dict[str, str] = {}
        open_sec: dict[str, str] = {}
        picks = []
        for d in days:
            keep = {tk for tk, u in open_until.items() if u >= d}
            open_until = {tk: open_until[tk] for tk in keep}
            open_sec = {tk: open_sec[tk] for tk in keep}
            cnt = 0
            for _, r in Dt[Dt["entry"] == d].iterrows():
                if cnt >= 5:
                    break
                tk = r["ticker"]
                if tk in open_until:
                    continue
                sec = secmap.get(tk) or f"__unk_{tk}"
                same = sum(1 for s in open_sec.values() if s == sec)
                if same >= SECTOR_CAP:
                    continue
                cnt += 1
                exit_d = days[min(di[d] + r["exoff"], len(days) - 1)]
                open_until[tk] = exit_d
                open_sec[tk] = sec
                picks.append({"entry": d, "year": r["year"], "ticker": tk, "pnl": r["pnl"]})
        return pd.DataFrame(picks)

    for label, price_cap, size in TIERS:
        P = select(D0, price_cap)
        P.to_csv(f"_bt10y_picks_{price_cap}.csv", index=False)
        print("\n" + "=" * 104, flush=True)
        print(f"  {label}（株価≤{price_cap:,}円・現行ロジック=寄指+cap3・{SINCE}〜{new['end']}）")
        print("=" * 104)
        s = summary(P)
        print(f"  全期間: {s['total']}件 勝率{s['win']:.1f}% 平均{s['avg']:+.3f}%/件 "
              f"PF{s['pf']:.2f} 累積{s['cum']:+.1f}% MaxDD{s['dd']:+.1f}% "
              f"（円換算 累積{s['cum']*size/100:+,.0f} / DD{s['dd']*size/100:+,.0f}）")
        for era, y0, y1 in (("2017-2021(検証)", 2017, 2021), ("2022-2026(既知)", 2022, 2026)):
            g = P[(P["year"] >= y0) & (P["year"] <= y1)]
            s = summary(g)
            print(f"  {era}: {s['total']}件 勝率{s['win']:.1f}% 平均{s['avg']:+.3f}% "
                  f"PF{s['pf']:.2f} 累積{s['cum']:+.1f}% MaxDD{s['dd']:+.1f}%")
        print(f"  {'年':>6}{'件数':>7}{'勝率':>8}{'平均/件':>10}{'PF':>7}{'累積%':>10}{'MaxDD%':>10}")
        print("  " + "-" * 60)
        for y in YEARS:
            g = P[P["year"] == y]
            if len(g) == 0:
                print(f"  {y:>6}{'-':>7}")
                continue
            s = summary(g)
            print(f"  {y:>6}{s['total']:>7}{s['win']:>7.1f}%{s['avg']:>+9.3f}%"
                  f"{s['pf']:>7.2f}{s['cum']:>+9.1f}%{s['dd']:>+9.1f}%")


if __name__ == "__main__":
    main()

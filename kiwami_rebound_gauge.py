# -*- coding: utf-8 -*-
"""kiwami_rebound_gauge.py — 極み買いの「反発指標」（止め時の合図）を本番データ(J-Quants)で毎日計算する。

指標 = 現行入口条件(RSI≤45/25MA乖離≤-1.5/(rr≥1.5|vr≥2.0)/前日代金≥20億/ATR%≤3)に該当した全候補の
       「2日目リターン」(翌々日終値 ÷ 翌日寄り − 1) の日次平均を、直近126営業日で平均したもの。
26年検証(_bt_buy_regime_gate.py): -0.25 を割っている期間は玉の平均-0.16%/PF0.86。2022-26は一度も割っていない。
用途: 表示・監視（発注ルールには入れない）。-0.25割れが続いたら縮小/停止を判断。
実行: python -X utf8 kiwami_rebound_gauge.py  → 表示 + kiwami_rebound_gauge.json
"""
from __future__ import annotations
import json, pickle
import numpy as np, pandas as pd
from screener import is_etf_ticker

TH = -0.25
def load():
    old = pickle.load(open("jquants_cache_2016_2021.pkl","rb")); new = pickle.load(open("jquants_cache.pkl","rb"))
    nm = dict(old["name_map"]); nm.update(new["name_map"]); data = {}
    for s in (old["all_data"], new["all_data"]):
        for tk, df in s.items(): data.setdefault(tk, []).append(df)
    out = {}
    for tk, dfs in data.items():
        df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index(); out[tk] = df[~df.index.duplicated(keep="last")]
    return out, nm

def gauge(all_data, nm, lookback=126):
    rows = []
    for tk, df in all_data.items():
        name = nm.get(tk)          # 名前が無い(本番の直取得)ならコード帯でETF判定
        if is_etf_ticker(tk, name) or len(df) < 140: continue
        df = df.dropna(subset=["Close"]); o = df["Open"].astype(float); h = df["High"].astype(float); l = df["Low"].astype(float)
        cl = df["Close"].astype(float); v = df["Volume"].astype(float)
        dlt = cl.diff(); ag = dlt.clip(lower=0).ewm(alpha=1/14, min_periods=14).mean(); al = (-dlt).clip(lower=0).ewm(alpha=1/14, min_periods=14).mean()
        rsi = 100 - 100/(1 + ag/al.replace(0, np.nan)); ma25 = cl.rolling(25).mean(); dev = (cl-ma25)/ma25*100
        pc = cl.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1); atr = tr.rolling(14).mean()
        rr = (h-l).shift(1)/atr.shift(1); vr = v.shift(1)/v.shift(2).rolling(20).mean(); tov = cl.shift(1)*v.shift(1); atr_pct = atr/cl*100
        cand = ((rsi<=45)&(dev<=-1.5)&((rr>=1.5)|(vr>=2.0))&(tov>=2e9)&(atr_pct<=3.0)).fillna(False).to_numpy()
        on = o.to_numpy(); cn = cl.to_numpy(); idx = df.index
        for t in np.where(cand)[0]:
            if t+2 < len(cn) and on[t+1] > 0:
                rows.append((idx[t+1], (cn[t+2]-on[t+1])/on[t+1]*100))
    D = pd.DataFrame(rows, columns=["entry","d2"]).groupby("entry").d2.mean().sort_index()
    ind = D.rolling(lookback, min_periods=60).mean()
    return D, ind

def compute_live(today, cal_days: int = 400) -> dict | None:
    """本番用: J-Quants から直近 cal_days 日の全銘柄日足を取り、反発指標の最新値と月末推移を返す。
    月次レポート(shadow_exit.monthly_report)から呼ぶ。失敗したら None（レポート本体は無傷）。2026-09-03。"""
    from datetime import timedelta
    from screener import batch_download_jquants, _jquants_id_token
    start = (today - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    data = batch_download_jquants(_jquants_id_token(), start=start, end=today.strftime("%Y-%m-%d"))
    if not data:
        return None
    D, ind = gauge(data, {})
    last = ind.dropna()
    if last.empty:
        return None
    m = ind.resample("ME").last().dropna().tail(12)
    out = {"date": str(last.index[-1].date()), "value": round(float(last.iloc[-1]), 4), "threshold": TH,
           "monthly": {k.strftime("%Y-%m"): round(float(v), 4) for k, v in m.items()},
           "n_days": int(len(D))}
    try:
        json.dump(out, open("kiwami_rebound_gauge.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    except Exception:
        pass
    return out


def gauge_embed(g: dict) -> dict:
    """Discord embed（月次レポートの末尾に付ける・表示専用）。"""
    v = g["value"]; th = g["threshold"]
    if v > 0:
        verdict, color = "🟢 反発レジーム（押し目が翌々日に戻っている）", 0x2ECC71
    elif v > th:
        verdict, color = "🟡 弱め（基準の手前・様子見）", 0xF1C40F
    else:
        verdict, color = "🔴 基準割れ（この状態が続くなら縮小/停止を検討）", 0xE74C3C
    trend = "  ".join(f"{k[2:]}:{x:+.2f}" for k, x in list(g["monthly"].items())[-6:])
    return {"title": f"🧭 反発指標 {g['date']}: {v:+.3f}（基準 {th:+.2f}）",
            "description": (f"{verdict}\n月末推移: {trend}\n"
                            "※極み買いの入口条件に該当した全候補の「翌々日終値÷翌日寄り−1」の日次平均を直近126営業日で平均。"
                            "26年検証で基準割れの期間は玉の平均-0.16%/PF0.86、2022年以降は一度も割れていない。発注ルールには入れない（表示専用）。"),
            "color": color}


if __name__ == "__main__":
    all_data, nm = load(); D, ind = gauge(all_data, nm)
    last = ind.dropna()
    print(f"反発指標（直近126営業日の2日目リターン平均・基準 {TH:+.2f}）")
    print(f"  最新 {last.index[-1].date()}: {last.iloc[-1]:+.3f}   （{'反発レジーム' if last.iloc[-1] > TH else '⚠️ 基準割れ'}）")
    m = ind.resample("ME").last().dropna().tail(12)
    print("  月末値(直近12ヶ月):", {k.strftime('%Y-%m'): round(v, 2) for k, v in m.items()})
    y = ind.resample("YE").mean().dropna(); print("  年平均:", {k.year: round(v, 2) for k, v in y.items()})
    json.dump({"date": str(last.index[-1].date()), "value": round(float(last.iloc[-1]), 4), "threshold": TH,
               "monthly": {k.strftime('%Y-%m'): round(float(v), 4) for k, v in m.items()}}, open("kiwami_rebound_gauge.json","w"), ensure_ascii=False, indent=1)

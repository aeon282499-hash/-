# -*- coding: utf-8 -*-
"""pattern_screener.py — 🎖リアルライブ軍団：「株リアルライブ型 ★買い時かも」のチャート形を全銘柄から探す。2026-11-28 本人依頼。

■ 何を探すか（スライド16枚＝ゼンショー/ファストリ/日東電工/コーエーテクモ/KDDI/NEC/リクルート/富士通/
  スルガ銀/大塚HD/任天堂/中外製薬 の共通項を数値化）
  必須（全部満たす）
    1. 一目均衡表: 終値が雲の上（または雲の上端の7割以上まで来て突き抜けつつある）
    2. パラボリック(0.02,0.2): 上げで点灯（SAR < 終値）
    3. RSI(14): 50〜78
    4. ボリンジャー(20): 終値が +0.6σ〜+2.3σ（スライドの「+1σ近辺」「+1σと+2σの間」「+2σ近辺」）
    5. 終値 > 25日線（中期の上昇トレンド）
    6. 直近60日高値まで 0〜8%（「直近高値を出来高を伴って突破すれば確度が上がる」の手前）
  加点（スコア）
    MACD: 0より上で上向き＝上昇トレンド / シグナル上抜け / ヒストグラム増加（「不明確」は0点）
    先行スパン（26日先の雲）が上昇＝「雲が上昇・せり上がっている」/ 陽雲
    パラボリックが最近上げに転換 / 雲を最近上抜け / 出来高増 / 高値に近い
    過熱（RSI>70 or +2σ超え）は減点し「押し目待ち」と表示
  ブレイク価格＝直近60日高値。「この値を出来高を伴って抜けたら」の監視用。

■ 市場: 既定はグロース＋スタンダード。--markets growth,standard,prime でプライムも。
■ データ: J-Quants /equities/bars/daily（調整済み）＋ /equities/master（市場区分）。JQUANTS_API_KEY 必須。
  カブコム（kabuステーションAPI）は本人PCの localhost でしか動かないので、ここでは使わない
  （PCで場中にブレイク価格を監視する時に使える）。

■ 実行
  python pattern_screener.py                       # 最新営業日でスクリーニング（グロース+スタンダード）
  python pattern_screener.py --markets growth,standard,prime --top 30
  python pattern_screener.py --date 2026-10-16     # 過去日で再現（NEC等がスライドと一致するか確認用）
  python pattern_screener.py --bt                  # 検証: jquants_cache.pkl（無ければAPIで --bt-days 日取得）で
                                                   #   シグナル翌日寄り買い→5/10/20日後の成績を全銘柄平均と比較
  Discord: DISCORD_WEBHOOK_PATTERN_URL が設定されていれば上位を送る（--dry で送らない）

※ 情報提供のみ。売買判断は自己責任。
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

JST = timezone(timedelta(hours=9))
OUT_JSON = "pattern_signals.json"
OUT_MD = "pattern_signals.md"

# ── 閾値（ここを触れば条件が変わる）──────────────────────────────
RSI_LO, RSI_HI = 50.0, 78.0
BBZ_LO, BBZ_HI = 0.6, 2.3          # ボリンジャー z（σ単位）
HIGH_WIN = 60                      # 直近高値の窓（営業日）
HIGH_DIST_MAX = 8.0                # 高値までの距離（%）の上限
CLOUD_BREAK_FRAC = 0.7             # 雲の中でも「下端からの位置」がこの割合以上なら「突き抜けつつある」
MIN_TURNOVER_OKU = 1.0             # 20日平均売買代金（億円）の下限
MIN_PRICE = 200.0
LOOKBACK_DAYS = 140                # 取得営業日数（一目52+26・75日線に余裕を持たせる）
MARKET_KEYS = {"growth": "グロース", "standard": "スタンダード", "prime": "プライム"}


# ── 指標 ────────────────────────────────────────────────────────
def psar(high: np.ndarray, low: np.ndarray, af0=0.02, af_step=0.02, af_max=0.2):
    """パラボリックSAR。戻り値 (sar, is_up)。"""
    n = len(high)
    sar = np.full(n, np.nan)
    up = np.zeros(n, dtype=bool)
    if n < 3:
        return sar, up
    is_up = high[1] + low[1] >= high[0] + low[0]
    af = af0
    ep = high[1] if is_up else low[1]
    s = low[0] if is_up else high[0]
    sar[1], up[1] = s, is_up
    for i in range(2, n):
        s = s + af * (ep - s)
        if is_up:
            s = min(s, low[i - 1], low[i - 2])
            if low[i] < s:                      # 反転→下げ
                is_up, s, ep, af = False, ep, low[i], af0
            elif high[i] > ep:
                ep, af = high[i], min(af + af_step, af_max)
        else:
            s = max(s, high[i - 1], high[i - 2])
            if high[i] > s:                     # 反転→上げ
                is_up, s, ep, af = True, ep, high[i], af0
            elif low[i] < ep:
                ep, af = low[i], min(af + af_step, af_max)
        sar[i], up[i] = s, is_up
    return sar, up


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    """日足(Open/High/Low/Close/Volume) → 指標の全系列。最終行が「その日」の判定。"""
    c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]
    o = pd.DataFrame(index=df.index)
    o["close"] = c
    o["sma5"], o["sma25"], o["sma75"] = c.rolling(5).mean(), c.rolling(25).mean(), c.rolling(75).mean()
    # 一目均衡表
    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    span_a_raw = (tenkan + kijun) / 2                       # 26日先に描かれる値
    span_b_raw = (h.rolling(52).max() + l.rolling(52).min()) / 2
    sa, sb = span_a_raw.shift(26), span_b_raw.shift(26)     # 今日の位置の雲
    o["cloud_top"], o["cloud_bot"] = np.maximum(sa, sb), np.minimum(sa, sb)
    thick = (o["cloud_top"] - o["cloud_bot"]).replace(0, np.nan)
    o["cloud_pos"] = ((c - o["cloud_bot"]) / thick).where(thick.notna(), np.where(c > o["cloud_top"], 9.0, -9.0))
    o["above_cloud"] = c > o["cloud_top"]
    prev_all_above = o["above_cloud"].astype(float).shift(1).rolling(10).min().fillna(0) > 0
    o["cloud_break_recent"] = o["above_cloud"] & ~prev_all_above
    lead_top = np.maximum(span_a_raw, span_b_raw)
    o["lead_cloud_up"] = lead_top > lead_top.shift(5)        # 先の雲がせり上がっている
    o["lead_yang"] = span_a_raw > span_b_raw                 # 先の雲が陽雲
    o["tenkan_gt_kijun"] = tenkan > kijun
    # パラボリック
    s, up = psar(h.to_numpy(float), l.to_numpy(float))
    o["sar"], o["sar_up"] = s, up
    up_s = pd.Series(up, index=df.index)
    flip = up_s & ~up_s.shift(1, fill_value=False)
    o["sar_up_days"] = up_s.groupby((~up_s).cumsum()).cumsum()   # 上げ点灯からの日数
    o["sar_flip_today"] = flip
    # MACD
    ema12, ema26 = c.ewm(span=12, adjust=False).mean(), c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    o["macd"], o["macd_sig"], o["macd_hist"] = macd, sig, macd - sig
    # RSI (Wilder)
    d = c.diff()
    g = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    o["rsi"] = 100 - 100 / (1 + g / ls.replace(0, np.nan))
    # ボリンジャー
    m20, sd20 = c.rolling(20).mean(), c.rolling(20).std(ddof=0)
    o["bbz"] = (c - m20) / sd20.replace(0, np.nan)
    o["bb_width_up"] = (sd20 / m20) > (sd20 / m20).shift(5)
    # 高値・出来高・売買代金
    o["hi60"] = h.rolling(HIGH_WIN).max()
    o["dist_hi"] = (o["hi60"] / c - 1) * 100
    o["vol_ratio"] = v / v.rolling(20).mean().replace(0, np.nan)
    o["turnover20_oku"] = (c * v).rolling(20).mean() / 1e8
    o["sma25_up"] = o["sma25"] > o["sma25"].shift(5)
    return o


def signal_mask(o: pd.DataFrame) -> pd.Series:
    """必須条件（全部）。BTでも同じ関数を使う。"""
    cloud_ok = o["above_cloud"] | (o["cloud_pos"] >= CLOUD_BREAK_FRAC)
    return (cloud_ok & o["sar_up"]
            & o["rsi"].between(RSI_LO, RSI_HI)
            & o["bbz"].between(BBZ_LO, BBZ_HI)
            & (o["close"] > o["sma25"])
            & o["dist_hi"].between(0, HIGH_DIST_MAX)
            & (o["turnover20_oku"] >= MIN_TURNOVER_OKU)
            & (o["close"] >= MIN_PRICE)
            & o["sma75"].notna() & o["cloud_top"].notna())


def score_row(r: pd.Series) -> tuple[float, dict]:
    """加点。スライドで「確度が高い」と言っている要素ほど重く。"""
    sc, why = 0.0, {}
    # MACD
    hist_up = r["macd_hist"] > r["macd_hist_prev"]
    if r["macd"] > 0 and r["macd"] > r["macd_sig"] and hist_up:
        sc += 3; why["macd"] = "上昇トレンド"
    elif r["macd"] > r["macd_sig"]:
        sc += 2; why["macd"] = "プラス圏で上向き" if r["macd"] > 0 else "シグナル上抜け（0の下）"
    elif hist_up:
        sc += 1; why["macd"] = "上向きになりかかっている"
    else:
        why["macd"] = "不明確"
    # 雲
    if r["above_cloud"]:
        sc += 2
        why["cloud"] = "雲を上抜けてきた" if r["cloud_break_recent"] else "雲を上回っている"
    else:
        sc += 1; why["cloud"] = "雲を下から上へ突き破りつつある"
    if r["lead_cloud_up"]:
        sc += 1; why["cloud"] += "・先の雲がせり上がっている"
    if r["lead_yang"]:
        sc += 0.5
    if r["tenkan_gt_kijun"]:
        sc += 0.5
    # パラボリック
    if r["sar_up_days"] <= 5:
        sc += 1; why["sar"] = f"上げに転換（{int(r['sar_up_days'])}日目）"
    else:
        why["sar"] = "上げで点灯"
    # RSI / BB
    if 55 <= r["rsi"] <= 70:
        sc += 1
    if 0.8 <= r["bbz"] <= 2.0:
        sc += 1
    if r["bb_width_up"]:
        sc += 0.5
    # 移動平均の並び
    if r["sma5"] > r["sma25"] > r["sma75"]:
        sc += 1
    if r["sma25_up"]:
        sc += 0.5
    # 高値の近さ・出来高
    if r["dist_hi"] <= 3:
        sc += 1.5
    elif r["dist_hi"] <= 5:
        sc += 0.5
    if r["vol_ratio"] >= 1.5:
        sc += 1
    # 過熱
    hot = r["rsi"] > 70 or r["bbz"] > 2.0
    if hot:
        sc -= 1
    why["hot"] = hot
    return sc, why


def comment(r: pd.Series, why: dict) -> str:
    """スライド風の所見。"""
    z = r["bbz"]
    bb = "+2σ近辺" if z >= 1.8 else ("+1σと+2σの間" if z >= 1.2 else "+1σ近辺")
    rsi = int(round(r["rsi"] / 5) * 5)
    rsi_txt = f"{rsi}近辺" + ("で過熱感はある" if r["rsi"] > 70 else ("でやや過熱感はある" if r["rsi"] > 63 else "で推移"))
    lines = [f"・一目均衡表は、{why['cloud']}。", f"・パラボリックは、{why['sar']}。",
             f"・MACDは、{why['macd']}。", f"・RSIは、{rsi_txt}。", f"・BBは、{bb}で推移。"]
    hi = r["hi60"]
    tail = f"直近高値{hi:,.0f}円（あと{r['dist_hi']:.1f}%）を出来高を伴って突破すれば確度が上がる形。"
    if why["hot"]:
        tail += "過熱感があるので押し目を狙うなどの注意が必要。"
    return "\n".join(lines) + "\n" + tail


# ── データ ──────────────────────────────────────────────────────
def load_master(token: str) -> dict[str, dict]:
    from screener import _jquants_get, is_common_stock_code
    items = _jquants_get("/equities/master", token).get("data", [])
    out = {}
    for it in items:
        if not is_common_stock_code(it.get("Code", "")):
            continue
        code4 = str(it.get("Code", ""))[:4]
        out[code4 + ".T"] = {"name": it.get("CoName", code4), "market": it.get("MktNm", ""),
                             "sector": it.get("S33Nm", "") or it.get("Sector33CodeName", "")}
    return out


def load_prices(token: str, end: str | None, days: int) -> dict[str, pd.DataFrame]:
    from screener import batch_download_jquants
    if end:
        e = datetime.strptime(end, "%Y-%m-%d")
        s = e - timedelta(days=int(days * 1.5) + 10)
        return batch_download_jquants(token, start=s.strftime("%Y-%m-%d"), end=end)
    return batch_download_jquants(token, lookback_trading_days=days)


def market_filter(master: dict, markets: list[str]) -> set[str]:
    keys = [MARKET_KEYS[m] for m in markets]
    return {t for t, m in master.items() if any(k in m["market"] for k in keys)}


# ── スクリーニング ──────────────────────────────────────────────
def screen(prices: dict[str, pd.DataFrame], master: dict, universe: set[str], on: str | None) -> list[dict]:
    rows = []
    for t, df in prices.items():
        if t not in universe:
            continue
        if on:
            df = df[df.index <= pd.Timestamp(on)]
        if len(df) < 90:
            continue
        o = indicators(df)
        o["macd_hist_prev"] = o["macd_hist"].shift(1)
        if not bool(signal_mask(o).iloc[-1]):
            continue
        r = o.iloc[-1]
        sc, why = score_row(r)
        m = master.get(t, {})
        rows.append({
            "code": t[:4], "name": m.get("name", t[:4]), "market": m.get("market", ""), "sector": m.get("sector", ""),
            "date": o.index[-1].strftime("%Y-%m-%d"), "score": round(sc, 1),
            "close": float(r["close"]), "break_price": float(r["hi60"]), "dist_hi_pct": round(float(r["dist_hi"]), 2),
            "rsi": round(float(r["rsi"]), 1), "bbz": round(float(r["bbz"]), 2),
            "macd": why["macd"], "sar_up_days": int(r["sar_up_days"]), "above_cloud": bool(r["above_cloud"]),
            "vol_ratio": round(float(r["vol_ratio"]), 2), "turnover20_oku": round(float(r["turnover20_oku"]), 1),
            "hot": bool(why["hot"]), "comment": comment(r, why),
        })
    rows.sort(key=lambda x: (-x["score"], x["dist_hi_pct"]))
    return rows


def write_outputs(rows: list[dict], markets: list[str], top: int) -> None:
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(JST).isoformat(timespec="seconds"),
                   "markets": markets, "count": len(rows), "signals": rows}, f, ensure_ascii=False, indent=1)
    d = rows[0]["date"] if rows else "-"
    md = [f"# 🎖リアルライブ軍団 ★買い時かも 候補（{d}・{'/'.join(MARKET_KEYS[m] for m in markets)}）", "",
          f"該当 {len(rows)} 銘柄 / 上位 {min(top, len(rows))}", "",
          "| # | コード | 銘柄 | 市場 | 点 | 終値 | ブレイク価格 | 高値まで | RSI | BBσ | MACD | 出来高比 | 注意 |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for i, r in enumerate(rows[:top], 1):
        md.append(f"| {i} | {r['code']} | {r['name']} | {r['market']} | {r['score']} | {r['close']:,.0f} | "
                  f"{r['break_price']:,.0f} | {r['dist_hi_pct']}% | {r['rsi']} | {r['bbz']} | {r['macd']} | "
                  f"{r['vol_ratio']} | {'過熱・押し目待ち' if r['hot'] else ''} |")
    md += ["", "## 所見（上位）", ""]
    for r in rows[:min(top, 10)]:
        md += [f"### {r['name']}（{r['code']}）{r['market']}", "", r["comment"].replace("\n", "  \n"), ""]
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))


def send_discord(rows: list[dict], top: int) -> None:
    url = os.getenv("DISCORD_WEBHOOK_PATTERN_URL", "").strip()
    if not url:
        print("[discord] DISCORD_WEBHOOK_PATTERN_URL 未設定 → 送信しない")
        return
    import requests
    d = rows[0]["date"] if rows else datetime.now(JST).strftime("%Y-%m-%d")
    lines = [f"**🎖リアルライブ軍団 ★買い時かも {d}**（{len(rows)}件中 上位{min(top, len(rows))}）"]
    for r in rows[:top]:
        lines.append(f"`{r['code']}` {r['name']}［{r['market'][:3]}］{r['score']}点 終値{r['close']:,.0f} "
                     f"→ 抜け{r['break_price']:,.0f}（+{r['dist_hi_pct']}%）RSI{r['rsi']:.0f} {r['bbz']:+.1f}σ "
                     f"MACD:{r['macd']}{' ⚠過熱' if r['hot'] else ''}")
    msg, chunks = "", []
    for ln in lines:
        if len(msg) + len(ln) > 1900:
            chunks.append(msg); msg = ""
        msg += ln + "\n"
    chunks.append(msg)
    for ch in chunks:
        requests.post(url, json={"content": ch}, timeout=20).raise_for_status()
    print(f"[discord] {len(chunks)} 通 送信")


# ── 検証 ────────────────────────────────────────────────────────
def backtest(prices: dict[str, pd.DataFrame], universe: set[str] | None, min_score: float) -> None:
    """シグナル日の翌営業日寄りで買い → 5/10/20日後の終値。同じ日の全銘柄（同条件の流動性）平均との差も見る。"""
    sig_rows, base_rows = [], []
    for t, df in prices.items():
        if universe is not None and t not in universe:
            continue
        if len(df) < 120:
            continue
        o = indicators(df)
        o["macd_hist_prev"] = o["macd_hist"].shift(1)
        nxt_open = df["Open"].shift(-1)
        fw = {k: df["Close"].shift(-k) / nxt_open - 1 for k in (5, 10, 20)}
        liq = (o["turnover20_oku"] >= MIN_TURNOVER_OKU) & (o["close"] >= MIN_PRICE) & nxt_open.notna()
        b = pd.DataFrame({f"r{k}": fw[k] for k in fw})[liq]
        b["date"] = b.index
        base_rows.append(b.iloc[::5])                     # ベースラインは5日おきに間引き（重さ対策）
        m = signal_mask(o) & nxt_open.notna()
        if not m.any():
            continue
        for ts in o.index[m]:
            r = o.loc[ts]
            sc, _ = score_row(r)
            if sc < min_score:
                continue
            sig_rows.append({"date": ts, "code": t[:4], "score": sc, "hot": r["rsi"] > 70 or r["bbz"] > 2.0,
                             **{f"r{k}": fw[k].loc[ts] for k in fw}})
    if not sig_rows:
        print("[bt] シグナル0件"); return
    s = pd.DataFrame(sig_rows)
    base = pd.concat(base_rows)
    bmean = base.groupby("date")[["r5", "r10", "r20"]].mean()
    s = s.join(bmean, on="date", rsuffix="_mkt")
    print(f"[bt] 期間 {s['date'].min():%Y-%m-%d}〜{s['date'].max():%Y-%m-%d}  シグナル {len(s):,} 件")

    def summ(x: pd.DataFrame, label: str):
        out = [label.ljust(22), f"n={len(x):>6,}"]
        for k in (5, 10, 20):
            r = x[f"r{k}"].dropna()
            ex = (x[f"r{k}"] - x[f"r{k}_mkt"]).dropna()
            pf = r[r > 0].sum() / max(-r[r < 0].sum(), 1e-9)
            out.append(f"{k}日 平均{r.mean()*100:+.2f}% 勝率{(r>0).mean()*100:.0f}% PF{pf:.2f} 超過{ex.mean()*100:+.2f}%")
        print("  " + " | ".join(out))
    summ(s, "全シグナル")
    summ(s[~s["hot"]], "過熱なし")
    summ(s[s["hot"]], "過熱あり")
    for q in (8, 10, 12):
        summ(s[s["score"] >= q], f"スコア{q}以上")
    s["year"] = s["date"].dt.year
    for y, g in s.groupby("year"):
        summ(g, f"{y}年")
    # 同じ日に同じ銘柄の重複は無いが、連日シグナルを1回目だけに絞った版（実際の運用に近い）
    s = s.sort_values(["code", "date"])
    first = s[s.groupby("code")["date"].diff().dt.days.fillna(999) > 14]
    summ(first, "初回のみ(14日空け)")


# ── 条件ごとの診断 ──────────────────────────────────────────────
def explain(prices: dict[str, pd.DataFrame], pairs: list[tuple[str, str]]) -> None:
    """銘柄:日付 ごとに、必須条件のどれで落ちたかを表示（スライドとの答え合わせ用）。"""
    for code, d in pairs:
        df = prices.get(code + ".T")
        if df is None:
            print(f"{code} {d}: データ無し"); continue
        df = df[df.index <= pd.Timestamp(d)]
        o = indicators(df); o["macd_hist_prev"] = o["macd_hist"].shift(1)
        r = o.iloc[-1]
        cloud_ok = bool(r["above_cloud"] or r["cloud_pos"] >= CLOUD_BREAK_FRAC)
        checks = {
            f"雲(位置{r['cloud_pos']:.2f})": cloud_ok, "SAR上げ": bool(r["sar_up"]),
            f"RSI{r['rsi']:.1f}": RSI_LO <= r["rsi"] <= RSI_HI, f"BB{r['bbz']:+.2f}σ": BBZ_LO <= r["bbz"] <= BBZ_HI,
            ">25MA": bool(r["close"] > r["sma25"]), f"高値まで{r['dist_hi']:.1f}%": 0 <= r["dist_hi"] <= HIGH_DIST_MAX,
            f"代金{r['turnover20_oku']:.1f}億": r["turnover20_oku"] >= MIN_TURNOVER_OKU,
        }
        ok = all(checks.values())
        sc, why = score_row(r)
        print(f"{code} {o.index[-1]:%Y-%m-%d} {'◎通過' if ok else '×'} {sc:.1f}点 MACD:{why['macd']} | "
              + " ".join(f"{k}{'○' if v else '×'}" for k, v in checks.items()))


# ── main ────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--markets", default="growth,standard")
    ap.add_argument("--date", default=None, help="判定日 YYYY-MM-DD / today（省略=前営業日まで）")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--bt", action="store_true")
    ap.add_argument("--bt-days", type=int, default=500)
    ap.add_argument("--bt-min-score", type=float, default=0)
    ap.add_argument("--cache", default="jquants_cache.pkl")
    ap.add_argument("--explain", default="", help="6701:2026-10-16,6702:2026-10-07 … 条件ごとの○×")
    a = ap.parse_args()
    markets = [m.strip().lower() for m in a.markets.split(",") if m.strip()]
    for m in markets:
        if m not in MARKET_KEYS:
            sys.exit(f"--markets は growth/standard/prime: {m}")

    token = None
    try:
        from screener import _jquants_id_token
        token = _jquants_id_token()
    except Exception as e:
        print(f"[auth] {e}")

    master = load_master(token) if token else {}
    universe = market_filter(master, markets) if master else None
    if master:
        print(f"[master] {'/'.join(MARKET_KEYS[m] for m in markets)}: {len(universe)} 銘柄")

    if a.explain:
        pairs = [tuple(x.split(":")) for x in a.explain.split(",") if ":" in x]
        last = max(d for _, d in pairs)
        first = min(d for _, d in pairs)
        span = (datetime.strptime(last, "%Y-%m-%d") - datetime.strptime(first, "%Y-%m-%d")).days
        prices = load_prices(token, last, LOOKBACK_DAYS + int(span * 0.7))
        explain(prices, pairs)
        return

    if a.bt:
        if os.path.exists(a.cache):
            with open(a.cache, "rb") as f:
                payload = pickle.load(f)
            prices = payload["all_data"] if isinstance(payload, dict) and "all_data" in payload else payload
            print(f"[bt] キャッシュ {a.cache}: {len(prices)} 銘柄")
        elif token:
            prices = load_prices(token, a.date, a.bt_days)
        else:
            sys.exit("JQUANTS_API_KEY もキャッシュも無い")
        if universe is None:
            print("[bt] 市場区分が取れない → 全銘柄で検証")
        print("[bt] ※市場区分は現在のもの（過去の上場廃止銘柄は入らない＝生存バイアスあり）")
        backtest(prices, universe, a.bt_min_score)
        return

    if not token:
        sys.exit("JQUANTS_API_KEY が未設定")
    if a.date == "today":
        a.date = datetime.now(JST).strftime("%Y-%m-%d")
    prices = load_prices(token, a.date, LOOKBACK_DAYS)
    if not prices:
        sys.exit("株価が取れなかった")
    last = max(df.index.max() for df in prices.values())
    if a.date and last < pd.Timestamp(a.date):
        print(f"[skip] {a.date} の日足が無い（休場日 or まだ未公開）。最新は {last:%Y-%m-%d} → 何もしない")
        return
    rows = screen(prices, master, universe, a.date)
    write_outputs(rows, markets, a.top)
    print(open(OUT_MD, encoding="utf-8").read())
    if not a.dry:
        send_discord(rows, a.top)


if __name__ == "__main__":
    main()

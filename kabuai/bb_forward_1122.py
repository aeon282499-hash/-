# -*- coding: utf-8 -*-
"""bb_forward_1122.py — 「GU株のBB上抜け逆張り売り」を、ルールを決めた後の新しい期間で確かめる（2026-11-22）。

元の検証: bt_1031/bb_sigma/RESULTS_bb_sigma.md（2026-10-31・5分足 7/24〜10/22 の60日で 276,000セル総当たり）
  生き残りの代表セル = BB40本/2σ・終値が上バンドの0.5%以上外・10:00以降の最初の点灯・次足始値で売り・
  中心線(40本MA)回帰で買い戻し・当日GU+3%の銘柄・決算(開示当日と翌営業日)除外
  → n=281・勝率65%・PF2.75・1件+1.04%（コストA＝往復0.10%＋入口/出口で不利1tick）
本スクリプト: 同じ規約（bb_engine.py と同じ式）を 10/23 以降の「一度も見ていない日」に当てる。
  ① 前半の重なり期間（〜10/22）＝元の検証の再現チェック（数字が近ければ実装が同じ）
  ② 10/23〜 ＝本当の前向き検証
  比較: 「寄値+7%超えで売り」（BBを使わない型）・同じ銘柄日のランダムな時刻で売り（200回）

データ（CI で動く）:
  J-Quants 日足 → 「寄値÷前日終値 ≥ +2.5%・前日代金1億+・株価100〜10万円」の銘柄日を候補に（5分足の取得を絞るため。
                   最終判定は元と同じく5分足だけで行う＝9:05足の始値÷前日の最終5分足の終値 ≥ +3%）
  yfinance 5分足（直近60日まで）→ 候補銘柄だけ取得
  J-Quants /fins/summary → 決算・業績修正の開示日（当日と翌営業日を除外）
実行: cd kabuai && python bb_forward_1122.py
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from build_data import _is_common_stock, _jquants_get, _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SPLIT = "2026-10-23"          # この日以降が前向き検証（元の検証の最終日 10/22 の翌営業日）
P, SIG, DEV = 40, 2.0, 0.005  # BB40本/2σ・上バンドの0.5%以上外
W_LO, W_HI = 10 * 60, 14 * 60 + 25
COST_RT = 0.10
N_RANDOM = 200
STD_UB = np.array([3000, 5000, 30000, 50000, 300000, 500000, 3e6, 5e6, 3e7, 1e12])
STD_TK = np.array([1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 10000], float)
T5_UB = np.array([1000, 3000, 10000, 30000, 100000, 300000, 1e6, 3e6, 1e7, 1e12])
T5_TK = np.array([0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000, 1000], float)


def tick_of(px: float, is500: bool) -> float:
    return float(T5_TK[np.searchsorted(T5_UB, px - 1e-9)] if is500 else STD_TK[np.searchsorted(STD_UB, px - 1e-9)])


def get(path, token, params):
    while True:
        try:
            return _jquants_get(path, token, params)
        except Exception as e:
            if "429" in str(e):
                time.sleep(60); continue
            raise


def jq_daily(token, days):
    rows = []
    for ds in days:
        pk = None
        while True:
            pr = {"date": ds}
            if pk:
                pr["pagination_key"] = pk
            d = get("/equities/bars/daily", token, pr)
            for r in d.get("data", []):
                c = str(r.get("Code", ""))
                if _is_common_stock(c):
                    rows.append((c[:4], r["Date"], r.get("AdjO"), r.get("AdjC"), r.get("AdjVo"), r.get("AdjO"), r.get("AdjC")))
            pk = d.get("pagination_key"); time.sleep(1.2)
            if not pk:
                break
    D = pd.DataFrame(rows, columns=["code", "date", "O", "C", "Vo", "AdjO", "AdjC"])
    for k in ("O", "C", "Vo", "AdjO", "AdjC"):
        D[k] = pd.to_numeric(D[k], errors="coerce")
    D["Va"] = D.AdjC * D.Vo          # 売買代金の近似（調整後終値×調整後出来高）
    return D


def jq_fins(token, days):
    hit = set()
    for ds in days:
        pk = None
        while True:
            pr = {"date": ds}
            if pk:
                pr["pagination_key"] = pk
            try:
                d = get("/fins/summary", token, pr)
            except Exception as e:
                print(f"  fins {ds} 失敗: {e}"); break
            for r in d.get("data", []):
                c = str(r.get("Code", ""))[:4]
                dd = str(r.get("DiscDate") or r.get("DisclosedDate") or ds)[:10]
                hit.add((c, dd))
            pk = d.get("pagination_key"); time.sleep(1.2)
            if not pk:
                break
    return hit


def yf_5m(codes):
    import yfinance as yf
    out = {}
    codes = sorted(codes)
    for i in range(0, len(codes), 50):
        batch = [f"{c}.T" for c in codes[i:i + 50]]
        for attempt in range(3):
            try:
                df = yf.download(batch, period="60d", interval="5m", group_by="ticker", progress=False,
                                 auto_adjust=False, threads=True)
                break
            except Exception as e:
                print(f"  yfinance 失敗 {attempt+1}回目: {e}"); time.sleep(10); df = None
        if df is None or df.empty:
            continue
        for t in batch:
            try:
                s = df[t] if isinstance(df.columns, pd.MultiIndex) else df
            except KeyError:
                continue
            s = s.dropna(subset=["Open", "High", "Low", "Close"])
            if s.empty:
                continue
            idx = pd.to_datetime(s.index)
            idx = (idx.tz_convert("Asia/Tokyo") if idx.tz is not None else idx).tz_localize(None)
            s = s.set_axis(idx)
            out[t[:4]] = s.rename(columns={"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"})[["o", "h", "l", "c", "v"]]
        print(f"  5分足 {min(i+50, len(codes))}/{len(codes)} 銘柄", flush=True)
        time.sleep(2)
    return out


def is500_of(c: np.ndarray) -> bool:
    px = c[::7]
    q = px / STD_TK[np.searchsorted(STD_UB, px - 1e-9)]
    return (np.abs(q - np.round(q)) > 1e-6).mean() > 0.01


def trades_for(code, s, cand_days, earn, rng):
    """1銘柄: 元の規約で BB 売り・寄値+7% 売り・ランダム売り の玉を作る。"""
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]   # 9:00 の気配足を捨てる・9:05〜15:30
    if len(s) < 300:
        return [], []
    full = float(pd.Series(s.index.normalize()).value_counts().median())   # その銘柄の1日の標準本数
    c = s.c.to_numpy(float); o = s.o.to_numpy(float)
    ser = pd.Series(c)
    ma = ser.rolling(P).mean().to_numpy(); sd = ser.rolling(P).std(ddof=0).to_numpy()
    up = ma + SIG * sd
    is5 = is500_of(c)
    day = s.index.normalize()
    udays = day.unique()
    tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    out, rnd = [], []
    prev = None
    for dd in udays:
        ix = np.flatnonzero(day == dd)
        st, en = ix[0], ix[-1] + 1
        if prev is not None:
            pst, pen = prev
            pc = c[pen - 1]; ptv = float((c[pst:pen] * s.v.to_numpy(float)[pst:pen]).sum()); pn = pen - pst
            ds = dd.strftime("%Y-%m-%d")
            gu = o[st] / pc - 1
            ok = (100 <= pc <= 100000) and ptv >= 1e8 and pn >= 0.9 * full and gu >= 0.03 and (code, ds) in cand_days \
                and (code, ds) not in earn
            if ok:
                o_day = o[st]

                def sim(e, label):
                    if e is None or e + 1 >= en:
                        return None
                    ep = o[e + 1] - tick_of(o[e + 1], is5)          # 売り＝不利1tick（安く売れる）
                    state = np.sign(c[e] - ma[e])
                    px = c[en - 1]
                    for k in range(e + 1, en):
                        if (c[k] - ma[k]) * state <= 0:
                            px = o[k + 1] if k + 1 < en else c[en - 1]
                            break
                    px_eff = px + tick_of(px, is5)                  # 買い戻し＝不利1tick（高く買う）
                    r = -(px_eff - ep) / ep * 100 - COST_RT
                    eod = -(c[en - 1] + tick_of(c[en - 1], is5) - ep) / ep * 100 - COST_RT
                    return {"code": code, "date": ds, "kind": label, "time": s.index[e].strftime("%H:%M"), "ep": o[e + 1],
                            "ret": r, "ret_eod": eod, "gu": gu * 100, "fwd": ds >= SPLIT}

                win = [k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI]
                e_bb = next((k for k in win if np.isfinite(up[k]) and c[k] > up[k] and (c[k] - up[k]) / c[k] >= DEV), None)
                e_7 = next((k for k in win if c[k] >= o_day * 1.07), None)
                for e, lab in ((e_bb, "BB40/2σ 上0.5%外"), (e_7, "寄値+7%超え")):
                    t = sim(e, lab)
                    if t:
                        out.append(t)
                if e_bb is not None and win:
                    rs = [sim(int(k), "rand") for k in rng.choice(win, size=N_RANDOM)]
                    rnd.append({"date": ds, "fwd": ds >= SPLIT, "rets": [x["ret"] for x in rs if x]})
        prev = (st, en)
    return out, rnd


def stat(r: pd.Series) -> str:
    if not len(r):
        return "n=0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return (f"n={len(r):>4} 勝率{(r>0).mean()*100:>4.1f}% PF{(gp/gl if gl else np.inf):>5.2f} 1件{r.mean():>+6.2f}% "
            f"合計{r.sum():>+7.1f}% 中央値{r.median():>+6.2f}%")


def main() -> None:
    token = _jquants_key()
    days = _recent_trading_days(62)
    print(f"[bb_fwd] J-Quants 日足 {days[0]}〜{days[-1]}（{len(days)}営業日）", flush=True)
    D = jq_daily(token, days)
    D = D.sort_values(["code", "date"])
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1)
    D["gu"] = D.AdjO / D.pc - 1
    C = D[(D.gu >= 0.025) & (D.pva >= 1e8) & (D.pc >= 100) & (D.pc <= 100000)]
    cand_days = set(zip(C.code, C.date.astype(str).str[:10]))
    codes = set(C.code)
    print(f"[bb_fwd] 候補（日足で GU+2.5%以上）{len(C):,}銘柄日・{len(codes):,}銘柄", flush=True)
    earn = jq_fins(token, days)
    # 開示日の翌営業日も除外
    dl = sorted(set(D.date.astype(str).str[:10]))
    nxt = {dl[i]: dl[i + 1] for i in range(len(dl) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    print(f"[bb_fwd] 決算・業績修正の除外 {len(earn):,}銘柄日", flush=True)
    M = yf_5m(codes)
    print(f"[bb_fwd] 5分足 取得 {len(M):,}/{len(codes):,}銘柄", flush=True)
    if not M:
        print("⚠ yfinance の5分足が取れなかった"); sys.exit(1)

    rng = np.random.default_rng(0)
    T, R = [], []
    for code, s in M.items():
        t, r = trades_for(code, s, cand_days, earn, rng)
        T += t; R += r
    T = pd.DataFrame(T)
    if T.empty:
        print("玉なし"); return
    alld = sorted({x.strftime("%Y-%m-%d") for s in M.values() for x in s.index.normalize().unique()})
    print(f"\n[bb_fwd] 5分足の期間 {alld[0]}〜{alld[-1]}（{len(alld)}営業日）・前向き検証は {SPLIT} 以降")

    for lab in ("BB40/2σ 上0.5%外", "寄値+7%超え"):
        print(f"\n================ {lab}（中心線回帰で買い戻し・コストA）================")
        for nm, m in (("①重なり期間（〜10/22・元の検証と同じ期間の一部）", ~T.fwd), ("②前向き（10/23〜・未使用の日）", T.fwd)):
            d = T[(T.kind == lab) & m]
            print(f"  {nm}")
            print(f"    中心線回帰: {stat(d.ret)}")
            print(f"    参考 引け : {stat(d.ret_eod)}")
            if len(d):
                top = d.groupby("date").ret.sum().sort_values(ascending=False)
                print(f"    上位3日を除く 合計{d[~d.date.isin(top.index[:3])].ret.sum():+.1f}% ／ 最大の日 {top.index[0]} {top.iloc[0]:+.1f}%（{(d.date==top.index[0]).sum()}件）")
                per = d.groupby("date").size().reindex([x for x in alld if (x >= SPLIT) == (nm.startswith('②'))], fill_value=0)
                print(f"    1日あたり本数: 平均{per.mean():.1f} 中央値{per.median():.0f} 最大{per.max()} 出なかった日 {(per==0).sum()}/{len(per)}")
            if lab.startswith("BB"):
                rr = [x for x in R if x["fwd"] == nm.startswith("②")]
                if rr and len(d):
                    sims = np.array([np.mean([x["rets"][k] for x in rr if len(x["rets"]) > k]) for k in range(N_RANDOM)])
                    z = (d.ret.mean() - sims.mean()) / (sims.std() + 1e-9)
                    print(f"    同じ銘柄日のランダムな時刻で売り（{N_RANDOM}回）: 1件{sims.mean():+.2f}±{sims.std():.2f}% → z={z:+.1f}")
    fw = T[(T.kind == "BB40/2σ 上0.5%外") & T.fwd].sort_values(["date", "time"])
    if len(fw):
        print("\n=== ②前向き期間のBB売りの玉（全部） ===")
        print(fw[["date", "time", "code", "gu", "ep", "ret", "ret_eod"]].to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    print("\n読み方: ①が元の検証（PF2.75・1件+1.04%）に近ければ実装は同じ。②が本当の答え。"
          "\n        空売りできるか（一般信用の在庫・売り禁）は見ていない＝実際は撃てない玉が混ざる（楽観側）。")


if __name__ == "__main__":
    main()

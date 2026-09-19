# -*- coding: utf-8 -*-
"""_bt_fibo_news_split_0920.py — 本人「その材料はニュース／ファンダ（ロイターとか）ってこと？」（2026-09-20）
噴いた銘柄を「決算・業績修正の直後か」（J-Quants statements: 前営業日15時以降〜当日9時前の開示）と
「寄り後15分の出来高が平時の何%か」（材料の強さの代理）で分け、押し目買い(23.6・+2%／引け)・買い下がりA・9:30売り・寄→引の差を見る。
データ: _rankonly_cache_0920.pkl（寄りギャップ順位/9:15順位/5分足）＋ _fins_history.pkl（〜7/31）＋ J-Quants /fins/statements 8/1〜9/2（取得してキャッシュ）
実行: python -X utf8 _bt_fibo_news_split_0920.py > _log_fibo_news_split_0920.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, pickle, time, sys, os, json
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read()
exec(src.split("t0 = time.time(); C = pickle.load")[0])
sim_src = src.split("def simulate(e, alloc, target, stop_mode):")[1].split("\n\n\nrows = []")[0]
exec("def simulate(e, alloc, target, stop_mode):" + sim_src); TPV["引け"] = 9.99
t0 = time.time()
D, bars_all = pickle.load(open("_rankonly_cache_0920.pkl", "rb"))
U = D[(D.rk_gap <= 30) | (D.rk_15 <= 30) | (D.rise_o >= 5)].copy()
print(f"[U] 対象 {len(U):,}銘柄日（ギャップ順位≤30 or 9:15順位≤30 or 寄り後+5%）", flush=True)

# ── 決算/修正の開示（〜7/31はpkl・8/1〜9/2はAPI）──
F = pickle.load(open("_fins_history.pkl", "rb"))[["Code", "DiscDate", "DiscTime", "DocType"]].copy()
CACHE = "_fins_aug_sep_0920.pkl"
if os.path.exists(CACHE):
    F2 = pickle.load(open(CACHE, "rb"))
else:
    for ln in open(".env", encoding="utf-8-sig"):          # 鍵は .env（daytrade_paper と同じ読み方）
        if "=" in ln and not ln.startswith("#"):
            k_, v_ = ln.strip().split("=", 1); os.environ.setdefault(k_.strip(), v_.strip().strip('"'))
    from screener import _jquants_get, _jquants_id_token
    tok = _jquants_id_token(); recs = []
    for d in pd.bdate_range("2026-08-01", "2026-09-02"):
        key = None
        for _ in range(20):
            q = {"date": d.strftime("%Y-%m-%d")}
            if key: q["pagination_key"] = key
            try:
                j = _jquants_get("/fins/statements", tok, q)
            except Exception as ex:
                print(f"  [fins] {d.date()} 取得失敗 {ex}"); break
            k = next((x for x in j if x != "pagination_key"), None); rows_ = j.get(k) or [] if k else []
            for r in rows_:
                recs.append(dict(Code=str(r.get("Code") or r.get("LocalCode") or ""), DiscDate=str(r.get("DiscDate") or r.get("DisclosedDate") or ""),
                                 DiscTime=str(r.get("DiscTime") or r.get("DisclosedTime") or ""), DocType=str(r.get("DocType") or r.get("TypeOfDocument") or "")))
            key = j.get("pagination_key")
            if not key: break
        time.sleep(0.2)
    F2 = pd.DataFrame(recs); pickle.dump(F2, open(CACHE, "wb")); print(f"  [fins] 8/1〜9/2 取得 {len(F2)}件", flush=True)
FA = pd.concat([F[F.DiscDate < "2026-08-01"], F2], ignore_index=True)
FA["c4"] = FA.Code.astype(str).str[:4]; FA["t"] = FA.DiscTime.astype(str).str[:5]
FA = FA[FA.DiscDate >= "2026-06-01"]
def kind(dt):
    dt = str(dt)
    if "FinancialStatements" in dt: return "決算"
    if "EarnForecastRevision" in dt: return "業績修正"
    if "Dividend" in dt: return "配当修正"
    return "その他"
FA["kind"] = FA.DocType.map(kind)
days = sorted(U.day.unique()); prev_day = {d: (days[i - 1] if i > 0 else None) for i, d in enumerate(days)}
# 開示→「翌営業日の寄りに効く」対応: 前営業日15:00以降 or 当日09:00前
news = {}
for r in FA.itertuples():
    if r.DiscDate not in days and r.DiscDate not in set(prev_day.values()): pass
    if r.t >= "15:00":
        nxt = next((d for d in days if d > r.DiscDate), None)
        if nxt and (r.DiscDate == prev_day.get(nxt) or r.DiscDate < nxt):
            if r.DiscDate == prev_day.get(nxt) or r.DiscDate not in days: news.setdefault((r.c4, nxt), set()).add(r.kind)
    elif r.t < "09:00" and r.DiscDate in days:
        news.setdefault((r.c4, r.DiscDate), set()).add(r.kind)
U["news"] = [("決算" if "決算" in news.get((tk[:4], d), set()) else "業績修正" if "業績修正" in news.get((tk[:4], d), set()) else "配当修正" if news.get((tk[:4], d)) else "なし") for tk, d in zip(U.tk, U.day)]
print("  ニュース区分: " + str(U.news.value_counts().to_dict()), flush=True)

# ── 寄り後15分の出来高／平時1日出来高（20日平均）──
C5 = pickle.load(open("_intraday_cache_5m.pkl", "rb")); A = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
v15 = {}
need = U.groupby("tk").day.apply(set).to_dict()
for tk, ds in need.items():
    df = C5.get(tk); dd = A.get(tk)
    if df is None or dd is None: continue
    dt = pd.to_datetime(df["dt"]).dt.tz_localize(None); g = df.assign(day=dt.dt.date.astype(str), hm=dt.dt.strftime("%H:%M"))
    vol_avg = dd["Volume"].astype(float).rolling(20).mean().shift(1)
    for d in ds:
        s = g[(g.day == d) & (g.hm >= "09:05") & (g.hm <= "09:15")]
        va = vol_avg.get(pd.Timestamp(d), np.nan)
        if len(s) and va and va > 0: v15[(tk, d)] = s.v.sum() / va * 100
U["v15"] = [v15.get((tk, d), np.nan) for tk, d in zip(U.tk, U.day)]
U["vband"] = pd.cut(U.v15, [-1, 10, 30, 100, 1e9], labels=["<10%", "10-30%", "30-100%", ">100%"])
print(f"  15分出来高/平時1日 中央値 {U.v15.median():.0f}%・付与率{U.v15.notna().mean()*100:.0f}%（{time.time()-t0:.0f}s）", flush=True)
COST = 0.1
def evaluate(sub, label):
    rows = []
    for r in sub.itertuples():
        e = dict(tk=r.tk, day=r.day, o=r.o, c=r.c, h15=r.h15, l15=r.l15, bars=bars_all[(r.tk, r.day)]); b = e["bars"]
        out = dict(drift=(r.c / r.o - 1) * 100 - COST, day=r.day)
        hm = b.hm.to_numpy(); cl = b.c.to_numpy(float); i30 = np.where(hm == "09:30")[0]
        out["short930"] = ((cl[i30[0]] - r.c) / cl[i30[0]] * 100 - COST) if len(i30) and cl[i30[0]] > 0 else np.nan
        for nm, alloc, tg, sm in (("E+2%", ALLOC["E 23.6のみ"], "+2%", "安値割れ"), ("E引け", ALLOC["E 23.6のみ"], "引け", "安値割れ"), ("A+5%", ALLOC["A 23.6/38.2/50=1:2:3"], "+5%", "1段下")):
            s = simulate(e, alloc, tg, sm)
            out[nm + "_R"] = s["R"] if s else np.nan; out[nm + "_fill"] = bool(s and s["n_fill"] > 0); out[nm + "_win"] = (1.0 if s["pnl_yen"] > 0 else 0.0) if s and s["n_fill"] > 0 else np.nan
        rows.append(out)
    R = pd.DataFrame(rows)
    if len(R) == 0: return
    def cell(nm):
        f = R[R[nm + "_fill"]]; return f"{R[nm + '_R'].mean():>+6.2f}R({f[nm + '_win'].mean()*100 if len(f) else 0:>3.0f}%)"
    print(f"  {label:<30} n{len(R):>5} 寄→引{R.drift.mean():>+5.2f}%(上昇{(R.drift > 0).mean()*100:>3.0f}%) | " + " ".join(f"{nm}{cell(nm)}" for nm in ("E+2%", "E引け", "A+5%")) + f" | 9:30売り{R.short930.mean():>+5.2f}%({(R.short930 > 0).mean()*100:>3.0f}%)", flush=True)
for uname, um in (("9:15値上がり順位≤20", U.rk_15 <= 20), ("寄り後+5%噴き(T2)", U.rise_o >= 5), ("寄りギャップ順位≤20", U.rk_gap <= 20)):
    print(f"\n■ {uname}: ニュース（決算/業績修正が前営業日引け後〜当日朝）の有無で分ける", flush=True)
    for nk in ("決算", "業績修正", "配当修正", "なし"):
        evaluate(U[um & (U.news == nk)], f"{nk}")
    print(f"■ {uname}: 寄り後15分の出来高（平時1日比）で分ける", flush=True)
    for vb in ("<10%", "10-30%", "30-100%", ">100%"):
        evaluate(U[um & (U.vband == vb)], f"出来高 {vb}")
    print(f"■ {uname}: ニュースなし × 出来高帯", flush=True)
    for vb in ("<10%", "10-30%", "30-100%", ">100%"):
        evaluate(U[um & (U.news == "なし") & (U.vband == vb)], f"ニュースなし×{vb}")
    evaluate(U[um & (U.news != "なし") & (U.v15 >= 30)], "ニュースあり×出来高30%以上")
print(f"\n[done] {time.time()-t0:.0f}s")

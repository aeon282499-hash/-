import os, json, numpy as np, pandas as pd
os.chdir(r"C:/Users/星野/Downloads/trade_bt/uwick_bt")
G = {}; exec(open("fade5y/top3_tune.py", encoding="utf-8").read().split("L = []; W = L.append")[0], G)
C, NAME, lim, tk = G["C"], G["NAME"], G["lim"], G["tk"]
X = C[(C.C >= 100) & (C.gain >= 12) & (C.Va >= 1e8) & (C.Va < 3e8)]
X = pd.DataFrame({"sig": X.Date.dt.strftime("%Y-%m-%d"), "d": X.nday.dt.strftime("%Y-%m-%d"), "code": X.code, "g": X.gain, "va": X.Va,
                  "px": X.C, "o": X.tO, "h": X.tH, "c": X.tC, "pin": X.pin.astype(bool)})
# パネルの後(10/29取引分)はキャッシュの日足から足す
cd = os.path.join(os.environ["LOCALAPPDATA"], "fade_notify_cache")
ld = lambda d: {r["Code"]: r for r in json.load(open(os.path.join(cd, f"daily_{d}.json"), encoding="utf-8"))}
last = X.d.max(); add = []
for s, t, p in [("2026-10-28", "2026-10-29", "2026-10-27"), ("2026-10-29", None, "2026-10-28")]:
    if t and t <= last: continue
    S, P = ld(s), ld(p); T = ld(t) if t else {}
    for code, r in S.items():
        q = P.get(code)
        if not q or not r["C"] or not q["C"] or code[-1] != "0" or r.get("AdjFactor", 1) != 1: continue
        g = (r["C"] / q["C"] - 1) * 100
        if g >= 12 and r["C"] >= 100 and 1e8 <= r["Va"] < 3e8:
            z = T.get(code) or {}
            add.append(dict(sig=s, d=t or "2026-10-30(今日)", code=code[:4], g=g, va=r["Va"], px=r["C"], o=z.get("O"), h=z.get("H"), c=z.get("C"),
                            pin=bool(z) and str(z.get("UL")) == "1" and z.get("O", 0) >= z.get("H", 1)))
X = pd.concat([X, pd.DataFrame(add)]).sort_values(["sig", "g"], ascending=[True, False])
X["rk"] = X.groupby("sig").cumcount() + 1
X = X[X.sig >= "2026-09-01"]
out = ["| 取引日 | 銘柄 | 前日比 | 代金 | 前日終値→指値 | 寄 / 高 / 引 | 結果 | 25万の損益 | 1日1本 |", "|---|---|---|---|---|---|---|---|---|"]
tot = {True: [], False: []}
for r in X.itertuples():
    L = lim(r.px, 0.12); sh = int(250000 / L // 100 * 100) or 100
    nm = f"{r.code} {str(NAME.get(r.code, ''))[:10]}"
    if r.o is None or pd.isna(r.o): res, y = "これから", None
    elif r.pin: res, y = "S高張り付き→見送り", None
    else:
        s = r.o if r.o >= L else (L if r.h >= L else None)
        if s is None: res, y = "届かず", None
        else:
            y = (s - r.c * 1.001) * sh; res = f"{'寄りで' if s == r.o else ''}{s:,.0f}円で約定 {(s/r.c-1)*100 if False else (s - r.c)/s*100:+.1f}%"
            tot[False].append(y)
            if r.rk == 1: tot[True].append(y)
    ohlc = "" if r.o is None or pd.isna(r.o) else f"{r.o:,.0f} / {r.h:,.0f} / {r.c:,.0f}"
    out.append(f"| {r.d[5:]} | {nm} | +{r.g:.1f}% | {r.va/1e8:.1f}億 | {r.px:,.0f}→**{L:,.0f}** | {ohlc} | {res} | {'' if y is None else f'{y/1e4:+.1f}万'} | {'○' if r.rk == 1 else ''} |")
print("\n".join(out))
for k, v in tot.items():
    v = np.array(v); print(("1日1本" if k else "全部"), len(v), "本 勝ち", (v > 0).sum(), "合計", round(v.sum()/1e4, 1), "万 PF", round(v[v>0].sum()/-v[v<0].sum(), 2) if (v<0).any() else "inf")

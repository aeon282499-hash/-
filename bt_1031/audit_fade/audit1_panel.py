# 監査1: 5年パネル(_panel.pkl)の作りと「今日の3本」BT(vamin_bt.one)の再現・感度。読むだけ・BTファイルは変更しない。
# cwd = C:\Users\星野\Downloads\trade_bt\uwick_bt
import os, sys, pickle, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_fade"
L = []
def W(*a):
    s = " ".join(str(x) for x in a); print(s, flush=True); L.append(s)

P = pd.read_pickle("fade5y/_panel.pkl")
W("panel cols:", P.columns.tolist()); W("panel shape", P.shape, P.Date.min(), P.Date.max(), "codes", P.code.nunique())

# ── (a) 営業日の整合: market_calendar.csv と比べる ──
cal = pd.read_csv(r"C:\Users\星野\Documents\my-first-project\market_calendar.csv", dtype={"Date": str})
cal_days = set(pd.to_datetime(cal.loc[cal.HolDiv.isin([1, 2]), "Date"]))
pdays = np.sort(P.Date.unique()); pdays_set = set(pd.to_datetime(pdays))
lo, hi = pd.Timestamp("2021-10-01"), pd.Timestamp("2026-10-28")
cal_in = {d for d in cal_days if lo <= d <= hi}
W("(a) panel days", len(pdays), "calendar days in range", len(cal_in))
W("  calendar days missing from panel:", sorted(str(d.date()) for d in cal_in - pdays_set)[:30])
W("  panel days not in calendar:", sorted(str(d.date()) for d in pdays_set - cal_in)[:30])
cnt = P.groupby("Date").size(); W("  rows/day min", cnt.min(), "at", cnt.idxmin().date(), " median", int(cnt.median()), " days<3000 rows:", list(cnt[cnt < 3000].index.strftime("%Y-%m-%d"))[:10])
# nday gap (暦日) >4 or trading-day gap>1
gap_bd = np.busday_count(P.Date.values.astype("datetime64[D]"), P.nday.fillna(P.Date).values.astype("datetime64[D]"))
W("  D→T 平日カウント分布 (全行):", pd.Series(gap_bd).value_counts().sort_index().to_dict())

# ── (c) AdjFactor の向き: AF!=1 の行で C_t/(C_{t-1}*AF_t) ──
P = P.sort_values(["code", "Date"])
pc = P.groupby("code").C.shift(1)
m = (P.AdjFactor != 1) & P.AdjFactor.notna() & pc.notna() & (P.C > 0)
rr = (P.C[m] / (pc[m] * P.AdjFactor[m]))
rr2 = (P.C[m] / (pc[m] / P.AdjFactor[m]))
W("(c) AdjFactor!=1 rows", int(m.sum()), " median C/(prevC*AF)=", round(rr.median(), 3), " (正しければ≈1)   median C/(prevC/AF)=", round(rr2.median(), 3))
W("   AF 値の分布:", P.AdjFactor[m].round(3).value_counts().head(8).to_dict())
# ret1 と AF の整合: ret1 (BT) vs 生の C/prevC-1 の差が AF!=1 の行だけに出るか
raw = P.C / pc - 1
dif = (P.ret1 - raw).abs() > 1e-9
W("   ret1≠生前日比 の行数", int(dif.sum()), " うち AF!=1:", int((dif & m).sum()), " うち前行が別日(欠け)で起きたもの等:", int((dif & ~m).sum()))

# ── (d) UL フラグの意味 ──
def band(p):
    for lim, w in ((100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700), (7000, 1000),
                   (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000)):
        if p < lim: return w
    return p * 0.15
ulr = P[(P.UL == 1) & P.C.notna() & pc.notna()]
pcu = pc[ulr.index]
hit = ulr.H >= pcu + pcu.map(band) - 1e-9
W("(d) UL==1 rows", len(ulr), " うち 高値==前日終値+値幅(表で計算):", f"{hit.mean()*100:.1f}%", " うち 終値<高値(S高で引けていない):", f"{(ulr.C < ulr.H).mean()*100:.1f}%",
  " うち O==H==L==C(終日張り付き):", f"{((ulr.O == ulr.H) & (ulr.L == ulr.C) & (ulr.O == ulr.C)).mean()*100:.1f}%")
# 逆: 高値が表の値幅上限に達している行のうち UL==1 の割合(AF==1 の行だけ)
m2 = (P.AdjFactor == 1) & pc.notna() & P.H.notna()
hit2 = (P.H[m2] >= pc[m2] + pc[m2].map(band) - 1e-9)
W("   高値が表の上限に達した行", int(hit2.sum()), " うち UL==1:", f"{(P.UL[m2][hit2] == 1).mean()*100:.1f}%", "  → UL=「日中にS高を付けた」フラグか(90%超なら yes)")
del ulr, pcu, hit, m2, hit2

# ── (e) top3_tune の C と同じ候補表を作る ──
MQ = pd.read_pickle("fade5y/_master_q.pkl"); MQ["code"] = MQ.Code.str[:4]
etf_codes = set(MQ.loc[MQ.S17 == "99", "code"])
W("(i) S17==99 で落とす銘柄数", len(etf_codes), " MktNm例:", MQ[MQ.S17 == "99"].Mkt.value_counts().head(5).to_dict())
P = P[~P.code.isin(etf_codes)]
first_day = P.groupby("code").Date.min()
C = P[(P.ret1 >= 0.05) & (P.C >= 50) & (P.Va >= 5e7) & P.tO.notna() & (P.Date >= "2021-11-01")].copy()
W("   候補(ret1≥5%・C≥50・Va≥5千万) 行数", len(C), " うち tAF!=1 で落ちる:", int((C.tAF.fillna(1) != 1).sum()))
C = C[C.tAF.fillna(1) == 1]
C["gain"] = C.ret1 * 100
C["pin"] = (C.tUL == 1) & (C.tO >= C.tH)
C["first"] = C.code.map(first_day)
del P
W("   D→T 平日カウント (候補):", pd.Series(np.busday_count(C.Date.values.astype("datetime64[D]"), C.nday.values.astype("datetime64[D]"))).value_counts().sort_index().to_dict())

def tk(p): return 1 if p <= 3000 else 5 if p <= 5000 else 10 if p <= 30000 else 50 if p <= 50000 else 100
def lim(c, k): return np.ceil(c * (1 + k) / tk(c)) * tk(c)
def tk_real(p): return tk(p)   # 指値の値段で見た呼値
U = C[C.C >= 100].copy()
U["capok"] = [lim(p, 0.03) * 100 <= 1e6 for p in U.C]
T = U[(U.gain >= 12) & (U.Va >= 3e8) & U.capok].sort_values(["Date", "gain", "code"], ascending=[True, False, True])
T = T.assign(rk=T.groupby("Date").cumcount() + 1); T3 = T[T.rk <= 3].copy()
W("(l) 3本の候補 行数", len(T3), " 日数", T3.Date.nunique(), " 置いた日のうち3本そろう日", f"{(T3.groupby('Date').size() == 3).mean()*100:.0f}%",
  " 1位が+17%以上の日", f"{(T3[T3.rk == 1].gain >= 17).mean()*100:.0f}%", " pin(寄りS高)で見送り", int(T3.pin.sum()))
W("   全営業日", len(pdays), " に対し候補のある日", T3.Date.nunique())
T3.to_pickle(os.path.join(OUT, "_T3.pkl")); U.to_pickle(os.path.join(OUT, "_U.pkl"))

# ── (f) one() の再現 と 感度 ──
SLIP = 0.001
def fill(O, H, Lp, strict=False, tick=1):
    if O >= Lp: return O
    if H >= Lp + (tick if strict else 0): return Lp
    return None
def one(Pc, O, H, Cl, k2=0.12, strict=False, slip=SLIP, cost=0.0, pin_sell=False):
    L3 = lim(Pc, 0.03); s3 = fill(O, H, L3, strict, tk(L3))
    if s3 is None: return None
    L2 = lim(Pc, k2); h3 = int(250000 / L3 // 100 * 100); h2 = int(250000 / L2 // 100 * 100)
    buy = Cl * (1 + slip)
    if h3 < 100 or h2 < 100:
        sh = int(500000 / L3 // 100 * 100); sh = sh if sh >= 100 else 100
        nt = s3 * sh; return (s3 - buy) * sh - nt * cost, nt, 1, (s3 == O), (H == L3)
    s2 = fill(O, H, L2, strict, tk(L2)); y = (s3 - buy) * h3; nt = s3 * h3; legs = 1; touch = (H == L3) and s3 != O
    if s2 is not None: y += (s2 - buy) * h2; nt += s2 * h2; legs = 2; touch = touch or ((H == L2) and s2 != O)
    return y - nt * cost, nt, legs, (s3 == O), touch
def run(T, pinmask=None, **kw):
    rows = []
    pm = T.pin if pinmask is None else pinmask
    for r, p in zip(T.itertuples(), pm):
        if p: continue
        z = one(r.C, r.tO, r.tH, r.tC, **kw)
        if z: rows.append(dict(d=r.nday, sig=r.Date, code=r.code, rk=r.rk, yen=z[0], nt=z[1], legs=z[2], at_open=z[3], touch=z[4], tUL=r.tUL, tC=r.tC, tH=r.tH, tO=r.tO, tL=r.tL, C=r.C, first=r.first))
    Z = pd.DataFrame(rows); Z["m"] = Z.d.dt.strftime("%Y-%m"); return Z
pf = lambda v: v[v > 0].sum() / -v[v < 0].sum() if (v < 0).any() else np.inf
def st(Z):
    day = Z.groupby("d").yen.sum().sort_index(); cc = day.cumsum().to_numpy(); dd = (np.maximum.accumulate(np.r_[0, cc])[1:] - cc).max()
    mo = Z.groupby("m").yen.sum()
    return f"n={len(Z)} 勝率{(Z.yen > 0).mean()*100:.0f}% PF{pf(Z.yen.to_numpy()):.2f} 合計{Z.yen.sum()/1e4:+,.0f}万 DD{dd/1e4:.0f}万 最悪月{mo.min()/1e4:+.0f}万 負け月{int((mo < 0).sum())}/{len(mo)} 平均建玉{Z.nt.mean()/1e4:.0f}万"
Z0 = run(T3); W("\n(f) 基準(再現) :", st(Z0), "  ← RESULTS_vamin: 2435 65% PF2.23 +3,100万 DD36万")
Z0.to_pickle(os.path.join(OUT, "_Z0.pkl"))
W("   寄りで約定(寄り≥+3%指値)", f"{Z0.at_open.mean()*100:.0f}%", " 2段目も約定", f"{(Z0.legs == 2).mean()*100:.0f}%",
  " 高値がちょうど指値(1tickも抜けず)の約定を含む本", int(Z0.touch.sum()), f"({Z0.touch.mean()*100:.0f}%)", " その損益合計", f"{Z0[Z0.touch].yen.sum()/1e4:+.0f}万")
W("   厳しめ(1tick抜け) :", st(run(T3, strict=True)))
W("   滑り0.3%           :", st(run(T3, slip=0.003)))
for c in (0.003, 0.005, 0.01, 0.02, 0.03):
    W(f"   コスト建玉比{c*100:.1f}%  :", st(run(T3, cost=c)))
W("   厳しめ+コスト1%     :", st(run(T3, strict=True, cost=0.01)))
# pin 判定の別案
pinA = (T3.tO >= T3.C + T3.C.map(band) - 1e-9)                    # 本番: 寄り ≥ 前日終値+値幅(表)
pinB = (T3.tUL == 1) & (T3.tO == T3.tH) & (T3.tL == T3.tC) & (T3.tO == T3.tC)   # 終日張り付き
W("\n(4) pin 判定: 今=(tUL==1)&(tO>=tH)", int(T3.pin.sum()), " 本番式=寄り≥終値+値幅", int(pinA.sum()), " 両方", int((T3.pin & pinA).sum()), " 終日張り付き(O=H=L=C&UL)", int(pinB.sum()))
W("   今の pin だが本番式では pin でない行:", int((T3.pin & ~pinA).sum()), "  本番式 pin だが今は pin でない:", int((pinA & ~T3.pin).sum()))
W("   pin を本番式にした場合 :", st(run(T3, pinmask=pinA)))
W("   pin を終日張り付きだけにした(寄りS高でも寄りで売れた扱い):", st(run(T3, pinmask=pinB)))
W("   pin 除外なし(全部売った扱い):", st(run(T3, pinmask=np.zeros(len(T3), bool))))
# (5) 引けS高の扱い
cl_ul = (Z0.tUL == 1) & (Z0.tC >= Z0.tH)
W("\n(5) 売った日に引けS高(担がれ)の本数", int(cl_ul.sum()), " 損益合計", f"{Z0[cl_ul].yen.sum()/1e4:+.0f}万", " 平均", f"{(Z0[cl_ul].yen/Z0[cl_ul].nt).mean()*100:+.1f}%",
  " うち売った後に寄り<高値(場中に張り付き)", int((cl_ul & (Z0.tO < Z0.tH)).sum()))
allday = cl_ul & (Z0.tO == Z0.tH) & (Z0.tL == Z0.tC)
W("   引けS高のうち O=H=L=C(終日張り付きだが pin 判定をすり抜けた=ありえないはず)", int(allday.sum()))
# 引けS高で終値での買い戻しが怪しい日(引けが特別気配→比例配分)
W("   引けS高の損益を「翌日寄りで買い戻し」にした場合の差は別スクリプト(翌々日データ要)")
# (6) 株数の丸め: 本番 top3_ladder vs BT one()
def prod_ladder(close):
    t = tk(close); l1 = float(np.ceil(close * 1.03 / t) * t); l2 = float(np.ceil(close * 1.12 / t) * t)
    h1, h2 = int(500000 / 2 / l1 // 100 * 100), int(500000 / 2 / l2 // 100 * 100)
    if h1 >= 100 and h2 >= 100: return [(l1, h1), (l2, h2)]
    sh = int(500000 / l1 // 100 * 100); sh = sh if sh >= 100 else (100 if l1 * 100 <= 1e6 else 0)
    return [(l1, sh)]
def bt_ladder(Pc):
    L3 = lim(Pc, 0.03); L2 = lim(Pc, 0.12); h3 = int(250000 / L3 // 100 * 100); h2 = int(250000 / L2 // 100 * 100)
    if h3 < 100 or h2 < 100:
        sh = int(500000 / L3 // 100 * 100); sh = sh if sh >= 100 else 100
        return [(L3, sh)]
    return [(L3, h3), (L2, h2)]
bad = [p for p in np.arange(100, 9800, 1.0) if lim(p, 0.03) * 100 <= 1e6 and prod_ladder(p) != bt_ladder(p)]
W("\n(6) 株数: 本番 top3_ladder と BT one() が食い違う前日終値(100〜9,800円・1円刻み):", len(bad), bad[:10])
W("   例 C=155:", prod_ladder(155), " C=2200:", prod_ladder(2200), " C=2500:", prod_ladder(2500), " C=4900:", prod_ladder(4900), " C=9000:", prod_ladder(9000))
cross = [(p, lim(p, 0.03)) for p in (2950, 2980, 4900, 4950, 29500)]
W("   呼値の帯またぎ(指値が3000/5000/30000を超えるのに前日終値の呼値で丸め):", cross, " 該当本数(3%段):", int(sum(tk(l) != tk(p) for p, l in zip(Z0.C, [lim(p, 0.03) for p in Z0.C]))),
  " (12%段):", int(sum(tk(lim(p, 0.12)) != tk(p) for p in Z0.C)))
# (9) 母集団
W("\n(9) 上場2日目(D が銘柄の最初の行の翌営業日)の玉:", int((Z0.first == Z0.sig).sum()), " 上場初日からD まで5営業日以内:", int((np.busday_count(Z0["first"].values.astype("datetime64[D]"), Z0.sig.values.astype("datetime64[D]")) <= 5).sum()))
mk = MQ.sort_values("snap").groupby("code").Mkt.last()
W("   玉の市場区分:", Z0.code.map(mk).fillna("(master無し)").value_counts().to_dict())
# (12) 年別・順位別
W("\n(12) 年別:", {y: f"{v/1e4:+.0f}万" for y, v in Z0.groupby(Z0.d.dt.year).yen.sum().items()})
W("   順位別 PF:", {k: f"{pf(g.yen.to_numpy()):.2f}/{g.yen.sum()/1e4:+.0f}万" for k, g in Z0.groupby("rk")})
# 1本の損益率の分布(低位株の tick 影響)
r = Z0.yen / Z0.nt
W("   1本の損益率: 中央値", f"{r.median()*100:+.2f}%", " 平均", f"{r.mean()*100:+.2f}%", " 下位1%", f"{r.quantile(0.01)*100:+.1f}%", " 最悪", f"{r.min()*100:+.1f}%", " 株価帯別本数:", pd.cut(Z0.C, [0, 200, 500, 1000, 3000, 10000]).value_counts().sort_index().to_dict())
W("   株価帯別 PF:", {str(k): f"{pf(g.yen.to_numpy()):.2f}" for k, g in Z0.groupby(pd.cut(Z0.C, [0, 200, 500, 1000, 3000, 10000]))})
open(os.path.join(OUT, "audit1_out.txt"), "w", encoding="utf-8").write("\n".join(L) + "\n")

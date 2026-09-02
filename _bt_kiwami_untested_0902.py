# -*- coding: utf-8 -*-
"""_bt_kiwami_untested_0902.py — 極み買いの「本当に未検証だった」6軸（2026-09-02・本番無変更）。

本人「このルールが一番儲かるんだよね？隅々まであらゆる手を使ってチェックして」。
8/20〜8/29で入口5軸・データ4軸・枠/サイズ・出口500セル・クールダウン・寄指係数・市場区分・
執行セッション・満杯見送りまで決着済み。メモとコードを突き合わせて残っていた未計測軸:

  A 財務（_fins_history.pkl・開示日<エントリー日の最新開示）: 赤字/自己資本比率/時価総額概算/
    PBR概算/予想増益/配当利回り/開示からの日数   ※フェードは8/29に測定済・極み買いは未測定
  B カレンダー: 曜日 / 月初・月末3営業日 / SQ週
  C トレーリングストップ（500セルはSTOP×TP×HOLD×RSIの水準だけで追随型は未計測）
  D 寄りギャップの下側（8/20④は上側の絞り込みだけ）
  E 同一銘柄の前回結果（負けた直後の再シグナルを飛ばす）
  F 前日の米国株（SP500前日騰落・FREDキャッシュ）
  G 所属業種の33業種指数の地合い（25MA上下・5日騰落）

エンジン=_bt_kiwami_exit_axes.py（公式+3,108,516円と1円一致）を exec で流用。
採用バー: 両期間改善 × 高原 × 上位20玉除去でも残る × 機構の説明。
実行: python -X utf8 _bt_kiwami_untested_0902.py
"""
import re
import pickle
import numpy as np
import pandas as pd

_src = open("_bt_kiwami_exit_axes.py", encoding="utf-8").read().split("# ══ ⓪")[0]
exec(_src)

# ── 選定シム（run と同一。row に候補index i と前回結果を刻む） ──────────────────
def run2(mask, pnl, exo, skip_after_loss=False):
    ok = np.isfinite(pnl) & mask
    ou: dict = {}; os_: dict = {}
    last: dict = {}          # ticker -> 直前に「建てた」玉の損益（決済済みのみ参照）
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
            picks.append((d, ex, int(YEAR[i]), float(pnl[i]), float(E[i]), TICK[i], i))
    live: list = []
    rows = []
    for d, ex, y, p, e, tk, i in picks:
        live = [x for x in live if x >= d]
        if len(live) >= SLOTS:
            continue
        prev = last.get(tk)
        if skip_after_loss and prev is not None and prev[1] < 0 and prev[0] < d:
            continue
        sh = int(SIZE / e / 100) * 100
        if sh <= 0:
            continue
        live.append(ex)
        rows.append({"i": i, "d": d, "exit": ex, "y": y, "ticker": tk, "pnl": p,
                     "yen": p / 100 * sh * e,
                     "prev": (np.nan if prev is None or prev[0] >= d else prev[1])})
        last[tk] = (ex, p)
    return pd.DataFrame(rows)


def quint(R, col, label, nq=5):
    x = R[np.isfinite(R[col].astype(float))].copy()
    if len(x) < 100:
        print(f"  {label}: n不足 {len(x)}"); return
    x["q"] = pd.qcut(x[col].rank(method="first"), nq, labels=False)
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"),
                           win=("pnl", lambda v: (v > 0).mean() * 100),
                           yen=("yen", "sum"), lo=(col, "min"), hi=(col, "max"))
    print(f"\n  ▶ {label} 五分位（建てた玉ベース・件あたり%）")
    for q, r in g.iterrows():
        print(f"     Q{q+1} n={int(r.n):>4} 平均{r.avg:+.3f}% 勝率{r.win:.1f}% 円{r.yen:>+12,.0f}  [{r.lo:.3g}〜{r.hi:.3g}]")


def grp(R, col, label):
    g = R.groupby(col).agg(n=("pnl", "size"), avg=("pnl", "mean"),
                           win=("pnl", lambda v: (v > 0).mean() * 100), yen=("yen", "sum"))
    print(f"\n  ▶ {label}")
    for k, r in g.iterrows():
        print(f"     {str(k):<10} n={int(r.n):>4} 平均{r.avg:+.3f}% 勝率{r.win:.1f}% 円{r.yen:>+12,.0f}")


def sim(label, mask, pnl=None, exo=None):
    pnl = pnl0 if pnl is None else pnl; exo = exo0 if exo is None else exo
    st = stats(run2(mask, pnl, exo)); print(line(label, st)); return st


# ══ ⓪ ベースライン ══
pnl0, exo0 = replay(5.0, 3.0, 3, 50)
R0 = run2(BASE, pnl0, exo0)
st0 = stats(R0)
print("\n" + "=" * 122)
print("⓪ ベースライン（公式+3,108,516円と一致必須）")
print("=" * 122); print(HDR); print(line("現行（採用構成）", st0))
assert abs(st0["total"] - 3_108_516) < 2, st0["total"]

ENTRY_DT = pd.to_datetime(pd.Series(ALLDAYS))
DDAY = ENTRY_DT.iloc[R0.d.to_numpy()].reset_index(drop=True)

# ══ A 財務 ══
print("\n" + "=" * 122); print("A 財務（開示日<エントリー日の最新開示を結合）"); print("=" * 122)
F = pd.read_pickle("_fins_history.pkl")
F = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)].copy()
F["code4"] = F.Code.astype(str).str[:4]
num = lambda c: pd.to_numeric(F[c], errors="coerce")
F["np_"] = num("NP"); F["eps"] = num("EPS"); F["eqv"] = num("Eq"); F["eqar"] = num("EqAR")
F["sales"] = num("Sales"); F["fsales"] = num("FSales"); F["fnp"] = num("FNP"); F["divfy"] = num("FDivFY")
F["shares"] = np.where((F.eps.abs() > 0) & F.np_.notna(), F.np_ / F.eps, np.nan)
F["discd"] = pd.to_datetime(F.DiscDate, errors="coerce")
Fj = F.dropna(subset=["discd"])[["code4", "discd", "np_", "eqv", "eqar", "sales", "fsales", "fnp", "divfy", "shares"]].sort_values("discd")
Cc = pd.DataFrame({"_i": np.arange(n), "code4": C0.ticker.str[:4], "entry": C0.entry}).sort_values("entry")
M = pd.merge_asof(Cc, Fj, left_on="entry", right_on="discd", by="code4", allow_exact_matches=False).sort_values("_i")
MCAP = (E * M.shares.abs().to_numpy())
PBR = np.where(M.eqv.to_numpy() > 0, MCAP / M.eqv.to_numpy(), np.nan)
LOSS = (M.np_.to_numpy() < 0)
EQAR = M.eqar.to_numpy()
SALESG = np.where(M.sales.to_numpy() > 0, M.fsales.to_numpy() / M.sales.to_numpy() - 1, np.nan)
NPG = np.where(M.np_.to_numpy() > 0, M.fnp.to_numpy() / M.np_.to_numpy() - 1, np.nan)
DIVY = np.where(E > 0, M.divfy.to_numpy() / E * 100, np.nan)
SINCE = (C0.entry.to_numpy() - M.discd.to_numpy()).astype("timedelta64[D]").astype(float)
HASF = M.discd.notna().to_numpy()
print(f"  結合率: {HASF[BASE].mean()*100:.1f}%（BASE候補）/ 赤字比率 {LOSS[BASE & HASF].mean()*100:.1f}%")
for col, arr in (("mcap", MCAP), ("pbr", PBR), ("eqar", EQAR), ("salesg", SALESG), ("npg", NPG), ("divy", DIVY), ("since", SINCE)):
    R0[col] = arr[R0.i.to_numpy()]
R0["loss"] = np.where(HASF[R0.i.to_numpy()], LOSS[R0.i.to_numpy()], np.nan)
quint(R0, "mcap", "時価総額概算(円)"); quint(R0, "pbr", "PBR概算"); quint(R0, "eqar", "自己資本比率")
quint(R0, "salesg", "予想売上成長"); quint(R0, "npg", "予想純利益成長(黒字のみ)"); quint(R0, "divy", "予想配当利回り%")
quint(R0, "since", "直近開示からの日数")
grp(R0, "loss", "赤字(1.0)/黒字(0.0)")
print("\n  ▶ フィルタ・シム（欠損はフェイルオープン＝残す。本番と同じ流儀）"); print(HDR)
print(line("現行", st0))
fo = lambda cond: BASE & ~(HASF & ~cond)      # 財務ありで条件不成立の玉だけ落とす
for lab, cond in (("赤字除外", ~LOSS), ("赤字だけ", LOSS),
                  ("時価総額≥300億", MCAP >= 3e10), ("時価総額≥1000億", MCAP >= 1e11), ("時価総額≤1000億", MCAP <= 1e11),
                  ("PBR≤1", PBR <= 1), ("PBR≤2", PBR <= 2), ("PBR≥2", PBR >= 2),
                  ("自己資本≥30%", EQAR >= 0.3), ("自己資本≥50%", EQAR >= 0.5), ("自己資本≤30%", EQAR <= 0.3),
                  ("予想増益", NPG > 0), ("予想減益除外", ~(NPG < 0)), ("予想増収", SALESG > 0),
                  ("配当利回り≥2%", DIVY >= 2), ("配当利回り≥3%", DIVY >= 3),
                  ("開示から≤30日除外", ~(SINCE <= 30)), ("開示から≤10日除外", ~(SINCE <= 10))):
    sim(lab, fo(np.nan_to_num(cond.astype(float), nan=0) > 0))

# ══ B カレンダー ══
print("\n" + "=" * 122); print("B カレンダー（曜日 / 月初・月末3営業日 / SQ週）"); print("=" * 122)
WD = ENTRY_DT.dt.weekday.to_numpy()
ym = ENTRY_DT.dt.to_period("M")
pos_in_m = ENTRY_DT.groupby(ym).cumcount().to_numpy()
cnt_m = ENTRY_DT.groupby(ym).transform("size").to_numpy()
MPOS = np.where(pos_in_m < 3, "月初3日", np.where(cnt_m - pos_in_m <= 3, "月末3日", "月中"))
def is_sq_week(ts):
    d = ts.replace(day=1); first_fri = d + pd.Timedelta(days=(4 - d.weekday()) % 7)
    sq = first_fri + pd.Timedelta(days=7)
    return (sq - pd.Timedelta(days=sq.weekday())) <= ts <= sq + pd.Timedelta(days=4 - sq.weekday())
SQW = np.array([is_sq_week(t) for t in ENTRY_DT])
R0["wd"] = ["月火水木金"[w] for w in WD[R0.d.to_numpy()]]
R0["mpos"] = MPOS[R0.d.to_numpy()]; R0["sq"] = SQW[R0.d.to_numpy()]
grp(R0, "wd", "曜日"); grp(R0, "mpos", "月内位置"); grp(R0, "sq", "SQ週(True)")
print("\n  ▶ フィルタ・シム"); print(HDR); print(line("現行", st0))
dmask = lambda cond_d: BASE & cond_d[np.searchsorted(np.arange(len(ALLDAYS)), np.array([GDI[x] for x in C0.entry]))]
DIDX = np.array([GDI[x] for x in C0.entry])
for w, lab in enumerate("月火水木金"):
    sim(f"{lab}曜除外", BASE & (WD[DIDX] != w))
sim("月初3日除外", BASE & (MPOS[DIDX] != "月初3日")); sim("月末3日除外", BASE & (MPOS[DIDX] != "月末3日"))
sim("SQ週除外", BASE & ~SQW[DIDX])

# ══ C トレーリングストップ ══
print("\n" + "=" * 122); print("C トレーリングストップ（前日までの高値×(1-t%)を損切りに繰り上げ・当日内の順序は保守側）"); print("=" * 122)
def replay_trail(tp, stop, hold, rsith, trail, arm, be=False):
    valid = np.isfinite(E) & np.isfinite(CL[:, hold - 1])
    pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = E * (1 - stop / 100)
    tl = E * (1 + tp / 100) if tp is not None else np.full(n, np.inf)
    runmax = np.full(n, -np.inf)
    for k in range(hold):
        live = ~done
        if not live.any():
            break
        if k > 0:
            runmax = np.fmax(runmax, HI[:, k - 1])
            armed = runmax >= E * (1 + arm / 100)
            tsl = np.where(armed, (E if be else runmax * (1 - trail / 100)), -np.inf)
            slk = np.fmax(sl, tsl)
            op = OP[:, k]
            g = live & np.isfinite(op) & (op > 0) & ((op <= slk) | (op >= tl))
            pnl[g] = (op[g] - E[g]) / E[g] * 100; exo[g] = k; done |= g; live = ~done
        else:
            slk = sl
        s = live & (LO[:, k] <= slk); pnl[s] = (slk[s] - E[s]) / E[s] * 100; exo[s] = k; done |= s; live = ~done
        if tp is not None:
            t = live & (HI[:, k] >= tl); pnl[t] = tp; exo[t] = k; done |= t; live = ~done
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]) if rsith is not None else np.zeros(n, bool)
        r = live & (rc | (k == hold - 1))
        pnl[r] = (CL[r, k] - E[r]) / E[r] * 100; exo[r] = k; done |= r
    return pnl, exo
pchk, echk = replay_trail(5.0, 3.0, 3, 50, 99.0, 99.0)
assert abs(stats(run2(BASE, pchk, echk))["total"] - st0["total"]) < 2, "trail無効時に現行と不一致"
print(HDR); print(line("現行", st0))
for be, arm in ((True, 1.0), (True, 2.0), (True, 3.0)):
    p_, e_ = replay_trail(5.0, 3.0, 3, 50, 0.0, arm, be=True); sim(f"建値ストップ(+{arm}%到達後)", BASE, p_, e_)
for trail in (1.5, 2.0, 3.0, 4.0):
    for arm in (0.0, 1.0, 2.0, 3.0):
        p_, e_ = replay_trail(5.0, 3.0, 3, 50, trail, arm); sim(f"trail{trail}%/arm+{arm}%", BASE, p_, e_)
print("  " + "-" * 100 + "  TPなし・HOLD5（トレーリングで利を伸ばす型）")
for trail in (2.0, 3.0, 4.0):
    for arm in (1.0, 2.0):
        p_, e_ = replay_trail(None, 3.0, 5, 50, trail, arm); sim(f"TP∞/H5/trail{trail}%/arm+{arm}%", BASE, p_, e_)
        p_, e_ = replay_trail(None, 3.0, 5, None, trail, arm); sim(f"TP∞/H5/RSI∞/trail{trail}%/arm+{arm}%", BASE, p_, e_)

# ══ D 寄りギャップの下側 ══
print("\n" + "=" * 122); print("D 寄りギャップの下側（大きく安く寄った玉を見送る）"); print("=" * 122)
R0["gap"] = GAP[R0.i.to_numpy()]
quint(R0, "gap", "寄りギャップ%")
print(HDR); print(line("現行", st0))
for g in (-0.5, -1.0, -1.5, -2.0, -3.0):
    sim(f"gap<{g}%除外", BASE & ~(GAP < g))
sim("gap<0%除外(上寄りだけ)", BASE & ~(GAP < 0)); sim("gap>0%除外(下寄りだけ)", BASE & ~(GAP > 0))

# ══ E 前回結果 ══
print("\n" + "=" * 122); print("E 同一銘柄の前回結果（建てた玉の直前トレード）"); print("=" * 122)
R0["prevc"] = np.where(R0.prev.isna(), "初回", np.where(R0.prev < 0, "前回負け", "前回勝ち"))
grp(R0, "prevc", "前回結果別")
print(HDR); print(line("現行", st0))
print(line("前回負け銘柄は見送り", stats(run2(BASE, pnl0, exo0, skip_after_loss=True))))

# ══ F 前日の米国株 ══
print("\n" + "=" * 122); print("F 前日の米国株（SP500・エントリー日より前の最新終値の騰落）"); print("=" * 122)
try:
    _idx, spy = pickle.load(open("_overnight_sector_cache.pkl", "rb"))
    spy = pd.Series(spy).sort_index(); spy.index = pd.to_datetime(spy.index)
    ret = (spy.pct_change() * 100).dropna(); ret5 = (spy.pct_change(5) * 100).dropna()
    def prev_val(s):
        idx = np.searchsorted(s.index.values, ENTRY_DT.values, side="left") - 1
        return np.where(idx >= 0, s.values[np.clip(idx, 0, len(s) - 1)], np.nan)
    US1 = prev_val(ret)[DIDX]; US5 = prev_val(ret5)[DIDX]
    R0["us1"] = US1[R0.i.to_numpy()]; R0["us5"] = US5[R0.i.to_numpy()]
    print(f"  SP500 {spy.index.min().date()}〜{spy.index.max().date()} 結合率{np.isfinite(US1[BASE]).mean()*100:.1f}%")
    quint(R0, "us1", "SP500前日騰落%"); quint(R0, "us5", "SP500 5日騰落%")
    print(HDR); print(line("現行", st0))
    for th in (-2.0, -1.0, -0.5):
        sim(f"SP500前日<{th}%除外", BASE & ~(US1 < th))
    for th in (0.5, 1.0):
        sim(f"SP500前日>{th}%除外", BASE & ~(US1 > th))
    sim("SP500前日マイナス除外", BASE & ~(US1 < 0)); sim("SP500前日プラス除外", BASE & ~(US1 > 0))
except Exception as ex:
    print("  SP500データなし:", ex)

# ══ G 所属業種指数の地合い ══
print("\n" + "=" * 122); print("G 所属業種の33業種指数（25MA上下・5日騰落・エントリー前日まで）"); print("=" * 122)
S33MAP = {
    "水産・農林業": "0050", "鉱業": "1050", "建設業": "2050", "食料品": "3050",
    "繊維製品": "3100", "パルプ・紙": "3150", "化学": "3200", "医薬品": "3250",
    "石油･石炭製品": "3300", "ゴム製品": "3350", "ガラス･土石製品": "3400", "鉄鋼": "3450",
    "非鉄金属": "3500", "金属製品": "3550", "機械": "3600", "電気機器": "3650",
    "輸送用機器": "3700", "精密機器": "3750", "その他製品": "3800", "電気･ガス業": "4050",
    "陸運業": "5050", "海運業": "5100", "空運業": "5150", "倉庫･運輸関連業": "5200",
    "情報･通信業": "5250", "卸売業": "6050", "小売業": "6100", "銀行業": "7050",
    "証券･商品先物取引業": "7100", "保険業": "7150", "その他金融業": "7200",
    "不動産業": "8050", "サービス業": "9050",
}
I = pd.read_pickle("_indices_10y.pkl"); I["C"] = pd.to_numeric(I.C, errors="coerce"); I["Date"] = pd.to_datetime(I.Date)
feat = []
for code, g in I.groupby("Code"):
    s = g.sort_values("Date").set_index("Date").C.dropna()
    feat.append(pd.DataFrame({"Date": s.index, "s33": code, "above": (s > s.rolling(25).mean()).astype(float).values,
                              "chg5": (s.pct_change(5) * 100).values, "chg1": (s.pct_change() * 100).values}))
FT = pd.concat(feat).sort_values("Date")
Cs = pd.DataFrame({"_i": np.arange(n), "s33": C0.ticker.map(SECMAP).map(S33MAP), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Cs, FT, left_on="entry", right_on="Date", by="s33", allow_exact_matches=False).sort_values("_i")
ABOVE = J.above.to_numpy(); CHG5 = J.chg5.to_numpy(); CHG1 = J.chg1.to_numpy()
print(f"  結合率 {np.isfinite(CHG5[BASE]).mean()*100:.1f}%")
R0["s_above"] = ABOVE[R0.i.to_numpy()]; R0["s_chg5"] = CHG5[R0.i.to_numpy()]; R0["s_chg1"] = CHG1[R0.i.to_numpy()]
grp(R0.dropna(subset=["s_above"]), "s_above", "業種指数25MA上(1.0)/下(0.0)")
quint(R0, "s_chg5", "業種指数5日騰落%"); quint(R0, "s_chg1", "業種指数前日騰落%")
print(HDR); print(line("現行", st0))
sim("業種25MA下だけ", BASE & ~(ABOVE == 1)); sim("業種25MA上だけ", BASE & ~(ABOVE == 0))
for th in (-3.0, -2.0):
    sim(f"業種5日<{th}%除外", BASE & ~(CHG5 < th))
for th in (2.0, 3.0):
    sim(f"業種5日>{th}%除外", BASE & ~(CHG5 > th))
sim("業種前日<-1%除外", BASE & ~(CHG1 < -1)); sim("業種前日>+1%除外", BASE & ~(CHG1 > 1))

print("\n[done]")

# -*- coding: utf-8 -*-
"""_bt_gold_levels_0927.py — 金(XM GOLD.)の新しい軸「価格の水準」(G1)（2026-09-27）
本人「あしたの朝までにゴールドの新しい軸さがして・未検証も再検証でもいい・必ずさがして」。
仮説(Osler 2003/2005): 利確注文はキリ番に集まる→手前で反転 / 損切り注文はキリ番のすぐ外→抜けると加速。
  前日/前週の高安・当日始値・アジア時間の高安も同じ「注文が溜まる水準」。
事前登録(全部このファイルに固定):
  水準11種: RN5/RN10/RN25/RN50/RN100(キリ番) PD(前日高安) PW(前週高安) OD(サーバー01:00始値) OL(ロンドン08:00始値) ON(NY08:20始値) AS(アジア=ロンドン00-07時の高安)
  方向2: 下から(上値の水準=売りの逆張り/買いの順張り) / 上から(下値の水準=買いの逆張り/売りの順張り)
  入り方5: RL=水準に指値で逆張り / RM=初タッチの次の足の寄りで逆張り / C0.5,C1,C2=水準±$0.5/1/2 の逆指値で順張り(初タッチから60分以内)
  保有5: 5/15/30/60/120分(手仕舞いは時間で・損切りなし)  → 11×2×5×5 = 550セル
  初タッチ: キリ番/PD/OD/OL/ON=その日(サーバー日)の初タッチ・PW=その週の初タッチ・AS=ロンドン07-16時の初タッチ
  ロールオーバー帯(サーバー23:00〜翌02:00)に建て/手仕舞いがかかる玉は除外(人工物)
対照: キリ番は +0.37S/+0.63S ずらした偽の格子、PD=7営業日前の高安、PW=3週前の高安、始値系=+97分の価格、AS=前日ロンドン16-23時の高安。
コスト: 実勢$0.593/oz往復(合格判定)・参考KIWAMI想定$0.30。t は日ごとにまとめた(クラスタ)t。
ノイズ床: 日ごとの符号反転(全セル同じ符号・コスト抜き)300回の「最大t」の95%点。
実行: python -X utf8 _bt_gold_levels_0927.py > _bt_gold_levels_0927.log
"""
import pandas as pd, numpy as np, time, sys, pickle

t0 = time.time()
COST = 0.593
HOLDS = (5, 15, 30, 60, 120)
TRADES = ("RL", "RM", "C0.5", "C1", "C2")
ROLL_LO, ROLL_HI = 2 * 60, 23 * 60          # 建て/手仕舞いはサーバー02:00〜22:59 の中だけ
SRC = sys.argv[1] if len(sys.argv) > 1 else "xm"

# ---------------- データ ----------------
if SRC == "xm":
    d = pd.read_pickle("_xm_m1_GOLD._0926.pkl")
    d = d[d.index >= "2015-01-01"]
    idx = d.index                                               # サーバー時刻(naive)=NY+7h
    SP = d["spread"].to_numpy(np.float32) * 0.01
else:                                                           # Dukascopy(UTC)→サーバー時刻に変換(NY+7h)
    d = pd.read_pickle("_fx_xauusd_m1.pkl")
    if not isinstance(d.index, pd.DatetimeIndex): d = d.set_index(d.columns[0])
    d = d.sort_index(); d = d[d.index >= "2016-01-01"]
    utc = d.index.tz_localize("UTC") if d.index.tz is None else d.index.tz_convert("UTC")
    idx = (utc.tz_convert("America/New_York").tz_localize(None) + pd.Timedelta(hours=7))
    d.index = idx
    SP = np.full(len(d), 0.30, np.float32)
    d.columns = [c.lower() for c in d.columns]
O = d["open"].to_numpy(np.float64); H = d["high"].to_numpy(np.float64); L = d["low"].to_numpy(np.float64); C = d["close"].to_numpy(np.float64)
ny = idx - pd.Timedelta(hours=7)
lon = ny.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").tz_convert("Europe/London").tz_localize(None)
MIN = (idx.hour * 60 + idx.minute).to_numpy()
LMIN = np.where(pd.isna(lon), -1, lon.hour * 60 + lon.minute)
NMIN = (ny.hour * 60 + ny.minute).to_numpy()
DAY = idx.normalize()
days, dstart = np.unique(DAY.values, return_index=True)
dend = np.append(dstart[1:], len(d))
days = pd.DatetimeIndex(days)
WEEK = (days - pd.to_timedelta(days.dayofweek, unit="D")).values          # 週の月曜
print(f"[{SRC}] {len(d):,}本 {idx.min()}〜{idx.max()} 日数{len(days)}  {time.time()-t0:.0f}s", flush=True)

# 前日/前週の高安(建てに使える時間帯 02:00-22:59 の足だけで計算＝ロールオーバーの跳ねを除く)
dayH = np.full(len(days), np.nan); dayL = np.full(len(days), np.nan)
for k, (a, b) in enumerate(zip(dstart, dend)):
    m = (MIN[a:b] >= ROLL_LO) & (MIN[a:b] < ROLL_HI)
    if m.sum() > 300: dayH[k] = H[a:b][m].max(); dayL[k] = L[a:b][m].min()
wk = pd.Series(np.arange(len(days)), index=days)
wkH = pd.Series(dayH, index=days).groupby(WEEK).max(); wkL = pd.Series(dayL, index=days).groupby(WEEK).min()
wlist = list(wkH.index)


def first_touch_events(a, b, levels, fam, start_min=ROLL_LO, end_min=ROLL_HI, clock=None, natural=None, need_away=None, t0_idx=None):
    """[a,b) の足で各水準の初タッチを探す。levels: [(price, tag)]。natural: 'H'なら下からだけ・'L'なら上からだけ。
    need_away: (始値の水準) T0から0.15%以上離れた後の初タッチだけ。戻り値: イベントのリスト"""
    out = []
    mm = MIN[a:b] if clock is None else clock[a:b]
    hh = H[a:b]; ll = L[a:b]; cc = C[a:b]
    for (lv, tag) in levels:
        if not np.isfinite(lv): continue
        touch = (ll <= lv) & (hh >= lv)
        if t0_idx is not None:                                   # 始値系: T0+30分以降・離れてから
            touch[: t0_idx + 30] = False
            away = np.abs(cc - lv) >= 0.0015 * lv
            away[: t0_idx] = False
            first_away = np.argmax(away) if away.any() else -1
            if first_away < 0: continue
            touch[: first_away + 1] = False
        pos = np.flatnonzero(touch)
        if len(pos) == 0: continue
        i = pos[0]
        if i == 0: continue
        if not (start_min <= mm[i] < end_min): continue           # 初タッチが時間帯の外(ロールオーバー等)なら無し
        prev = cc[i - 1]
        if prev < lv: dirn = "下から"
        elif prev > lv: dirn = "上から"
        else: continue
        if natural == "H" and dirn != "下から": continue
        if natural == "L" and dirn != "上から": continue
        out.append((a + i, lv, dirn, fam, tag))
    return out


def trades_from_event(ev):
    """イベント→入り方5種の(建て足, 建値, 売買方向)。売買: +1買い/-1売り"""
    i, lv, dirn, fam, tag = ev
    up = dirn == "下から"                                        # 下から来た=上値の水準
    res = {}
    # RL: 指値で逆張り(下から来たら水準で売り・上から来たら水準で買い)
    if up:
        if H[i] >= lv: res["RL"] = (i, max(lv, O[i]) if O[i] > lv else lv, -1)
    else:
        if L[i] <= lv - SP[i]: res["RL"] = (i, min(lv, O[i]) if O[i] < lv else lv, +1)
    # RM: 次の足の寄りで逆張り
    if i + 1 < len(O): res["RM"] = (i + 1, O[i + 1], -1 if up else +1)
    # C: 逆指値で順張り(初タッチから60分以内)
    for dl in (0.5, 1.0, 2.0):
        trg = lv + dl if up else lv - dl
        j_end = min(len(O), i + 61)
        if up:
            hit = np.flatnonzero(H[i:j_end] >= trg)
        else:
            hit = np.flatnonzero(L[i:j_end] <= trg)
        if len(hit) == 0: continue
        j = i + hit[0]
        if j == i: px = trg
        else: px = max(trg, O[j]) if up else min(trg, O[j])
        res[f"C{dl:g}"] = (j, px, +1 if up else -1)
    return res


def collect(level_fn, label):
    """level_fn(k, a, b) → その日のイベントのリスト。全日を回して、入り方×保有の損益を行にする"""
    rows = []
    for k in range(len(days)):
        a, b = dstart[k], dend[k]
        evs = level_fn(k, a, b)
        if not evs: continue
        mpos = np.full(1440, -1); mpos[MIN[a:b]] = np.arange(a, b)
        for ev in evs:
            for tr, (j, px, side) in trades_from_event(ev).items():
                m0 = MIN[j]
                for h in HOLDS:
                    mx = m0 + h
                    if mx >= ROLL_HI or m0 < ROLL_LO: continue
                    e = -1
                    for q in range(3):
                        if mx + q < 1440 and mpos[mx + q] >= 0: e = mpos[mx + q]; break
                    if e < 0: continue
                    g = side * (O[e] - px)
                    rows.append((k, ev[3], ev[4], ev[2], tr, h, side, g))
    df = pd.DataFrame(rows, columns=["k", "fam", "tag", "dir", "trade", "hold", "side", "gross"])
    print(f"  {label}: {len(df):,}行  {time.time()-t0:.0f}s", flush=True)
    return df


# ---------------- 水準の定義 ----------------
def rn_fn(S, off=0.0):
    def f(k, a, b):
        lo = L[a:b].min(); hi = H[a:b].max()
        n0 = int(np.floor((lo - off) / S)); n1 = int(np.ceil((hi - off) / S))
        levels = [(n * S + off, "") for n in range(n0, n1 + 1)]
        return first_touch_events(a, b, levels, f"RN{S:g}" if off == 0 else f"RN{S:g}偽{off/S:.2f}")
    return f


def pd_fn(lag=1, fam="PD"):
    def f(k, a, b):
        if k - lag < 0: return []
        hv, lvv = dayH[k - lag], dayL[k - lag]
        return (first_touch_events(a, b, [(hv, "H")], fam, natural="H") + first_touch_events(a, b, [(lvv, "L")], fam, natural="L"))
    return f


week_first = {}
def pw_fn(lagw=1, fam="PW"):
    touched = {}
    def f(k, a, b):
        w = WEEK[k]; wi = wlist.index(pd.Timestamp(w)) if pd.Timestamp(w) in wkH.index else -1
        if wi - lagw < 0: return []
        hv, lvv = wkH.iloc[wi - lagw], wkL.iloc[wi - lagw]
        out = []
        for (lv, nat) in ((hv, "H"), (lvv, "L")):
            key = (w, nat)
            if touched.get(key): continue
            # その週の中で既に触れていれば(前の日で)イベント無し
            ev = first_touch_events(a, b, [(lv, nat)], fam, natural=None)
            # 今日の足で触れたか(方向問わず)を記録
            if (L[a:b] <= lv).any() and (H[a:b] >= lv).any() and ((L[a:b] <= lv) & (H[a:b] >= lv)).any():
                touched[key] = True
            ev = [e for e in ev if (nat == "H" and e[2] == "下から") or (nat == "L" and e[2] == "上から")]
            out += ev
        return out
    return f


def open_fn(which, placebo=False):
    fam = {"OD": "OD", "OL": "OL", "ON": "ON"}[which] + ("偽" if placebo else "")
    def f(k, a, b):
        if which == "OD":
            mm = MIN[a:b]; tgt = 60
        elif which == "OL":
            mm = LMIN[a:b]; tgt = 8 * 60
        else:
            mm = NMIN[a:b]; tgt = 8 * 60 + 20
        if placebo: tgt += 97
        pos = np.flatnonzero(mm == tgt)
        if len(pos) == 0: return []
        t0i = pos[0]; lv = O[a + t0i]
        return first_touch_events(a, b, [(lv, "")], fam, t0_idx=t0i)
    return f


def as_fn(placebo=False):
    fam = "AS偽" if placebo else "AS"
    def f(k, a, b):
        lm = LMIN[a:b]
        if placebo:
            if k == 0: return []
            pa, pb = dstart[k - 1], dend[k - 1]
            w = (LMIN[pa:pb] >= 16 * 60) & (LMIN[pa:pb] < 23 * 60)
            if w.sum() < 200: return []
            hv, lvv = H[pa:pb][w].max(), L[pa:pb][w].min()
        else:
            w = (lm >= 0) & (lm < 7 * 60)
            if w.sum() < 200: return []
            hv, lvv = H[a:b][w].max(), L[a:b][w].min()
        # イベントはロンドン07:00-16:00の初タッチ(その窓の中での初タッチ)
        win = np.flatnonzero((lm >= 7 * 60) & (lm < 16 * 60))
        if len(win) == 0: return []
        a2, b2 = a + win[0], a + win[-1] + 1
        return (first_touch_events(a2, b2, [(hv, "H")], fam, natural="H", start_min=0, end_min=1440, clock=LMIN)
                + first_touch_events(a2, b2, [(lvv, "L")], fam, natural="L", start_min=0, end_min=1440, clock=LMIN))
    return f


if __name__ == "__main__":
    parts = []
    for S in (5, 10, 25, 50, 100):
        parts.append(collect(rn_fn(S), f"RN{S}"))
        for fr in (0.37, 0.63):
            parts.append(collect(rn_fn(S, off=fr * S), f"RN{S}偽{fr}"))
    parts.append(collect(pd_fn(1, "PD"), "PD")); parts.append(collect(pd_fn(7, "PD偽"), "PD偽(7日前)"))
    parts.append(collect(pw_fn(1, "PW"), "PW")); parts.append(collect(pw_fn(3, "PW偽"), "PW偽(3週前)"))
    for w in ("OD", "OL", "ON"):
        parts.append(collect(open_fn(w), w)); parts.append(collect(open_fn(w, True), w + "偽"))
    parts.append(collect(as_fn(), "AS")); parts.append(collect(as_fn(True), "AS偽"))
    E = pd.concat(parts, ignore_index=True)
    E["day"] = days[E.k.values]
    E.to_pickle(f"_bt_gold_levels_0927_events_{SRC}.pkl")
    print(f"イベント行 {len(E):,}  保存 {time.time()-t0:.0f}s", flush=True)

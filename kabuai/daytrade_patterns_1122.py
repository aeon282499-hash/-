# -*- coding: utf-8 -*-
"""daytrade_patterns_1122.py — デイトレの代表的な型を一度に並べ、「後付けでない勝ち」があるかを探す（2026-11-22）。

本人「色々なパターン考えてデイトレで勝てるようにして」。
方針: 型の数を絞り（10系統・各2〜4通り）、全部を同じ規約・同じコストで測る。勝ちの判定は次の4つを全部通ったものだけ。
  ① 選定期間（最初の2/3の日）と確認期間（残り1/3）の両方で PF>1.10
  ② 確認期間で 30回以上
  ③ 確認期間で「同じ銘柄日・同じ時間帯・同じ向き・同じ出口でランダムな時刻に入った場合」より良い（z>2）
  ④ 全期間で 利益上位3日を除いても合計プラス
  調べる通り数が多いほど偶然の★が増える（20通りに1つは偶然 p<0.05）ので、★でも「次の60日で再確認」が前提。

規約（bb_engine.py と同じ）:
  5分足（9:00 の気配足は捨てる）・シグナルは足の確定後・建玉は次の足の始値＋不利1tick・出口も不利1tick・往復0.10%。
  1銘柄1日1回（型ごと）。日足の条件は前日までの情報＋当日の寄値だけ（先読みなし）。決算・業績修正の開示当日と翌営業日は除外。
母集団: 前日の売買代金3億円以上・株価300〜20,000円（J-Quants 日足で絞り、yfinance で5分足を取る）。

型（売り＝空売り。空売りできるか＝在庫・売り禁は見ていない＝売りの型は楽観側）:
  A オープニングレンジ・ブレイク（最初の15/30分の高値抜けで買い・安値割れで売り。損切りはレンジの反対側・大引け）
  B ギャップ・フェード（GU+3%以上を9:30/12:30に売り・大引け）／ギャップダウンの買い（GD−3%以下を9:30/12:30に買い）
  C ギャップ・アンド・ゴー（GU+3%以上で最初の15分が陽線→9:20に買い・大引け）
  D 伸び切り売り（10:00以降に寄値+5%/+7%を超えた最初の足→売り・40本移動平均まで戻るか大引け）
  E VWAP回帰（10:00以降に VWAP から±2%離れた最初の足→VWAP方向に入る・VWAPに戻るか大引け）
  F 朝の投げの戻り（10:00時点で寄値から−3%以下→買い・大引け）／朝の吹きの押し（+3%以上→売り）
  G 引けにかけての勢い（14:30時点で前日比+3%以上→買い／−3%以下→売り・大引け）
  H 前日急騰の翌日（前日+8%以上→寄りで売り・大引け）／前日急落（−8%以下→寄りで買い）
  I 寄り後30分の強弱（9:30時点の寄りからの騰落が全体の上位10%→買い・下位10%→売り・大引け）
  J 後場寄りの反転（前場で+3%以上→12:30に売り／−3%以下→12:30に買い・大引け）
実行: cd kabuai && python bb_forward_1122.py と同じ環境（JQUANTS_API_KEY）で  python daytrade_patterns_1122.py
"""
from __future__ import annotations

import sys
import time

import numpy as np
import pandas as pd

from bb_forward_1122 import is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

COST_RT = 0.10
N_RAND = 20
SEL_FRAC = 2 / 3


def hm(h, m):
    return h * 60 + m


# ───────────── 1銘柄1日の足 ─────────────
class Day:
    __slots__ = ("code", "date", "t", "o", "h", "l", "c", "v", "vwap", "ma40", "pc", "gu", "pchg", "is5", "n")

    def __init__(self, code, date, t, o, h, l, c, v, ma40, pc, ppc, is5):
        self.code, self.date, self.t, self.o, self.h, self.l, self.c, self.v = code, date, t, o, h, l, c, v
        pv = np.cumsum(c * v); vv = np.cumsum(v)
        self.vwap = np.where(vv > 0, pv / np.maximum(vv, 1), c)
        self.ma40, self.pc, self.is5, self.n = ma40, pc, is5, len(c)
        self.gu = o[0] / pc - 1
        self.pchg = pc / ppc - 1 if ppc and ppc > 0 else np.nan

    def first(self, cond_fn, lo, hi):
        for k in range(self.n - 1):
            if lo <= self.t[k] <= hi and cond_fn(k):
                return k
        return None

    def at(self, tmin):
        """その時刻に始まる足の index（無ければ直後の足）。"""
        for k in range(self.n):
            if self.t[k] >= tmin:
                return k
        return None


def simulate(d: Day, e: int, side: int, exit_kind: str, stop: float | None = None, entry_at_open_of: int | None = None) -> float | None:
    """e=シグナル足（確定後）→ e+1 の始値で建てる。entry_at_open_of を指定するとその足の始値で建てる（時刻固定の型）。
    exit_kind: close / ma / vwap。stop: 価格（足の高安で判定・窓で飛び越えたら始値）。戻り: コスト込みリターン%。"""
    j = e + 1 if entry_at_open_of is None else entry_at_open_of
    if j is None or j >= d.n:
        return None
    ep = d.o[j] + side * tick_of(d.o[j], d.is5)
    px = d.c[-1]
    state = np.sign(d.c[e] - (d.ma40[e] if exit_kind == "ma" else d.vwap[e])) if e is not None and exit_kind in ("ma", "vwap") else 0
    for k in range(j, d.n):
        if stop is not None:
            if k > j and (d.o[k] - stop) * side <= 0:
                px = d.o[k]; break
            if (d.l[k] <= stop) if side > 0 else (d.h[k] >= stop):
                px = stop; break
        if exit_kind in ("ma", "vwap"):
            ref = d.ma40[k] if exit_kind == "ma" else d.vwap[k]
            if np.isfinite(ref) and (d.c[k] - ref) * state <= 0:
                px = d.o[k + 1] if k + 1 < d.n else d.c[-1]; break
    xp = px - side * tick_of(px, d.is5)
    return side * (xp - ep) / ep * 100 - COST_RT


# ───────────── 型の定義: (名前, 関数) 関数は Day → [(シグナル足 e, side, exit, stop, 固定足, ランダム用の時間帯)] ─────────────
def pat_orb(mins):
    def f(d: Day):
        end = hm(9, 0) + mins
        rng_ix = [k for k in range(d.n) if d.t[k] < end]
        if not rng_ix:
            return []
        hi, lo = d.h[rng_ix].max(), d.l[rng_ix].min()
        out = []
        e = d.first(lambda k: d.c[k] > hi, end, hm(11, 0))
        if e is not None:
            out.append((e, 1, "close", lo, None, (end, hm(11, 0))))
        e = d.first(lambda k: d.c[k] < lo, end, hm(11, 0))
        if e is not None:
            out.append((e, -1, "close", hi, None, (end, hm(11, 0))))
        return out
    return f


def pat_fixed(cond, side, tmin):
    """条件を満たす銘柄日に、時刻 tmin の足の始値で入って大引け。"""
    def f(d: Day):
        if not cond(d):
            return []
        j = d.at(tmin)
        if j is None:
            return []
        return [(j - 1 if j >= 1 else None, side, "close", None, j, (tmin, tmin))]
    return f


def pat_gapgo(d: Day):
    if not (d.gu >= 0.03):
        return []
    j = d.at(hm(9, 20))
    if j is None or j < 1:
        return []
    first15 = [k for k in range(d.n) if d.t[k] < hm(9, 20)]
    if not first15 or d.c[first15[-1]] <= d.o[0]:
        return []
    return [(j - 1, 1, "close", None, j, (hm(9, 20), hm(9, 20)))]


def pat_extension(th):
    def f(d: Day):
        e = d.first(lambda k: d.c[k] >= d.o[0] * (1 + th), hm(10, 0), hm(14, 25))
        return [(e, -1, "ma", None, None, (hm(10, 0), hm(14, 25)))] if e is not None else []
    return f


def pat_vwap(th):
    def f(d: Day):
        out = []
        e = d.first(lambda k: d.c[k] <= d.vwap[k] * (1 - th), hm(10, 0), hm(14, 25))
        if e is not None:
            out.append((e, 1, "vwap", None, None, (hm(10, 0), hm(14, 25))))
        e = d.first(lambda k: d.c[k] >= d.vwap[k] * (1 + th), hm(10, 0), hm(14, 25))
        if e is not None:
            out.append((e, -1, "vwap", None, None, (hm(10, 0), hm(14, 25))))
        return out
    return f


def pat_morning(side):
    def f(d: Day):
        j = d.at(hm(10, 0))
        if j is None or j < 1:
            return []
        r = d.c[j - 1] / d.o[0] - 1
        if (side > 0 and r <= -0.03) or (side < 0 and r >= 0.03):
            return [(j - 1, side, "close", None, j, (hm(10, 0), hm(10, 0)))]
        return []
    return f


def pat_lasthour(side):
    def f(d: Day):
        j = d.at(hm(14, 30))
        if j is None or j < 1:
            return []
        r = d.c[j - 1] / d.pc - 1
        if (side > 0 and r >= 0.03) or (side < 0 and r <= -0.03):
            return [(j - 1, side, "close", None, j, (hm(14, 30), hm(14, 30)))]
        return []
    return f


def pat_pm_reversal(side):
    def f(d: Day):
        j = d.at(hm(12, 30))
        if j is None or j < 1:
            return []
        r = d.c[j - 1] / d.pc - 1
        if (side < 0 and r >= 0.03) or (side > 0 and r <= -0.03):
            return [(j - 1, side, "close", None, j, (hm(12, 30), hm(12, 30)))]
        return []
    return f


PATTERNS = {
    "A1 ORB15分（上抜け買い/下抜け売り）": pat_orb(15),
    "A2 ORB30分（上抜け買い/下抜け売り）": pat_orb(30),
    "B1 GU+3% を9:30に売り": pat_fixed(lambda d: d.gu >= 0.03, -1, hm(9, 30)),
    "B2 GU+3% を12:30に売り": pat_fixed(lambda d: d.gu >= 0.03, -1, hm(12, 30)),
    "B3 GD−3% を9:30に買い": pat_fixed(lambda d: d.gu <= -0.03, 1, hm(9, 30)),
    "B4 GD−3% を12:30に買い": pat_fixed(lambda d: d.gu <= -0.03, 1, hm(12, 30)),
    "C1 GU+3%・最初15分陽線→9:20買い": pat_gapgo,
    "D1 10時以降 寄値+5%超え→売り(MA40回帰)": pat_extension(0.05),
    "D2 10時以降 寄値+7%超え→売り(MA40回帰)": pat_extension(0.07),
    "E1 VWAP±2%乖離→VWAPへ回帰": pat_vwap(0.02),
    "E2 VWAP±3%乖離→VWAPへ回帰": pat_vwap(0.03),
    "F1 10時に寄値−3%以下→買い": pat_morning(1),
    "F2 10時に寄値+3%以上→売り": pat_morning(-1),
    "G1 14:30に前日比+3%以上→買い": pat_lasthour(1),
    "G2 14:30に前日比−3%以下→売り": pat_lasthour(-1),
    "H1 前日+8%以上→寄りで売り": pat_fixed(lambda d: d.pchg >= 0.08, -1, hm(9, 5)),
    "H2 前日−8%以下→寄りで買い": pat_fixed(lambda d: d.pchg <= -0.08, 1, hm(9, 5)),
    "J1 前場+3%以上→12:30に売り": pat_pm_reversal(-1),
    "J2 前場−3%以下→12:30に買い": pat_pm_reversal(1),
}


def build_days(M: dict, cand: set, earn: set) -> list[Day]:
    days = []
    for code, s in M.items():
        s = s[(s.c > 0) & (s.o > 0)]
        tm = s.index.hour * 60 + s.index.minute
        s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
        if len(s) < 300:
            continue
        c_all = s.c.to_numpy(float)
        ma = pd.Series(c_all).rolling(40).mean().to_numpy()
        is5 = is500_of(c_all)
        full = float(pd.Series(s.index.normalize()).value_counts().median())
        dn = s.index.normalize(); ud = dn.unique()
        t_all = (s.index.hour * 60 + s.index.minute).to_numpy()
        prev, pprev_close = None, None
        for dd in ud:
            ix = np.flatnonzero(dn == dd); st, en = ix[0], ix[-1] + 1
            ds = dd.strftime("%Y-%m-%d")
            if prev is not None:
                pst, pen = prev
                pc = c_all[pen - 1]
                if (code, ds) in cand and (code, ds) not in earn and (pen - pst) >= 0.9 * full and (en - st) >= 0.9 * full:
                    days.append(Day(code, ds, t_all[st:en], s.o.to_numpy(float)[st:en], s.h.to_numpy(float)[st:en],
                                    s.l.to_numpy(float)[st:en], c_all[st:en], s.v.to_numpy(float)[st:en], ma[st:en], pc, pprev_close, is5))
                pprev_close = pc
            prev = (st, en)
    return days


def stat(r: np.ndarray) -> tuple:
    if len(r) == 0:
        return (0, np.nan, np.nan, np.nan)
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    return (len(r), (r > 0).mean() * 100, gp / gl if gl > 0 else np.inf, r.mean())


def main() -> None:
    token = _jquants_key()
    dl = _recent_trading_days(62)
    D = jq_daily(token, dl).sort_values(["code", "date"])
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1)
    C = D[(D.pva >= 3e8) & (D.pc >= 300) & (D.pc <= 20000)]
    cand = set(zip(C.code, C.date.astype(str).str[:10]))
    codes = set(C.code)
    print(f"[dt] 母集団（前日代金3億+・300〜2万円）{len(C):,}銘柄日・{len(codes):,}銘柄", flush=True)
    earn = jq_fins(token, dl)
    dd = sorted(set(D.date.astype(str).str[:10])); nxt = {dd[i]: dd[i + 1] for i in range(len(dd) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    M = yf_5m(codes)
    days = build_days(M, cand, earn)
    alld = sorted({d.date for d in days})
    cut = alld[int(len(alld) * SEL_FRAC)]
    print(f"[dt] 銘柄日 {len(days):,}（{alld[0]}〜{alld[-1]}・{len(alld)}日）／ 選定期間 〜{alld[int(len(alld)*SEL_FRAC)-1]}・確認期間 {cut}〜", flush=True)

    # 型 I（横断の強弱）は全銘柄日がそろってから
    by_date: dict = {}
    for d in days:
        j = d.at(hm(9, 30))
        if j and j >= 1:
            by_date.setdefault(d.date, []).append((d.c[j - 1] / d.o[0] - 1, d, j))
    I_trades = []
    for ds, lst in by_date.items():
        if len(lst) < 50:
            continue
        r = np.array([x[0] for x in lst]); hi, lo = np.quantile(r, 0.9), np.quantile(r, 0.1)
        for v, d, j in lst:
            if v >= hi:
                I_trades.append((d, (j - 1, 1, "close", None, j, (hm(9, 30), hm(9, 30)))))
            elif v <= lo:
                I_trades.append((d, (j - 1, -1, "close", None, j, (hm(9, 30), hm(9, 30)))))

    rng = np.random.default_rng(0)
    rows = []
    for name, fn in list(PATTERNS.items()) + [("I1 9:30の強弱 上位10%買い/下位10%売り", None)]:
        recs = []
        it = ((d, sig) for d in days for sig in fn(d)) if fn else iter(I_trades)
        for d, (e, side, ex, stop, fixed, win) in it:
            r = simulate(d, e, side, ex, stop, fixed)
            if r is None:
                continue
            # ランダム: 同じ銘柄日・同じ時間帯（固定時刻の型は前場/後場の同じ側の任意の足）・同じ向き・同じ出口
            lo_, hi_ = win
            if lo_ == hi_:
                lo_, hi_ = (hm(9, 5), hm(11, 20)) if lo_ < hm(12, 0) else (hm(12, 30), hm(15, 0))
            pool = [k for k in range(d.n - 1) if lo_ <= d.t[k] <= hi_] if d.date >= cut else []   # ランダム比較は確認期間だけ
            rr = [simulate(d, int(k), side, ex, stop, None) for k in rng.choice(pool, size=N_RAND)] if pool else []
            recs.append((d.date, d.code, side, r, np.array([x if x is not None else np.nan for x in rr])))
        if not recs:
            rows.append({"型": name, "n": 0}); continue
        dates = np.array([x[0] for x in recs]); ret = np.array([x[3] for x in recs])
        sel, cf = dates < cut, dates >= cut
        rnd = np.array([x[4] if len(x[4]) == N_RAND else np.full(N_RAND, np.nan) for x in recs])
        rnd_cf = np.nanmean(rnd[cf], axis=0) if cf.any() else np.full(N_RAND, np.nan)
        z = (ret[cf].mean() - np.nanmean(rnd_cf)) / (np.nanstd(rnd_cf) + 1e-9) if cf.any() else np.nan
        daysum = pd.Series(ret).groupby(dates).sum().sort_values(ascending=False)
        ex3 = ret[~np.isin(dates, daysum.index[:3])].sum()
        s_all, s_sel, s_cf = stat(ret), stat(ret[sel]), stat(ret[cf])
        star = (s_sel[2] > 1.10 and s_cf[2] > 1.10 and s_cf[0] >= 30 and z > 2 and ex3 > 0)
        nb = sum(1 for x in recs if x[2] > 0); ns = len(recs) - nb
        rows.append({"型": name, "★": "★" if star else "", "n": s_all[0], "買/売": f"{nb}/{ns}", "勝率": s_all[1], "PF": s_all[2],
                     "1件%": s_all[3], "選定PF": s_sel[2], "選定1件%": s_sel[3], "確認n": s_cf[0], "確認PF": s_cf[2],
                     "確認1件%": s_cf[3], "確認ランダム1件%": np.nanmean(rnd_cf), "z": z, "上3日除く合計%": ex3})
        print(f"  {name}: n={s_all[0]} PF{s_all[2]:.2f} 確認PF{s_cf[2]:.2f} z={z:+.1f}", flush=True)

    R = pd.DataFrame(rows)
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
    print("\n================ 一覧（コスト: 往復0.10%＋入口/出口で不利1tick）================")
    print(R.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    import os
    os.makedirs("../results_daytrade_patterns", exist_ok=True)
    R.to_csv("../results_daytrade_patterns/summary.csv", index=False, encoding="utf-8-sig")
    st = R[R["★"] == "★"]
    print("\n=== 判定 ===")
    if len(st):
        print("  4条件を全部通った型（★）: " + " / ".join(st["型"]))
        print(f"  ※ {len(R)}通りを調べたので、偶然の★が1つ前後出てもおかしくない。★は次の60日で再確認するまで実弾にしない。")
    else:
        print("  ★なし＝コストを払って、ランダムな時刻より確かに勝てる型は見つからなかった。")
    print("  売りの型は空売りできるか（在庫・売り禁）を見ていない＝実際はもっと悪い。")
    print("  注意: D（MA40回帰）・E（VWAP回帰）は、ランダムな時刻に入るとすぐ回帰条件に当たってコスト負けしやすい＝z が甘く出る。"
          "\n        この2系統は z より「確認PF>1.1」と「上3日除く合計」を重く見る。")


if __name__ == "__main__":
    main()

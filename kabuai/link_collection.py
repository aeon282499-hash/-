# -*- coding: utf-8 -*-
"""link_collection.py — 📚おすすめの連動銘柄集（2026-10-15 本人「おすすめの連動銘柄集作りたい」）。

何を: 「今日だけ熱い」ではなく、期間を前半・後半に割っても両方で連動が成立し、代金があって売買できる
       銘柄グループを毎日作り直す。flow_builder（当日の資金セクター・主役→連動）とは別物で、落ち着いた
       日にも中身がある「連動の地図」。
作り:  母集団＝20日中央値の代金 LIQ_MIN 以上・200〜15,000円・当日の足がある銘柄。
       直近 N 日（最大120・データがあるだけ）の日次リターンを市場（等加重）で残差化し、全期間と前半/後半の3つの相関を出す。
       辺＝全期間 ≥ floor かつ 前半 ≥ floor_half かつ 後半 ≥ floor_half（floor はシャッフルの偶然の99.9%点+0.05・0.50〜0.60）。
       辺を強い順に union-find でつなぐ（1グループ最大 GROUP_MAX 本・溢れたら別グループ）。
       ラベル＝メンバーの半分以上を含む最小の株探テーマ／33業種、無ければ中心銘柄の名前。
       おすすめ順＝安定度（全ペアの min(前半,後半) の平均）× 本数の√。
注意:  連動の強さを測っただけ。儲かるかは別（後追い買いは日足10年 n39,800 で −0.21%／勝率45%＝買えない）。
       使い道は「同じグループを同時に持たない」「主役の急落を仲間の警報にする」「ペアの乖離を見る」。
"""
from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from flow_builder import _f, _load_themes, _name, next_trading_day

JST = timezone(timedelta(hours=9))
HERE = Path(__file__).resolve().parent

N_MAX = 120          # 相関の窓（営業日）上限。CI の取得は約95日なのでそれに合わせて縮む
N_MIN = 60           # これ未満なら作らない
LIQ_MIN = 5e8        # 20日中央値の代金 ≥5億（売買できる）
PX_LO, PX_HI = 200, 15000
GROUP_MAX = 12
GROUPS_SHOW = 30
RULES = [
    f"母集団＝20日中央値の代金{LIQ_MIN/1e8:.0f}億以上・{PX_LO}〜{PX_HI:,}円・当日の足がある銘柄",
    f"連動＝直近N日（最大{N_MAX}営業日・データがあるだけ）の日次リターンを市場(等加重)で残差化した相関。全期間と前半/後半の3つを出し、3つともしきい値以上のペアだけを辺にする",
    "しきい値＝列シャッフルで出る偶然の99.9%点＋0.05（0.50〜0.60）。前半/後半は日数が少ないぶん高くなる",
    f"グループ＝辺を強い順につないだもの（1グループ最大{GROUP_MAX}本）。ラベルはメンバーの半分以上を含む最小の株探テーマ／33業種",
    "おすすめ順＝安定度（全ペアの min(前半,後半) の平均）×√本数。🧩＝複数の業種にまたがる（テーマ辞書に無い隠れた鎖）",
]
STATS = {
    "chase": "連動株の翌日買い（日足10年・2016-07〜2026-08・n39,800）: 寄→引 平均−0.21%・勝率45%・母集団全体は−0.02%。祭り参加+GUで寄る型は−1.9%・勝率36%＝買えない",
    "use": "使い道: ①同じグループを同時に持たない（同じ賭けを2回している） ②主役の急落を仲間の警報にする ③ペアの乖離（片方だけ動いた日）を見る。買いの根拠にはしない",
}


def _resid_corr(R: pd.DataFrame) -> np.ndarray:
    """市場(等加重)で残差化した相関行列（NaN→0）。"""
    M = R.mean(axis=1)
    Rf = R.sub(R.mean()).fillna(0.0)
    Mf = (M - M.mean()).values
    beta = (Rf.values * Mf[:, None]).sum(0) / max(float((Mf ** 2).sum()), 1e-12)
    RES = Rf.values - Mf[:, None] * beta[None, :]
    sd = RES.std(0); sd[sd == 0] = np.nan
    Z = RES / sd
    CM = np.nan_to_num((Z.T @ Z) / max(len(Z) - 1, 1), nan=0.0)
    np.fill_diagonal(CM, 1.0)
    return CM, Z


def _null_floor(Z: np.ndarray, seed: int = 7) -> tuple[float, float]:
    rng_ = np.random.default_rng(seed)
    sub = rng_.choice(Z.shape[1], size=min(300, Z.shape[1]), replace=False)
    nulls = []
    for _ in range(5):
        S = np.column_stack([rng_.permutation(np.nan_to_num(Z[:, j])) for j in sub])
        cc = (S.T @ S) / max(len(S) - 1, 1)
        nulls.append(cc[np.triu_indices_from(cc, 1)])
    nulls = np.concatenate(nulls); nulls = nulls[np.isfinite(nulls)]
    q = float(np.quantile(nulls, 0.999)) if len(nulls) else 0.5
    return q, round(min(0.60, max(0.50, q + 0.05)), 2)


def build_collection(data: dict, name_map: dict, data_date: str, iss: dict | None, alert: dict | None,
                     sec_of: dict | None = None, themes_path: Path | None = None) -> dict:
    t0 = time.time()
    iss = iss or {}; alert = alert or {}; sec_of = sec_of or {}
    dd = pd.Timestamp(data_date)
    tgt = next_trading_day(dd.date())

    C, V, H, L = {}, {}, {}, {}
    for tk, df in data.items():
        if df is None or len(df) < N_MIN:
            continue
        code4 = str(tk).replace(".T", "")[:4]
        if not re.fullmatch(r"\d{3}[0-9A-Z]", code4):
            continue
        x = df.tail(N_MAX + 30)
        try:
            idx = pd.to_datetime(x.index)
        except Exception:
            continue
        C[code4] = pd.Series(x["Close"].values, index=idx, dtype="float64")
        V[code4] = pd.Series(x["Volume"].values, index=idx, dtype="float64")
        H[code4] = pd.Series(x["High"].values, index=idx, dtype="float64")
        L[code4] = pd.Series(x["Low"].values, index=idx, dtype="float64")
    PC = pd.DataFrame(C).sort_index()
    if PC.empty or dd not in PC.index:
        return {"error": f"当日({data_date})の足がない", "date": data_date}
    PC = PC.loc[:dd]; PV = pd.DataFrame(V).reindex(PC.index); PH = pd.DataFrame(H).reindex(PC.index); PL = pd.DataFrame(L).reindex(PC.index)
    TOV = PC * PV
    last = PC.iloc[-1]; prev = PC.iloc[-2]
    has_today = last.notna() & TOV.iloc[-1].notna() & (TOV.iloc[-1] > 0)
    med20 = TOV.iloc[-21:-1].median()
    r1 = (last / prev - 1) * 100
    r5 = (last / PC.iloc[-6] - 1) * 100
    rng20 = ((PH - PL) / PC * 100).iloc[-20:].median()
    uni = [c for c in PC.columns[has_today & (med20 >= LIQ_MIN) & (last >= PX_LO) & (last <= PX_HI)]
           if _name(name_map, c) != c]   # name_map に無いコード（ETF・ETN等）は除く
    if len(uni) < 30:
        return {"error": f"母集団が少なすぎる({len(uni)})", "date": data_date}

    Rall = PC[uni].pct_change(fill_method=None).iloc[1:]
    Rall = Rall.mask(Rall.abs() > 0.25)   # 分割未調整・一発の特異日は相関を支配するので、その日だけ欠損にする（1日25%超）
    n_days = int(min(N_MAX, len(Rall)))
    if n_days < N_MIN:
        return {"error": f"日数が足りない({n_days}<{N_MIN})", "date": data_date}
    R = Rall.iloc[-n_days:]
    R = R.dropna(axis=1, thresh=int(n_days * 0.9))
    uni = list(R.columns)
    half = n_days // 2
    R1 = R.iloc[:half]; R2 = R.iloc[half:]
    CM, Z = _resid_corr(R)
    CM1, Z1 = _resid_corr(R1)
    CM2, Z2 = _resid_corr(R2)
    null_full, floor = _null_floor(Z)
    null_half, floor_half = _null_floor(Z2)
    pos = {c: i for i, c in enumerate(uni)}

    # 辺＝3つとも floor 以上
    iu, ju = np.triu_indices(len(uni), 1)
    ok = (CM[iu, ju] >= floor) & (CM1[iu, ju] >= floor_half) & (CM2[iu, ju] >= floor_half)
    E = [(float(CM[i, j]), int(i), int(j)) for i, j in zip(iu[ok], ju[ok])]
    E.sort(reverse=True)

    parent = list(range(len(uni))); size = [1] * len(uni)

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]; a = parent[a]
        return a

    for _, i, j in E:
        a, b = find(i), find(j)
        if a == b:
            continue
        if size[a] + size[b] > GROUP_MAX:
            continue
        if size[a] < size[b]:
            a, b = b, a
        parent[b] = a; size[a] += size[b]
    comps: dict[int, list[int]] = {}
    for i in range(len(uni)):
        r = find(i)
        comps.setdefault(r, []).append(i)
    comps = [m for m in comps.values() if len(m) >= 2]

    themes = _load_themes(themes_path)
    theme_sets = {k: set(v) for k, v in themes.items()}

    def label_for(codes: list[str]) -> tuple[str, str, list[str], str]:
        """(ラベル, 種別, 候補テーマ上位, 業種内訳)。業種が6割以上なら業種、無ければカバー率最大のテーマ（同率なら小さいテーマ）。"""
        cs = set(codes)
        secs = pd.Series([sec_of.get(c) for c in codes if sec_of.get(c)]).value_counts()
        sec_txt = "・".join(f"{k}{int(v)}" for k, v in secs.head(4).items())
        cands = []
        for th, st in theme_sets.items():
            cov = len(cs & st) / len(cs)
            if cov >= 0.5:
                cands.append((cov, -len(st), th))
        cands.sort(reverse=True)
        alts = [t for _, _, t in cands[:3]]
        if len(secs) and secs.iloc[0] / len(codes) >= 0.6:
            return str(secs.index[0]), "業種", alts, sec_txt
        if cands:
            return cands[0][2], "テーマ", alts[1:], sec_txt
        hub = max(codes, key=lambda c: float(np.mean([CM[pos[c], pos[o]] for o in codes if o != c])))
        return f"{_name(name_map, hub)} の鎖", "無名", [], sec_txt

    def minfo(c: str, mem: list[str]) -> dict:
        al = alert.get(c) or {}
        others = [o for o in mem if o != c]
        return {"code": c, "name": _name(name_map, c), "close": _f(last[c], 1), "r1": _f(r1[c]), "r5": _f(r5[c], 1),
                "tov_med_oku": _f(med20[c] / 1e8, 1), "rng20": _f(rng20[c], 1), "sector": sec_of.get(c),
                "hub": _f(float(np.mean([CM[pos[c], pos[o]] for o in others]))) if others else None,
                "shortable": "○" if str(iss.get(c, "")) == "2" else ("×" if iss else None),
                "jsf_stop": bool(al.get("jsf_stop")), "daily_pub": bool(al.get("daily_pub")), "tse_reg": bool(al.get("tse_reg"))}

    groups = []
    for m in comps:
        codes = [uni[i] for i in m]
        pairs = [(a, b) for k, a in enumerate(m) for b in m[k + 1:]]
        full = float(np.mean([CM[a, b] for a, b in pairs]))
        stab = float(np.mean([min(CM1[a, b], CM2[a, b]) for a, b in pairs]))
        h1 = float(np.mean([CM1[a, b] for a, b in pairs])); h2 = float(np.mean([CM2[a, b] for a, b in pairs]))
        label, kind, alts, sec_txt = label_for(codes)
        secs = {sec_of.get(c) for c in codes if sec_of.get(c)}
        mem = sorted(codes, key=lambda c: -float(np.mean([CM[pos[c], pos[o]] for o in codes if o != c])))
        groups.append({"label": label, "kind": kind, "alts": alts, "sectors": sec_txt, "n": len(codes), "cross": len(secs) >= 2 and kind != "業種",
                       "corr": _f(full), "stab": _f(stab), "h1": _f(h1), "h2": _f(h2),
                       "score": _f(stab * np.sqrt(len(codes)), 3),
                       "avg_r1": _f(float(r1[codes].mean()), 1), "avg_r5": _f(float(r5[codes].mean()), 1),
                       "tov_med_oku": _f(float(med20[codes].median() / 1e8), 1),
                       "members": [minfo(c, codes) for c in mem]})
    groups.sort(key=lambda g: -(g["score"] or 0))
    n_all = len(groups)
    groups = groups[:GROUPS_SHOW]
    return {
        "date": data_date, "target_date": tgt.isoformat(), "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "n_universe": len(uni), "n_days": n_days, "half_days": half,
        "period": f"{R.index[0].strftime('%m/%d')}〜{R.index[-1].strftime('%m/%d')}",
        "floor": floor, "null_full": _f(null_full), "floor_half": floor_half, "null_half": _f(null_half),
        "n_edges": len(E), "n_groups": n_all, "groups": groups,
        "iss_known": bool(iss), "alert_known": bool(alert),
        "rules": RULES, "stats": STATS, "elapsed_s": round(time.time() - t0, 1),
    }

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
LIQ_MIN = 3e8        # 20日中央値の代金 ≥3億（売買できる。10/15 本人「テーマ連動でまとめて」→小型テーマも拾うため5億→3億）
PX_LO, PX_HI = 200, 15000
GROUP_MAX = 12
GROUPS_SHOW = 40
CORE_MIN = 3         # テーマの核＝3本以上
OVERLAP_SKIP = 0.5   # 既に採ったグループと核が半分以上重なるテーマは省く
DISPLAY_OVERLAP_SKIP = 0.34  # 表示する銘柄が既出と1/3以上重なるグループも省く（SBI登録用に別物だけ残す）
RULES = [
    f"母集団＝20日中央値の代金{LIQ_MIN/1e8:.0f}億以上・{PX_LO}〜{PX_HI:,}円・当日の足がある銘柄（ETF除く）",
    f"連動＝直近N日（最大{N_MAX}営業日）の日次リターンを市場(等加重)で残差化した相関。全期間と前半/後半の3つを出し、3つともしきい値以上のペアだけを辺にする",
    "しきい値＝列シャッフルで出る偶然の99.9%点＋0.05（0.50〜0.60）",
    f"グループ＝株探テーマごとに、辺でつながった最大のかたまり（核・{CORE_MIN}本以上・最大{GROUP_MAX}本は仲間との平均相関が高い順）。ラベルは核を一番よく覆うテーマ（他の候補はタグ）。テーマで拾えない組は33業種で穴埋め。核が半分以上、または表示銘柄が1/3以上重なるグループは省く",
    "おすすめ順＝安定度（核の全ペアの min(前半,後半) の平均）×√本数。📋コピー＝SBIの登録銘柄に貼る用のコード列",
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

    themes = _load_themes(themes_path)

    def minfo(c: str, mem: list[str]) -> dict:
        al = alert.get(c) or {}
        others = [o for o in mem if o != c]
        return {"code": c, "name": _name(name_map, c), "close": _f(last[c], 1), "r1": _f(r1[c]), "r5": _f(r5[c], 1),
                "tov_med_oku": _f(med20[c] / 1e8, 1), "rng20": _f(rng20[c], 1), "sector": sec_of.get(c),
                "hub": _f(float(np.mean([CM[pos[c], pos[o]] for o in others]))) if others else None,
                "shortable": "○" if str(iss.get(c, "")) == "2" else ("×" if iss else None),
                "jsf_stop": bool(al.get("jsf_stop")), "daily_pub": bool(al.get("daily_pub")), "tse_reg": bool(al.get("tse_reg"))}

    # 辺の隣接表
    adj: dict[int, set[int]] = {}
    for _, i, j in E:
        adj.setdefault(i, set()).add(j); adj.setdefault(j, set()).add(i)

    def core_of(codes: list[str]) -> list[int]:
        """テーマ内の辺でつながった最大のかたまり（頂点番号）。"""
        ids = [pos[c] for c in codes if c in pos]
        idset = set(ids); seen: set[int] = set(); best: list[int] = []
        for v in ids:
            if v in seen:
                continue
            comp = []; stack = [v]; seen.add(v)
            while stack:
                x = stack.pop(); comp.append(x)
                for y in adj.get(x, ()):
                    if y in idset and y not in seen:
                        seen.add(y); stack.append(y)
            if len(comp) > len(best):
                best = comp
        return best

    cands_def: list[tuple[str, str, list[str]]] = [("テーマ", th, list(mem)) for th, mem in themes.items()]
    by_sec: dict[str, list[str]] = {}
    for c in uni:
        if sec_of.get(c):
            by_sec.setdefault(sec_of[c], []).append(c)
    cands_def += [("業種", sname, mem) for sname, mem in by_sec.items()]

    def hub_of(v: int, comp: list[int]) -> float:
        return float(np.mean([CM[v, o] for o in comp if o != v])) if len(comp) > 1 else 0.0

    theme_in_uni = {th: set(c for c in mem if c in pos) for th, mem in themes.items()}

    def best_label(core_codes: set[str], kind0: str, label0: str) -> tuple[str, str, int, list[str]]:
        """核を一番よく覆うテーマ（同率なら小さいテーマ）。無ければ元のラベル。(ラベル, 種別, そのテーマの母集団内本数, 他の候補テーマ)"""
        cands = []
        for th, st in theme_in_uni.items():
            if not st:
                continue
            cov = len(core_codes & st) / len(core_codes)
            if cov >= 0.5:
                cands.append((cov, -len(st), th))
        cands.sort(reverse=True)
        if cands:
            return cands[0][2], "テーマ", -cands[0][1], [t for _, _, t in cands[1:4]]
        return label0, kind0, len(core_codes), [t for _, _, t in cands[:3]]

    built = []
    seen_cores: list[set[str]] = []
    for kind, label, mem in cands_def:
        comp = core_of(mem)
        if len(comp) < CORE_MIN:
            continue
        n_core = len(comp)
        core_codes = set(uni[v] for v in comp)
        # 同じ核（半分以上重なる）が別のテーマから既に出ていれば省く（銀行の核がクレジットカード/事業承継/証券…から何度も出るのを防ぐ）
        if any(len(core_codes & sc) / len(core_codes) >= OVERLAP_SKIP for sc in seen_cores):
            continue
        seen_cores.append(core_codes)
        label, kind, n_theme, alts = best_label(core_codes, kind, label)
        comp = sorted(comp, key=lambda v: -hub_of(v, comp))[:GROUP_MAX]
        pairs = [(a_, b_) for k, a_ in enumerate(comp) for b_ in comp[k + 1:]]
        full = float(np.mean([CM[a_, b_] for a_, b_ in pairs]))
        stab = float(np.mean([min(CM1[a_, b_], CM2[a_, b_]) for a_, b_ in pairs]))
        h1 = float(np.mean([CM1[a_, b_] for a_, b_ in pairs])); h2 = float(np.mean([CM2[a_, b_] for a_, b_ in pairs]))
        codes = [uni[v] for v in comp]
        secs = pd.Series([sec_of.get(c) for c in codes if sec_of.get(c)]).value_counts()
        built.append({"kind": kind, "label": label, "alts": alts, "codes": codes, "core": core_codes, "n_core": n_core, "n_theme": n_theme,
                      "full": full, "stab": stab, "h1": h1, "h2": h2, "score": stab * np.sqrt(len(codes)),
                      "sectors": "・".join(f"{k}{int(v)}" for k, v in secs.head(4).items()), "cross": len(secs) >= 2 and kind != "業種"})
    # テーマ優先→業種で穴埋め。半分以上重なるものは省く
    built.sort(key=lambda g: (g["kind"] != "テーマ", -g["score"]))
    picked: list[dict] = []
    for g in built:
        cs = g["core"]
        if any(len(cs & p["core"]) / len(cs) >= OVERLAP_SKIP for p in picked):
            continue
        if any(len(set(g["codes"]) & set(p["codes"])) / len(g["codes"]) >= DISPLAY_OVERLAP_SKIP for p in picked):
            continue   # 表示する12本が既出と1/3以上重なるものも省く（SBIに登録する前提＝別のリストとして意味があるものだけ）
        picked.append(g)
    groups = []
    for g in picked:
        codes = g["codes"]
        groups.append({"label": g["label"], "kind": g["kind"], "alts": g["alts"], "sectors": g["sectors"], "n": len(codes), "n_core": g["n_core"], "n_theme": g["n_theme"],
                       "cross": g["cross"], "corr": _f(g["full"]), "stab": _f(g["stab"]), "h1": _f(g["h1"]), "h2": _f(g["h2"]),
                       "score": _f(g["score"], 3),
                       "avg_r1": _f(float(r1[codes].mean()), 1), "avg_r5": _f(float(r5[codes].mean()), 1),
                       "tov_med_oku": _f(float(med20[codes].median() / 1e8), 1),
                       "codes": " ".join(codes),
                       "members": [minfo(c, codes) for c in codes]})
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

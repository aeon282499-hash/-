# -*- coding: utf-8 -*-
"""flow_builder.py — 💴「今日 資金が入ったセクター」→ 🔗「その連動株」→ 🌙PTS（2026-10-14・本人依頼
「毎日PTSや資金の入ってるセクターを見つけて連動株を見つけてチンパンに配信・連動を中心に」）。

■ 何を出すか
  1. 資金セクター: 33業種＋株探テーマ辞書(themes_dict.json)のグループごとに、当日の売買代金が
     20日中央値の何倍か(メンバーの中央値)・上昇した本数・平均騰落・5日・連動の強さ(残差相関)を集計し、
     「中央値で2倍以上かつ熱い本数2本以上」のグループを資金倍率順に並べる。
  2. 連動: 直近60日の日次リターンを市場(ユニバース等加重)で残差化した相関。グループ内は主役との相関、
     グループ外からも「主役と相関がしきい値以上」の銘柄を拾う(=隠れた連動株)。しきい値は銘柄ごとの
     シャッフルで出る偶然の最大値＋0.02(リンクリスト 2026-10-05 と同じ作法)。
  3. 主役→連動: 当日 +7%以上・代金5億以上の主役ごとに連動株を並べる(連動を中心に見る表)。
  4. PTS: 表示する銘柄の夜間PTS(株探の個別ページ＝ジャパンネクスト)を取る。取れない回は無しで出す。

■ 言わないこと（検証済み・数字はバナーに出す）
  後追い買いは効かない(2026-10-05 5分足: 主役+5%→次の足で連動株買い→引け 平均−0.6%・14日中4勝)。
  テーマ祭り3日目以降の翌日は 寄→引 −1.9%/勝率37%(2016〜2026 n3,967)・GU+3%以上は −3.8〜−5.2%。
  → 使い道は「同じグループを同時に持たない」「主役の急落を警報に」「翌日GUは寄り売りの土俵」。
     上がる/下がる・勝てるは書かない。

■ データ
  build_data.py の data({ticker: DataFrame[Open,High,Low,Close,Volume]})・name_map・SEC_OF(33業種)・
  iss(貸借区分 "2"=貸借)・alert({code4:{jsf_stop,jsf_warn,tse_reg,daily_pub}})。
"""
from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

JST = timezone(timedelta(hours=9))
HERE = Path(__file__).resolve().parent

WINDOW = 60            # 残差相関の窓（営業日）
TOV_MIN = 3e8          # 20日中央値の代金 ≥3億
PX_LO, PX_HI = 200, 15000
GROUP_MIN = 3          # ユニバース内に3本以上あるグループだけ
HOT_R1, HOT_TOV = 3.0, 2.0      # 熱い本: 当日+3%以上 かつ 代金2倍以上
FLOW_MIN_RATIO = 1.3   # 資金セクター: メンバー代金倍率の中央値 ≥1.3（2倍以上なら「強い」印）
FLOW_STRONG = 2.0
FLOW_MIN_HOT = 2       # かつ 熱い本 ≥2
TOP_GROUPS = 8
MEMBERS_MAX = 12       # (v7.1 互換・未使用)
CHAIN_MAX = 14         # 鎖(主役と floor 以上)の表示上限
OUT_MAX = 8            # 鎖の外(同グループで floor 未満)の表示上限・代金順
HIDDEN_MAX = 6         # 群全体(平均残差)に floor 以上で連動する群外銘柄の上限
LINKED_MAX = 6
MOVER_R1, MOVER_TOV = 7.0, 5e8
MOVERS_MAX = 12
PTS_MAX = 90           # 夜間PTSを取る銘柄数の上限
OVERLAP_SKIP = 0.7     # 上位グループとメンバーが7割重なるグループは省く

RULES = [
    f"母集団＝20日中央値の代金{TOV_MIN/1e8:.0f}億以上・{PX_LO}〜{PX_HI:,}円・当日の足がある銘柄",
    f"資金セクター＝33業種と株探テーマ。メンバーの代金倍率(当日÷20日中央値)の中央値が{FLOW_MIN_RATIO}倍以上かつ熱い本(当日+{HOT_R1:.0f}%・代金{HOT_TOV:.0f}倍)が{FLOW_MIN_HOT}本以上。倍率×(1+熱い本の割合)の順・{FLOW_STRONG:.0f}倍以上は🔥。該当が3つ未満の日は弱い順でも埋めて⚠️",
    f"連動＝直近{WINDOW}日の日次リターンを市場(等加重)で残差化した相関。しきい値はシャッフルで出る偶然の最大＋0.02",
    f"主役＝当日+{MOVER_R1:.0f}%以上・代金{MOVER_TOV/1e8:.0f}億以上。連動株はしきい値以上の相関の順",
    "グループの各銘柄＝主役との相関・鎖の本数(同グループ内でしきい値以上の相手の数)・同日連動率(主役が+5%の日にプラスだった割合)。鎖の外＝同じテーマでも主役としきい値未満(偶然の範囲)。🧲＝群外で群全体の平均残差に連動する銘柄",
    "PTS＝株探の個別ページ(ジャパンネクスト)の夜間最終値。取れなかった回は空欄",
]
STATS = {
    "chase": "後追い買い(2026-10-05・8/12〜9/30・5分足): 主役(ティアフォー)が前日比+5%到達→次の足で連動株(アイサン)を買い→引け＝平均−0.6%・14日中4勝。連動は『同じ日・同じ5分足』で起きる。翌日遅れも5分遅れも無い",
    "day3": "テーマ祭り3日目以降の翌日(2016〜2026・n3,967): 寄→引 平均−1.9%・勝率37%。翌日GU+3%以上で寄ると −3.8〜−5.2%(勝率29〜32%)。11年すべてマイナス",
    "use": "使い道: ①同じグループを同時に持たない ②主役の急落を連動株の警報にする ③翌日のGUは寄り売り(フェード型A・12:30空売り→引成)の土俵。買いの根拠にはしない",
}

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
      "Accept-Language": "ja"}


# ── 営業日 ──────────────────────────────────────────────────────────────
def _is_trading_day(d: date) -> bool:
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return False
    except Exception:
        pass
    if d.weekday() >= 5:
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def next_trading_day(d: date) -> date:
    cur = d + timedelta(days=1)
    while not _is_trading_day(cur):
        cur += timedelta(days=1)
    return cur


def _f(v, nd=2):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(x):
        return None
    return round(x, nd)


# ── PTS（株探 個別ページ） ─────────────────────────────────────────────
_PTS_RE = re.compile(r'kabuka1">\s*PTS\s*</div>.*?kabuka2">(.*?)</div>.*?kabuka3">(.*?)</div>', re.S)
_PX_RE = re.compile(r'<span class="kabuka">([\d,\.]+)円')


def _strip(x: str) -> str:
    return re.sub(r"<[^>]+>", "", x).replace("\xa0", " ").replace("&nbsp;", " ").strip()


def fetch_pts_one(code4: str, timeout: float = 6.0) -> dict | None:
    import requests
    try:
        r = requests.get(f"https://kabutan.jp/stock/?code={code4}", headers=UA, timeout=timeout)
        if r.status_code != 200:
            return {"_err": f"http{r.status_code}"}
        s = r.text
        m = _PTS_RE.search(s)
        px = _PX_RE.search(s)
        if not m or not px:
            return None
        raw = _strip(m.group(1)).replace("円", "").replace(",", "")
        if not re.fullmatch(r"[\d.]+", raw):
            return None                       # 「－」＝PTSの約定なし
        pts = float(raw)
        close = float(px.group(1).replace(",", ""))
        if close <= 0 or pts <= 0:
            return None
        return {"px": pts, "close": close, "dev": round((pts / close - 1) * 100, 2), "time": _strip(m.group(2))}
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)[:60]}


def asof_key(t: str) -> tuple:
    """株探のPTS時刻「HH:MM　MM/DD」を (MM/DD, HH:MM) に。ナイトタイムは翌日06:00まで続くので文字列maxでは「23:59 10/13」が「00:11 10/14」に勝ってしまう。"""
    parts = str(t or "").replace("　", " ").split()
    return (parts[1], parts[0]) if len(parts) >= 2 else ("", str(t or ""))


def latest_asof(ptsmap: dict) -> str | None:
    vals = [v.get("time") for v in ptsmap.values() if v.get("time")]
    return max(vals, key=asof_key) if vals else None


def fetch_pts(codes: list[str], workers: int = 8) -> tuple[dict, dict]:
    """{code4: {px,dev,time}} と メタ(試行/成功/エラー種)。"""
    meta = {"tried": len(codes), "ok": 0, "err": {}}
    out: dict = {}
    if not codes:
        return out, meta
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for code, res in zip(codes, ex.map(fetch_pts_one, codes)):
            if res and "_err" in res:
                k = res["_err"]
                meta["err"][k] = meta["err"].get(k, 0) + 1
            elif res:
                out[code] = res
                meta["ok"] += 1
    return out, meta


# ── 本体 ────────────────────────────────────────────────────────────────
def _load_themes(path: Path | None) -> dict[str, list[str]]:
    p = path or (HERE / "themes_dict.json")
    try:
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        return {k: list(v) for k, v in (j.get("themes") or {}).items()}
    except Exception:
        return {}


def _name(name_map: dict, code4: str) -> str:
    for k in (code4 + ".T", code4, code4 + "0"):
        if k in name_map:
            return str(name_map[k])
    return code4


def build_flow(data: dict, name_map: dict, data_date: str, iss: dict | None, alert: dict | None,
               sec_of: dict | None = None, themes_path: Path | None = None, today: date | None = None,
               want_pts: bool | None = None) -> dict:
    t0 = time.time()
    iss = iss or {}; alert = alert or {}; sec_of = sec_of or {}
    if want_pts is None:
        want_pts = os.getenv("KABUAI_PTS", "1") != "0"
    dd = pd.Timestamp(data_date)
    tgt = next_trading_day(dd.date())

    # 1) パネル
    C, V, H, L = {}, {}, {}, {}
    for tk, df in data.items():
        if df is None or len(df) < 45:
            continue
        code4 = str(tk).replace(".T", "")[:4]
        x = df.tail(WINDOW + 30)
        try:
            idx = pd.to_datetime(x.index)
        except Exception:
            continue
        C[code4] = pd.Series(x["Close"].values, index=idx, dtype="float64")
        V[code4] = pd.Series(x["Volume"].values, index=idx, dtype="float64")
        H[code4] = pd.Series(x["High"].values, index=idx, dtype="float64")
        L[code4] = pd.Series(x["Low"].values, index=idx, dtype="float64")
    PC = pd.DataFrame(C).sort_index(); PV = pd.DataFrame(V).reindex(PC.index)
    PH = pd.DataFrame(H).reindex(PC.index); PL = pd.DataFrame(L).reindex(PC.index)
    if PC.empty or dd not in PC.index:
        return {"error": f"当日({data_date})の足がない", "date": data_date}
    PC = PC.loc[:dd]; PV = PV.loc[:dd]; PH = PH.loc[:dd]; PL = PL.loc[:dd]
    TOV = PC * PV
    last = PC.iloc[-1]; prev = PC.iloc[-2]
    has_today = last.notna() & TOV.iloc[-1].notna() & (TOV.iloc[-1] > 0)
    med20 = TOV.iloc[-21:-1].median()
    r1 = (last / prev - 1) * 100
    r5 = (last / PC.iloc[-6] - 1) * 100
    ma25 = PC.iloc[-25:].mean(); dev25 = (last / ma25 - 1) * 100
    tov_ratio = TOV.iloc[-1] / med20
    rng = (PH.iloc[-1] - PL.iloc[-1]) / last * 100
    # 20日中央値が小さくても当日に代金が爆発した銘柄（倍率600倍の祭り銘柄など）は母集団に入れる（10/13 サイバートラストが落ちた）
    uni = PC.columns[(has_today) & ((med20 >= TOV_MIN) | (TOV.iloc[-1] >= MOVER_TOV)) & (last >= PX_LO) & (last <= PX_HI)]
    uni = [c for c in uni if re.fullmatch(r"\d{3}[0-9A-Z]", c)]
    n_uni = len(uni)
    if n_uni < 30:
        return {"error": f"母集団が少なすぎる({n_uni})", "date": data_date}

    # 2) 残差相関
    R = PC[uni].pct_change(fill_method=None).iloc[-WINDOW:]
    R = R.dropna(axis=1, thresh=int(WINDOW * 0.9))
    uni = list(R.columns)
    M = R.mean(axis=1)
    Rf = R.sub(R.mean()).fillna(0.0)
    Mf = (M - M.mean()).values
    beta = (Rf.values * Mf[:, None]).sum(0) / max(float((Mf ** 2).sum()), 1e-12)
    RES = Rf.values - Mf[:, None] * beta[None, :]
    sd = RES.std(0); sd[sd == 0] = np.nan
    Z = RES / sd
    CM = np.nan_to_num((Z.T @ Z) / (len(Z) - 1), nan=0.0)
    np.fill_diagonal(CM, 1.0)
    pos = {c: i for i, c in enumerate(uni)}
    # 偶然の水準: 列ごとにシャッフル（300本抽出×5回）
    rng_ = np.random.default_rng(7)
    sub = rng_.choice(Z.shape[1], size=min(300, Z.shape[1]), replace=False)
    nulls = []
    for _ in range(5):
        S = np.column_stack([rng_.permutation(Z[:, j]) for j in sub])
        cc = (S.T @ S) / (len(S) - 1)
        nulls.append(cc[np.triu_indices_from(cc, 1)])
    nulls = np.concatenate(nulls); nulls = nulls[np.isfinite(nulls)]
    null_max = float(np.quantile(nulls, 0.999)) if len(nulls) else 0.5   # 99.9%点（最大値は外れ値に引っぱられる）
    floor = round(min(0.60, max(0.50, null_max + 0.05)), 2)   # 60日窓で0.50は t≈4.4

    def corr(a: str, b: str) -> float | None:
        if a in pos and b in pos:
            return float(CM[pos[a], pos[b]])
        return None

    def coh(codes: list[str]) -> float | None:
        ix = [pos[c] for c in codes if c in pos]
        if len(ix) < 2:
            return None
        sub = CM[np.ix_(ix, ix)]
        return float(np.nanmean(sub[np.triu_indices(len(ix), 1)]))

    def minfo(c: str, lead: str | None = None, ptsmap: dict | None = None) -> dict:
        al = alert.get(c) or {}
        d = {"code": c, "name": _name(name_map, c), "close": _f(last[c], 1), "r1": _f(r1[c]), "r5": _f(r5[c], 1),
             "tov_oku": _f(TOV.iloc[-1][c] / 1e8, 1), "tov_ratio": _f(tov_ratio[c], 1), "dev25": _f(dev25[c], 1),
             "rng": _f(rng[c], 1), "sector": sec_of.get(c),
             "shortable": "○" if str(iss.get(c, "")) == "2" else ("×" if iss else None),
             "jsf_stop": bool(al.get("jsf_stop")), "daily_pub": bool(al.get("daily_pub")), "tse_reg": bool(al.get("tse_reg"))}
        if lead and lead != c:
            d["corr"] = _f(corr(lead, c))
        if ptsmap and c in ptsmap:
            d["pts"] = ptsmap[c]["px"]; d["pts_dev"] = ptsmap[c]["dev"]; d["pts_time"] = ptsmap[c]["time"]
        return d

    # 3) グループ
    uset = set(uni)
    groups_def: list[tuple[str, str, list[str]]] = []
    by_sec: dict[str, list[str]] = {}
    for c in uni:
        s = sec_of.get(c)
        if s:
            by_sec.setdefault(s, []).append(c)
    for s, mem in by_sec.items():
        groups_def.append(("業種", s, mem))
    for th, mem in _load_themes(themes_path).items():
        m2 = [c for c in mem if c in uset]
        if len(m2) >= GROUP_MIN:
            groups_def.append(("テーマ", th, m2))
    cand = []
    for kind, label, mem in groups_def:
        if len(mem) < GROUP_MIN:
            continue
        tr = tov_ratio[mem].replace([np.inf, -np.inf], np.nan).dropna()
        if tr.empty:
            continue
        ratio_med = float(tr.median())
        hot = [c for c in mem if r1[c] >= HOT_R1 and tov_ratio[c] >= HOT_TOV]
        rr = r1[mem].dropna()
        passed = ratio_med >= FLOW_MIN_RATIO and len(hot) >= FLOW_MIN_HOT
        cand.append({"kind": kind, "label": label, "n": len(mem), "ratio_med": ratio_med, "passed": passed,
                     "score": ratio_med * (1 + len(hot) / len(mem)),
                     "ratio_sum": float(TOV.iloc[-1][mem].sum() / max(float(med20[mem].sum()), 1.0)),
                     "breadth": float((rr > 0).mean() * 100), "avg_r1": float(rr.mean()), "avg_r5": float(r5[mem].mean()),
                     "n_hot": len(hot), "coh": coh(mem), "mem": mem, "hot": hot})
    cand.sort(key=lambda g: (not g["passed"], -g["score"], -g["n_hot"], -g["breadth"]))
    n_passed = sum(1 for g in cand if g["passed"])
    picked: list[dict] = []
    for g in cand:
        if not g["passed"] and len(picked) >= 3:
            break                     # 弱い日は3つまで埋める（⚠️付き）
        ms = set(g["mem"])
        if any(len(ms & set(p["mem"])) / len(ms) >= OVERLAP_SKIP for p in picked):
            continue
        picked.append(g)
        if len(picked) >= TOP_GROUPS:
            break

    # 4) 主役→連動
    movers = [c for c in uni if r1[c] >= MOVER_R1 and TOV.iloc[-1][c] >= MOVER_TOV]
    movers.sort(key=lambda c: -float(TOV.iloc[-1][c]))
    movers = movers[:MOVERS_MAX]
    Rv = R  # 同日連動率用

    def linked(lead: str, exclude: set[str], k: int = LINKED_MAX) -> list[dict]:
        i = pos[lead]
        order = np.argsort(-CM[i])
        out = []
        for j in order:
            c = uni[j]
            if c == lead or c in exclude:
                continue
            v = float(CM[i, j])
            if v < floor:
                break
            # 主役が+5%以上の日に相手がプラスだった割合（同日連動率）
            big = Rv[lead] >= 0.05
            same = float((Rv[c][big] > 0).mean() * 100) if int(big.sum()) >= 2 else None
            d = minfo(c, lead)
            d["same_day"] = _f(same, 0); d["n_big"] = int(big.sum())
            out.append(d)
            if len(out) >= k:
                break
        return out

    # 5) PTS（表示する銘柄だけ）
    show_codes: list[str] = []
    for g in picked:
        lead = max(g["hot"], key=lambda c: float(TOV.iloc[-1][c])) if g["hot"] else max(g["mem"], key=lambda c: float(TOV.iloc[-1][c]))
        g["lead"] = lead
        mem = list(g["mem"]); memset = set(mem); li = pos[lead]
        ix = [pos[c] for c in mem]
        sub = CM[np.ix_(ix, ix)]
        # 鎖の本数＝同グループ内で floor 以上の相手の数（対角=自分は除く）
        g["nlinks"] = {c: int((sub[k] >= floor).sum()) - 1 for k, c in enumerate(mem)}
        big = Rv[lead] >= 0.05; nbig = int(big.sum()); g["n_big"] = nbig
        g["same"] = {c: (float((Rv[c][big] > 0).mean() * 100) if nbig >= 2 else None) for c in mem}
        chain = sorted([c for c in mem if c != lead and CM[li, pos[c]] >= floor], key=lambda c: -float(CM[li, pos[c]]))
        out = sorted([c for c in mem if c != lead and CM[li, pos[c]] < floor], key=lambda c: -float(TOV.iloc[-1][c]))
        g["n_chain"] = len(chain); g["n_out"] = len(out)
        g["chain"] = chain[:CHAIN_MAX]; g["out"] = out[:OUT_MAX]
        g["mem_sorted"] = [lead] + g["chain"]
        g["linked_out"] = linked(lead, memset)
        # 🧲 群全体(メンバーの平均残差Z)に連動する群外銘柄
        g["hidden"] = []
        try:
            GZ = np.nanmean(Z[:, ix], axis=1)
            if np.isfinite(GZ).all() and GZ.std() > 0:
                GZ = (GZ - GZ.mean()) / GZ.std()
                cc = np.nan_to_num((Z.T @ GZ) / (len(Z) - 1), nan=0.0)
                for j in np.argsort(-cc):
                    if cc[j] < floor or len(g["hidden"]) >= HIDDEN_MAX:
                        break
                    if uni[j] in memset:
                        continue
                    g["hidden"].append((uni[j], float(cc[j])))
        except Exception:  # noqa: BLE001
            pass
        show_codes += g["mem_sorted"] + g["out"] + [d["code"] for d in g["linked_out"]] + [c for c, _ in g["hidden"]]
    mov_rows = []
    for c in movers:
        lk = linked(c, set())
        mov_rows.append((c, lk))
        show_codes += [c] + [d["code"] for d in lk]
    seen = set(); show_codes = [c for c in show_codes if not (c in seen or seen.add(c))][:PTS_MAX]
    ptsmap, pts_meta = ({}, {"tried": 0, "ok": 0, "err": {}, "skipped": True})
    if want_pts and show_codes:
        ptsmap, pts_meta = fetch_pts(show_codes)
    pts_src = "ci"
    if not ptsmap:
        # PCから置いた kabuai/pts_latest.json（pts_fetch_pc.py・Actions のIPは株探が405を返すため）。同じ引け日のものだけ使う
        try:
            with open(HERE / "pts_latest.json", encoding="utf-8") as f:
                pj = json.load(f)
            if str(pj.get("date")) == str(data_date) and pj.get("pts"):
                ptsmap = {c: v for c, v in pj["pts"].items() if c in set(show_codes)} or dict(pj["pts"])
                pts_meta = {"tried": int(pj.get("tried", 0)), "ok": len(ptsmap), "err": pj.get("err", {}), "skipped": False}
                pts_src = f"pc {pj.get('fetched_at', '')}"
        except FileNotFoundError:
            pass
        except Exception as _e:  # noqa: BLE001
            pts_meta["err"] = dict(pts_meta.get("err", {}), pc_file=str(_e)[:60])
    pts_asof = latest_asof(ptsmap)

    groups_out = []
    for g in picked:
        groups_out.append({
            "kind": g["kind"], "label": g["label"], "n": g["n"], "n_hot": g["n_hot"], "passed": bool(g["passed"]),
            "strong": bool(g["ratio_med"] >= FLOW_STRONG), "score": _f(g["score"], 2),
            "ratio_med": _f(g["ratio_med"], 1), "ratio_sum": _f(g["ratio_sum"], 1), "breadth": _f(g["breadth"], 0),
            "avg_r1": _f(g["avg_r1"], 1), "avg_r5": _f(g["avg_r5"], 1), "coh": _f(g["coh"]),
            "lead": dict(minfo(g["lead"], None, ptsmap), n_links=g["nlinks"].get(g["lead"]), n_big=g["n_big"]),
            "n_chain": g["n_chain"], "n_out": g["n_out"], "n_big": g["n_big"],
            "members": [dict(minfo(c, g["lead"], ptsmap), n_links=g["nlinks"].get(c), same_day=_f(g["same"].get(c), 0), n_big=g["n_big"]) for c in g["mem_sorted"]],
            "members_out": [dict(minfo(c, g["lead"], ptsmap), n_links=g["nlinks"].get(c), same_day=_f(g["same"].get(c), 0), n_big=g["n_big"]) for c in g["out"]],
            "linked_out": [dict(d, **({"pts": ptsmap[d["code"]]["px"], "pts_dev": ptsmap[d["code"]]["dev"]} if d["code"] in ptsmap else {})) for d in g["linked_out"]],
            "hidden": [dict(minfo(c, None, ptsmap), corr_grp=_f(v)) for c, v in g["hidden"]],
        })
    movers_out = []
    for c, lk in mov_rows:
        movers_out.append({"lead": minfo(c, None, ptsmap),
                           "linked": [dict(d, **({"pts": ptsmap[d["code"]]["px"], "pts_dev": ptsmap[d["code"]]["dev"]} if d["code"] in ptsmap else {})) for d in lk]})

    return {
        "date": data_date, "target_date": tgt.isoformat(), "fresh": True,
        "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "n_universe": n_uni, "window": WINDOW, "floor": floor, "null_max": _f(null_max),
        "n_groups_cand": n_passed, "weak_day": n_passed < 3, "groups": groups_out, "movers": movers_out,
        "pts": {"known": bool(ptsmap), "ok": pts_meta.get("ok", 0), "tried": pts_meta.get("tried", 0),
                "asof": pts_asof, "err": pts_meta.get("err", {}), "skipped": bool(pts_meta.get("skipped")), "src": pts_src},
        "iss_known": bool(iss), "alert_known": bool(alert),
        "rules": RULES, "stats": STATS, "elapsed_s": round(time.time() - t0, 1),
    }

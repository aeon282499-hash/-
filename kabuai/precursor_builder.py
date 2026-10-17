# -*- coding: utf-8 -*-
"""precursor_builder.py — 🧪「C/D に材料が出た」（v7.7・2026-10-17 本人「新規にDやCの銘柄にセクターやIRがでたとか」）。
まだ走っていない（モメンタムランク C/D）銘柄のうち、今日
  📰 IR が出た（📰IR の判定が 好材料 / 要精査 / 悪材料・中立と事務は除く）
  🔥 株探の人気テーマ（ベスト30・kabutan_hot.json）のメンバー（themes_dict.json で引く）で、出来高か値が動き始めた
  💴 資金セクターの熱いグループ（flow.groups）のメンバーで、出来高か値が動き始めた
のどれかに当たる物を並べる。「動き始めた」＝当日出来高が20日平均の2倍以上 または 前日比+3%以上。
10年BT（_bt_cd_to_a_1017.py）: C/D が20日以内に A 以上になる率は 0.8%・初動ブレイク点灯日でも 2.5%＝この一覧も『候補を並べる道具』であって予測ではない。
出力: latest.json["precursor"] = {date, n_cd, rows:[{code,name,grade,momentum,price,r1,vol_x,turnover_oku,sector,ir,themes,groups,n_trig,moving}], counts, note}
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
MIN_TOV_OKU = 1.0          # 代金20日平均 1億以上（初動ブレイクと同じ）
MOVE_VOLX = 2.0            # 当日出来高÷20日平均
MOVE_R1 = 3.0              # 前日比 %
MAX_ROWS = 80


def _load(path: Path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def build(rows: list[dict], ir: dict | None, flow: dict | None, data_date: str) -> dict:
    hot = _load(HERE / "kabutan_hot.json") or {}
    themes = (_load(HERE / "themes_dict.json") or {}).get("themes") or {}
    hot_themes = [(t.get("rank"), t.get("name")) for t in (hot.get("themes") or []) if t.get("name")]
    theme_of: dict[str, list[dict]] = {}
    for rank, name in hot_themes:
        for code in themes.get(name) or []:
            theme_of.setdefault(str(code), []).append({"rank": rank, "name": name})
    group_of: dict[str, list[str]] = {}
    for g in (flow or {}).get("groups") or []:
        if not g.get("passed", True):
            continue
        for m in (g.get("members") or []) + (g.get("members_out") or []):
            c = str(m.get("code") or "")
            if c:
                group_of.setdefault(c, []).append(f"{g.get('kind', '')}{g.get('label', '')}")
    ir_of: dict[str, dict] = {}
    if ir and not ir.get("error"):
        today = (ir.get("window") or [""])[0]
        for r in ir.get("rows") or []:
            if r.get("date") != today:
                continue
            f = r.get("final") or r.get("rule") or {}
            lab = f.get("label")
            if lab not in ("好材料", "要精査", "悪材料"):
                continue
            cur = ir_of.get(r["code"])
            rank = {"好材料": 0, "要精査": 1, "悪材料": 2}[lab]
            if cur is None or rank < cur["_rank"]:
                ir_of[r["code"]] = {"_rank": rank, "label": lab, "score": f.get("score"), "title": (r.get("title") or "")[:80], "time": r.get("time"),
                                    "intraday": bool(r.get("intraday")), "pdf": r.get("pdf")}
    out = []
    n_cd = 0
    for r in rows:
        if r.get("grade") not in ("C", "D"):
            continue
        tov = r.get("turnover_oku")
        if tov is None or tov < MIN_TOV_OKU:
            continue
        n_cd += 1
        code = str(r.get("code"))
        volx = r.get("vol_x")
        if volx is None:
            volx = r.get("vr")
        r1 = r.get("r1")
        moving = bool((volx is not None and volx >= MOVE_VOLX) or (r1 is not None and r1 >= MOVE_R1))
        irx = ir_of.get(code)
        th = theme_of.get(code) or []
        gr = group_of.get(code) or []
        trig = []
        if irx:
            trig.append("ir")
        if th and moving:
            trig.append("theme")
        if gr and moving:
            trig.append("flow")
        if not trig:
            continue
        out.append({"code": code, "name": r.get("name"), "grade": r.get("grade"), "momentum": r.get("momentum"), "price": r.get("price"),
                    "r1": r1, "vol_x": (round(float(volx), 2) if volx is not None else None), "turnover_oku": tov, "sector": r.get("sector"),
                    "ir": ({k: v for k, v in irx.items() if not k.startswith("_")} if irx else None),
                    "themes": sorted(th, key=lambda x: (x["rank"] or 99))[:4], "groups": gr[:3], "trig": trig, "n_trig": len(trig), "moving": moving})
    def key(x):
        irr = x["ir"]
        ir_rank = {"好材料": 0, "要精査": 1, "悪材料": 3}.get((irr or {}).get("label"), 2)
        return (ir_rank, -x["n_trig"], -(x["vol_x"] or 0))
    out.sort(key=key)
    counts = {"ir": sum(1 for x in out if x["ir"]), "theme": sum(1 for x in out if "theme" in x["trig"]), "flow": sum(1 for x in out if "flow" in x["trig"])}
    return {"date": data_date, "n_cd": n_cd, "n": len(out), "rows": out[:MAX_ROWS], "counts": counts,
            "hot_time": hot.get("page_time"), "n_hot_themes": len(hot_themes), "n_groups": len({g for v in group_of.values() for g in v}),
            "note": "まだ走っていない（ランクC/D・代金1億以上）銘柄のうち、今日 IR が出た／株探の人気テーマや資金セクターの熱いグループの仲間で動き始めた（出来高2倍か+3%）物。"
                    "10年BT: C/Dが20日以内にA以上になる率は0.8%・初動ブレイク点灯日でも2.5%＝予測ではなく、調べる候補の一覧。"}

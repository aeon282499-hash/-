# -*- coding: utf-8 -*-
"""_edinet_lvh_fetch.py — 大量保有報告書(docTypeCode=350)の本文CSV一括取得（2026-08-25）。

本人「あらたに儲かる売買シグナルないかなぁ」→次点候補メモ（PEAD検証時）の
「EDINET大量保有イベントドライブ・データは手元・未検証」を検証するための素材作り。
edinet_docs_2022_2026.json の350型5,414件について type=5 CSV を落とし、
発行会社(=買われた銘柄)・株券等保有割合(今回/前回)・保有目的・報告義務発生日を抽出して
_edinet_lvh.jsonl に1行1書類で追記（チェックポイント式・再実行で続きから）。

実行: python -X utf8 _edinet_lvh_fetch.py   （約0.55秒/件 ≒ 50〜60分）
"""
from __future__ import annotations

import io
import json
import os
import time
import zipfile

import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(".env")
KEY = os.getenv("EDINET_API_KEY", "").strip()
assert KEY, "EDINET_API_KEY 未設定"
OUT = "_edinet_lvh.jsonl"
API = "https://api.edinet-fsa.go.jp/api/v2/documents/{}"


def parse_csv(raw: bytes) -> dict:
    txt = raw.decode("utf-16-le", errors="replace")
    icode = iname = purpose = oblig = None
    ratios, prevs = [], []
    for ln in txt.splitlines()[1:]:
        cols = [c.strip('"') for c in ln.split("\t")]
        if len(cols) < 9:
            continue
        eid, val = cols[0], cols[8]
        if "SecurityCodeOfIssuer" in eid and not icode:
            icode = val.strip()
        elif "NameOfIssuer" in eid and not iname:
            iname = val.strip()
        elif "PurposeOfHolding" in eid and not purpose:
            purpose = val.strip()[:80]
        elif "DateWhenFilingRequirementArose" in eid and not oblig:
            oblig = val.strip()[:10]
        elif "HoldingRatioOfShareCertificatesEtc" in eid:
            try:
                v = float(val)
            except ValueError:
                continue
            (prevs if "Prev" in eid else ratios).append(v)
    return {"icode": icode, "iname": iname, "purpose": purpose, "oblig": oblig,
            "ratio": max(ratios) if ratios else None,
            "prev": max(prevs) if prevs else None}


def main() -> None:
    docs = []
    d = json.load(open("edinet_docs_2022_2026.json", encoding="utf-8"))
    for day, lst in sorted(d.items()):
        for x in lst:
            if x.get("docTypeCode") == "350":
                docs.append(x)
    done = set()
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            for ln in f:
                try:
                    done.add(json.loads(ln)["docID"])
                except Exception:
                    pass
    todo = [x for x in docs if x["docID"] not in done]
    print(f"[lvh] 対象{len(docs):,}件 / 取得済{len(done):,} / 残り{len(todo):,}", flush=True)
    f = open(OUT, "a", encoding="utf-8")
    t0 = time.time()
    for i, x in enumerate(todo):
        rec = {"docID": x["docID"], "submit": x.get("submitDateTime"),
               "filer": x.get("filerName"), "form": x.get("formCode")}
        for attempt in range(3):
            try:
                r = requests.get(API.format(x["docID"]),
                                 params={"type": 5, "Subscription-Key": KEY},
                                 timeout=60, verify=False)
                if r.status_code == 429:
                    time.sleep(30)
                    continue
                if r.status_code != 200:
                    rec["err"] = f"http{r.status_code}"
                    break
                z = zipfile.ZipFile(io.BytesIO(r.content))
                names = [n for n in z.namelist() if n.endswith(".csv")]
                if not names:
                    rec["err"] = "nocsv"
                    break
                rec.update(parse_csv(z.read(names[0])))
                break
            except Exception as e:
                if attempt == 2:
                    rec["err"] = str(e)[:80]
                else:
                    time.sleep(10)
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if (i + 1) % 200 == 0:
            f.flush()
            el = time.time() - t0
            print(f"  {i+1:,}/{len(todo):,} ({el/60:.0f}分・残り約{el/(i+1)*(len(todo)-i-1)/60:.0f}分)", flush=True)
        time.sleep(0.45)
    f.close()
    print("[lvh] 完了", flush=True)


if __name__ == "__main__":
    main()

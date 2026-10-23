# -*- coding: utf-8 -*-
"""biz_text_pc.py — 全上場会社の有報「事業の内容」「研究開発活動」を EDINET から取って、🔍銘柄検索の「事業内容で探す」に使う
（2026-10-22 本人「PHP銘柄さがせていないのに腹がたってる」→ ニュースの単語から「その語が事業内容に入っている会社」を引く索引）。

なぜ: 株探の人気テーマにも TDnet の開示にも「PHP」は出ない。5250 GMOプライム・ストラテジー（KUSANAGI＝PHP実行環境）は
      有報の事業の内容にしか書いていない。10/21 は終日 1,099円で動かず、翌 10/22 にストップ高＝丸一日の窓を、索引が無くて逃した。
何を: EDINET API v2 の書類一覧キャッシュ（../edinet_docs_2022_2026.json ＋ ../edinet_docs_2026H2.json）から
      各 secCode の最新の有価証券報告書（docTypeCode 120 / formCode 030000）を選び、type=5（XBRL→CSV）の zip を取って
        jpcrp_cor:DescriptionOfBusinessTextBlock（事業の内容）        → t（先頭 2,000字）
        jpcrp_cor:ResearchAndDevelopmentActivitiesTextBlock（研究開発活動）→ r（先頭 600字）
      を HTML から本文だけにして貯める。
どこへ: kabuai/biz/<コード先頭1文字>.json（10分割・画面は検索した時だけ順に読む）＋ kabuai/biz/index.json。
        data/ ではなく kabuai/biz/ に置く＝git にコミットして Pages にそのまま乗る（有報は年1回・月1更新で足りる）。
使い方: python -X utf8 kabuai/biz_text_pc.py            # 取得（途中再開可・_biz_cache.json）→ biz/ を書く
        python -X utf8 kabuai/biz_text_pc.py --build    # 取得せず手元のキャッシュから biz/ だけ書き直す
        python -X utf8 kabuai/biz_text_pc.py --limit 50 # 試し
"""
from __future__ import annotations

import csv
import html
import io
import json
import os
import re
import sys
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
JST = timezone(timedelta(hours=9))
DOC_LISTS = [ROOT / "edinet_docs_2022_2026.json", ROOT / "edinet_docs_2026H2.json"]
CACHE = HERE / "_biz_cache.json"
OUT_DIR = HERE / "biz"
LOG = HERE / "_biz_text_pc.log"
API = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc}"
T_CHARS = 6000   # 10/23 本人「子会社や株主関係や仕入れ販売関係まで調べたか」→ 2,000字で1,287社が切れていた。6,000字で全社ほぼ全文
R_CHARS = 600
A_CHARS = 1500   # 関係会社の状況（子会社・親会社・その他の関係会社）
S_CHARS = 900    # 大株主の状況
C_CHARS = 500    # 主要な顧客（販売先・割合）
M_CHARS = 6000   # 企業結合の注記＝買った子会社の事業の中身（10/23 4260ドコドアの「WordPress型開発」はここにしか無かった）
THREADS = 3
PAUSE = 0.15
EL_BIZ = "jpcrp_cor:DescriptionOfBusinessTextBlock"
EL_RD = "jpcrp_cor:ResearchAndDevelopmentActivitiesTextBlock"
EL_AFF = "jpcrp_cor:OverviewOfAffiliatedEntitiesTextBlock"
EL_SH = "jpcrp_cor:MajorShareholdersTextBlock"
EL_M_SUB = "NotesBusinessCombinations"   # IFRS/JGAAP で要素名が違うので部分一致
_CUST_RE = re.compile(r"主要な顧客(?:ごと|に関する)の?情報.{0,500}")

_WS = re.compile(r"\s+")
_JA_SP = re.compile(r"(?<=[^\x20-\x7e]) | (?=[^\x20-\x7e])")


def log(msg: str) -> None:
    line = f"{datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def normalize(text: str) -> str:
    """ir_text.normalize と同じ: NFKC → 空白1つ → 日本語の間の空白を落とす（英単語の間は残す）。"""
    t = unicodedata.normalize("NFKC", text or "")
    t = _WS.sub(" ", t).strip()
    t = _JA_SP.sub("", t)
    return t


def html_to_text(h: str) -> str:
    h = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", h or "")
    h = re.sub(r"(?i)<br\s*/?>|</p>|</tr>|</li>|</div>|</h\d>|</td>", " ", h)
    h = re.sub(r"<[^>]+>", "", h)
    t = normalize(html.unescape(h))
    return re.sub(r"^\d*【(事業の内容|研究開発活動)】", "", t)   # 先頭の「3【事業の内容】」は落とす


def load_key() -> str:
    key = os.environ.get("EDINET_API_KEY", "").strip()
    if not key:
        try:
            for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
                if line.startswith("EDINET_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    if not key:
        raise SystemExit("EDINET_API_KEY が .env にありません")
    return key


def latest_reports() -> dict[str, dict]:
    """4桁コード → 最新の有報レコード。"""
    latest: dict[str, dict] = {}
    for p in DOC_LISTS:
        if not p.exists():
            log(f"[warn] 書類一覧が無い: {p.name}")
            continue
        for recs in json.load(open(p, encoding="utf-8")).values():
            for r in recs:
                if r.get("docTypeCode") != "120" or r.get("formCode") != "030000" or not r.get("secCode"):
                    continue
                code = str(r["secCode"])[:4]
                if code not in latest or str(r.get("submitDateTime") or "") > str(latest[code].get("submitDateTime") or ""):
                    latest[code] = r
    return latest


def clean_name(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    return re.sub(r"株式会社|\(株\)", "", s).strip()


def fetch_doc(sess: requests.Session, key: str, doc: str) -> dict:
    """{"t":事業の内容, "r":研究開発, "a":関係会社, "s":大株主, "c":主要顧客, "e":エラー}。"""
    err = ""
    for attempt in range(4):
        try:
            r = sess.get(API.format(doc=doc), params={"type": 5, "Subscription-Key": key}, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code != 200:
                return {"e": f"http{r.status_code}"}
            z = zipfile.ZipFile(io.BytesIO(r.content))
            names = [n for n in z.namelist() if "jpcrp" in n and n.lower().endswith(".csv")]
            if not names:
                return {"e": "csv無し"}
            biz = rd = aff = sh = cust = mna = ""
            for n in names:
                txt = z.read(n).decode("utf-16", errors="ignore")
                for row in csv.reader(io.StringIO(txt), delimiter="	"):
                    if not row or not row[-1]:
                        continue
                    el = row[0]
                    if el == EL_BIZ and not biz:
                        biz = html_to_text(row[-1])
                    elif el == EL_RD and not rd:
                        rd = html_to_text(row[-1])
                    elif el == EL_AFF and not aff:
                        aff = re.sub(r"^\d*【関係会社の状況】", "", html_to_text(row[-1]))
                    elif el == EL_SH and not sh:
                        sh = re.sub(r"^\(\d+\)【大株主の状況】", "", html_to_text(row[-1]))
                    elif EL_M_SUB in el and "TextBlock" in el:
                        mna = (mna + " " + html_to_text(row[-1]))[:M_CHARS]
                    elif not cust and "TextBlock" in el and "Segment" in el:
                        m = _CUST_RE.search(html_to_text(row[-1]))
                        if m:
                            cust = m.group(0)
            if not biz and not rd:
                return {"e": "本文なし"}
            return {"t": biz[:T_CHARS], "r": rd[:R_CHARS], "a": aff[:A_CHARS], "s": sh[:S_CHARS], "c": cust[:C_CHARS], "m": mna[:M_CHARS], "e": ""}
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:60]}"
            time.sleep(3 * (attempt + 1))
    return {"e": err}


def load_cache() -> dict:
    try:
        j = json.load(open(CACHE, encoding="utf-8"))
        if isinstance(j, dict) and isinstance(j.get("items"), dict):
            return j
    except Exception:
        pass
    return {"updated_at": "", "items": {}}


def save_cache(c: dict) -> None:
    c["updated_at"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    tmp = CACHE.with_suffix(".tmp")
    tmp.write_text(json.dumps(c, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(CACHE)


def build(cache: dict, latest: dict[str, dict]) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    shards: dict[str, list] = {str(d): [] for d in range(10)}
    n_text = 0
    for code, rec in sorted(latest.items()):
        it = cache["items"].get(rec["docID"])
        if not it or not (it.get("t") or it.get("r")):
            continue
        n_text += 1
        m = re.search(r"\((\d{4}/\d{2}/\d{2})-(\d{4}/\d{2}/\d{2})\)", unicodedata.normalize("NFKC", rec.get("docDescription") or ""))
        shards.setdefault(code[0], []).append({
            "code": code, "name": clean_name(rec.get("filerName") or ""), "fy": m.group(2) if m else "",
            "doc": rec["docID"], "t": it.get("t") or "", "r": it.get("r") or "",
        })   # a/s/c/m は PC の _biz_cache.json にだけ置く（biz_grep_pc.py で引く）。shard に入れると 43MB になりアプリの検索が重い
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    idx = []
    for k, rows in sorted(shards.items()):
        p = OUT_DIR / f"{k}.json"
        p.write_text(json.dumps({"k": k, "updated": now, "n": len(rows), "rows": rows}, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        idx.append({"k": k, "n": len(rows), "kb": round(p.stat().st_size / 1024)})
    (OUT_DIR / "index.json").write_text(json.dumps({
        "updated": now, "n": len(latest), "n_text": n_text, "shards": idx,
        "source": "EDINET 有価証券報告書（各社の最新）・事業の内容 先頭6,000字＋研究開発活動 600字（関係会社・大株主・主要顧客・企業結合は PC の biz_grep_pc.py）",
    }, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"[build] 会社 {len(latest)}・本文あり {n_text}・合計 {sum(x['kb'] for x in idx):,}KB → {OUT_DIR}")


def main() -> None:
    args = sys.argv[1:]
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else 0
    latest = latest_reports()
    log(f"[plan] 有報あり {len(latest)}社")
    cache = load_cache()
    if "--build" not in args:
        key = load_key()
        items = cache["items"]
        todo = [r for c, r in sorted(latest.items()) if r["docID"] not in items
                or (not items[r["docID"]].get("t") and not items[r["docID"]].get("r") and int(items[r["docID"]].get("n") or 0) < 2
                    and not str(items[r["docID"]].get("e") or "").startswith("本文なし"))
                or "--refetch" in args
                or ("m" not in items[r["docID"]])                       # 旧形式（関係会社・大株主・顧客・企業結合なし）
                or len(items[r["docID"]].get("t") or "") == 2000]       # 旧上限で切れていた
        if limit:
            todo = todo[:limit]
        log(f"[plan] 取得 {len(todo)}件（キャッシュ {len(items)}件）")
        sess = requests.Session()
        t0 = time.monotonic()
        done = ok = 0

        def work(rec):
            time.sleep(PAUSE)
            return rec["docID"], fetch_doc(sess, key, rec["docID"])

        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            futs = [ex.submit(work, r) for r in todo]
            for f in as_completed(futs):
                doc, got = f.result()
                prev = items.get(doc) or {}
                items[doc] = {"t": got.get("t", ""), "r": got.get("r", ""), "a": got.get("a", ""), "s": got.get("s", ""), "c": got.get("c", ""), "m": got.get("m", ""),
                              "e": got.get("e", ""), "n": int(prev.get("n") or 0) + 1}
                done += 1
                ok += 1 if (got.get("t") or got.get("r")) else 0
                if done % 100 == 0 or done == len(todo):
                    save_cache(cache)
                    el = time.monotonic() - t0
                    log(f"[fetch] {done}/{len(todo)} 本文あり {ok}・{el:.0f}s・残り約 {el / done * (len(todo) - done) / 60:.0f}分")
        save_cache(cache)
    build(cache, latest)


if __name__ == "__main__":
    main()

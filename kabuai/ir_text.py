# -*- coding: utf-8 -*-
"""ir_text.py — 📰IR の「開示検索」用に TDnet の PDF 本文（冒頭3ページ・2,000字）を取って置いておく（v7.6・2026-10-17 本人
「開示検索…コメント検索できるといい 例えば フィジカルAIとかエヌヴィディア サイバーセキュリティとか」）。
表題だけでは「エヌヴィディア」「フィジカルAI」は出てこない（本文にしか無い）ので、本文の冒頭を検索対象にする。

仕組み:
  - ir_builder.build_ir が答え合わせ用に取った全開示（直近 STATS_DAYS 営業日）を受け取り、未取得の PDF だけを新しい順に取る。
  - 取った本文は kabuai/ir_text.json に貯める（Actions では actions/cache で持ち回り・KEEP_DAYS で古い物を落とす）。
    1回のビルドの上限は時間 KABUAI_IR_TEXT_DEADLINE 秒／件数 KABUAI_IR_TEXT_MAX。足りない分は次のビルドが続きを取る（1日4便）。
  - 画面用に data/ir_text/<日付>.json（その日の全開示＋本文）と data/ir_text/index.json（日付一覧）を書く。
    latest.json には入れない（1日≈2MB・検索した時だけ読む）。
  - 本文の空白は、日本語の間（PDF 抽出の「株 式 会 社」）だけ落として英単語の間は残す。検索側は両方の空白を全部落として照合する。

実測（2026-10-17・8スレッド）: 60件 9.8秒・平均279KB・1,556字・画像PDF 0。
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
JST = timezone(timedelta(hours=9))
CACHE_PATH = HERE / "ir_text.json"
OUT_DIR = HERE / "data" / "ir_text"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}
MAX_PAGES = int(os.environ.get("KABUAI_IR_TEXT_PAGES", "3"))
MAX_CHARS = int(os.environ.get("KABUAI_IR_TEXT_CHARS", "2000"))
DEADLINE_SEC = float(os.environ.get("KABUAI_IR_TEXT_DEADLINE", "200"))
MAX_NEW = int(os.environ.get("KABUAI_IR_TEXT_MAX", "1200"))
THREADS = int(os.environ.get("KABUAI_IR_TEXT_THREADS", "8"))
KEEP_DAYS = 30
MAX_PDF_BYTES = 12 * 1024 * 1024
RETRY_MAX = 2            # 取れなかった PDF（タイムアウト等）を再挑戦する回数

logging.getLogger("pypdf").setLevel(logging.ERROR)     # 「Ignoring wrong pointing object」の洪水を止める

_WS = re.compile(r"\s+")
_JA_SP = re.compile(r"(?<=[^\x20-\x7e]) | (?=[^\x20-\x7e])")   # 片側が非ASCII（日本語）の空白だけ落とす


def normalize(text: str) -> str:
    """NFKC → 空白を1つに → 日本語の間の空白を落とす。英単語の間の空白は残す。"""
    t = unicodedata.normalize("NFKC", text or "")
    t = _WS.sub(" ", t).strip()
    t = _JA_SP.sub("", t)
    return t


def load_cache() -> dict:
    try:
        with open(CACHE_PATH, encoding="utf-8") as f:
            j = json.load(f)
        if isinstance(j, dict) and isinstance(j.get("items"), dict):
            return j
    except Exception:
        pass
    return {"updated_at": "", "items": {}}


def save_cache(j: dict) -> None:
    cutoff = (datetime.now(JST).date() - timedelta(days=KEEP_DAYS)).isoformat()
    j["items"] = {k: v for k, v in j["items"].items() if str(v.get("d") or "9999") >= cutoff}
    j["updated_at"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    CACHE_PATH.write_text(json.dumps(j, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def pdf_text(url: str, timeout: float = 25.0) -> tuple[str, int, str]:
    """(本文, 総ページ数, エラー)。本文が取れなければ ''。"""
    if not url:
        return "", 0, "pdf無し"
    try:
        import requests
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code != 200:
            return "", 0, f"http{r.status_code}"
        if len(r.content) > MAX_PDF_BYTES:
            return "", 0, "12MB超"
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(r.content))
        n = len(reader.pages)
        parts = []
        for page in reader.pages[:MAX_PAGES]:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        text = normalize("\n".join(parts))
        if len(text) < 20:
            return "", n, "画像PDF"
        return text[:MAX_CHARS], n, ""
    except Exception as e:
        return "", 0, f"{type(e).__name__}: {str(e)[:60]}"


def _transient(err: str) -> bool:
    return err.startswith("http5") or "Timeout" in err or "Connection" in err or err.startswith("http429")


def update_cache(rows: list[dict], deadline_sec: float = DEADLINE_SEC, max_new: int = MAX_NEW, threads: int = THREADS) -> dict:
    """未取得の PDF を新しい順に取ってキャッシュに足す。戻り値はキャッシュ（items: id → {d,t,p,e,n}）。"""
    t0 = time.monotonic()
    cache = load_cache()
    items = cache["items"]
    todo = []
    for r in sorted(rows, key=lambda x: (x["date"], x["time"]), reverse=True):
        it = items.get(r["id"])
        if it is None:
            todo.append(r)
        elif not it.get("t") and it.get("e") and _transient(it["e"]) and int(it.get("n") or 0) < RETRY_MAX:
            todo.append(r)
    todo = todo[:max_new]
    n_ok = n_ng = 0
    if todo:
        def work(r):
            text, pages, err = pdf_text(r.get("pdf") or "")
            return r["id"], r["date"], text, pages, err
        with ThreadPoolExecutor(max_workers=threads) as ex:
            chunk = threads * 4
            for i in range(0, len(todo), chunk):
                if time.monotonic() - t0 > deadline_sec:
                    break
                for did, d, text, pages, err in ex.map(work, todo[i:i + chunk]):
                    prev = items.get(did) or {}
                    items[did] = {"d": d, "t": text, "p": pages, "e": err, "n": int(prev.get("n") or 0) + 1}
                    if text:
                        n_ok += 1
                    else:
                        n_ng += 1
    save_cache(cache)
    cache["_run"] = {"todo": len(todo), "ok": n_ok, "ng": n_ng, "elapsed_s": round(time.monotonic() - t0)}
    return cache


def write_search_files(rows: list[dict], cache: dict) -> dict:
    """data/ir_text/<日付>.json と index.json を書く。rows は ir_builder の rows_all（最終判定・反応つき）。"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUT_DIR.glob("*.json"):
        try:
            old.unlink()
        except Exception:
            pass
    items = cache.get("items") or {}
    by_day: dict[str, list[dict]] = {}
    for r in rows:
        by_day.setdefault(r["date"], []).append(r)
    index = []
    n_total = n_text = 0
    for d in sorted(by_day, reverse=True):
        out_rows = []
        k_text = 0
        for r in sorted(by_day[d], key=lambda x: (x["time"], x["code"]), reverse=True):
            it = items.get(r["id"]) or {}
            t = it.get("t") or ""
            if t:
                k_text += 1
            f = r.get("final") or r.get("rule") or {}
            react = r.get("react") or {}
            out_rows.append({
                "id": r["id"], "time": r["time"], "code": r["code"], "name": r["name"], "title": r["title"][:140], "pdf": r.get("pdf"),
                "tags": r.get("tags") or [], "intraday": bool(r.get("intraday")), "label": f.get("label"), "score": f.get("score"),
                "react": {"day_chg": react.get("day_chg"), "next_gap": react.get("next_gap"), "next_chg": react.get("next_chg"), "next_date": react.get("next_date")},
                "t": t, "p": it.get("p") or 0,
            })
        payload = {"date": d, "n": len(out_rows), "n_text": k_text, "rows": out_rows}
        with open(OUT_DIR / f"{d}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        index.append({"date": d, "n": len(out_rows), "n_text": k_text})
        n_total += len(out_rows)
        n_text += k_text
    meta = {"days": index, "n": n_total, "n_text": n_text, "pending": n_total - n_text,
            "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "pages": MAX_PAGES, "chars": MAX_CHARS,
            "note": f"本文は PDF の冒頭{MAX_PAGES}ページ・{MAX_CHARS:,}字まで。未取得分は次のビルドで続きを取る。"}
    with open(OUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, separators=(",", ":"))
    return meta


def build_search(rows: list[dict], deadline_sec: float = DEADLINE_SEC) -> dict:
    """ir_builder から呼ぶ入口。キャッシュ更新 → 画面用ファイル。戻り値は latest.json["ir"]["search"] 用のメタ。"""
    cache = update_cache(rows, deadline_sec=deadline_sec)
    meta = write_search_files(rows, cache)
    meta["run"] = cache.get("_run")
    return meta


if __name__ == "__main__":
    # 手元テスト: python -X utf8 ir_text.py [_ir_test.json] [秒]  → data/ir_text/ を作る
    import sys
    src = sys.argv[1] if len(sys.argv) > 1 else "_ir_test.json"
    sec = float(sys.argv[2]) if len(sys.argv) > 2 else 60.0
    with open(src, encoding="utf-8") as f:
        j = json.load(f)
    rows = j["rows"] if isinstance(j, dict) else j
    m = build_search(rows, deadline_sec=sec)
    print(json.dumps(m, ensure_ascii=False, indent=1))

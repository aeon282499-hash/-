# -*- coding: utf-8 -*-
"""kabutan_themes.py — 株探の「テーマ別銘柄一覧」を巡回して テーマ→銘柄コード の辞書を作る（本人用・2026-09-14 本人指示「やれや」）。

対象: sitemap-base1.xml に載る /themes/?theme=◯◯ 全テーマ（約1,500）。各テーマは50銘柄/ページで
      複数ページ（例: 半導体=8ページ）。robots.txt の Crawl-delay: 3 を守り 1要求/3秒＝全件で約2時間。
      本人専用（再配布なし）・辞書は kabutan_themes.json に保存し、途中保存で再開可能。
使い方: python -X utf8 kabutan_themes.py            # 全テーマ（既存JSONがあれば7日以内のテーマは飛ばす）
        python -X utf8 kabutan_themes.py --force    # 全部取り直し
        python -X utf8 kabutan_themes.py --limit 20 # 動作確認
出力: kabutan_themes.json {"fetched": ..., "themes": {name: {"members": [code...], "n": N, "fetched": ts}}}
利用: tachibana_live_flow.py が起動時に読み、手作り34テーマ（theme_members.json）と合わせて資金フローを集計。
Windowsタスク: KabutanThemes（日曜 03:00・週1更新）
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
OUT = ROOT / "kabutan_themes.json"
LOG = ROOT / "logs" / "kabutan_themes.log"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
      "Accept-Language": "ja,en;q=0.8"}
DELAY = 3.2            # robots.txt Crawl-delay: 3
CODE_RE = re.compile(r'href="/stock/\?code=([1-9][0-9][0-9][0-9A-Z])"')
THEME_RE = re.compile(r'href="/themes/\?theme=([^"&]+)')


def log_setup():
    LOG.parent.mkdir(exist_ok=True)
    lg = logging.getLogger("kabutan")
    lg.setLevel(logging.INFO)
    if not lg.handlers:
        fh = logging.FileHandler(LOG, encoding="utf-8"); fh.setFormatter(logging.Formatter("%(asctime)s %(message)s")); lg.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout); sh.setFormatter(logging.Formatter("%(asctime)s %(message)s")); lg.addHandler(sh)
    return lg


def get(url: str, s: requests.Session, log, tries: int = 3) -> str:
    for i in range(tries):
        try:
            r = s.get(url, headers=UA, timeout=30)
            if r.status_code == 200:
                return r.text
            log.warning(f"HTTP {r.status_code} {url}")
            if r.status_code in (403, 429, 503):
                time.sleep(60)
        except Exception as e:  # noqa: BLE001
            log.warning(f"通信失敗({i+1}): {e}")
            time.sleep(10)
        time.sleep(DELAY)
    return ""


def theme_list(s: requests.Session, log) -> list[str]:
    x = get("https://kabutan.jp/sitemap-base1.xml", s, log)
    names = []
    for loc in re.findall(r"<loc>(.*?)</loc>", x):
        if "/themes/?theme=" in loc:
            names.append(urllib.parse.unquote(loc.split("theme=", 1)[1]))
    names = sorted(set(names))
    log.info(f"テーマ一覧 {len(names)}件（sitemap-base1）")
    return names


def fetch_theme(name: str, s: requests.Session, log) -> tuple[list[str], int]:
    codes: list[str] = []
    seen = set()
    pages = 0
    for page in range(1, 40):
        url = f"https://kabutan.jp/themes/?theme={urllib.parse.quote(name)}&market=0&capitalization=-1&dispmode=normal&stc=&stm=0&page={page}"
        h = get(url, s, log)
        pages += 1
        if not h:
            break
        # 銘柄一覧テーブルだけを対象にする（ヘッダの指数リンク等を除く）
        i = h.find("stock_st_table") if "stock_st_table" in h else h.find("<table")
        body = h[i:] if i >= 0 else h
        new = []
        for c in CODE_RE.findall(body):      # 1行に同じコードのリンクが2つあるので順序を保って重複排除
            if c not in seen:
                seen.add(c); codes.append(c); new.append(c)
        # 次ページのリンクが無ければ終了
        if f"page={page+1}" not in h or not new:
            break
        time.sleep(DELAY)
    return codes, pages


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-age-days", type=int, default=7)
    a = ap.parse_args()
    log = log_setup()
    data = {"fetched": None, "themes": {}}
    if OUT.exists() and not a.force:
        try:
            data = json.load(open(OUT, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
    s = requests.Session()
    names = theme_list(s, log)
    if a.limit:
        names = names[:a.limit]
    cutoff = (datetime.now() - timedelta(days=a.max_age_days)).strftime("%Y-%m-%d")
    todo = [n for n in names if a.force or n not in data["themes"] or (data["themes"][n].get("fetched", "") < cutoff)]
    log.info(f"取得対象 {len(todo)}/{len(names)}（{a.max_age_days}日以内の既存はスキップ）・見込み {len(todo)*DELAY*1.6/60:.0f}分")
    t0 = time.time()
    for i, name in enumerate(todo, 1):
        time.sleep(DELAY)
        codes, pages = fetch_theme(name, s, log)
        data["themes"][name] = {"members": codes, "n": len(codes), "pages": pages, "fetched": datetime.now().strftime("%Y-%m-%d")}
        if i % 10 == 0 or i == len(todo):
            data["fetched"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            OUT.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            log.info(f"{i}/{len(todo)} {name} {len(codes)}銘柄/{pages}p  経過{(time.time()-t0)/60:.0f}分")
    data["fetched"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    OUT.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    n = len(data["themes"]); m = sum(v["n"] for v in data["themes"].values())
    log.info(f"完了: {n}テーマ・延べ{m}銘柄 → {OUT.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# -*- coding: utf-8 -*-
"""kabutan_hot_pc.py — 株探の「人気テーマ【ベスト30】（3日間のアクセスランキング）」を このPCから取って GitHub に置く
（2026-10-17 本人「株たんみながら人気テーマを取得できるようにして」→ 📰IRの🔍開示検索の例チップに使う）。

なぜPC: GitHub Actions のIPからは株探が 405 を返す（pts_fetch_pc.py と同じ事情）。
何を:   https://kabutan.jp/info/accessranking/3_2 の 順位・テーマ名・代表3銘柄 を kabuai/kabutan_hot.json に書き、
        GitHub Contents API（gh api PUT）で main に直接コミット → kabuai.yml のビルドが走り latest.json["ir"]["hot_themes"] に載る。
いつ:   Windowsタスク「KabuAI_HotThemes」平日 08:15 と 16:30（ログオン中のみ）＋ pts_fetch_pc.py（23:50）の最後にも呼ぶ。
使い方: python -X utf8 kabuai/kabutan_hot_pc.py            # 取得→アップロード
        python -X utf8 kabuai/kabutan_hot_pc.py --dry-run  # 取得だけ
"""
from __future__ import annotations

import base64
import html
import json
import re
import subprocess
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
JST = timezone(timedelta(hours=9))
URL = "https://kabutan.jp/info/accessranking/3_2"
REPO = "aeon282499-hash/-"
DEST = "kabuai/kabutan_hot.json"
OUT = HERE / "kabutan_hot.json"
LOG = HERE / "_kabutan_hot_pc.log"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}

_ROW = re.compile(r'<td class="acrank_url"><a href="/themes/\?theme=([^"&]+)[^"]*">([^<]+)</a></td>\s*<td>(.*?)</td>', re.S)
_STK = re.compile(r'<a href="/stock/\?code=([0-9A-Z]{4})">([^<]+)</a>')


def log(msg: str) -> None:
    line = f"{datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def fetch() -> dict:
    import requests
    r = requests.get(URL, headers=UA, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"http{r.status_code}")
    s = r.text
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2})時(\d{1,2})分", s)
    page_time = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d} {int(m.group(4)):02d}:{int(m.group(5)):02d}" if m else ""
    themes = []
    for i, (q, name, rest) in enumerate(_ROW.findall(s), 1):
        nm = html.unescape(name).strip() or urllib.parse.unquote(q)
        stocks = [{"code": c, "name": html.unescape(n).strip()} for c, n in _STK.findall(rest)][:3]
        themes.append({"rank": i, "name": nm, "stocks": stocks})
    if len(themes) < 10:
        raise RuntimeError(f"テーマが{len(themes)}件しか取れない（ページ構造が変わった？）")
    return {"fetched_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "page_time": page_time, "period": "3日間のアクセス",
            "src": "株探 人気テーマ【ベスト30】(PCから取得)", "url": URL, "themes": themes}


def upload(path: Path, message: str, dest: str = DEST) -> bool:
    """gh api で main に PUT（既存なら sha を付けて更新）。pts_fetch_pc.upload と同じ。"""
    content = base64.b64encode(path.read_bytes()).decode()
    sha = None
    try:
        r = subprocess.run(["gh", "api", f"repos/{REPO}/contents/{dest}", "--jq", ".sha"], capture_output=True, text=True, timeout=60)
        if r.returncode == 0 and r.stdout.strip():
            sha = r.stdout.strip()
    except Exception as e:
        log(f"sha取得失敗(新規扱い): {e}")
    args = ["gh", "api", "-X", "PUT", f"repos/{REPO}/contents/{dest}", "-f", f"message={message}", "-f", "branch=main", "-f", f"content={content}"]
    if sha:
        args += ["-f", f"sha={sha}"]
    r = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        log(f"アップロード失敗: {r.stderr.strip()[:300]}")
        return False
    return True


def run(dry: bool = False) -> int:
    try:
        j = fetch()
    except Exception as e:
        log(f"株探 人気テーマ 取得失敗: {e}")
        return 1
    prev = None
    try:
        prev = json.loads(OUT.read_text(encoding="utf-8"))
    except Exception:
        pass
    OUT.write_text(json.dumps(j, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    top = "・".join(t["name"] for t in j["themes"][:5])
    log(f"人気テーマ {len(j['themes'])}件（株探 {j['page_time']}）上位: {top}")
    if dry:
        log(f"dry-run: {OUT} に書いた（アップロードなし）")
        return 0
    if prev and [t["name"] for t in prev.get("themes", [])] == [t["name"] for t in j["themes"]] and prev.get("page_time") == j["page_time"]:
        log("前回と同じ順位・同じ時刻なのでアップロードしない（無駄なビルドを避ける）")
        return 0
    ok = upload(OUT, f"chore(kabuai): 株探 人気テーマ {j['page_time']}（PCから取得）[hot-themes]")
    log("アップロード成功 → kabuai.yml のビルドが走る" if ok else "アップロード失敗")
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(run("--dry-run" in sys.argv))

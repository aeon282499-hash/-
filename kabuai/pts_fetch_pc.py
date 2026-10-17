# -*- coding: utf-8 -*-
"""pts_fetch_pc.py — 夜間PTSの最終値を「このPCから」取って GitHub に置く（2026-10-14）。

なぜPC: GitHub Actions のIPからは株探が 405 を返す（10/14 01:33 のビルドで 26/26 本が http405）。
        PCの回線からは取れる（10/13 夜に 65本中38本取得・残りは約定なし）。
何を:   本番の latest.json（Pages）から表示している銘柄コード＝💴資金/連動の全銘柄＋🎯土俵＋🕯️上髭 買い/売り を集め、
        株探の個別ページ（ジャパンネクスト）から PTS 最終値・時刻を取り、kabuai/pts_latest.json を
        GitHub Contents API（gh api PUT）で main に直接コミットする（このPCのローカルgitの状態には触らない）。
        kabuai/** への push なので kabuai.yml のビルドが走り、latest.json["flow"] にPTSが載る。朝7:00/8:40の便でも同じ日付なら使われる。
いつ:   Windowsタスク「KabuAI_PTS_Night」平日 23:45（PTSナイトタイムは 23:59 まで・最終値に十分近い）。
        ログオン中のみ（他のPCタスクと同じ）。走らなかった日はPTS無しでアプリは出る。
使い方: python -X utf8 kabuai/pts_fetch_pc.py            # 取得→アップロード
        python -X utf8 kabuai/pts_fetch_pc.py --dry-run  # 取得だけ（ファイルはローカルに書く）
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import flow_builder as fb  # noqa: E402  fetch_pts を共用

JST = timezone(timedelta(hours=9))
LATEST_URL = "https://aeon282499-hash.github.io/-/data/latest.json"
REPO = "aeon282499-hash/-"
DEST = "kabuai/pts_latest.json"
MAX_CODES = 220
LOG = HERE / "_pts_fetch_pc.log"


def log(msg: str) -> None:
    line = f"{datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def target_codes(latest: dict) -> list[str]:
    codes: list[str] = []
    f = latest.get("flow") or {}
    for g in f.get("groups") or []:
        codes += [m["code"] for m in g.get("members") or []] + [m["code"] for m in g.get("members_out") or []] + [m["code"] for m in g.get("linked_out") or []] + [m["code"] for m in g.get("hidden") or []]
        if g.get("lead"):
            codes.append(g["lead"]["code"])
    for m in f.get("movers") or []:
        codes.append((m.get("lead") or {}).get("code"))
        codes += [x["code"] for x in m.get("linked") or []]
    for r in (latest.get("arena") or {}).get("rows") or []:
        codes.append(r.get("code"))
    u = latest.get("uwick") or {}
    for side in ("buy", "sell"):
        codes += [r["code"] for r in u.get(side) or []]
    seen: set[str] = set()
    out = []
    for c in codes:
        c = str(c or "")[:4]
        if len(c) == 4 and c not in seen:
            seen.add(c); out.append(c)
    return out[:MAX_CODES]


def upload(path: Path, message: str) -> bool:
    """gh api で main に PUT（既存なら sha を付けて更新）。"""
    content = base64.b64encode(path.read_bytes()).decode()
    sha = None
    try:
        r = subprocess.run(["gh", "api", f"repos/{REPO}/contents/{DEST}", "--jq", ".sha"], capture_output=True, text=True, timeout=60)
        if r.returncode == 0 and r.stdout.strip():
            sha = r.stdout.strip()
    except Exception as e:
        log(f"sha取得失敗(新規扱い): {e}")
    args = ["gh", "api", "-X", "PUT", f"repos/{REPO}/contents/{DEST}", "-f", f"message={message}", "-f", "branch=main", "-f", f"content={content}"]
    if sha:
        args += ["-f", f"sha={sha}"]
    r = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        log(f"アップロード失敗: {r.stderr.strip()[:300]}")
        return False
    return True


def main() -> int:
    dry = "--dry-run" in sys.argv
    import requests
    try:
        latest = requests.get(LATEST_URL, headers=fb.UA, timeout=30).json()
    except Exception as e:
        log(f"latest.json 取得失敗: {e}")
        return 1
    data_date = str(latest.get("data_date") or "")
    codes = target_codes(latest)
    log(f"latest {data_date} / 対象 {len(codes)} 銘柄")
    if not codes:
        return 0
    t0 = time.time()
    ptsmap, meta = fb.fetch_pts(codes, workers=6)
    asof = fb.latest_asof(ptsmap)
    log(f"PTS取得 {meta['ok']}/{meta['tried']} err={meta['err']} asof={asof} {time.time()-t0:.1f}s")
    out = {"date": data_date, "fetched_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "source": "kabutan(JNX) from PC",
           "asof": asof, "tried": meta["tried"], "ok": meta["ok"], "err": meta["err"], "pts": ptsmap}
    path = HERE / "pts_latest.json"
    path.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    if dry:
        log(f"dry-run: {path} に書いた（アップロードなし）")
        return 0
    if meta["ok"] == 0:
        log("1本も取れなかったのでアップロードしない")
        return 0
    ok = upload(path, f"chore(kabuai): 夜間PTS {data_date} {meta['ok']}本（PCから取得・{out['fetched_at']}）")
    log("アップロード成功 → kabuai.yml のビルドが走る" if ok else "アップロード失敗")
    try:   # 株探 人気テーマ（2026-10-17・📰IR 開示検索の例チップ）も同じ便で更新。失敗してもPTSの結果には影響しない
        import kabutan_hot_pc
        kabutan_hot_pc.run(dry=False)
    except Exception as e:
        log(f"人気テーマの更新はスキップ: {e}")
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())

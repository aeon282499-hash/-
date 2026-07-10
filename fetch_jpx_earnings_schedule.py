# -*- coding: utf-8 -*-
"""fetch_jpx_earnings_schedule.py — JPX公式「決算発表予定日」取得（決算持ち越しシグナル用）。

https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html
に決算期末月ごとのExcel（kessanNN_MMDD.xlsx・週次更新）が置かれる。
全Excelをダウンロードして {発表予定日: [{code,name,type}...]} に正規化し
jpx_earnings_schedule.json へ保存する。週次（毎営業日でも可）実行想定。

実行: python fetch_jpx_earnings_schedule.py
"""
from __future__ import annotations

import io
import json
import re
import sys
from datetime import date

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings()

BASE = "https://www.jpx.co.jp"
INDEX = f"{BASE}/listing/event-schedules/financial-announcement/index.html"
OUT = "jpx_earnings_schedule.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"}


def fetch_excel_links() -> list[str]:
    r = requests.get(INDEX, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    links = re.findall(r'href="([^"]+\.xlsx?)"', r.text)
    return [BASE + l if l.startswith("/") else l for l in links]


def parse_schedule(content: bytes) -> list[dict]:
    """JPXのExcel(Listシート・5行目がヘッダ)から (発表予定日, code, 会社名, 種別) を抽出。"""
    df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=4)
    df.columns = [str(c).split("\n")[0].strip() for c in df.columns]
    need = ["決算発表予定日", "コード", "会社名", "種別"]
    for col in need:
        if col not in df.columns:
            raise ValueError(f"想定外のExcel構造: {col} 列なし（列={list(df.columns)}）")
    rows = []
    for _, r in df.iterrows():
        d = pd.to_datetime(r["決算発表予定日"], errors="coerce")
        code = str(r["コード"]).strip()
        if pd.isna(d) or not code or code == "nan":
            continue
        code = code.replace(".0", "")
        if not re.fullmatch(r"[0-9A-Z]{4}", code):
            continue
        rows.append({"date": d.strftime("%Y-%m-%d"), "code": code,
                     "name": str(r["会社名"]).strip(),
                     "type": str(r["種別"]).strip()})
    return rows


def main() -> None:
    links = fetch_excel_links()
    if not links:
        print("[jpx] Excelリンクが見つからない（ページ構造変更?）→ 中止")
        sys.exit(1)
    print(f"[jpx] Excel {len(links)}本: " + ", ".join(l.rsplit('/', 1)[-1] for l in links))

    all_rows: list[dict] = []
    for url in links:
        r = requests.get(url, headers=HEADERS, timeout=60, verify=False)
        if r.status_code != 200:
            print(f"[jpx] WARN {url.rsplit('/', 1)[-1]} HTTP {r.status_code} → スキップ")
            continue
        rows = parse_schedule(r.content)
        print(f"[jpx] {url.rsplit('/', 1)[-1]}: {len(rows)}件")
        all_rows.extend(rows)

    # 同一(code,date)重複は1本化（月またぎファイルの重複対策）
    sched: dict[str, list] = {}
    seen = set()
    for x in sorted(all_rows, key=lambda x: (x["date"], x["code"])):
        key = (x["date"], x["code"])
        if key in seen:
            continue
        seen.add(key)
        sched.setdefault(x["date"], []).append(
            {"code": x["code"], "name": x["name"], "type": x["type"]})

    today = date.today().strftime("%Y-%m-%d")
    future_days = {d: len(v) for d, v in sched.items() if d >= today}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"fetched": today, "schedule": sched}, f, ensure_ascii=False, indent=1)
    print(f"[jpx] 保存: {OUT} / 全{sum(len(v) for v in sched.values())}件 "
          f"/ 今日以降 {sum(future_days.values())}件({len(future_days)}日分)")
    for d in sorted(future_days)[:10]:
        print(f"   {d}: {future_days[d]}件")


if __name__ == "__main__":
    main()

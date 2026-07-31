# -*- coding: utf-8 -*-
"""fetch_fins_history.py — J-Quants /fins/summary の10年分をキャッシュする（2026-07-31）。

用途: 決算持ち越しシグナルに「ファンダメンタル軸」（直近業績が伸びているか等）を
足せるか検証するための素材。本人発案「決算てファンダメンタルやん・直近業績が
うなぎのぼりとか」。

開示のある日だけ叩く（earnings_calendar 2本の日付の和集合＝約2,400日）。
1リクエスト約0.5秒なので通しで20分前後。途中経過を逐次保存するので再開可能。

出力: _fins_history.pkl（DataFrame）

実行: python -X utf8 fetch_fins_history.py
"""
from __future__ import annotations

import json
import os
import time
import warnings

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
urllib3.disable_warnings()
load_dotenv()

BASE = "https://api.jquants.com/v2"
HEAD = {"x-api-key": os.getenv("JQUANTS_API_KEY")}
OUT = "_fins_history.pkl"

# 解析に使う列だけ残す（生は107列あるので落とす）
KEEP = [
    "Code", "DiscDate", "DiscTime", "DocType", "CurPerType",
    "CurFYSt", "CurFYEn", "CurPerSt", "CurPerEn",
    "Sales", "OP", "OdP", "NP", "EPS", "TA", "Eq", "EqAR",
    "FSales", "FOP", "FOdP", "FNP", "FEPS",
    "RetroRst", "ChgAcEst",
    # 配当（2026-08-01追加）: 「増配基調か」を測るのに要る。
    # 初版のKEEPに入れ忘れており _fins_history.pkl に無かった。
    "DivFY", "DivAnn", "FDivAnn", "FDivFY", "NxFDivAnn", "PayoutRatioAnn",
]


def fetch_date(d: str) -> list[dict]:
    rows, key = [], None
    for _ in range(50):                      # ページネーション上限
        p = {"date": d}
        if key:
            p["pagination_key"] = key
        for attempt in range(4):             # 一過性の失敗はリトライ
            try:
                r = requests.get(f"{BASE}/fins/summary", headers=HEAD, params=p,
                                 timeout=(10, 50), verify=False)
                if r.status_code == 200:
                    break
                time.sleep(2 * (attempt + 1))
            except Exception:
                time.sleep(2 * (attempt + 1))
        else:
            print(f"  [warn] {d} 取得失敗（スキップ）", flush=True)
            return rows
        j = r.json()
        rows += j.get("data", [])
        key = j.get("pagination_key")
        if not key:
            break
    return rows


def main() -> None:
    dates: set[str] = set()
    for p in ("earnings_calendar_2016_2021.json", "earnings_calendar.json"):
        for ds in json.load(open(p, encoding="utf-8")).values():
            dates.update(str(d) for d in ds)
    # 未来の予測日は実績が無いので取りに行かない
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    days = sorted(d for d in dates if "2016-01-01" <= d <= today)
    print(f"[fins] 開示日 {len(days)}日分を取得 ({days[0]}〜{days[-1]})", flush=True)

    done: set[str] = set()
    acc: list[pd.DataFrame] = []
    if os.path.exists(OUT):
        prev = pd.read_pickle(OUT)
        acc.append(prev)
        done = set(prev["DiscDate"].astype(str).unique())
        print(f"[fins] 既存 {len(prev):,}行 / {len(done)}日 を再利用", flush=True)

    todo = [d for d in days if d not in done]
    t0 = time.time()
    buf: list[dict] = []
    for i, d in enumerate(todo, 1):
        buf += fetch_date(d)
        if i % 200 == 0 or i == len(todo):
            if buf:
                df = pd.DataFrame(buf)
                acc.append(df[[c for c in KEEP if c in df.columns]])
                buf = []
            pd.concat(acc, ignore_index=True).to_pickle(OUT)
            el = time.time() - t0
            print(f"  {i}/{len(todo)}日 経過{el/60:.1f}分 "
                  f"残り約{el/i*(len(todo)-i)/60:.0f}分", flush=True)

    D = pd.concat(acc, ignore_index=True) if acc else pd.DataFrame()
    D = D.drop_duplicates(subset=["Code", "DiscDate", "DocType", "CurPerEn"], keep="last")
    D.to_pickle(OUT)
    print(f"[fins] 完了: {len(D):,}行 / {D['Code'].nunique():,}銘柄 "
          f"/ {D['DiscDate'].min()}〜{D['DiscDate'].max()}", flush=True)


if __name__ == "__main__":
    main()

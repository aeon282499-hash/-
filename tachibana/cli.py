# -*- coding: utf-8 -*-
"""立花証券 e支店・API コマンドライン。

    python -m tachibana.cli login              # 仮想URL(1日券)を取得して .tachibana/session.json に保存
    python -m tachibana.cli status             # 口座サマリ（可能額・保証金率・追証フラグ）＋営業日
    python -m tachibana.cli date               # 営業日情報
    python -m tachibana.cli price 7203 6501    # 時価スナップショット
    python -m tachibana.cli board 7203         # 板10本
    python -m tachibana.cli history 7203 --csv out.csv   # 20年日足
    python -m tachibana.cli short 7203 6501    # 空売り可否（規制/一極集中/逆日歩/証金貸株残）
    python -m tachibana.cli margin 7203 6501   # 証金残・信用残・逆日歩・銘柄詳細をまとめて
    python -m tachibana.cli detail 7203        # PER/PBR/利回り/年初来高安
    python -m tachibana.cli news [YYYYMMDD] [--code 7203]
    python -m tachibana.cli positions          # 現物保有＋信用建玉
    python -m tachibana.cli orders [--status 1]
    python -m tachibana.cli master --out issue_master.csv   # 株式銘柄マスタ＋市場マスタ＋規制を結合
    python -m tachibana.cli logout
    共通: --demo でデモ環境、--json で生JSON出力
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from .client import TachibanaClient, TachibanaError, DEFAULT_PRICE_COLUMNS, PRICE_COLUMNS, BOARD_COLUMNS


def _print(obj: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(obj, ensure_ascii=False, indent=2))
        return
    import pandas as pd

    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        with pd.option_context("display.max_columns", 200, "display.width", 250, "display.max_rows", 500):
            print(pd.DataFrame(obj).to_string(index=False))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                print(f"[{k}] {len(v)}件")
                _print(v, False)
            else:
                print(f"{k:34s} {v}")
    elif hasattr(obj, "to_string"):
        print(obj.to_string())
    else:
        print(obj)


def _rename_price(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{("code" if k == "sIssueCode" else PRICE_COLUMNS.get(k, k)): v for k, v in r.items()} for r in rows]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tachibana", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--demo", action="store_true", help="デモ環境に接続")
    ap.add_argument("--json", action="store_true", help="生JSONで出力")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("login")
    sub.add_parser("logout")
    sub.add_parser("status")
    sub.add_parser("date")
    p = sub.add_parser("price"); p.add_argument("codes", nargs="+"); p.add_argument("--cols", default=",".join(DEFAULT_PRICE_COLUMNS))
    p = sub.add_parser("board"); p.add_argument("code")
    p = sub.add_parser("history"); p.add_argument("code"); p.add_argument("--csv"); p.add_argument("--tail", type=int, default=10)
    p = sub.add_parser("short"); p.add_argument("codes", nargs="+")
    p = sub.add_parser("margin"); p.add_argument("codes", nargs="+")
    p = sub.add_parser("detail"); p.add_argument("codes", nargs="+")
    p = sub.add_parser("news"); p.add_argument("date", nargs="?"); p.add_argument("--code"); p.add_argument("--body", action="store_true")
    sub.add_parser("positions")
    p = sub.add_parser("orders"); p.add_argument("--status", default=""); p.add_argument("--code", default="")
    p = sub.add_parser("master"); p.add_argument("--out")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    tc = TachibanaClient(demo=a.demo)
    try:
        if a.cmd == "login":
            info = tc.login()
            print("ログイン成功 / 仮想URL保存:", tc.session_file)
            _print({**tc.login_notices(), "urls": {k: v[:60] + "..." for k, v in info["urls"].items()}}, a.json)
            return 0
        if a.cmd == "logout":
            r = tc.logout(); _print(r, a.json); return 0
        if a.cmd == "date":
            _print(tc.date_info(), a.json); return 0
        if a.cmd == "status":
            d = tc.date_info()
            s = tc.zan_summary()
            keys = ["sUpdateDate", "sGenbutuKabuKaituke", "sSinyouSinkidate", "sSinyouGenbiki", "sHosyouKinritu",
                    "sSyukkin", "sFusokugaku", "sOisyouHasseiFlg", "sTatekaekinHasseiFlg",
                    "sGenbutuOrderCount", "sGenbutuYakuzyouCount", "sSinyouOrderCount", "sSinyouYakuzyouCount"]
            out = {"当日": d.get("sTheDay"), "翌営業日": d.get("sYokuEigyouDay_1"), "株式受渡日": d.get("sKabuUkewatasiDay")}
            out.update({k: s.get(k) for k in keys})
            out.update(tc.login_notices())
            _print(s if a.json else out, a.json); return 0
        if a.cmd == "price":
            rows = tc.market_price(a.codes, a.cols.split(","))
            _print(rows if a.json else _rename_price(rows), a.json); return 0
        if a.cmd == "board":
            b = tc.board(a.code)
            if a.json:
                _print(b, True); return 0
            print(f"{a.code} 現在値 {b.get('pDPP')} ({b.get('tDPP:T')})  OVER {b.get('pQOV')}")
            for i in range(10, 0, -1):
                print(f"  売 {b.get(f'pGAV{i}', ''):>10} @ {b.get(f'pGAP{i}', '')}")
            for i in range(1, 11):
                print(f"  買 {b.get(f'pGBV{i}', ''):>10} @ {b.get(f'pGBP{i}', '')}")
            print(f"  UNDER {b.get('pQUV')}")
            return 0
        if a.cmd == "history":
            df = tc.price_history(a.code)
            print(f"{a.code}: {len(df)}行 {df.index.min().date() if len(df) else ''} 〜 {df.index.max().date() if len(df) else ''}")
            if a.csv:
                df.to_csv(a.csv, encoding="utf-8-sig"); print("保存:", a.csv)
            _print(df.tail(a.tail), False); return 0
        if a.cmd == "short":
            _print(tc.short_sell_status(a.codes), a.json); return 0
        if a.cmd == "margin":
            out = {"証金残": tc.syoukin_zan(a.codes), "信用残(週次)": tc.shinyou_zan(a.codes),
                   "逆日歩": tc.hibu_info(a.codes), "銘柄詳細": tc.issue_detail(a.codes)}
            _print(out, a.json); return 0
        if a.cmd == "detail":
            _print(tc.issue_detail(a.codes), a.json); return 0
        if a.cmd == "news":
            rows = tc.news(a.date)
            if a.code:
                rows = [r for r in rows if a.code in str(r.get("p_ISL", "")).split("|")]
            if a.json:
                _print(rows, True); return 0
            for r in rows:
                print(f"{r.get('p_TM')} [{r.get('p_ISL', '')}] {r.get('p_HDL', '')}")
                if a.body:
                    print("   ", str(r.get("p_TX", "")).replace("\n", "\n    ")[:2000])
            print(f"{len(rows)}件")
            return 0
        if a.cmd == "positions":
            g = tc.genbutu_list(); s = tc.shinyou_tategyoku_list()
            if a.json:
                _print({"genbutu": g, "shinyou": s}, True); return 0
            print(f"== 現物 評価額 {g.get('sTotalGaisanHyoukagakuGoukei')} / 評価損益 {g.get('sTotalGaisanHyoukaSonekiGoukei')}")
            _print(g.get("aGenbutuKabuList") or [], False)
            print(f"== 信用 建玉代金 {s.get('sTotalDaikin')} / 評価損益 {s.get('sTotalHyoukaSonekiGoukei')}")
            _print(s.get("aShinyouTategyokuList") or [], False)
            return 0
        if a.cmd == "orders":
            _print(tc.order_list(code=a.code, status=a.status), a.json); return 0
        if a.cmd == "master":
            import pandas as pd

            m = pd.DataFrame(tc.issue_master())
            mm = pd.DataFrame(tc.issue_market_master())
            rg = pd.DataFrame(tc.regulation_master())
            df = m.merge(mm, on="sIssueCode", how="left")
            if not rg.empty:
                df = df.merge(rg.drop(columns=[c for c in ("sZyouzyouSizyou",) if c in rg.columns]), on="sIssueCode", how="left")
            print(f"銘柄マスタ {len(m)} / 市場マスタ {len(mm)} / 規制 {len(rg)} → 結合 {len(df)}")
            if a.out:
                df.to_csv(a.out, index=False, encoding="utf-8-sig"); print("保存:", a.out)
            else:
                _print(df.head(20), False)
            return 0
    except TachibanaError as e:
        print(f"[立花API エラー] {e}", file=sys.stderr)
        if tc.last_response and a.verbose:
            print(json.dumps(tc.last_response, ensure_ascii=False)[:2000], file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

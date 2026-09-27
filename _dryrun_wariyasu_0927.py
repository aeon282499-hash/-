# -*- coding: utf-8 -*-
"""_dryrun_wariyasu_0927.py — 割安B の本番コードをライブの J-Quants データで空回しする（2026-09-27）
1. 本番の状態ファイルを作る（wariyasu_signal.init_state・帳簿は 9/28 から＝それより前の候補は通知していないので建てない）
2. 8/25〜9/25 の毎日をライブのデータで判定し、BT(_audit_wariyasu_bt_recent_<rule>.json＝パネルに本番の関数を当てた候補・〜9/18)と並べる
3. 8/25 に空の帳簿で始めていたら、という紙の帳簿を本番の関数で1日ずつ進める（約定・手仕舞い・9/25 夜の指値）
4. 9/25 の夜に配信していたら、の文面（空の帳簿から）
実行: python -X utf8 _dryrun_wariyasu_0927.py > _dryrun_wariyasu_0927.log   （J-Quants を約120回・10分ほど）
"""
import copy
import json
import os
import sys
import time

import numpy as np

import wariyasu_signal as W

t0 = time.time()
tok = W._token()
cal = W.Cal()
earn = W.EarnCal()
FIRST = "2026-09-28"
bc, vc = {}, {}
print(f"ルール[{W.WY_RULE}] 1枠{W.WY_SIZE:,}×{W.WY_SLOTS}  {W.WY_BT_NOTE}", flush=True)

# 1. 本番の状態
st = W.init_state(cal, tok, FIRST, bars_cache=bc, val_cache=vc)
W._save(W.STATE_FILE, st)
if not os.path.exists(W.BOOK_FILE):
    W._save(W.BOOK_FILE, [])
print(f"[1] {W.STATE_FILE}: 配当予想 {len(st['divs'])}銘柄・決算短信 {len(st['stmt'])}銘柄・開示は{st['fins_last']}まで・"
      f"帳簿を進めた日={st['settled']}（{FIRST} の夕方から動く） {time.time() - t0:.0f}s", flush=True)

# 2〜3. 8/25〜9/25
days = [d for d in cal.days if "2026-08-25" <= d <= "2026-09-25"]
bt = {}
try:
    bt = json.load(open(f"_audit_wariyasu_bt_recent_{W.WY_RULE}.json", encoding="utf-8"))["cands"]
except FileNotFoundError:
    print("   （BTの直近の候補ファイルが無い → 並べない）")
sim = copy.deepcopy(st)
sim.update(settled=cal.days[cal.idx(days[0]) - 1], pend=[], cands_hist={}, b_hist={}, sent=[])
book = []
eng = W.Engine()
names_all = {}
print("\n[2] 毎日の候補（ライブのデータ・本番の関数）とBT（パネル・〜9/18）", flush=True)
n_same = n_cmp = 0
day = None
for d in days:
    day = W.compute_day(d, cal, sim, tok, bars_cache=bc, val_cache=vc)
    if day.get("nodata"):
        print(f"  {d} 足なし")
        continue
    ev = W.settle_day(book, sim, d, cal, day, eng, earn)
    sim.setdefault("cands_hist", {})[d] = [[c, round(s, 6), earn.next_date(c, d, d, cal, sim.get("stmt", {}))] for c, s in day["cands"]]
    sim["settled"] = d
    lc = [c for c, _ in day["cands"]]
    tag = ""
    if d in bt:
        n_cmp += 1
        if bt[d] == lc:
            n_same += 1
            tag = "BTと同じ"
        else:
            miss = [c for c in bt[d] if c not in lc]; extra = [c for c in lc if c not in bt[d]]
            same_set = not miss and not extra
            tag = ("BTと同じ銘柄・並びが違う" if same_set else f"BTだけ{miss[:6]} ライブだけ{extra[:6]}")
    top = " ".join(f"{c}({s:+.2f})" for c, s in day["cands"][:5])
    evs = " ".join(f"{e['kind']}:{e['code']}" + (f"@{e['px']:.0f}" if e["kind"] == "entry" else (f" {e['why']}{e['r'] * 100:+.1f}%" if e["kind"] == "exit" else ""))
                   for e in ev)
    print(f"  {d} 割安{int(day['B'].sum())} 候補{len(day['cands'])}（値がさ除く） 上位 {top}  {tag}" + (f"  帳簿: {evs}" if evs else ""), flush=True)
print(f"  → BTと比べられた {n_cmp}日のうち候補の並びまで同じ {n_same}日  {time.time() - t0:.0f}s", flush=True)

need = sorted({p["code"] for p in book} | {x[0] for x in sim.get("pend", [])} | {c for c, _ in day["cands_all"][:10]})
names = W.fetch_names(tok, need)
print("\n[3] 8/25 に空の帳簿で始めていたら（本番の関数で1日ずつ）")
for p in book:
    nm = names.get(p["code"], "")
    if p["status"] == "closed":
        print(f"  {p['code']} {nm} {p['entry_date']} {p['px']:,.0f}円 → {p['exit_date']} {p['why']} {p['exit_px']:,.1f}円 {p['r'] * 100:+.2f}%（{p['pnl_yen'] / 1e4:+.1f}万）")
    else:
        print(f"  {p['code']} {nm} {p['entry_date']} {p['px']:,.0f}円 → 保有中 {p['last']:,.0f}円（{(p['last'] / p['px'] - 1) * 100:+.1f}%）")
closed = [p for p in book if p["status"] == "closed"]
print(f"  決済 {len(closed)}件 {sum(p['pnl_yen'] for p in closed) / 1e4:+.1f}万・保有 {len(book) - len(closed)}件")
if W.WY_ENTRY == "T4":
    od = W.order_rows(sim, book, cal, days[-1], names)
    print(f"  9/25 夜の時点の指値（9/28 の分）: " + " / ".join(f"{'🟢' if o['live'] else '⚪'}{o['code']} {names.get(o['code'], '')} 指値{o['limit']:,.0f}円 {o['exp']}まで スコア{o['score']:+.2f}"
                                                 for o in od[:6]))

# 4. 9/25 の夜に空の帳簿から配信していたら
print("\n[4] 9/25 の夜の配信の文面（空の帳簿から・ドライラン）")
fresh = copy.deepcopy(st)
fresh.update(settled=cal.days[cal.idx(days[-1]) - 1], pend=[], cands_hist={}, sent=[])
book0 = []
day = W.compute_day(days[-1], cal, fresh, tok, bars_cache=bc, val_cache=vc)
ev = W.settle_day(book0, fresh, days[-1], cal, day, W.Engine(), earn)
names = W.fetch_names(tok, sorted({c for c, _ in day["cands_all"][:12]} | {x[0] for x in fresh.get("pend", [])}))
rows = W.cand_rows(day, cal, earn, fresh, book0, names)
orders = W.order_rows(fresh, book0, cal, days[-1], names) if W.WY_ENTRY == "T4" else None
emb = W.build_embed(days[-1], cal, day, rows, book0, ev, names, W.Engine(), orders=orders)
print(emb["title"])
print(emb["description"])
print("---")
print(emb["footer"]["text"])
json.dump({"date": days[-1], "orders": orders, "cands": rows[:20], "embed": emb}, open("_dryrun_wariyasu_0927.json", "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print(f"\n終わり {time.time() - t0:.0f}s")

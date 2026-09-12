# -*- coding: utf-8 -*-
"""_fix_report.py — GoldFixShort(LBMA値決め直前15分ショート)の当日約定をMT5から読んで fix_trades.csv に追記する。
毎日18:45/19:45にタスクで実行(XM_MT5_Report_*)。手動: python -X utf8 mt5/_fix_report.py [--days N]
"""
import sys, os, datetime as dt, csv
import MetaTrader5 as mt5

MAGICS = {20260908: "GOLD.", 20260909: "JP225Cash", 20260913: "US500Cash"}   # 金15分ショート / 日経夜ドリフト
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fix_trades.csv")
days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 1

if not mt5.initialize(timeout=60000):
    print("MT5接続失敗", mt5.last_error()); sys.exit(1)
ai = mt5.account_info()
frm = dt.datetime.now() - dt.timedelta(days=days)
deals = mt5.history_deals_get(frm, dt.datetime.now() + dt.timedelta(days=1)) or []
mt5.shutdown()

deals = [d for d in deals if d.magic in MAGICS and d.symbol == MAGICS[d.magic]]
by_pos = {}
for d in deals:
    by_pos.setdefault(d.position_id, []).append(d)

seen = set()
if os.path.exists(OUT):
    with open(OUT, encoding="utf-8-sig") as f:
        seen = {r["position_id"] for r in csv.DictReader(f)}
rows = []
for pid, ds in by_pos.items():
    ds.sort(key=lambda x: x.time)
    ent = [x for x in ds if x.entry == mt5.DEAL_ENTRY_IN]
    ext = [x for x in ds if x.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT)]
    if not ent or not ext:
        continue
    e, x = ent[0], ext[-1]
    pnl = sum(d.profit for d in ds); comm = sum(d.commission for d in ds); swap = sum(d.swap for d in ds)
    rows.append(dict(position_id=str(pid), system=("金" if e.magic == 20260908 else "日経" if e.magic == 20260909 else "US500"), date=dt.datetime.fromtimestamp(e.time).strftime("%Y-%m-%d"),
                     entry_time=dt.datetime.fromtimestamp(e.time).strftime("%H:%M:%S"), exit_time=dt.datetime.fromtimestamp(x.time).strftime("%H:%M:%S"),
                     lot=e.volume, sell=e.price, cover=x.price, gross_usd_oz=round(e.price - x.price, 2),
                     profit_jpy=round(pnl, 0), commission_jpy=round(comm, 0), swap_jpy=round(swap, 0), net_jpy=round(pnl + comm + swap, 0),
                     exit_reason=x.comment, balance_after=ai.balance))
new = [r for r in rows if r["position_id"] not in seen]
if new:
    write_header = not os.path.exists(OUT)
    with open(OUT, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(new[0].keys()))
        if write_header: w.writeheader()
        for r in new: w.writerow(r)
for r in rows:
    print(f"[{r['system']}] {r['date']} {r['entry_time']}→{r['exit_time']} lot{r['lot']:.2f} 建{r['sell']:.2f}→返{r['cover']:.2f} 差{r['gross_usd_oz']:+.2f} "
          f"損益{r['profit_jpy']:+,.0f} 手数料{r['commission_jpy']:,.0f} 純{r['net_jpy']:+,.0f}円 {r['exit_reason']}")
print(f"[{dt.datetime.now():%Y-%m-%d %H:%M}] 約定{len(rows)}件(新規{len(new)}) 残高{ai.balance:,.0f}円 有効証拠金{ai.equity:,.0f}円 → {OUT}")

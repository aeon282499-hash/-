#!/usr/bin/env python3
"""本人chの注文（今日の3本・友達だけ・遊び・2桁）の「空売り在庫」を毎晩記録する（2026-11-01 本人「松井とSBIの在庫も今日から記録しといて」・11/9から友達と2桁も）。

ログインなしで取れるもの（毎営業日夕方更新）:
  松井  一日信用 プレミアム空売り: premium.csv（在庫 0=×/1=△/2=〇/3=☆・空売り料 円/株/日・約定日）
        一日信用 取扱銘柄: d_margin_list.csv（載っていれば一日信用で売建可）・売建上限: max_hold.csv
  三菱UFJ eスマート(カブコム) 一般信用(長期・デイトレ)売建可能銘柄: meigara_list.csv（対象かどうかだけ。入札の在庫数・料はログイン後）
ログインが要るもの（SBI HYPER空売り・楽天いちにち信用・カブコムの入札結果）は本人が見て伝える → --set で手入力。
記録先: kabuai/zaiko_log.json（取引日ごと）。本人chに1本だけ要約を送る。発注機能なし。
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LEDGER = HERE / "fade_notify_ledger.json"
LOG = HERE / "zaiko_log.json"
OWN_WEBHOOK_ENV = os.getenv("FADE_NOTIFY_OWN_ENV", "DISCORD_WEBHOOK_OWN_FADE_URL")
JST = timezone(timedelta(hours=9))
UA = {"User-Agent": "Mozilla/5.0 (kabuai-zaiko-log)"}
MATSUI_PREMIUM = "https://www.matsui.co.jp/stock/margin/d-margin/premium/list/csv/premium.csv?p=1"
MATSUI_DAY = "https://www.matsui.co.jp/stock/margin/d-margin/csv/d_margin_list.csv"
MATSUI_MAX = "https://www.matsui.co.jp/stock/margin/d-margin/premium/max_hold/max_hold.csv"
KABUCOM = "https://kabu.com/pdf/Gmkpdf/shinyou/meigara_list.csv"
ZAIKO_MARK = {"0": "×", "1": "△", "2": "〇", "3": "☆"}
BROKERS_MANUAL = ("sbi", "rakuten", "kabucom_bid")   # 手入力: SBI HYPER / 楽天いちにち / カブコム入札の結果


def log(msg: str) -> None:
    print(f"[{datetime.now(JST):%H:%M:%S}] {msg}", flush=True)


def fetch_csv(url: str) -> list[list[str]]:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        b = r.read()
    for enc in ("cp932", "utf-8-sig"):
        try:
            s = b.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        s = b.decode("utf-8", "ignore")
    return [row for row in csv.reader(io.StringIO(s)) if row]


def load_sources() -> dict:
    src: dict = {}
    try:
        rows = fetch_csv(MATSUI_PREMIUM)
        hdr = rows[0]; ci = {h: i for i, h in enumerate(hdr)}
        src["matsui_premium"] = {r[ci["銘柄コード"]]: dict(zaiko=ZAIKO_MARK.get(r[ci["在庫"]], r[ci["在庫"]]), fee=float(r[ci["空売り料"]] or 0),
                                                        day=r[ci["約定日"]]) for r in rows[1:] if len(r) == len(hdr)}
        log(f"松井プレミアム {len(src['matsui_premium'])}銘柄")
    except Exception as e:  # noqa: BLE001
        log(f"松井プレミアム 取得失敗: {type(e).__name__}"); src["matsui_premium"] = None
    try:
        rows = fetch_csv(MATSUI_DAY); src["matsui_day"] = {r[0] for r in rows[1:]}
        log(f"松井一日信用 {len(src['matsui_day'])}銘柄")
    except Exception as e:  # noqa: BLE001
        log(f"松井一日信用 取得失敗: {type(e).__name__}"); src["matsui_day"] = None
    try:
        rows = fetch_csv(MATSUI_MAX); src["matsui_max"] = {r[0]: int(r[2].replace(",", "").strip() or 0) for r in rows[1:] if len(r) >= 3}
    except Exception as e:  # noqa: BLE001
        log(f"松井売建上限 取得失敗: {type(e).__name__}"); src["matsui_max"] = None
    try:
        rows = fetch_csv(KABUCOM); kc: dict = {}
        for r in rows:
            if len(r) >= 4 and r[0] in ("デイトレ", "長期"):
                kc.setdefault(r[2], set()).add(r[0])
        src["kabucom"] = kc; log(f"カブコム一般信用売建 {len(kc)}銘柄")
    except Exception as e:  # noqa: BLE001
        log(f"カブコム 取得失敗: {type(e).__name__}"); src["kabucom"] = None
    return src


def plan_legs(plan: dict) -> list[dict]:
    """本人chの注文を全部（今日の3本・友達だけ・遊び・2桁）。2026-11-09 本人「在庫とれそうか確認できない？」で友達と2桁も記録に足した。
    fade_notify.own_orders が読めない時は今までどおり今日の3本＋遊びだけ。"""
    try:
        if str(HERE) not in sys.path:
            sys.path.insert(0, str(HERE))
        import fade_notify as fn
        out = []
        for o in fn.own_orders(plan):
            x = o["x"]
            d = dict(kind=o["kind"], code=x["code"], name=x.get("name", ""), close=x.get("close"), seido=x.get("seido"), jsf_stop=x.get("jsf_stop"))
            if o["kind"] == "遊び":
                d["lim"] = x.get("lim")
            out.append(d)
        return out
    except Exception as e:  # noqa: BLE001
        log(f"本人chの注文一覧が読めない→今日の3本と遊びだけ記録: {type(e).__name__}")
    legs = []
    for x in (plan.get("top3") or {}).get("rows", []):
        legs.append(dict(kind="3本", code=x["code"], name=x["name"], close=x.get("close"), seido=x.get("seido"), jsf_stop=x.get("jsf_stop")))
    a = plan.get("asobi")
    if a and a.get("shares"):
        legs.append(dict(kind="遊び", code=a["code"], name=a["name"], close=a.get("close"), lim=a.get("lim"), seido=a.get("seido"), jsf_stop=a.get("jsf_stop")))
    return legs


def build_record(leg: dict, src: dict, trade_day: str) -> dict:
    c = leg["code"]; rec = dict(leg)
    mp = src.get("matsui_premium"); md = src.get("matsui_day"); mm = src.get("matsui_max"); kc = src.get("kabucom")
    m: dict = {}
    if mp is not None:
        p = mp.get(c)
        if p:
            m.update(zaiko=p["zaiko"], fee=p["fee"], day=p["day"], stale=(p["day"] != f"{int(trade_day[5:7])}/{int(trade_day[8:10])}"))
            if leg.get("close"):
                m["fee_pct"] = round(p["fee"] / float(leg["close"]) * 100, 2)
        else:
            m["zaiko"] = None   # プレミアム一覧に無い＝料0円で売れる(貸借)か、そもそも対象外
    if md is not None:
        m["day_ok"] = c in md
    if mm is not None and c in mm:
        m["max_sh"] = mm[c]
    rec["matsui"] = m
    if kc is not None:
        k = kc.get(c, set()); rec["kabucom"] = dict(daytrade="デイトレ" in k, choki="長期" in k)
    for b in BROKERS_MANUAL:
        rec.setdefault(b, None)
    return rec


def matsui_text(m: dict) -> str:
    if not m:
        return "松井 取得失敗"
    if m.get("zaiko"):
        s = f"松井 {m['zaiko']} 料{m['fee']:g}円"
        if "fee_pct" in m:
            s += f"({m['fee_pct']}%)"
        if m.get("max_sh"):
            s += f" 上限{m['max_sh']:,}株"
        if m.get("stale"):
            s += "（約定日が違う＝古い一覧）"
        return s
    if m.get("day_ok"):
        return "松井 一日信用○ 料0円"
    return "松井 対象外"


def kabucom_text(k: dict | None) -> str:
    if k is None:
        return "カブコム 取得失敗"
    if k["daytrade"] and k["choki"]:
        return "カブコム デイトレ○長期○"
    if k["daytrade"]:
        return "カブコム デイトレ○(要入札)"
    if k["choki"]:
        return "カブコム 長期のみ"
    return "カブコム 対象外"


def payload(trade_day: str, recs: list[dict]) -> dict:
    td = date.fromisoformat(trade_day)
    lines = []
    for r in recs:
        mark = "🎲" if r["kind"] in ("遊び", "2桁") else "🩳"
        man = "・".join(f"{b.upper() if b != 'kabucom_bid' else '入札'} {r[b]}" for b in BROKERS_MANUAL if r.get(b))
        tag = ("制度○" if r.get("seido") == "○" else "非貸借") + ("・売り禁" if r.get("jsf_stop") else "")
        lines.append(f"{mark} `{r['code']}` {r['name']}（{tag}）: {matsui_text(r.get('matsui', {}))} / {kabucom_text(r.get('kabucom'))}" + (f" / {man}" if man else ""))
    if not lines:
        lines.append("対象なし")
    lines.append("-# 制度○=貸借銘柄(売り禁でなければどこでも制度信用で売れる)。松井=プレミアム空売り一覧(〇余裕/△わずか/×受付不可/☆補充)・料は1株1日。カブコム=一般信用売建の対象かどうかだけ(在庫と料は19:30〜20:30の入札)。SBI・楽天はログインが要るので見たら教えて。")
    return {"embeds": [{"title": f"🧾 {td:%m/%d(%a)} 空売り在庫（松井・カブコム）", "description": "\n".join(lines)[:4000], "color": 0x868E96}]}


def post(p: dict, dry_run: bool) -> bool:
    hook = os.getenv(OWN_WEBHOOK_ENV, "").strip()
    if dry_run or not hook:
        print("---- 在庫（dry-run / 未設定なら送らない）----")
        for e in p["embeds"]:
            print(f"## {e['title']}"); print(e["description"]); print()
        return False
    body = json.dumps(p, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(hook, data=body, headers={"Content-Type": "application/json", "User-Agent": "kabuai-zaiko-log/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            log(f"在庫 送信OK status={r.status}"); return True
    except Exception as e:  # noqa: BLE001
        log(f"在庫 送信失敗: {type(e).__name__}"); return False


def load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def save_json(p: Path, d: dict) -> None:
    tmp = p.with_suffix(".tmp"); tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8"); os.replace(tmp, p)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="取引日 YYYY-MM-DD（既定=台帳の最新の計画）")
    ap.add_argument("--ledger", help="台帳の場所（既定 kabuai/fade_notify_ledger.json）")
    ap.add_argument("--dry-run", action="store_true", help="Discord に送らず表示だけ・ログも書かない")
    ap.add_argument("--no-post", action="store_true", help="ログは書くが Discord に送らない")
    ap.add_argument("--set", nargs="*", default=[], metavar="CODE:BROKER=VAL",
                    help="手入力 例: 4410:sbi=◎ 4410:rakuten=× 6029:kabucom_bid=落札1円 （broker は sbi/rakuten/kabucom_bid）")
    a = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(os.getenv("FADE_NOTIFY_DOTENV", "") or (HERE.parent / ".env"))
    except Exception:  # noqa: BLE001
        pass
    led = load_json(Path(a.ledger) if a.ledger else LEDGER)
    plans = led.get("plans", {})
    if a.date:
        plan = next((p for p in plans.values() if p.get("trade_day") == a.date), None)
        if plan is None:
            log(f"{a.date} の計画が台帳に無い"); return 0
    else:
        if not plans:
            log("台帳に計画が無い"); return 0
        plan = plans[max(plans)]
    td = plan["trade_day"]
    legs = plan_legs(plan)
    if not legs:
        log(f"{td} 本人chの注文なし"); return 0
    book = load_json(LOG)
    old = {r["code"]: r for r in (book.get(td) or {}).get("items", [])}
    if a.set:
        recs = [old[l["code"]] if l["code"] in old else build_record(l, {}, td) for l in legs]
    else:
        src = load_sources()
        recs = [build_record(l, src, td) for l in legs]
        for r in recs:                                   # 手入力は残す
            for b in BROKERS_MANUAL:
                if old.get(r["code"], {}).get(b):
                    r[b] = old[r["code"]][b]
    for s in a.set:
        try:
            code, rest = s.split(":", 1); b, v = rest.split("=", 1)
        except ValueError:
            log(f"--set の形が違う: {s}"); return 2
        if b not in BROKERS_MANUAL:
            log(f"broker は {BROKERS_MANUAL} のどれか: {s}"); return 2
        for r in recs:
            if r["code"] == code:
                r[b] = v
    p = payload(td, recs)
    sent = False if a.no_post else post(p, a.dry_run)
    if not a.dry_run:
        book[td] = dict(items=recs, made=datetime.now(JST).isoformat(timespec="seconds"), sent=bool(sent))
        save_json(LOG, book); log(f"zaiko_log.json に {td} を記録（{len(recs)}本）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# biz_grep_pc.py — 事業内容索引（有報の全文キャッシュ _biz_cache.json 4,230社）を言葉で引く。朝8:00の材料銘柄探し用（feedback_news_to_stocks_discovery）。
#   python -X utf8 biz_grep_pc.py PHP WordPress KUSANAGI      ← どれか1語でも入っていれば出す（大文字小文字は無視）
#   python -X utf8 biz_grep_pc.py --all 脆弱性 診断            ← 全部の語が入っている会社だけ
# 出力: コード 社名 (決算期) 一致した語 ／ 本文の一致箇所。biz/index.json で code↔doc を結ぶ。
import json, re, sys, glob, os
HERE = os.path.dirname(os.path.abspath(__file__))

def main():
    args = [a for a in sys.argv[1:] if a != "--all"]
    need_all = "--all" in sys.argv
    if not args:
        print(__doc__ or "使い方: python -X utf8 biz_grep_pc.py 語 [語...] [--all]"); return
    cache = json.load(open(os.path.join(HERE, "_biz_cache.json"), encoding="utf-8"))
    items = cache.get("items", {})
    meta = {}
    for f in glob.glob(os.path.join(HERE, "biz", "[0-9].json")):
        for r in json.load(open(f, encoding="utf-8")).get("rows", []):
            meta[r["doc"]] = r
    pats = [re.compile(re.escape(a), re.I) for a in args]
    out = []
    for doc, v in items.items():
        txt = (v.get("t", "") + "\n" + v.get("r", "")) if isinstance(v, dict) else str(v)
        hits = [a for a, p in zip(args, pats) if p.search(txt)]
        if not hits or (need_all and len(hits) < len(args)): continue
        m = meta.get(doc, {})
        p = pats[args.index(hits[0])]
        mm = p.search(txt); s = max(0, mm.start() - 50); snip = txt[s:mm.end() + 70].replace("\n", " ")
        out.append((len(hits), m.get("code", "?"), m.get("name", doc), m.get("fy", ""), "・".join(hits), snip))
    out.sort(key=lambda x: (-x[0], x[1]))
    print(f"{len(out)}社  語={args}  更新={cache.get('updated_at')}")
    for _, code, name, fy, hits, snip in out:
        print(f"{code} {name} ({fy}) [{hits}]\n    …{snip}…")

if __name__ == "__main__":
    main()

# biz_grep_pc.py — 事業内容索引（有報の本文キャッシュ _biz_cache.json 4,230社）を言葉で引く。朝8:00の材料銘柄探し用（feedback_news_to_stocks_discovery）。
#   python -X utf8 biz_grep_pc.py PHP WordPress KUSANAGI      ← どれか1語でも入っていれば出す（大文字小文字は無視）
#   python -X utf8 biz_grep_pc.py --all 脆弱性 診断            ← 全部の語が入っている会社だけ
#   python -X utf8 biz_grep_pc.py --in s エアトリ              ← 欄を絞る: t=事業の内容 r=研究開発 a=関係会社 s=大株主 c=主要顧客 m=企業結合の注記＝買った子会社の中身（複数可 --in as）
# 出力: コード 社名 (決算期) 一致した語 ／ [欄] 本文の一致箇所。biz/[0-9].json で code↔doc を結ぶ。
# 10/23: 欄を t/r だけでなく a(関係会社の状況) s(大株主の状況) c(主要な顧客) まで広げた（本人「子会社や株主関係や仕入れ販売関係まで調べたか」）。
#        t は 6,000字（10/22 までは 2,000字で 1,287社が途中で切れていた）。
import json, re, sys, glob, os
HERE = os.path.dirname(os.path.abspath(__file__))
FIELDS = {"t": "事業の内容", "r": "研究開発", "a": "関係会社", "s": "大株主", "c": "主要顧客", "m": "企業結合(買った子会社)"}


def main():
    argv = sys.argv[1:]
    fields = "trascm"
    if "--in" in argv:
        i = argv.index("--in")
        fields = argv[i + 1]
        del argv[i:i + 2]
    need_all = "--all" in argv
    args = [a for a in argv if a != "--all"]
    if not args:
        print("使い方: python -X utf8 biz_grep_pc.py 語 [語...] [--all] [--in trasc]")
        return
    cache = json.load(open(os.path.join(HERE, "_biz_cache.json"), encoding="utf-8"))
    items = cache.get("items", {})
    meta = {}
    for f in glob.glob(os.path.join(HERE, "biz", "[0-9].json")):
        for r in json.load(open(f, encoding="utf-8")).get("rows", []):
            meta[r["doc"]] = r
    pats = [re.compile(re.escape(a), re.I) for a in args]
    out = []
    for doc, v in items.items():
        if not isinstance(v, dict):
            continue
        parts = [(f, v.get(f) or "") for f in fields if v.get(f)]
        if not parts:
            continue
        txt = "\n".join(t for _, t in parts)
        hits = [a for a, p in zip(args, pats) if p.search(txt)]
        if not hits or (need_all and len(hits) < len(args)):
            continue
        m = meta.get(doc, {})
        p = pats[args.index(hits[0])]
        snip = ""
        for f, t in parts:
            mm = p.search(t)
            if mm:
                s = max(0, mm.start() - 50)
                snip = f"[{FIELDS.get(f, f)}] " + t[s:mm.end() + 70].replace("\n", " ")
                break
        out.append((len(hits), m.get("code", "?"), m.get("name", doc), m.get("fy", ""), "・".join(hits), snip))
    out.sort(key=lambda x: (-x[0], x[1]))
    print(f"{len(out)}社  語={args}  欄={''.join(FIELDS.get(f, f) + '/' for f in fields)[:-1]}  更新={cache.get('updated_at')}")
    for _, code, name, fy, hits, snip in out:
        print(f"{code} {name} ({fy}) [{hits}]\n    …{snip}…")


if __name__ == "__main__":
    main()

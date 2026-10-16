# -*- coding: utf-8 -*-
"""ir_judge.py — 📰 IR の中身を Claude が読んで「上がる/下がる」を判定（2026-10-17・本人「君が判断」）

ir_builder.py のルール判定が「🔍要精査」にした物（決算短信・業績予想の修正・月次・中計・資本提携の割当…）と、
材料タグの付いた物を対象に、TDnet の PDF の冒頭（最大3ページ・6,000字）を Claude に読ませて判定する。

出力（kabuai/ir_judged.json・開示IDで引く・60日で古い物を捨てる）:
  {"model": "...", "updated_at": "...", "items": {"<開示ID>": {label, score, conf, reason, facts, priced, horizon, model, date, code, title, src}}}

動かし方:
  - GitHub Actions: ANTHROPIC_API_KEY が Secrets にあれば build_data.py → ir_builder.build_ir の中で自動で呼ばれる
    （actions/cache で ir_judged.json を持ち回る＝同じ開示を何度も判定しない）。キーが無ければ何もしない（ルール判定だけ）。
  - PC: python -X utf8 kabuai/ir_judge.py --pc   … 親の .env の ANTHROPIC_API_KEY を使って今日の分を判定し、
    ir_judged.json を gh api で main にコミット（pts_fetch_pc.py と同じ経路）→ kabuai.yml のビルドが走って画面に載る。
    --dry-run で判定だけ（アップロードなし）。--date YYYYMMDD で日付指定。

費用の目安: 1件あたり入力 ~3,000 トークン・出力 ~200 トークン。claude-opus-5-5（$4/$20 per 1M）で 1件 ≈ $0.016・
1日 40件 ≈ $0.65（≈100円）。モデルは KABUAI_IR_MODEL で差し替え可（例: claude-haiku-5-5 なら 1/40 の費用）。
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
JST = timezone(timedelta(hours=9))
JUDGED_PATH = HERE / "ir_judged.json"
MODEL = os.environ.get("KABUAI_IR_MODEL", "claude-opus-5-5")
MAX_JUDGE = int(os.environ.get("KABUAI_IR_MAX", "60"))        # 1回の実行で判定する上限（費用の天井）
MAX_CHARS = int(os.environ.get("KABUAI_IR_CHARS", "6000"))     # PDF から渡す文字数
MAX_PAGES = 3
KEEP_DAYS = 60
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}

SYSTEM = """あなたは日本株の適時開示（TDnet のIR）を読んで、その開示が株価にとって好材料か悪材料かを判定する実務家です。
判定の基準（brisk 適時開示の分類を踏まえる）:
- 好材料: 業績予想の上方修正・一転黒字・増配/復配/特別配当・自社株買い・TOBや完全子会社化の対象になる・大型受注・市場予想を上回る決算（会社計画に対する進捗率が高い）・月次の前年比が強い・優待新設 など
- 悪材料: 下方修正・一転赤字・減配/無配・公募/第三者割当/MSワラント等の希薄化・行政処分・上場廃止/監理・不正/不適切会計・特別損失/減損・会社計画に届かない決算（進捗率が低い）・月次の前年比が弱い・優待廃止 など
- 中立: 事務的な開示、方向が決まらない物、規模が小さすぎて株価に関係しない物
決算短信は「通期計画に対する進捗率」「前年同期比」「予想の据え置きか修正か」を読み取って判定する。
既出（場中に出た開示）は、その日のうちに株価に織り込まれた可能性を priced で示す。
score は −3（強い悪材料）〜 +3（強い好材料）。confidence は 0〜100。reason は60字以内で、数字を1つは入れる。facts は判定の根拠になった数字や事実を箇条書き1行（80字以内）。
翌営業日の寄り付きの反応を予想する判定であって、投資助言ではない。"""


class IRJudgeOut:
    """pydantic が無い環境でも読めるように、スキーマは関数で作る。"""
    SCHEMA = {
        "type": "object",
        "properties": {
            "label": {"type": "string", "enum": ["好材料", "悪材料", "中立"]},
            "score": {"type": "integer", "minimum": -3, "maximum": 3},
            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
            "reason": {"type": "string"},
            "facts": {"type": "string"},
            "priced": {"type": "boolean"},
            "horizon": {"type": "string", "enum": ["翌日", "数日", "長期"]},
        },
        "required": ["label", "score", "confidence", "reason", "facts", "priced", "horizon"],
        "additionalProperties": False,
    }


def load_judged() -> dict:
    try:
        with open(JUDGED_PATH, encoding="utf-8") as f:
            j = json.load(f)
        if isinstance(j, dict) and isinstance(j.get("items"), dict):
            return j
    except Exception:
        pass
    return {"model": MODEL, "updated_at": "", "items": {}}


def save_judged(j: dict) -> None:
    cutoff = (datetime.now(JST).date() - timedelta(days=KEEP_DAYS)).isoformat()
    j["items"] = {k: v for k, v in j["items"].items() if str(v.get("date") or "9999") >= cutoff}
    j["updated_at"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    j["model"] = MODEL
    JUDGED_PATH.write_text(json.dumps(j, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def pdf_text(url: str, timeout: float = 25.0) -> tuple[str, str]:
    """PDF の冒頭テキスト。(text, note)。取れなければ ('', 理由)。"""
    if not url:
        return "", "pdf無し"
    try:
        import requests
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code != 200:
            return "", f"pdf http{r.status_code}"
        if len(r.content) > 12 * 1024 * 1024:
            return "", "pdf 12MB超"
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(r.content))
        parts = []
        for i, page in enumerate(reader.pages[:MAX_PAGES]):
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        text = re.sub(r"[ \t　]+", " ", "\n".join(parts)).strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        if len(text) < 40:
            return "", "pdf 文字が取れない（画像PDF）"
        return text[:MAX_CHARS], f"pdf {min(len(reader.pages), MAX_PAGES)}/{len(reader.pages)}p"
    except Exception as e:
        return "", f"pdf 失敗: {str(e)[:60]}"


def _px_ctx(data: dict, code: str) -> str:
    try:
        df = data.get(code + ".T") or data.get(code)
        if df is None or len(df) < 6:
            return ""
        c = float(df["Close"].iloc[-1]); c5 = float(df["Close"].iloc[-6])
        tov = float((df["Close"] * df["Volume"]).tail(20).median()) / 1e8
        return f"直近終値 {c:,.0f}円（5日で {((c / c5) - 1) * 100:+.1f}%）・20日代金中央値 {tov:.1f}億円"
    except Exception:
        return ""


def judge_one(client, row: dict, data: dict) -> dict | None:
    text, note = pdf_text(row.get("pdf") or "")
    ctx = _px_ctx(data, row["code"])
    when = f"{row['date']} {row['time']}（{'場中＝既出' if row.get('intraday') else '引け後'}）"
    user = (f"銘柄: {row['name']}（{row['code']}）\n開示: {when}\n表題: {row['title']}\nタグ: {'/'.join(row.get('tags') or [])}\n"
            f"表題だけのルール判定: {(row.get('rule') or {}).get('label')} {(row.get('rule') or {}).get('score')}（{(row.get('rule') or {}).get('reason')}）\n"
            f"{ctx}\n\n--- 本文（PDF冒頭・{note}） ---\n{text if text else '（本文が取れないので表題だけで判定。confidence を下げる）'}")
    resp = client.messages.create(
        model=MODEL, max_tokens=1024,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": IRJudgeOut.SCHEMA}},
        system=[{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user}],
    )
    if resp.stop_reason == "refusal":
        return None
    txt = next((b.text for b in resp.content if b.type == "text"), "")
    try:
        out = json.loads(txt)
    except Exception:
        return None
    usage = getattr(resp, "usage", None)
    return {"label": out["label"], "score": int(out["score"]), "conf": int(out["confidence"]), "reason": str(out["reason"])[:120],
            "facts": str(out["facts"])[:160], "priced": bool(out["priced"]), "horizon": out["horizon"], "model": MODEL,
            "date": row["date"], "code": row["code"], "title": row["title"][:80], "src": note, "judged_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
            "tok_in": getattr(usage, "input_tokens", None), "tok_out": getattr(usage, "output_tokens", None),
            "tok_cache": getattr(usage, "cache_read_input_tokens", None)}


def pick_targets(rows: list[dict], judged: dict, limit: int = MAX_JUDGE) -> list[dict]:
    """要精査 → 材料タグ付きの好/悪（確認） の順、流動性の高い順。判定済みは飛ばす。"""
    items = judged.get("items") or {}
    cand = [r for r in rows if r.get("id") not in items and (r.get("material") or (r.get("rule") or {}).get("label") == "要精査")
            and not r.get("admin")]
    def key(r):
        lab = (r.get("rule") or {}).get("label")
        pri = 0 if lab == "要精査" else 1 if lab in ("好材料", "悪材料") else 2
        return (pri, -(r.get("tov20_oku") or 0))
    cand.sort(key=key)
    return cand[:limit]


def judge_rows(rows: list[dict], data: dict, limit: int = MAX_JUDGE) -> tuple[int, str, str]:
    """rows（ir_builder の行）を判定して ir_judged.json を更新。(判定数, model, note)。キーが無ければ (0, model, 理由)。"""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return 0, MODEL, "ANTHROPIC_API_KEY なし"
    try:
        import anthropic
    except Exception as e:
        return 0, MODEL, f"anthropic SDK なし: {e}"
    client = anthropic.Anthropic(max_retries=3, timeout=90.0)
    judged = load_judged()
    targets = pick_targets(rows, judged, limit)
    n = 0; t0 = time.time(); tin = tout = tcache = 0; fails = 0
    for r in targets:
        try:
            res = judge_one(client, r, data)
        except anthropic.AuthenticationError:
            return n, MODEL, "APIキーが無効（AuthenticationError）"
        except anthropic.RateLimitError:
            time.sleep(20); fails += 1
            if fails >= 3:
                break
            continue
        except Exception as e:
            print(f"  [ir_judge] {r['code']} 失敗: {str(e)[:100]}")
            fails += 1
            if fails >= 8:
                break
            continue
        if not res:
            continue
        judged["items"][r["id"]] = res
        n += 1; tin += res.get("tok_in") or 0; tout += res.get("tok_out") or 0; tcache += res.get("tok_cache") or 0
        if n % 10 == 0:
            save_judged(judged)
    if n:
        save_judged(judged)
    cost = (tin * 4 + tout * 20 + tcache * 0.4) / 1e6 if "opus" in MODEL else (tin * 2 + tout * 10) / 1e6 if "sonnet" in MODEL else (tin * 0.1 + tout * 0.5) / 1e6
    note = f"{n}件判定（候補{len(targets)}・失敗{fails}・{time.time()-t0:.0f}s・入力{tin:,}+cache{tcache:,}/出力{tout:,}tok ≈ ${cost:.2f}）"
    print(f"  [ir_judge] {MODEL}: {note}")
    return n, MODEL, note


# ── PC から: 今日の TDnet を取って判定 → ir_judged.json を main にコミット ──
def _upload(path: Path, message: str) -> bool:
    import base64, subprocess
    REPO = "aeon282499-hash/-"; DEST = "kabuai/ir_judged.json"
    content = base64.b64encode(path.read_bytes()).decode()
    sha = None
    try:
        r = subprocess.run(["gh", "api", f"repos/{REPO}/contents/{DEST}", "--jq", ".sha"], capture_output=True, text=True, timeout=60)
        if r.returncode == 0 and r.stdout.strip():
            sha = r.stdout.strip()
    except Exception:
        pass
    args = ["gh", "api", "-X", "PUT", f"repos/{REPO}/contents/{DEST}", "-f", f"message={message}", "-f", "branch=main", "-f", f"content={content}"]
    if sha:
        args += ["-f", f"sha={sha}"]
    r = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        print(f"  [ir_judge] アップロード失敗: {r.stderr.strip()[:300]}")
        return False
    return True


def main_pc() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv(PARENT / ".env")
    except Exception:
        pass
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("[ir_judge] ANTHROPIC_API_KEY が .env に無い → 何もしない（キーを入れると動く）")
        return 0
    sys.path.insert(0, str(HERE))
    import ir_builder as ib
    args = sys.argv[1:]
    ymd = next((a.split("=", 1)[1] for a in args if a.startswith("--date=")), None) or datetime.now(JST).strftime("%Y%m%d")
    dry = "--dry-run" in args
    # 本番の latest.json があれば、同じ日の行（価格文脈つき）を使う。無ければ TDnet から直接。
    rows: list[dict] = []
    try:
        import requests
        latest = requests.get("https://aeon282499-hash.github.io/-/data/latest.json", headers=UA, timeout=30).json()
        d_iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
        rows = [r for r in ((latest.get("ir") or {}).get("rows") or []) if r.get("date") == d_iso]
    except Exception:
        rows = []
    lst = ib.fetch_tdnet_day(ymd) or []
    have = {r["id"] for r in rows}
    d_iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
    for r in lst:
        if r["id"] in have:
            continue
        tags = ib.tag_title(r["title"]); s, lab, why = ib.rule_verdict(r["title"], tags, r["name"])
        rows.append({"id": r["id"], "date": d_iso, "time": r["time"], "intraday": bool(r["time"]) and r["time"] <= ib.SESSION_END,
                     "code": r["code"], "name": r["name"], "title": r["title"], "pdf": r["pdf"], "tags": tags,
                     "material": any(t in ib.MATERIAL_TAGS for t in tags), "admin": all(t in ib.ADMIN_TAGS or t in ("その他", "事務", "訂正") for t in tags),
                     "rule": {"label": lab, "score": s, "reason": why}, "tov20_oku": None})
    print(f"[ir_judge] {ymd}: TDnet {len(lst)}件 → 判定候補を選ぶ")
    n, model, note = judge_rows(rows, {})
    print(f"[ir_judge] {note}")
    if dry or n == 0:
        return 0
    ok = _upload(JUDGED_PATH, f"chore(kabuai): IR判定 {ymd} {n}件（{model}・PCから）")
    print("[ir_judge] アップロード成功 → kabuai.yml のビルドが走る" if ok else "[ir_judge] アップロード失敗")
    return 0 if ok else 1


if __name__ == "__main__":
    if "--pc" in sys.argv:
        sys.exit(main_pc())
    print(__doc__)

"""
ai_summary.py — KabuAI クローン フェーズ6 AI要約バッチ（無料LLM・1日1回）

SPEC.md §5 準拠:
  - 出力フォーマットを固定: {"note": 警戒メモ, "stance": "警戒|中立|期待"}
  - LLM は .env の KABUAI_LLM_PROVIDER で切替（gemini | groq | ollama）
  - 無料枠を超えないよう、LLM呼び出しは上位 N 銘柄のみ。残りは指標ベースの
    決定論メモ（rule）でまかなう。プロバイダ未設定/失敗時は全件 rule にフォールバック。

設計方針（このプロジェクトの「正直さ＋オフライン堅牢性」）:
  - キー無し/オフライン（CI初期・サンドボックス）でも必ず妥当な 警戒メモ を返す。
  - LLM 由来か rule 由来かを source で明示し、フロントで正直にラベル表示する。
  - TDnet 材料（事実/要因）の取り込みは将来フェーズ（ここでは technical のみ要約）。

純関数中心。build_data.py から annotate(rows, market) を呼ぶだけで各 row に
row["ai"] = {"note","stance","source"} が付く。
"""
from __future__ import annotations

import json
import os
import re
import time

# シグナルキー → 表示ラベル（LLM プロンプト用。signals.SIGNAL_DEFS と一致）
_SIGNAL_LABEL = {
    "strong_accum": "強買い集め", "accum": "買い集め", "accel": "加速", "promote": "昇格",
    "strong_dip": "強押し目", "dip": "押し目", "strong_reversal": "強反転",
    "reversal": "反転", "buzz": "話題集中",
}

VALID_STANCE = ("警戒", "中立", "期待")


# ── 決定論メモ（指標ベース・常に動く・無料・即時） ─────────────────────
def rule_note(r: dict) -> dict:
    """指標値だけから 警戒メモ + スタンスを組み立てる。LLM 不要・必ず返る。"""
    mom = r.get("momentum", 0.0)
    rsi = r.get("rsi", 50.0)
    sr = r.get("sr", 0.0)
    r5 = r.get("r5", 0.0)
    r20 = r.get("r20", 0.0)
    sigs = r.get("signals") or []

    # 優先度順（上から最初に当たったものを採用）。スタンスは signals.SIGNAL_DEFS（BT実績で
    # 正直化済み）と一致させる: 買いエッジ実証=strong_accum/strong_reversal(期待)・accum/reversal、
    # 過熱/反落傾向=accel/buzz(警戒)・promote/strong_dip/dip/高指数(中立)。
    if rsi >= 82 and mom >= 70:
        note = "天井圏に近く過熱が顕著。新規は見送り、利確・過熱解消を優先したい。"
        stance = "警戒"
    elif "strong_reversal" in sigs:
        note = "深い下落からの力強い切り返し。過去実績は買い系で最も勝率が高い（出口8日/−12%で約59%）。"
        stance = "期待"
    elif "buzz" in sigs:
        note = "出来高・値幅が急膨張し値動きが荒い。過去は天井圏での膨張が多く、高値掴みに注意。"
        stance = "警戒"
    elif "reversal" in sigs:
        note = "下落からの反転初動。だまし戻りの可能性があり、確度を見ながら小さく。"
        stance = "警戒"
    elif rsi >= 75:
        note = "短期過熱気味。追随より押し目待ちが無難。"
        stance = "警戒"
    elif "strong_accum" in sigs:
        note = "大商いを伴う強い資金流入。トレンド継続を見たいが、過熱には目配りを。"
        stance = "期待"
    elif "accel" in sigs:
        note = "モメンタムが直近で急騰。短期は過熱しやすく、過去は数日内に反落する傾向（追随は慎重に）。"
        stance = "警戒"
    elif "accum" in sigs:
        note = "出来高を伴う緩やかな上昇。仕込みの兆しを確認したい。"
        stance = "中立"
    elif "promote" in sigs:
        note = "指数が上位バンドへ昇格。バンド入りだけでは継続性は限定的で、過去実績は中立〜やや弱め。"
        stance = "中立"
    elif "strong_dip" in sigs:
        note = "強い上昇中の深い押し目。ただし発動が稀で勝率4割弱・近年は逆風、追随は慎重に。"
        stance = "中立"
    elif "dip" in sigs:
        note = "上昇トレンド中の小休止。反発は安定せず、深い押しは様子見が無難。"
        stance = "中立"
    elif mom >= 80:
        note = "高水準のモメンタムを維持。ただし最上位ほど反落しやすく、追随より利確に注意。"
        stance = "中立"
    elif mom < 20:
        note = "失速・停滞局面。リバウンド余地はあるが、明確な手掛かり待ち。"
        stance = "中立"
    elif r20 < -8 and r5 > 0:
        note = "下落一服から戻りを試す段階。反発の本物度を見極めたい。"
        stance = "警戒"
    else:
        note = "目立った過熱・失速はなく中立。指標の変化を待ちたい。"
        stance = "中立"

    # SR を一言添える（強弱の根拠）
    if sr >= 4:
        note += f"（SR {sr:.1f}＝極めて高い上昇効率）"
    elif sr <= -1:
        note += f"（SR {sr:.1f}＝下押し優勢）"
    return {"note": note, "stance": stance, "source": "rule"}


# ── LLM プロバイダ抽象（gemini | groq | ollama） ───────────────────────
def _provider() -> str:
    p = os.getenv("KABUAI_LLM_PROVIDER", "").strip().lower()
    if p in ("", "off", "none", "rule"):
        return ""
    # キーが無いプロバイダは無効化（rule にフォールバック）
    if p == "gemini" and not os.getenv("GEMINI_API_KEY", "").strip():
        return ""
    if p == "groq" and not os.getenv("GROQ_API_KEY", "").strip():
        return ""
    return p


def _prompt(r: dict) -> str:
    siglabels = "・".join(_SIGNAL_LABEL.get(k, k) for k in (r.get("signals") or [])) or "なし"
    return (
        "あなたは日本株のテクニカル分析アシスタントです。以下の銘柄の指標から、"
        "投資家向けの短い『警戒メモ』を日本語で1文（最大60字）作ってください。"
        "断定や推奨はせず、過熱・押し目・失速など状態の注意点を述べること。\n"
        f"銘柄: {r.get('name')} ({r.get('code')})\n"
        f"モメンタム指数: {r.get('momentum')} / ランク {r.get('grade')}\n"
        f"SR: {r.get('sr')}  POWER: {r.get('power')}  RSI: {r.get('rsi')}  STAB: {r.get('stab')}\n"
        f"リターン 前日 {r.get('r1')}% / 5日 {r.get('r5')}% / 1ヶ月 {r.get('r20')}%\n"
        f"点灯シグナル: {siglabels}\n"
        '必ず次のJSON1個だけを返す: {"note":"<60字以内>","stance":"警戒|中立|期待"}'
    )


def _extract_json(text: str) -> dict | None:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        o = json.loads(m.group(0))
    except Exception:
        return None
    note = str(o.get("note", "")).strip()
    stance = str(o.get("stance", "")).strip()
    if not note or stance not in VALID_STANCE:
        return None
    return {"note": note[:80], "stance": stance}


def _call_gemini(prompt: str) -> str:
    import requests
    key = os.getenv("GEMINI_API_KEY", "").strip()
    model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    resp = requests.post(url, params={"key": key},
                         json={"contents": [{"parts": [{"text": prompt}]}],
                               "generationConfig": {"temperature": 0.4, "maxOutputTokens": 200}},
                         timeout=30)
    resp.raise_for_status()
    return resp.json()["candidates"][0]["content"]["parts"][0]["text"]


def _call_groq(prompt: str) -> str:
    import requests
    key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
    resp = requests.post("https://api.groq.com/openai/v1/chat/completions",
                         headers={"Authorization": f"Bearer {key}"},
                         json={"model": model, "temperature": 0.4,
                               "messages": [{"role": "user", "content": prompt}]},
                         timeout=30)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_ollama(prompt: str) -> str:
    import requests
    base = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
    model = os.getenv("OLLAMA_MODEL", "llama3.1")
    resp = requests.post(f"{base}/api/generate",
                         json={"model": model, "prompt": prompt, "stream": False,
                               "options": {"temperature": 0.4}},
                         timeout=60)
    resp.raise_for_status()
    return resp.json().get("response", "")


_CALLERS = {"gemini": _call_gemini, "groq": _call_groq, "ollama": _call_ollama}


def llm_note(r: dict, provider: str) -> dict | None:
    caller = _CALLERS.get(provider)
    if caller is None:
        return None
    try:
        text = caller(_prompt(r))
    except Exception:
        return None
    parsed = _extract_json(text)
    if parsed is None:
        return None
    parsed["source"] = provider
    return parsed


# ── バッチ入口 ────────────────────────────────────────────────────────
def annotate(rows: list[dict], *, llm_top: int | None = None) -> dict:
    """export対象 rows に row["ai"]={note,stance,source} を付与（in-place）。
    上位 llm_top 件のみ LLM（設定時）、それ以外は rule。戻り値はメタ情報。"""
    provider = _provider()
    if llm_top is None:
        llm_top = int(os.getenv("KABUAI_AI_LLM_TOP", "24"))

    ranked = sorted(rows, key=lambda x: x.get("rank", 1e9))
    llm_used = 0
    llm_budget = llm_top if provider else 0
    for r in ranked:
        ai = None
        if llm_used < llm_budget:
            ai = llm_note(r, provider)
            if ai is not None:
                llm_used += 1
                time.sleep(1.0)  # 無料枠レート配慮（Gemini 15rpm 等）
        if ai is None:
            ai = rule_note(r)
        r["ai"] = ai
    return {"provider": provider or "rule", "llm_notes": llm_used,
            "rule_notes": len(rows) - llm_used, "total": len(rows)}

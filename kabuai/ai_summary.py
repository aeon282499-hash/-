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

    # 優先度順（上から最初に当たった枝を採用）。はじめての人にも分かるよう用語をかみ砕く
    #   （RSI=買われすぎ度／指数=勢いスコア／押し目=上昇途中の一時的な下げ 等）。
    # スタンス(警戒/中立/期待)は「いまの過熱・リスク状態の注意書き」で、それ単体は買い推奨ではない。
    # ✅検証済み買い候補はスタンスとは別軸＝BTゲート(勝率52%以上&平均プラスの得意保有)を満たす
    #   strong_accum / accum / strong_reversal / reversal（フロント sigHasEdge と同じ判定）。
    #   ※reversal は注意書きが「警戒」でも買い候補に入る（だまし戻り注意のため小さく）。
    # signals.SIGNAL_DEFS（BT実績で正直化済み）とスタンス一致: 期待=strong_accum/strong_reversal、
    #   警戒=accel/buzz/reversal、中立=accum/promote/strong_dip/dip/高指数。
    # 具体的な勝率%はライブのトラックレコード/出口calloutで表示するため、ここでは陳腐化しない
    #   言葉（パターンの説明）で書く＝数字の古さで不正確にならないようにする。
    if rsi >= 82 and mom >= 70:
        note = "買われすぎ度(RSI)も勢いも極端に高く、天井圏で過熱しています。いま新規で買うと高値づかみになりやすく、持っている人は利益確定も考えたい場面。"
        stance = "警戒"
    elif "strong_reversal" in sigs:
        note = "大きく下げたあと、強く反発し始めた初動です。過去の検証では買い系の中で最も成績が良かったパターン。下の『出口の目安』も参考にしてください。"
        stance = "期待"
    elif "buzz" in sigs:
        note = "出来高と値幅が急に膨らみ、値動きが荒くなっています。過去はこうした急膨張が天井近くで起きやすく、飛びつくと高値づかみになりがち。"
        stance = "警戒"
    elif "reversal" in sigs:
        note = "下げ止まって上向きかけた初動で、検証では買い系の一つです。ただし一時的な戻り(だまし)で終わることも多く、本物か確かめながら小さく。"
        stance = "警戒"
    elif rsi >= 75:
        note = "買われすぎ度(RSI)が高く、短期的に過熱気味です。追いかけ買いより、一度下げて落ち着くのを待つ(押し目買い)ほうが無難。"
        stance = "警戒"
    elif "strong_accum" in sigs:
        note = "大きな出来高を伴って強い買いが入り、資金が集まっています。上昇トレンドの継続に期待できますが、過熱には気を配りたい。"
        stance = "期待"
    elif "accel" in sigs:
        note = "ここ数日で急に勢いが付いた急騰局面です。短期は過熱しやすく、過去は数日のうちに反落しがち。飛びつき買いは慎重に。"
        stance = "警戒"
    elif "accum" in sigs:
        note = "出来高を伴ってじわじわ上昇中。大きな買いが静かに入り始めた『仕込み』の兆しです。上昇が続くか確認したい段階。"
        stance = "中立"
    elif "promote" in sigs:
        note = "勢いスコアが一つ上のグレードに入ったところです。格上げだけで上昇が続くとは限らず、過去の成績は中立〜やや弱めでした。"
        stance = "中立"
    elif "strong_dip" in sigs:
        note = "強い上昇の途中での深い下げ(押し目)です。ただし出ること自体が稀で勝率は4割弱、近ごろは成績が振るわず、飛びつきは慎重に。"
        stance = "中立"
    elif "dip" in sigs:
        note = "上昇トレンド中の小さな一服(押し目)です。反発は安定しないことが多く、特に深い下げは様子見が無難。"
        stance = "中立"
    elif mom >= 80:
        note = "勢いスコアが高い水準を保っています。ただし最上位ほど反落しやすい傾向があり、追いかけより利益確定に注意したい場面。"
        stance = "中立"
    elif mom < 20:
        note = "勢いがなく、停滞・失速している局面です。反発の余地はありますが、上向くきっかけ待ち。"
        stance = "中立"
    elif r20 < -8 and r5 > 0:
        note = "1ヶ月では下げ、直近5日で戻りを試している段階です。その戻りが本物か(だましでないか)を見極めたい。"
        stance = "警戒"
    else:
        note = "目立った過熱も失速もなく、落ち着いた状態です。指標の変化を待ちたい。"
        stance = "中立"

    # 強弱の根拠を一言添える（SR＝上昇効率。プラスほど効率よく上げている）
    if sr >= 4:
        note += f"（上昇効率SR {sr:.1f}＝非常に高い）"
    elif sr <= -1:
        note += f"（上昇効率SR {sr:.1f}＝マイナスで下押し優勢）"
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

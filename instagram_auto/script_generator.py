"""
台本生成モジュール（Claude API）
- 型・商品・アカウント情報をもとに台本を生成
- AIには「テキストを考える」仕事だけを任せる
- テロップ整形・フォーマットチェックはプログラムが担当
"""
import json
import re
import anthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL


def _build_prompt(template: dict, product: dict, account: dict) -> str:
    scenes_desc = "\n".join(
        f"  シーン{s['order']} [{s['id']}] ({s['duration_sec']}秒): {s['purpose']}\n"
        f"    ナレーションヒント: {s['narration_hint']}"
        for s in template["scenes"]
    )
    return f"""あなたはInstagramのショート動画の台本ライターです。

【アカウント情報】
- アカウント名: {account['name']}
- ジャンル: {account['genre']}
- ターゲット: {account['target_audience']}
- ペルソナ: {account['persona']}
- 口調: {account['tone']}
- CTA文言: {account['cta_text']}

【紹介商品】
- 商品名: {product['name']}
- カテゴリ: {product['category']}
- 価格: {product['price']}
- 特徴: {', '.join(product['features'])}
- 解決する悩み: {product['target_problem']}
- 期待できる結果: {product['result']}

【動画の型】
- 型名: {template['name']}
- 説明: {template['description']}
- 合計尺: {template['total_duration_sec']}秒

【各シーン（この通りの数・順番で台本を書くこと）】
{scenes_desc}

【出力フォーマット（必ずこのJSONのみ出力すること）】
{{
  "scenes": [
    {{
      "id": "シーンID",
      "narration": "ナレーションテキスト（読み上げ用。句読点なし。自然な話し言葉で。）",
      "telop": "テロップテキスト（画面に表示する文言。15〜25文字以内。句読点なし。）"
    }}
  ],
  "caption": "投稿キャプション（200文字以内。ハッシュタグは含めない。絵文字OK。改行で読みやすく。）"
}}

【制約】
- シーンの数は必ず {len(template['scenes'])} 個
- シーンのidは上記の型の通りに使うこと
- narrationは句読点（。、！？）を使わない
- telopは句読点を使わない、15〜25文字以内
- captionにハッシュタグは含めない
- JSONのみ出力。前後に説明文は不要
"""


def _format_telop(text: str) -> str:
    """テロップを整形する（句読点削除・長すぎる場合の切り詰め）"""
    # 句読点削除
    text = re.sub(r'[。、！？!?.,]', '', text)
    # 25文字超の場合は切り詰め
    if len(text) > 25:
        text = text[:24] + '…'
    return text.strip()


def _format_narration(text: str) -> str:
    """ナレーションを整形する（句読点削除）"""
    text = re.sub(r'[。、！？!?]', ' ', text)
    return text.strip()


def _validate_and_fix(result: dict, template: dict) -> dict:
    """出力を検証・修正する"""
    expected_ids = [s["id"] for s in template["scenes"]]
    result_ids = [s["id"] for s in result.get("scenes", [])]

    if result_ids != expected_ids:
        raise ValueError(
            f"シーンIDが型と一致しません。期待: {expected_ids}, 実際: {result_ids}"
        )

    for scene in result["scenes"]:
        scene["telop"] = _format_telop(scene.get("telop", ""))
        scene["narration"] = _format_narration(scene.get("narration", ""))

    return result


def generate_script(template: dict, product: dict, account: dict) -> dict:
    """
    台本を生成する

    Args:
        template: テンプレート辞書
        product: 商品辞書
        account: アカウント辞書

    Returns:
        {
          "scenes": [{"id": str, "narration": str, "telop": str}, ...],
          "caption": str
        }
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = _build_prompt(template, product, account)

    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )

    raw_text = message.content[0].text.strip()

    # JSON部分だけ抽出（前後に余分なテキストがある場合に対応）
    json_match = re.search(r'\{[\s\S]*\}', raw_text)
    if not json_match:
        raise ValueError(f"JSONが見つかりません: {raw_text[:200]}")

    result = json.loads(json_match.group())
    result = _validate_and_fix(result, template)

    return result

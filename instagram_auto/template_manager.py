"""
テンプレート（型）管理モジュール
- テンプレートJSONの読み込み
- 直近の投稿と被らないようにランダム選択
"""
import json
import random
from pathlib import Path
from datetime import datetime, timedelta
from config import TEMPLATES_DIR, TEMPLATE_HISTORY_FILE


def load_all_templates() -> list[dict]:
    """templatesディレクトリ内のJSONをすべて読み込む"""
    templates = []
    for template_file in sorted(TEMPLATES_DIR.glob("*.json")):
        with open(template_file, "r", encoding="utf-8") as f:
            templates.append(json.load(f))
    return templates


def load_template_history() -> list[dict]:
    """直近の使用履歴を読み込む"""
    if not TEMPLATE_HISTORY_FILE.exists():
        return []
    with open(TEMPLATE_HISTORY_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_template_history(history: list[dict]) -> None:
    """使用履歴を保存（直近30件のみ保持）"""
    history = history[-30:]
    with open(TEMPLATE_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def select_template(avoid_recent: int = 3) -> dict:
    """
    直近 avoid_recent 件と被らないようにテンプレートを選択する

    Args:
        avoid_recent: 直近何件のテンプレートと被らないようにするか

    Returns:
        選択されたテンプレートの辞書
    """
    all_templates = load_all_templates()
    if not all_templates:
        raise ValueError("templates/ にテンプレートファイルが存在しません")

    history = load_template_history()
    recent_ids = [h["template_id"] for h in history[-avoid_recent:]]

    # 直近と被らない候補を絞り込む
    candidates = [t for t in all_templates if t["id"] not in recent_ids]

    # 全テンプレートが直近で使われている場合は全候補から選ぶ
    if not candidates:
        candidates = all_templates

    selected = random.choice(candidates)

    # 履歴に追記
    history.append({
        "template_id": selected["id"],
        "used_at": datetime.now().isoformat()
    })
    save_template_history(history)

    return selected

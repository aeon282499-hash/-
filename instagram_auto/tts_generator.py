"""
ナレーション音声生成モジュール（ElevenLabs TTS）
- 各シーンのnarrationテキストを音声ファイルに変換
- アカウント設定の voice_id に従って音声キャラクターを切り替える
"""
import json
import time
from pathlib import Path
import requests
from config import ELEVENLABS_API_KEY, LOGS_DIR

ELEVENLABS_API_BASE = "https://api.elevenlabs.io/v1"


def _get_voice_id(account: dict) -> str:
    """アカウント設定からvoice_idを取得（未設定の場合はデフォルト）"""
    voice_id = account.get("voice_id") or ""
    if not voice_id:
        # ElevenLabsのデフォルト無料音声
        voice_id = "21m00Tcm4TlvDq8ikWAM"  # Rachel (English default)
    return voice_id


def generate_audio_for_scene(
    narration_text: str,
    voice_id: str,
    output_path: Path,
    stability: float = 0.5,
    similarity_boost: float = 0.75,
    speed: float = 1.1,
) -> Path:
    """
    1シーン分のナレーション音声を生成する

    Args:
        narration_text: ナレーションテキスト
        voice_id: ElevenLabsの音声ID
        output_path: 出力先ファイルパス（.mp3）
        stability: 安定性（0〜1）
        similarity_boost: 類似度ブースト（0〜1）
        speed: 読み上げ速度（0.7〜1.2推奨）

    Returns:
        生成された音声ファイルのパス
    """
    url = f"{ELEVENLABS_API_BASE}/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        "Accept": "audio/mpeg",
    }
    payload = {
        "text": narration_text,
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability": stability,
            "similarity_boost": similarity_boost,
            "speed": speed,
        },
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)
    return output_path


def generate_narrations(
    scenes: list[dict],
    account: dict,
    output_dir: Path,
    job_id: str,
) -> list[dict]:
    """
    全シーンのナレーション音声を生成する

    Args:
        scenes: 台本のシーンリスト（{"id": str, "narration": str, ...}）
        account: アカウント設定辞書
        output_dir: 音声ファイルの出力先ディレクトリ
        job_id: ジョブID（ファイル名に使用）

    Returns:
        各シーンに audio_path を追加したリスト
    """
    voice_id = _get_voice_id(account)
    result = []

    for i, scene in enumerate(scenes):
        narration = scene.get("narration", "")
        if not narration.strip():
            result.append({**scene, "audio_path": None})
            continue

        audio_path = output_dir / f"{job_id}_scene_{scene['id']}.mp3"

        # レート制限対策：シーン間に少し待機
        if i > 0:
            time.sleep(0.5)

        generate_audio_for_scene(
            narration_text=narration,
            voice_id=voice_id,
            output_path=audio_path,
        )

        result.append({**scene, "audio_path": str(audio_path)})

    return result

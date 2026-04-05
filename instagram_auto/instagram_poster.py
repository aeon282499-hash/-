"""
Instagram投稿モジュール（Facebook Graph API）
- 動画をInstagramに予約投稿する
- Instagram Business Account + Facebook Graph API v19.0 を使用
- 公開サーバーに動画をホストして動画URLを渡す方式
"""
import json
import time
from datetime import datetime
from pathlib import Path

import requests

from config import (
    FACEBOOK_GRAPH_API_BASE,
    INSTAGRAM_ACCESS_TOKEN,
    INSTAGRAM_BUSINESS_ACCOUNT_ID,
    POSTING_HISTORY_FILE,
)


def _api_post(endpoint: str, params: dict) -> dict:
    """Graph API POSTリクエスト（共通ラッパー）"""
    params["access_token"] = INSTAGRAM_ACCESS_TOKEN
    response = requests.post(
        f"{FACEBOOK_GRAPH_API_BASE}/{endpoint}",
        data=params,
        timeout=60,
    )
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"Graph API エラー: {data['error']}")
    return data


def _api_get(endpoint: str, params: dict | None = None) -> dict:
    """Graph API GETリクエスト（共通ラッパー）"""
    params = params or {}
    params["access_token"] = INSTAGRAM_ACCESS_TOKEN
    response = requests.get(
        f"{FACEBOOK_GRAPH_API_BASE}/{endpoint}",
        params=params,
        timeout=60,
    )
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"Graph API エラー: {data['error']}")
    return data


def upload_reel(
    video_url: str,
    caption: str,
    hashtags: list[str],
    scheduled_publish_time: datetime | None = None,
) -> dict:
    """
    Instagramにリール動画をアップロード（予約投稿対応）

    Args:
        video_url: 公開アクセス可能な動画URL（mp4）
        caption: 投稿キャプション
        hashtags: ハッシュタグリスト（例: ["#美容", "#スキンケア"]）
        scheduled_publish_time: 予約投稿日時（Noneなら即時公開）

    Returns:
        {"creation_id": str, "published": bool, "scheduled_at": str}
    """
    full_caption = caption + "\n\n" + " ".join(hashtags)

    # Step1: メディアコンテナを作成
    container_params = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": full_caption,
        "share_to_feed": "true",
    }

    if scheduled_publish_time:
        container_params["published"] = "false"
        container_params["scheduled_publish_time"] = str(
            int(scheduled_publish_time.timestamp())
        )

    container_data = _api_post(
        f"{INSTAGRAM_BUSINESS_ACCOUNT_ID}/media",
        container_params,
    )
    creation_id = container_data["id"]

    # Step2: アップロード完了を待機（最大10分）
    _wait_for_upload(creation_id)

    if scheduled_publish_time:
        return {
            "creation_id": creation_id,
            "published": False,
            "scheduled_at": scheduled_publish_time.isoformat(),
        }

    # Step3: 即時公開
    publish_data = _api_post(
        f"{INSTAGRAM_BUSINESS_ACCOUNT_ID}/media_publish",
        {"creation_id": creation_id},
    )

    return {
        "creation_id": creation_id,
        "media_id": publish_data.get("id"),
        "published": True,
        "scheduled_at": None,
    }


def _wait_for_upload(creation_id: str, timeout_sec: int = 600, interval_sec: int = 10) -> None:
    """アップロード完了ステータスをポーリングして待機する"""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        status_data = _api_get(
            creation_id,
            params={"fields": "status_code,status"},
        )
        status = status_data.get("status_code", "")

        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(
                f"動画アップロードエラー: {status_data.get('status')}"
            )

        time.sleep(interval_sec)

    raise TimeoutError(f"動画アップロードがタイムアウトしました (creation_id={creation_id})")


def save_posting_history(record: dict) -> None:
    """投稿履歴をJSONファイルに追記する"""
    history = []
    if POSTING_HISTORY_FILE.exists():
        with open(POSTING_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)

    history.append(record)

    with open(POSTING_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_posting_history() -> list[dict]:
    """投稿履歴を読み込む"""
    if not POSTING_HISTORY_FILE.exists():
        return []
    with open(POSTING_HISTORY_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

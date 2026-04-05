"""
LINE通知モジュール（LINE Messaging API）
- 投稿完了・エラー等のイベントをLINEにプッシュ通知する
"""
import requests
from config import LINE_CHANNEL_ACCESS_TOKEN, LINE_USER_ID

LINE_PUSH_API_URL = "https://api.line.me/v2/bot/message/push"


def send_line_message(message: str, user_id: str | None = None) -> bool:
    """
    LINEにメッセージを送信する

    Args:
        message: 送信するテキストメッセージ
        user_id: 送信先ユーザーID（Noneの場合は設定値を使用）

    Returns:
        送信成功なら True
    """
    if not LINE_CHANNEL_ACCESS_TOKEN or not (user_id or LINE_USER_ID):
        print(f"[LINE通知スキップ] {message}")
        return False

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    payload = {
        "to": user_id or LINE_USER_ID,
        "messages": [{"type": "text", "text": message}],
    }

    response = requests.post(LINE_PUSH_API_URL, headers=headers, json=payload, timeout=30)
    return response.status_code == 200


def notify_post_complete(
    product_name: str,
    template_name: str,
    scheduled_at: str | None,
    job_id: str,
) -> None:
    """予約投稿完了通知"""
    if scheduled_at:
        time_str = f"予約投稿: {scheduled_at}"
    else:
        time_str = "即時公開済み"

    message = (
        f"✅ 予約投稿完了\n"
        f"商品: {product_name}\n"
        f"型: {template_name}\n"
        f"{time_str}\n"
        f"Job ID: {job_id}"
    )
    send_line_message(message)


def notify_error(job_id: str, error_message: str) -> None:
    """エラー通知"""
    message = (
        f"❌ エラーが発生しました\n"
        f"Job ID: {job_id}\n"
        f"エラー: {error_message[:200]}"
    )
    send_line_message(message)


def notify_daily_summary(total_posts: int, success: int, failed: int) -> None:
    """1日の投稿サマリー通知"""
    message = (
        f"📊 本日の投稿サマリー\n"
        f"合計: {total_posts}本\n"
        f"成功: {success}本\n"
        f"失敗: {failed}本"
    )
    send_line_message(message)

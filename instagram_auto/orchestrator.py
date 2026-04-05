"""
メインオーケストレーター
毎朝cronで起動し、以下を自動実行する：
  1. 型を選ぶ
  2. 商品を選ぶ
  3. Claude APIで台本を生成
  4. ElevenLabsでナレーション音声を生成
  5. moviepyで動画を合成
  6. S3に動画をアップロード
  7. Instagram Graph APIで予約投稿
  8. LINEに通知
"""
import json
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from config import (
    ACCOUNT_FILE,
    LOGS_DIR,
    MIN_INTERVAL_HOURS,
    OUTPUT_DIR,
    POSTS_PER_DAY,
    PRODUCTS_FILE,
)
from instagram_poster import load_posting_history, save_posting_history, upload_reel
from line_notifier import notify_daily_summary, notify_error, notify_post_complete
from script_generator import generate_script
from template_manager import select_template
from tts_generator import generate_narrations
from video_composer import compose_video
from video_uploader import upload_video

# ログ設定
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"run_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def load_account() -> dict:
    with open(ACCOUNT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_active_products() -> list[dict]:
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as f:
        products = json.load(f)
    return [p for p in products if p.get("active", True)]


def select_product(products: list[dict], history: list[dict]) -> dict:
    """直近で使われていない商品を優先して選択する"""
    recent_product_ids = [h.get("product_id") for h in history[-10:]]
    candidates = [p for p in products if p["id"] not in recent_product_ids]
    if not candidates:
        candidates = products
    return random.choice(candidates)


def calculate_scheduled_times(n: int, base_time: datetime) -> list[datetime]:
    """1日n本分の投稿時刻を計算する（base_timeから最低MIN_INTERVAL_HOURS間隔）"""
    times = []
    current = base_time
    for _ in range(n):
        times.append(current)
        current += timedelta(hours=MIN_INTERVAL_HOURS)
    return times


def run_single_post(
    product: dict,
    account: dict,
    scheduled_at: datetime,
    job_id: str,
) -> dict:
    """
    1本の動画を生成して予約投稿する

    Returns:
        投稿結果の記録辞書
    """
    audio_dir = OUTPUT_DIR / job_id
    audio_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"[{job_id}] 型を選択中...")
    template = select_template(avoid_recent=3)
    log.info(f"[{job_id}] 選択した型: {template['name']}")

    log.info(f"[{job_id}] 台本を生成中...")
    script = generate_script(template, product, account)
    log.info(f"[{job_id}] 台本生成完了 ({len(script['scenes'])} シーン)")

    log.info(f"[{job_id}] ナレーション音声を生成中...")
    scenes_with_audio = generate_narrations(
        script["scenes"], account, audio_dir, job_id
    )
    log.info(f"[{job_id}] 音声生成完了")

    log.info(f"[{job_id}] 動画を合成中...")
    video_path = compose_video(scenes_with_audio, template, job_id)
    log.info(f"[{job_id}] 動画合成完了: {video_path}")

    log.info(f"[{job_id}] 動画をアップロード中...")
    video_url = upload_video(video_path)
    log.info(f"[{job_id}] アップロード完了: {video_url}")

    log.info(f"[{job_id}] Instagramに予約投稿中 (予定: {scheduled_at})...")
    post_result = upload_reel(
        video_url=video_url,
        caption=script["caption"],
        hashtags=account.get("hashtags", []),
        scheduled_publish_time=scheduled_at,
    )
    log.info(f"[{job_id}] 予約投稿完了: {post_result}")

    record = {
        "job_id": job_id,
        "product_id": product["id"],
        "product_name": product["name"],
        "template_id": template["id"],
        "template_name": template["name"],
        "video_path": str(video_path),
        "video_url": video_url,
        "scheduled_at": scheduled_at.isoformat(),
        "post_result": post_result,
        "created_at": datetime.now().isoformat(),
    }

    notify_post_complete(
        product_name=product["name"],
        template_name=template["name"],
        scheduled_at=scheduled_at.isoformat(),
        job_id=job_id,
    )

    return record


def run_daily() -> None:
    """毎朝のメイン実行エントリーポイント"""
    log.info("=" * 50)
    log.info("Instagram自動投稿 開始")
    log.info("=" * 50)

    account = load_account()
    products = load_active_products()
    history = load_posting_history()

    if not products:
        log.error("有効な商品が products.json にありません")
        sys.exit(1)

    # 今日の投稿スケジュールを計算（9:00から2時間おきに n 本）
    now = datetime.now()
    base_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now.hour >= 9:
        # すでに9時を過ぎていたら次の時刻から
        base_time = now + timedelta(minutes=5)

    scheduled_times = calculate_scheduled_times(POSTS_PER_DAY, base_time)

    success_count = 0
    fail_count = 0

    for i, scheduled_at in enumerate(scheduled_times):
        job_id = f"{datetime.now().strftime('%Y%m%d')}_{i+1:02d}_{uuid.uuid4().hex[:8]}"
        product = select_product(products, history)

        log.info(f"\n--- 投稿 {i+1}/{POSTS_PER_DAY}: {product['name']} ---")

        try:
            record = run_single_post(product, account, scheduled_at, job_id)
            save_posting_history(record)
            history.append(record)
            success_count += 1
        except Exception as e:
            log.error(f"[{job_id}] エラー: {e}", exc_info=True)
            notify_error(job_id=job_id, error_message=str(e))
            fail_count += 1

    log.info("\n" + "=" * 50)
    log.info(f"完了: 成功 {success_count}本 / 失敗 {fail_count}本")
    log.info("=" * 50)

    notify_daily_summary(
        total_posts=POSTS_PER_DAY,
        success=success_count,
        failed=fail_count,
    )


if __name__ == "__main__":
    run_daily()

"""
動画ホスティングモジュール
- ローカルの動画ファイルを公開アクセス可能なURLにアップロードする
- Instagram Graph API は公開URLからの動画取得が必要なため
- AWS S3（推奨）または Cloudflare R2 を使用
"""
import os
from pathlib import Path

import boto3
from botocore.config import Config
from config import OUTPUT_DIR

# AWS S3 / Cloudflare R2 設定（.envから読み込む）
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-1")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")  # Cloudflare R2の場合は設定必要
S3_PUBLIC_BASE_URL = os.getenv("S3_PUBLIC_BASE_URL", "")  # 公開URL（CloudFront等）


def upload_video_to_s3(local_path: Path) -> str:
    """
    動画ファイルをS3（またはCloudflare R2）にアップロードして公開URLを返す

    Args:
        local_path: ローカルの動画ファイルパス

    Returns:
        公開アクセス可能な動画URL
    """
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET が設定されていません (.env を確認してください)")

    client_kwargs = dict(
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL

    s3 = boto3.client("s3", **client_kwargs)

    s3_key = f"instagram_videos/{local_path.name}"
    s3.upload_file(
        str(local_path),
        S3_BUCKET,
        s3_key,
        ExtraArgs={"ContentType": "video/mp4"},
    )

    if S3_PUBLIC_BASE_URL:
        return f"{S3_PUBLIC_BASE_URL.rstrip('/')}/{s3_key}"

    # パブリックバケットの場合のデフォルトURL
    if S3_ENDPOINT_URL:
        return f"{S3_ENDPOINT_URL.rstrip('/')}/{S3_BUCKET}/{s3_key}"

    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"


def upload_video(local_path: Path) -> str:
    """
    動画をホスティングサービスにアップロードして公開URLを返すエントリーポイント

    Args:
        local_path: ローカルの動画ファイルパス

    Returns:
        公開アクセス可能な動画URL
    """
    return upload_video_to_s3(local_path)

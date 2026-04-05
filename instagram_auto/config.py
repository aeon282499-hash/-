"""
Instagram自動アフィリエイト - 設定ファイル
環境変数から設定を読み込む
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-opus-4-6"

# ElevenLabs TTS
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "")  # アカウント属性に合わせた声

# Instagram Graph API
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN", "")
INSTAGRAM_BUSINESS_ACCOUNT_ID = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID", "")
FACEBOOK_GRAPH_API_VERSION = "v19.0"
FACEBOOK_GRAPH_API_BASE = f"https://graph.facebook.com/{FACEBOOK_GRAPH_API_VERSION}"

# LINE Messaging API
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")  # 通知先のユーザーID

# 動画設定
VIDEO_WIDTH = 1080
VIDEO_HEIGHT = 1920  # 9:16 (縦型ショート動画)
VIDEO_FPS = 30

# 投稿設定
POSTS_PER_DAY = int(os.getenv("POSTS_PER_DAY", "3"))
MIN_INTERVAL_HOURS = 2  # 最低2時間おき

# パス設定
TEMPLATES_DIR = BASE_DIR / "templates"
MATERIALS_DIR = BASE_DIR / "materials"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
POSTING_HISTORY_FILE = BASE_DIR / "posting_history.json"
TEMPLATE_HISTORY_FILE = BASE_DIR / "template_history.json"

# アカウント設定（products.json から読み込む）
PRODUCTS_FILE = BASE_DIR / "products.json"
ACCOUNT_FILE = BASE_DIR / "account.json"

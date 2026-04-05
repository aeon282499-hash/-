"""
動画合成モジュール（moviepy）
- 素材 + テロップ + ナレーション音声 + BGM を1本の動画に合成する
- 型（テンプレート）に従ってシーンを順番に並べる
"""
import random
from pathlib import Path

from moviepy import (
    AudioFileClip,
    ColorClip,
    CompositeAudioClip,
    CompositeVideoClip,
    ImageClip,
    TextClip,
    VideoFileClip,
    concatenate_videoclips,
)
from config import (
    MATERIALS_DIR,
    OUTPUT_DIR,
    VIDEO_FPS,
    VIDEO_HEIGHT,
    VIDEO_WIDTH,
)

# テロップスタイル定義
TEXT_STYLES = {
    "large_bold": {
        "font_size": 72,
        "color": "white",
        "stroke_color": "black",
        "stroke_width": 3,
        "method": "caption",
        "size": (VIDEO_WIDTH - 80, None),
    },
    "subtitle": {
        "font_size": 52,
        "color": "white",
        "stroke_color": "black",
        "stroke_width": 2,
        "method": "caption",
        "size": (VIDEO_WIDTH - 80, None),
    },
    "list_item": {
        "font_size": 54,
        "color": "yellow",
        "stroke_color": "black",
        "stroke_width": 2,
        "method": "caption",
        "size": (VIDEO_WIDTH - 80, None),
    },
    "cta_large": {
        "font_size": 64,
        "color": "#FFD700",  # ゴールド
        "stroke_color": "black",
        "stroke_width": 3,
        "method": "caption",
        "size": (VIDEO_WIDTH - 80, None),
    },
}

# テロップ縦位置
TEXT_POSITIONS = {
    "center": ("center", "center"),
    "bottom": ("center", VIDEO_HEIGHT - 200),
    "top": ("center", 80),
}


def _load_material_clip(material_type: str, duration: float) -> VideoFileClip | ColorClip:
    """
    material_typeに合う素材ファイルを materials/ から検索してロード。
    見つからない場合は黒画面で代替する。
    """
    search_dirs = [
        MATERIALS_DIR / material_type,
        MATERIALS_DIR / "video",
        MATERIALS_DIR,
    ]

    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        candidates = list(search_dir.glob("*.mp4")) + list(search_dir.glob("*.mov"))
        if candidates:
            chosen = random.choice(candidates)
            try:
                clip = VideoFileClip(str(chosen)).resized((VIDEO_WIDTH, VIDEO_HEIGHT))
                # 必要な秒数に切り詰めまたはループ
                if clip.duration < duration:
                    clip = clip.loop(duration=duration)
                else:
                    clip = clip.subclipped(0, duration)
                return clip
            except Exception:
                continue

    # 素材がなければ黒画面で代替
    return ColorClip(size=(VIDEO_WIDTH, VIDEO_HEIGHT), color=(0, 0, 0), duration=duration)


def _make_telop_clip(
    telop_text: str,
    style_name: str,
    position_name: str,
    duration: float,
) -> TextClip | None:
    """テキストオーバーレイClipを作成する"""
    if not telop_text.strip():
        return None

    style = TEXT_STYLES.get(style_name, TEXT_STYLES["subtitle"])

    txt_clip = TextClip(
        text=telop_text,
        font_size=style["font_size"],
        color=style["color"],
        stroke_color=style["stroke_color"],
        stroke_width=style["stroke_width"],
        method=style["method"],
        size=style["size"],
        duration=duration,
    )

    pos = TEXT_POSITIONS.get(position_name, TEXT_POSITIONS["bottom"])
    return txt_clip.with_position(pos)


def _make_scene_clip(scene_data: dict, template_scene: dict) -> CompositeVideoClip:
    """
    1シーン分のCompositeVideoClipを作成する

    Args:
        scene_data: 台本+音声パス付きシーン辞書
        template_scene: テンプレートのシーン設定辞書
    """
    duration = float(template_scene["duration_sec"])
    material_type = template_scene.get("material_type", "video")
    text_style = template_scene.get("text_style", "subtitle")
    text_position = template_scene.get("text_position", "bottom")

    # 背景映像
    bg_clip = _load_material_clip(material_type, duration)

    layers = [bg_clip]

    # テロップ
    telop = scene_data.get("telop", "")
    if telop:
        txt_clip = _make_telop_clip(telop, text_style, text_position, duration)
        if txt_clip:
            layers.append(txt_clip)

    composite = CompositeVideoClip(layers, size=(VIDEO_WIDTH, VIDEO_HEIGHT))

    # ナレーション音声
    audio_path = scene_data.get("audio_path")
    if audio_path and Path(audio_path).exists():
        narration_audio = AudioFileClip(audio_path)
        # 動画尺に合わせてトリム or パディング
        if narration_audio.duration > duration:
            narration_audio = narration_audio.subclipped(0, duration)
        composite = composite.with_audio(narration_audio)

    return composite


def _pick_bgm() -> Path | None:
    """BGM素材ディレクトリからランダムに1曲選ぶ"""
    bgm_dir = MATERIALS_DIR / "bgm"
    if not bgm_dir.exists():
        return None
    candidates = list(bgm_dir.glob("*.mp3")) + list(bgm_dir.glob("*.wav"))
    return random.choice(candidates) if candidates else None


def compose_video(
    scenes_with_audio: list[dict],
    template: dict,
    job_id: str,
) -> Path:
    """
    全シーンを合成して1本の動画ファイルを作成する

    Args:
        scenes_with_audio: audio_path付きシーンリスト
        template: テンプレート辞書
        job_id: ジョブID（出力ファイル名に使用）

    Returns:
        出力された動画ファイルのパス
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    template_scenes = {s["id"]: s for s in template["scenes"]}
    scene_clips = []

    for scene_data in scenes_with_audio:
        scene_id = scene_data["id"]
        template_scene = template_scenes[scene_id]
        clip = _make_scene_clip(scene_data, template_scene)
        scene_clips.append(clip)

    # シーンを結合
    final_clip = concatenate_videoclips(scene_clips, method="compose")

    # BGMをミックス（動画全体にループして小音量で乗せる）
    bgm_path = _pick_bgm()
    if bgm_path:
        bgm = AudioFileClip(str(bgm_path)).with_volume_scaled(0.15)
        if bgm.duration < final_clip.duration:
            bgm = bgm.loop(duration=final_clip.duration)
        else:
            bgm = bgm.subclipped(0, final_clip.duration)

        existing_audio = final_clip.audio
        if existing_audio:
            mixed_audio = CompositeAudioClip([existing_audio, bgm])
        else:
            mixed_audio = bgm
        final_clip = final_clip.with_audio(mixed_audio)

    output_path = OUTPUT_DIR / f"{job_id}.mp4"
    final_clip.write_videofile(
        str(output_path),
        fps=VIDEO_FPS,
        codec="libx264",
        audio_codec="aac",
        logger=None,
    )

    # クリップを閉じてリソース解放
    for clip in scene_clips:
        clip.close()
    final_clip.close()

    return output_path

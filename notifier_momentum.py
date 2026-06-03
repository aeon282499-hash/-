"""
notifier_momentum.py — デイトレモメンタム極み Discord通知

BT v6 (立花型出来高理論) のシグナルをDiscordに配信。
"""
import os
import requests
import urllib3
from datetime import datetime
import zoneinfo

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
JST = zoneinfo.ZoneInfo("Asia/Tokyo")

COLOR_BUY    = 0xFFB300
COLOR_NO_SIG = 0x757575
COLOR_HOT    = 0xFB8C00   # テーマ熱(notifier_theme と統一)


def _theme_embeds(ranked: list[dict] | None, hot: list[dict] | None) -> list[dict]:
    """🔥今ホットなテーマ ランキング＋ホットテーマ初動候補の embed 群。
    描画は notifier_theme のヘルパを再利用(単一の真実源)。失敗しても本体は止めない。"""
    if not ranked:
        return []
    try:
        from notifier_theme import _ranking_overview, _hot_theme_embed
    except Exception:
        return []
    embeds = [{
        "title": "🔥 今ホットなテーマ (追い風セクター)",
        "description": _ranking_overview(ranked),
        "color": COLOR_HOT,
    }]
    for r in (hot or []):
        embeds.append(_hot_theme_embed(r))
    return embeds


def _tailwind_field(s: dict) -> dict | None:
    """シグナルが属する追い風テーマを1フィールドにする。属さなければ None。"""
    if not s.get("theme"):
        return None
    heat = s.get("theme_heat")
    role = s.get("theme_role") or ""
    hot = s.get("theme_hot")
    name = "🔥 テーマ追い風" if hot else "🏷️ 所属テーマ"
    val = s["theme"]
    if heat is not None:
        val += f"  heat`{heat}`"
    if role:
        val += f"  〔{role}〕"
    if not hot:
        val += "  (点火閾値未満)"
    return {"name": name, "value": val, "inline": False}


def _webhook() -> str:
    url = os.getenv("DISCORD_WEBHOOK_URL_MOMENTUM", "").strip()
    if not url:
        raise ValueError("DISCORD_WEBHOOK_URL_MOMENTUM 未設定")
    return url


def _post(payload: dict) -> None:
    r = requests.post(_webhook(), json=payload, timeout=10, verify=False)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"Discord送信失敗: HTTP {r.status_code}\n{r.text}")


def _signal_embed(s: dict, idx: int, position_budget: int) -> dict:
    """1銘柄分のシグナルembed。テーマ追い風/純モメンタム共通。"""
    shares = int(position_budget / s["close"] / 100) * 100  # 100株単位
    if shares == 0:
        shares = 100  # 最低1単元
    fields = [
        {"name": "市場", "value": s["mkt"], "inline": True},
        {"name": "17業種", "value": s["s17nm"], "inline": True},
        {"name": "終値", "value": f"{s['close']:,.0f}円", "inline": True},
        {"name": "ATR(20)", "value": f"{s['atr_pct']:.2f}%", "inline": True},
        {"name": "20日リターン", "value": f"{s['ret20']*100:+.1f}%", "inline": True},
        {"name": "vol蓄積倍率", "value": f"{s['vol_ratio']:.2f}x", "inline": True},
        {"name": "売買代金", "value": f"{s['value_oku']:.1f}億円", "inline": True},
        {"name": "20日高値", "value": f"{s['high20_prev']:,.0f}円", "inline": True},
        {"name": "推奨数量", "value": f"{shares:,}株", "inline": True},
    ]
    sl_price = s["close"] * 0.95
    tp_price = s["close"] * 1.15
    tw = _tailwind_field(s)
    if tw:
        fields.append(tw)
    fields.append({
        "name": "📊 決済目安(終値基準)",
        "value": f"利確 +15% → **{tp_price:,.0f}円** / 損切 -5% → **{sl_price:,.0f}円**",
        "inline": False,
    })
    return {
        "title": f"#{idx}  {'🔥' if s.get('theme_hot') else ''}{s['code4']} {s['name']}",
        "url": f"https://kabutan.jp/stock/?code={s['code4']}",
        "color": COLOR_HOT if s.get("theme_hot") else COLOR_BUY,
        "fields": fields,
    }


def send_signals(theme_signals: list[dict], pure_signals: list[dict], target_date: str,
                 position_budget: int = 200_000,
                 ranked: list[dict] | None = None, hot: list[dict] | None = None) -> None:
    """theme_signals: 🔥点火中テーマ追い風のモメンタム初動
    pure_signals: ⚡テーマ無関係の純モメンタム初動
    target_date: 'YYYY-MM-DD' 当該シグナル日(=前営業日)
    position_budget: 1ポジ予算(円, デフォルト20万)
    ranked/hot: テーマトラッカー結果(同channelにまとめて配信する追い風セクター)
    """
    time_str = datetime.now(JST).strftime("%H:%M JST")
    theme_embeds = _theme_embeds(ranked, hot)
    total = len(theme_signals) + len(pure_signals)

    if total == 0:
        embeds = [{
            "title": "🎯 デイトレモメンタム極み",
            "description": f"**{target_date}** のシグナル: **該当なし**\n\n"
                           f"立花型出来高理論(凪→蓄積→ブレイク)の条件を満たす銘柄は本日無し。"
                           + ("\n下に本日の追い風テーマを掲載。" if theme_embeds else ""),
            "color": COLOR_NO_SIG,
            "footer": {"text": f"配信 {time_str} / BT実績 PF1.10 / 4年+133%"},
        }] + theme_embeds
        for chunk_start in range(0, len(embeds), 10):
            _post({"embeds": embeds[chunk_start:chunk_start + 10]})
        return

    header = {
        "title": f"🎯 デイトレモメンタム極み — {target_date}",
        "description": (
            f"**計{total}件** のシグナル発生"
            f"（🔥テーマ追い風 {len(theme_signals)}件 / ⚡純モメンタム {len(pure_signals)}件）。\n"
            f"立花型出来高理論「凪→出来高蓄積→20日高値ブレイク陽線」"
            f"\n1ポジ予算: **{position_budget:,}円** / 翌営業日Open買い → "
            f"TP+15% / SL-5% / 最大10営業日保有"
        ),
        "color": COLOR_BUY,
    }
    embeds = [header]

    idx = 1
    if theme_signals:
        embeds.append({
            "title": "━━━ 🔥 テーマ追い風モメンタム ━━━",
            "description": "点火中テーマ(heat≥25)に乗る初動。テーマ全体への資金流入が追い風。",
            "color": COLOR_HOT,
        })
        for s in theme_signals:
            embeds.append(_signal_embed(s, idx, position_budget))
            idx += 1

    if pure_signals:
        embeds.append({
            "title": "━━━ ⚡ 純モメンタム (テーマ無関係) ━━━",
            "description": "特定テーマに属さない単独の出来高ブレイク初動。vol蓄積倍率の高い順。",
            "color": COLOR_BUY,
        })
        for s in pure_signals:
            embeds.append(_signal_embed(s, idx, position_budget))
            idx += 1

    footer = {
        "title": "ℹ️ 運用ルール",
        "description": (
            "・**翌営業日 寄付買い** (gap+3%超は見送り)\n"
            "・TP+15% / SL-5% / 最大10営業日保有(満期は引け売り)\n"
            "・**資金分離必須**: 既存スイング/デイトレと別口\n"
            "・このシステムは「凪から動き出す本物の初動」狙い、QD級S高は取れません\n"
            "・BT過去4年: PF1.10 / 累積+133% / 2026直近は劣化中(要監視)"
        ),
        "color": COLOR_NO_SIG,
        "footer": {"text": f"配信 {time_str} / 立花型出来高理論 v6"},
    }
    embeds.append(footer)

    # テーマトラッカー(追い風セクター)を同じ配信にまとめる
    embeds.extend(theme_embeds)

    # Discordは1メッセージあたりembed最大10
    for chunk_start in range(0, len(embeds), 10):
        _post({"embeds": embeds[chunk_start:chunk_start+10]})


if __name__ == "__main__":
    # 単体テスト(Discordに実送信する)
    theme_sample = [{
        "ticker":"6613.T", "code4":"6613", "name":"テーマ追い風サンプル",
        "s17nm":"電機・精密", "s33nm":"電気機器", "mkt":"グロース",
        "close":1500, "atr_pct":3.2, "ret20":0.05, "vol_ratio":1.8,
        "value_oku":3.5, "high20_prev":1480,
        "theme":"半導体製造装置", "theme_heat":42.0, "theme_hot":True, "theme_role":"中核",
    }]
    pure_sample = [{
        "ticker":"7777.T", "code4":"7777", "name":"純モメンタムサンプル",
        "s17nm":"機械", "s33nm":"機械", "mkt":"プライム",
        "close":2200, "atr_pct":2.5, "ret20":0.08, "vol_ratio":2.4,
        "value_oku":12.0, "high20_prev":2150,
        "theme":None, "theme_heat":None, "theme_hot":False, "theme_role":None,
    }]
    send_signals(theme_sample, pure_sample, "2026-05-22")
    print("test sent")

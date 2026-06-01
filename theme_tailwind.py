"""
theme_tailwind.py — 初動キャッチャー(v6)にテーマトラッカーの「ホットセクター」を連動。

v6シグナル(凪→出来高蓄積→20日高値ブレイク陽線)に、その銘柄が属するテーマの
現在 heat を "追い風" として付与・加点する。テーマ熱は theme_tracker.run_theme_tracker
を v6 と同じ all_data で呼んで算出する(再ダウンロード無し)。

- build_reverse_map  : ticker -> [(theme, role)] 逆引き
- compute_theme_heat : (theme->heat dict, ranked, hot) を all_data から算出
- attach_tailwind    : 各シグナルに theme/theme_heat/theme_hot/theme_role を付与
- rerank             : ホットテーマ銘柄を優先(heat降順)→残りは vol_ratio 降順 で上位を選ぶ
"""
from __future__ import annotations

from theme_tracker import run_theme_tracker, load_theme_members, HEAT_FLOOR


def build_reverse_map() -> dict[str, list[tuple[str, str]]]:
    """theme_members.json から ticker -> [(theme名, role)] の逆引きマップを作る。"""
    themes = load_theme_members()
    rev: dict[str, list[tuple[str, str]]] = {}
    for tname, tinfo in themes.items():
        for m in tinfo.get("members", []):
            rev.setdefault(m["ticker"], []).append((tname, m.get("role", "")))
    return rev


def compute_theme_heat(all_data) -> tuple[dict[str, float], list[dict], list[dict]]:
    """v6が読み込んだ all_data からテーマ熱を算出。(heat_map, ranked, hot) を返す。"""
    ranked, hot = run_theme_tracker(data=all_data)
    heat_map = {r["theme"]: r["heat"] for r in ranked}
    return heat_map, ranked, hot


def attach_tailwind(signals: list[dict], heat_map: dict[str, float],
                    rev_map: dict[str, list[tuple[str, str]]],
                    heat_floor: float = HEAT_FLOOR) -> list[dict]:
    """各シグナルに、属する中で最もheatの高いテーマを追い風として付与する。"""
    for s in signals:
        best_theme = best_heat = best_role = None
        for tname, role in rev_map.get(s.get("ticker", ""), []):
            h = heat_map.get(tname)
            if h is None:
                continue
            if best_heat is None or h > best_heat:
                best_theme, best_heat, best_role = tname, h, role
        s["theme"] = best_theme
        s["theme_heat"] = round(best_heat, 1) if best_heat is not None else None
        s["theme_hot"] = (best_heat is not None and best_heat >= heat_floor)
        s["theme_role"] = best_role
    return signals


def rerank(signals: list[dict], max_signals: int) -> list[dict]:
    """ホットテーマ銘柄を優先(heat降順)、その他は vol_ratio 降順。同質内も vol_ratio で。"""
    def key(s: dict):
        return (
            1 if s.get("theme_hot") else 0,
            s.get("theme_heat") or 0.0,
            s.get("vol_ratio") or 0.0,
        )
    return sorted(signals, key=key, reverse=True)[:max_signals]

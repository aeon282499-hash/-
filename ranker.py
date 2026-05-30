"""
ranker.py — 銘柄 S/A/B/C スコアラー

テーマトラッカー(theme_tracker.py)が算出した各テーマの members メトリクスを使い、
1銘柄ずつ合成スコアを付けて S/A/B/C にティア分けする。

合成スコア = 銘柄モメンタム(出来高点火・短期スラスト・初動度)
            + テーマ文脈(heat が高いテーマの銘柄を底上げ)
            + 政策ボーナス(国策バックのテーマ)
            + 米株前夜の追い風(us_drivers の前夜騰落)

「資金がいま流入していて(出来高)、まだ走り始め(初動度)で、テーマが点火中(heat)、
 国策の構造的追い風があり(policy)、米震源も上げている(us_overnight)」銘柄ほど高スコア=S。
Twitterは使わない: 出来高こそが資金流入のグラウンドトゥルース。
"""
from __future__ import annotations

# --- ティア閾値 (合成スコア) ---
TIER_S = 95.0
TIER_A = 70.0
TIER_B = 45.0
# それ未満は C。表示下限(これ未満は資金流入の兆しなしとして除外)
SCORE_FLOOR = 20.0

# --- 政策ボーナス ---
POLICY_BONUS = 8.0

# --- 初動度(エントリー妥当性)の表示用しきい値 ---
# 強さスコアとは独立の "いま入れるか / もう走った後か" の別軸(2軸表示)。
# スコアには влияしない。dev(25MA乖離%)主導で判定する。
INIT_FRESH_DEV_MIN = -3.0   # ★★★(走り始め): MA近辺の下限
INIT_FRESH_DEV_MAX = 10.0   # ★★★(走り始め): MA近辺の上限
INIT_LATE_DEV = 18.0        # ★☆☆(伸びきり): 乖離がこれ超
INIT_LATE_R20 = 40.0        # ★☆☆: 20日リターン(%)がこれ超は問答無用に伸びきり
OVEREXT_DEV = 18.0          # 🔥伸びきりバッジ: 乖離
OVEREXT_R20 = 35.0          # 🔥伸びきりバッジ: 20日リターン(%)


def _stock_momentum(m: dict) -> tuple[float, dict]:
    """銘柄単体のモメンタム強度(0〜約85)。資金流入の "いま" を測る。"""
    vr = m.get("vr") or 0.0
    dev = m.get("dev")
    r1 = (m.get("r1") or 0.0) * 100
    r5 = (m.get("r5") or 0.0) * 100
    r20 = (m.get("r20") or 0.0) * 100
    above = bool(m.get("above_ma25"))

    # 出来高点火 (資金流入の本体)
    if vr >= 2.5:   vol_pts = 25
    elif vr >= 2.0: vol_pts = 20
    elif vr >= 1.5: vol_pts = 12
    elif vr >= 1.2: vol_pts = 5
    else:           vol_pts = 0

    # 短期スラスト (5営業日)
    if r5 >= 10:  thrust_pts = 20
    elif r5 >= 5: thrust_pts = 14
    elif r5 >= 2: thrust_pts = 8
    elif r5 >= 0: thrust_pts = 3
    else:         thrust_pts = 0

    # 当日勢い
    if r1 >= 3:   r1_pts = 10
    elif r1 >= 1: r1_pts = 6
    elif r1 > 0:  r1_pts = 3
    else:         r1_pts = 0

    # トレンド確認 (25MA上 + 中期は伸びすぎ手前)
    trend_pts = (10 if above else 0)
    if 0 < r20 <= 30:  trend_pts += 8
    elif r20 > 30:     trend_pts += 3  # 既に大相場化=出遅れ妙味薄
    # r20<=0 は +0

    # 初動度 (25MA乖離。ブレイク直後がベスト、伸びきりは減点)
    if dev is None:        pos_pts = 0
    elif -2 <= dev <= 8:   pos_pts = 12   # ブレイク直後スイートスポット
    elif 8 < dev <= 15:    pos_pts = 6
    elif -5 <= dev < -2:   pos_pts = 4    # MA直下の点火(押し目反転)
    elif dev > 15:         pos_pts = 2    # 伸びきり
    else:                  pos_pts = 0    # MA大幅下=落ちるナイフ

    total = vol_pts + thrust_pts + r1_pts + trend_pts + pos_pts
    detail = {
        "vol_pts": vol_pts, "thrust_pts": thrust_pts, "r1_pts": r1_pts,
        "trend_pts": trend_pts, "pos_pts": pos_pts,
    }
    return float(total), detail


def _init_timing(m: dict) -> tuple[int, bool]:
    """初動度(★)と伸びきりバッジ。強さスコアとは独立の "いま入れるか" 軸。

    dev(25MA乖離%)主導。スコアには влияしない純表示用。
    - ★★★(3): MA近辺で点火直後(走り始め)=エントリー妥当
    - ★★ (2): 中間 or dev不明
    - ★☆☆(1): 乖離が大 or 20日で走りすぎ(もう遅い)
    overextended: 乖離 or 20日リターンが過熱閾値超(🔥伸びきりバッジ)
    """
    dev = m.get("dev")
    r20 = (m.get("r20") or 0.0) * 100
    if dev is None:
        return 2, False
    overext = dev > OVEREXT_DEV or r20 > OVEREXT_R20
    if dev > INIT_LATE_DEV or dev < -8.0 or r20 > INIT_LATE_R20:
        stars = 1
    elif INIT_FRESH_DEV_MIN <= dev <= INIT_FRESH_DEV_MAX:
        stars = 3
    else:
        stars = 2
    return stars, overext


def _theme_context(theme_row: dict, us_tailwind: float | None) -> tuple[float, dict]:
    """テーマ文脈の加点。heat(点火度) + 政策 + 米前夜の追い風。"""
    heat = theme_row.get("heat", 0.0)
    heat_pts = max(0.0, min(heat, 100.0)) * 0.4   # heat=100 で +40

    policy_pts = POLICY_BONUS if theme_row.get("policy") else 0.0

    # 米株前夜: us_drivers の平均前夜騰落(%)。国内発(None)は中立=0。
    if us_tailwind is None:
        us_pts = 0.0
    elif us_tailwind >= 2.0: us_pts = 12.0
    elif us_tailwind >= 1.0: us_pts = 8.0
    elif us_tailwind >= 0.0: us_pts = 3.0
    else:                    us_pts = -5.0   # 米震源が下げ=逆風

    detail = {"heat_pts": round(heat_pts, 1), "policy_pts": policy_pts, "us_pts": us_pts}
    return heat_pts + policy_pts + us_pts, detail


def _tier(score: float) -> str:
    if score >= TIER_S: return "S"
    if score >= TIER_A: return "A"
    if score >= TIER_B: return "B"
    return "C"


def rank_stocks(
    ranked_themes: list[dict],
    us_tailwind_by_theme: dict[str, float | None] | None = None,
) -> list[dict]:
    """
    テーマランキング(各テーマに members メトリクス付き)を受け取り、
    全銘柄を合成スコア降順でティア付けして返す。

    us_tailwind_by_theme: {theme_name: avg_overnight_pct or None}。
      None or 未指定 = 米前夜レイヤー無効(全テーマ中立扱い)。
    同一銘柄が複数テーマに属する場合は最高スコアのテーマで代表させる。
    """
    us_tailwind_by_theme = us_tailwind_by_theme or {}
    best: dict[str, dict] = {}   # ticker -> scored row (最高スコアで上書き)

    for tr in ranked_themes:
        tname = tr["theme"]
        us_tw = us_tailwind_by_theme.get(tname)  # 未収集テーマは None=中立
        ctx_pts, ctx_detail = _theme_context(tr, us_tw)

        for m in tr.get("members", []):
            mom_pts, mom_detail = _stock_momentum(m)
            score = round(mom_pts + ctx_pts, 1)
            if score < SCORE_FLOOR:
                continue

            init_stars, overext = _init_timing(m)

            row = {
                "ticker": m["ticker"],
                "name": m["name"],
                "role": m.get("role", ""),
                "theme": tname,
                "theme_heat": tr.get("heat"),
                "policy": tr.get("policy", ""),
                "us_drivers": tr.get("us_drivers", []),
                "us_tailwind": us_tw,
                "score": score,
                "tier": _tier(score),
                "init_stars": init_stars,     # 3=★★★走り始め 2=★★ 1=★☆☆伸びきり
                "overextended": overext,      # 🔥伸びきり(過熱)バッジ
                "mom_pts": round(mom_pts, 1),
                "ctx_pts": round(ctx_pts, 1),
                # 生メトリクス(ダッシュボード表示用)
                "vr": m.get("vr"), "dev": m.get("dev"), "rsi": m.get("rsi"),
                "r1": m.get("r1"), "r5": m.get("r5"), "r20": m.get("r20"),
                "close": m.get("close"), "above_ma25": m.get("above_ma25"),
                "_detail": {**mom_detail, **ctx_detail},
            }
            prev = best.get(m["ticker"])
            if prev is None or score > prev["score"]:
                best[m["ticker"]] = row

    out = list(best.values())
    out.sort(key=lambda r: r["score"], reverse=True)
    return out


def tier_summary(rows: list[dict]) -> dict[str, int]:
    out = {"S": 0, "A": 0, "B": 0, "C": 0}
    for r in rows:
        out[r["tier"]] += 1
    return out


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    from theme_tracker import run_theme_tracker

    ranked, _hot = run_theme_tracker()

    rows = rank_stocks(ranked)   # 米前夜レイヤーなし(中立)で素点を確認
    summ = tier_summary(rows)

    print()
    print("=" * 90)
    print(f"  銘柄 S/A/B/C ランキング  (S={summ['S']} A={summ['A']} B={summ['B']} C={summ['C']})")
    print("  ※米株前夜レイヤー無効の素点。theme heat + policy のみ反映")
    print("=" * 90)
    for r in rows:
        drv = "/".join(r["us_drivers"][:3]) if r["us_drivers"] else "国内発"
        pol = " 政策" if r["policy"] else ""
        vr = r["vr"] if r["vr"] is not None else 0
        dev = r["dev"] if r["dev"] is not None else 0
        r5 = (r["r5"] or 0) * 100
        stars = {3: "★★★", 2: "★★☆", 1: "★☆☆"}.get(r["init_stars"], "★★☆")
        oe = " 🔥伸びきり" if r["overextended"] else ""
        print(f"  [{r['tier']}] 強{r['score']:5.1f} 初動{stars}{oe}  [{r['ticker']}] {r['name']:<16} "
              f"score(mom{r['mom_pts']:.0f}+ctx{r['ctx_pts']:.0f}) "
              f"vol{vr:.1f}x 乖離{dev:+.1f}% 5d{r5:+.1f}%  "
              f"〔{r['theme']} heat{r['theme_heat']:.0f}{pol} {drv}〕")
    print("=" * 90)

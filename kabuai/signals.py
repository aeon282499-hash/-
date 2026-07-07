"""
signals.py — KabuAI クローン フェーズ3 シグナル判定

build_data.py が算出した「計算済み指標の行」(rows) を走査し、SPEC §3.4 の
シグナル(強買い集め/強押し目/昇格/買い集め/加速/反転/話題集中)をルールで点灯する。
窓・閾値は仮（未最適化）。本家同様、後で BT 確定 → ここの定数を差し替える。

純関数 detect(rows):
  - 各 row に row["signals"] = [シグナルkey...] を付与（in-place）
  - グループ化した dict（key→{label,emoji,desc,stance,count,members}）を返す
"""
from __future__ import annotations


def _accel(row) -> float:
    """mom_hist(5日)の上昇幅。新しい−古い。"""
    h = [x for x in (row.get("mom_hist") or []) if x is not None]
    if len(h) < 2:
        return 0.0
    return float(h[-1] - h[0])


def _hist_min(row) -> float:
    h = [x for x in (row.get("mom_hist") or []) if x is not None]
    return min(h) if h else row["momentum"]


# ── BT所見(2026-06-04・bt_signal_tune.py / 2021-2026・全営業日・432万サンプル): ──
# accum/strong_accum/reversal は順方向リターンがプラスで買い系として有効。
# さらに reversal の上位ティア strong_reversal(2026-06-05・bt_reversal_tune.py で確定。
# reversal を r20<-10 & r5>=4 に絞った部分集合=深い下落からの力強い切り返し)は出口8日/
# -12%で勝率59%・全営業年プラスと買い系で最良。reversal の条件を強めただけで新規の
# 特徴量は足していない(過剰最適化回避)。一方
# accel/promote/dip/buzz は閾値をどう絞っても 5/10/20日の順方向リターンがマイナス〜
# ゼロ・勝率<48%(buzzは37%・2021は8%)で、追随買いに優位性なし＝モメンタム過熱／
# ブローオフの反落パターンだった。よって誇張を避けスタンスを正直化する(accel 期待→
# 警戒・promote 期待→中立)。閾値を再"最適化"してもロング側のエッジは出ないので注意
# (将来やるなら fade/ショート側で再設計すること)。
# strong_dip も同様(2026-06-05・bt_dip_tune.py で確定): 発動が極めて稀(2021-2026で155件)・
# 出口8日/-12%で勝率36%・近年(2025/2026)はマイナスで、近傍をどう再キャリブレーションしても
# 勝率<46%・どの保有でも52%ゲート未達=高ばらつきの「当たれば大きいが外れ多い」型。確たる
# エッジ無しと判断し stance 期待→中立 に正直化(発火ロジックは不変)。
#
# 各シグナル: (key, ラベル, 絵文字, 説明, スタンス, 判定関数)。判定関数は row(dict)→bool
SIGNAL_DEFS = [
    ("strong_accum", "強買い集め", "🔥",
     "大商い＋強い押し上げ。資金が集中して入っている強シグナル。", "期待",
     lambda r: r["vr"] >= 2.0 and r["power"] >= 4.0 and r["r5"] >= 5.0 and r["rsi"] < 85),
    ("accum", "買い集め", "📈",
     "出来高増加を伴う緩やかな上昇。仕込みの兆し。", "中立",
     lambda r: r["vr"] >= 1.5 and r["power"] >= 2.0 and r["r5"] >= 2.0 and r["rsi"] < 80),
    ("accel", "加速", "🚀",
     "モメンタム指数が直近5日で急騰。短期は過熱しやすく、過去実績では数日内に反落する傾向が強い（追随は慎重に）。", "警戒",
     lambda r: _accel(r) >= 15.0 and r["momentum"] >= 55),
    ("promote", "昇格", "⬆️",
     "指数が上位バンド（60／80）へ昇格。バンド入りだけでは継続性は限定的で、過去実績は中立〜やや弱め。", "中立",
     lambda r: (_hist_min(r) < 60 <= r["momentum"]) or (_hist_min(r) < 80 <= r["momentum"])),
    ("strong_dip", "強押し目", "🎯",
     "強い上昇トレンド中の深い押し目で反発を狙う形。ただし過去実績では発動が稀（5年で155件）・勝率4割弱で、近年（2025/2026）はむしろ逆風。当たれば大きいが外れも多い高ばらつき型で、追随は慎重に。", "中立",
     lambda r: r["r20"] >= 25 and r["r5"] < 0 and r["r1"] < 0 and r["momentum"] >= 50 and 35 <= r["rsi"] <= 60),
    ("dip", "押し目", "↩️",
     "上昇トレンド中の小休止。ただし過去実績では反発が安定せず、深い押しは様子見が無難。", "中立",
     lambda r: r["r20"] >= 15 and r["r1"] < 0 and r["momentum"] >= 45 and 40 <= r["rsi"] <= 65),
    ("strong_reversal", "強反転", "⚡",
     "深い下落（20日で−10%超）からの力強い切り返し（5日で+4%超）。反転の初動でも勢いが強く、過去実績は買い系で最も勝率が高い（出口8日/−12%で約59%・全年プラス）。", "期待",
     lambda r: r["r20"] < -10 and r["r5"] >= 4.0 and r["r1"] > 0 and r["rsi"] < 55),
    ("reversal", "反転", "🔄",
     "下落トレンドからの反転の初動。", "警戒",
     lambda r: r["r20"] < 0 and r["r5"] > 0 and r["r1"] > 0 and r["rsi"] < 55),
    # 話題集中(buzz)は撤去（2026-06-27）。BTで勝率37%・追随に優位性なしの過熱反落型で、
    # 「勝てる買い候補だけ出す」方針に不要。ショート側の検証は bt_fade_short.py を参照。
]

MEMBERS_PER_SIGNAL = 40  # 各シグナルの掲載上限（指数降順）


def detect(rows: list[dict]) -> dict:
    defs = SIGNAL_DEFS
    groups = {k: {"key": k, "label": lb, "emoji": em, "desc": ds, "stance": st,
                  "count": 0, "members": []}
              for (k, lb, em, ds, st, _fn) in defs}

    for r in rows:
        hit = []
        for (k, _lb, _em, _ds, _st, fn) in defs:
            try:
                if fn(r):
                    hit.append(k)
            except Exception:
                pass
        r["signals"] = hit
        for k in hit:
            groups[k]["count"] += 1

    member_fields = ("code", "name", "price", "momentum", "grade", "r1", "r5", "r10", "r20",
                     "rsi", "turnover_oku", "signals")
    for k, g in groups.items():
        members = [r for r in rows if k in r["signals"]]
        members.sort(key=lambda x: x["momentum"], reverse=True)
        # r.get(f): turnover_oku 等が無い row を渡されても壊れない（build以外の呼び出し耐性）
        g["members"] = [{f: r.get(f) for f in member_fields} for r in members[:MEMBERS_PER_SIGNAL]]

    # 点灯のあるシグナルだけ、SIGNAL_DEFS の順序を保って返す
    return {"order": [k for (k, *_ ) in defs],
            "groups": groups}

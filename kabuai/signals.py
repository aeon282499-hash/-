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


# 各シグナル: (key, ラベル, 絵文字, 説明, スタンス, 判定関数)
# 判定関数は row(dict) を受けて bool
SIGNAL_DEFS = [
    ("strong_accum", "強買い集め", "🔥",
     "大商い＋強い押し上げ。資金が集中して入っている強シグナル。", "期待",
     lambda r: r["vr"] >= 2.0 and r["power"] >= 4.0 and r["r5"] >= 5.0 and r["rsi"] < 85),
    ("accum", "買い集め", "📈",
     "出来高増加を伴う緩やかな上昇。仕込みの兆し。", "中立",
     lambda r: r["vr"] >= 1.5 and r["power"] >= 2.0 and r["r5"] >= 2.0 and r["rsi"] < 80),
    ("accel", "加速", "🚀",
     "モメンタム指数が直近5日で急上昇中。", "期待",
     lambda r: _accel(r) >= 15.0 and r["momentum"] >= 55),
    ("promote", "昇格", "⬆️",
     "指数が上位バンドへ昇格（60→ / 80→）。", "期待",
     lambda r: (_hist_min(r) < 60 <= r["momentum"]) or (_hist_min(r) < 80 <= r["momentum"])),
    ("strong_dip", "強押し目", "🎯",
     "強い上昇トレンド中の深い押し目。反発期待。", "期待",
     lambda r: r["r20"] >= 25 and r["r5"] < 0 and r["r1"] < 0 and r["momentum"] >= 50 and 35 <= r["rsi"] <= 60),
    ("dip", "押し目", "↩️",
     "上昇トレンド中の小休止。", "中立",
     lambda r: r["r20"] >= 15 and r["r1"] < 0 and r["momentum"] >= 45 and 40 <= r["rsi"] <= 65),
    ("reversal", "反転", "🔄",
     "下落トレンドからの反転の初動。", "警戒",
     lambda r: r["r20"] < 0 and r["r5"] > 0 and r["r1"] > 0 and r["rsi"] < 55),
    ("buzz", "話題集中", "💥",
     "急激な出来高・値幅の膨張。注目が一気に集中。", "警戒",
     lambda r: r["vr"] >= 2.5 and r["power"] >= 5.0 and abs(r["r1"]) >= 4.0),
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

    member_fields = ("code", "name", "price", "momentum", "grade", "r1", "r5", "r20", "signals")
    for k, g in groups.items():
        members = [r for r in rows if k in r["signals"]]
        members.sort(key=lambda x: x["momentum"], reverse=True)
        g["members"] = [{f: r[f] for f in member_fields} for r in members[:MEMBERS_PER_SIGNAL]]

    # 点灯のあるシグナルだけ、SIGNAL_DEFS の順序を保って返す
    return {"order": [k for (k, *_ ) in defs],
            "groups": groups}

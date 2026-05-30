"""
dashboard.py — 資金流入ダッシュボード (テーマ × モメンタム S/A/B/C)

build_dashboard.py が日次生成する dashboard_data.json を読んで表示するだけの
Streamlit フロント(ロード毎に J-Quants は叩かない)。

ローカル: streamlit run dashboard.py
デプロイ: Streamlit Community Cloud (このリポジトリを指定)
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_PATH = Path("dashboard_data.json")

TIER_COLOR = {"S": "#d32f2f", "A": "#f57c00", "B": "#1976d2", "C": "#757575"}
TIER_BG = {"S": "#fdecea", "A": "#fff3e0", "B": "#e3f2fd", "C": "#f5f5f5"}

st.set_page_config(page_title="資金流入ダッシュボード", page_icon="🔥", layout="wide")


@st.cache_data(ttl=300)
def load_data() -> dict | None:
    if not DATA_PATH.exists():
        return None
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _pct(x, digits=1):
    return f"{x*100:+.{digits}f}%" if x is not None else "-"


def tier_badge(t: str) -> str:
    c = TIER_COLOR.get(t, "#757575")
    return f"<span style='background:{c};color:#fff;border-radius:6px;padding:2px 10px;font-weight:700'>{t}</span>"


data = load_data()

st.title("🔥 資金流入ダッシュボード")
st.caption("テーマトラッカー × モメンタム銘柄を S/A/B/C にランク付け。"
           "出来高(資金流入)× テーマ熱 × 国策 × 米株前夜の追い風で合成スコア化。")

if data is None:
    st.error("dashboard_data.json が見つかりません。`python build_dashboard.py` を実行して生成してください。")
    st.stop()

# ---- ヘッダー: 更新時刻 + ティアサマリ ----
summ = data["tier_summary"]
c0, c1, c2, c3, c4 = st.columns([2, 1, 1, 1, 1])
c0.metric("データ日付", data["date"], help=f"生成: {data['generated_at']}")
c1.metric("🔴 S", summ["S"])
c2.metric("🟠 A", summ["A"])
c3.metric("🔵 B", summ["B"])
c4.metric("⚪ C", summ["C"])

stocks = pd.DataFrame(data["stocks"])
themes = pd.DataFrame(data["themes"])

tab1, tab2, tab3 = st.tabs(["📊 銘柄ランキング", "🔥 テーマ熱", "🇺🇸 米震源(前夜)"])

# ================= 銘柄ランキング =================
with tab1:
    with st.sidebar:
        st.header("フィルタ")
        sel_tiers = st.multiselect("ティア", ["S", "A", "B", "C"],
                                   default=["S", "A", "B"])
        all_themes = sorted(stocks["theme"].unique().tolist())
        sel_themes = st.multiselect("テーマ", all_themes, default=[])
        policy_only = st.checkbox("国策テーマのみ", value=False)
        st.divider()
        st.caption("初動度＝強さとは別軸の『いま入れるか』")
        init_filter = st.radio("初動度", ["全部", "★★★のみ", "★★以上"], index=0,
                               help="★★★=MA近辺で点火直後(走り始め) / ★☆☆=乖離大or走りすぎ(もう遅い)")
        drop_overext = st.checkbox("🔥伸びきりを除外", value=False,
                                   help="25MA乖離 or 20日リターンが過熱閾値超を非表示")
        kw = st.text_input("銘柄名/コード 検索", "")
        st.divider()
        compact = st.checkbox("📱 コンパクト表示", value=False,
                              help="スマホ向け。主要列(ティア/初動/スコア/銘柄/テーマ)だけ表示し横スクロールを減らす")

    df = stocks.copy()
    if sel_tiers:
        df = df[df["tier"].isin(sel_tiers)]
    if sel_themes:
        df = df[df["theme"].isin(sel_themes)]
    if policy_only:
        df = df[df["policy"].astype(bool)]
    if init_filter == "★★★のみ":
        df = df[df["init_stars"] == 3]
    elif init_filter == "★★以上":
        df = df[df["init_stars"] >= 2]
    if drop_overext:
        df = df[~df["overextended"].astype(bool)]
    if kw:
        m = df["name"].str.contains(kw, case=False, na=False) | df["ticker"].str.contains(kw, case=False, na=False)
        df = df[m]

    st.markdown(f"**{len(df)} 銘柄**（合成スコア降順）")

    _stars = {3: "★★★", 2: "★★☆", 1: "★☆☆"}
    view = pd.DataFrame({
        "ティア": df["tier"],
        "スコア": df["score"],
        "初動": df["init_stars"].map(_stars).fillna("★★☆"),
        "伸": df["overextended"].apply(lambda x: "🔥" if x else ""),
        "コード": df["ticker"],
        "銘柄": df["name"],
        "テーマ": df["theme"],
        "テーマ熱": df["theme_heat"],
        "出来高比": df["vr"],
        "25MA乖離%": df["dev"],
        "RSI": df["rsi"],
        "5d%": (df["r5"] * 100).round(1),
        "20d%": (df["r20"] * 100).round(1),
        "米前夜%": df["us_tailwind"],
        "国策": df["policy"].apply(lambda p: "🏛" if p else ""),
        "役割": df["role"],
    })

    if compact:
        view = view[["ティア", "初動", "伸", "スコア", "銘柄", "テーマ"]]

    def _row_style(row):
        bg = TIER_BG.get(row["ティア"], "#ffffff")
        return [f"background-color:{bg}"] * len(row)

    fmt = {"スコア": "{:.1f}", "テーマ熱": "{:.0f}", "出来高比": "{:.1f}x",
           "25MA乖離%": "{:+.1f}", "RSI": "{:.0f}", "5d%": "{:+.1f}",
           "20d%": "{:+.1f}", "米前夜%": "{:+.1f}"}
    fmt = {k: v for k, v in fmt.items() if k in view.columns}  # コンパクト時に欠ける列を除外
    styler = view.style.apply(_row_style, axis=1).format(fmt, na_rep="-")
    st.dataframe(styler, use_container_width=True, hide_index=True, height=620)

    st.caption("【2軸】スコア=資金流入の『強さ』／初動=『いま入れるか』の別軸。"
               "★★★=MA近辺で点火直後(走り始め)・★☆☆=乖離大or走りすぎ(もう遅い)・🔥=伸びきり過熱。"
               "Sの中でも『入れる/もう遅い』を即判別。"
               "出来高比=当日/直近20日平均。米前夜=テーマの米震源(us_drivers)の前夜平均騰落。国策🏛=構造的追い風。")

# ================= テーマ熱 =================
with tab2:
    st.markdown("**テーマ熱ランキング**（heat 降順）")
    tv = pd.DataFrame({
        "テーマ": themes["theme"],
        "heat": themes["heat"],
        "1d%": themes["avg_r1"],
        "5d%": themes["avg_r5"],
        "20d%": themes["avg_r20"],
        "25MA上%": (themes["pct_above_ma25"] * 100).round(0),
        "ブレイク": themes["breakout"].astype(str) + "/" + themes["n"].astype(str),
        "米前夜%": themes["us_tailwind"],
        "国策": themes["policy"].apply(lambda p: "🏛" if p else ""),
        "米震源": themes["us_drivers"].apply(lambda d: "/".join(d) if d else "国内発"),
    })
    st.dataframe(
        tv.style.background_gradient(subset=["heat"], cmap="OrRd")
        .format({"heat": "{:.1f}", "1d%": "{:+.1f}", "5d%": "{:+.1f}", "20d%": "{:+.1f}",
                 "25MA上%": "{:.0f}", "米前夜%": "{:+.1f}"}, na_rep="-"),
        use_container_width=True, hide_index=True, height=620,
    )

# ================= 米震源 =================
with tab3:
    st.markdown("**米震源(us_drivers)の前夜騰落率** — 前夜に動いた米株が翌日この日本バスケットへ波及")
    dr = data.get("driver_returns", {})
    if dr:
        dv = (pd.DataFrame({"シンボル": list(dr.keys()), "前夜%": list(dr.values())})
              .sort_values("前夜%", ascending=False))
        st.dataframe(
            dv.style.background_gradient(subset=["前夜%"], cmap="RdYlGn")
            .format({"前夜%": "{:+.2f}"}),
            use_container_width=True, hide_index=True, height=620,
        )
    else:
        st.info("米震源データなし。")

st.caption(f"生成: {data['generated_at']} ｜ Twitterは使わず価格・出来高(資金流入の実体)で判定。")

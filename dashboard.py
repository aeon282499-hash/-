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
TIER_ORDER = {"S": 0, "A": 1, "B": 2, "C": 3}  # ランク順ソート用(S→A→B→C)

st.set_page_config(page_title="資金流入ダッシュボード", page_icon="🔥", layout="wide",
                   initial_sidebar_state="collapsed")  # スマホ: フィルタ(サイドバー)を畳んで本体を先に出す

# --- スマホ最適化CSS: 余白圧縮・表を見やすく・指で押しやすいタブ ---
st.markdown("""
<style>
  .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
  @media (max-width: 640px) {
    .block-container { padding-left: 0.6rem; padding-right: 0.6rem; }
    h1 { font-size: 1.4rem !important; }
    .stTabs [data-baseweb="tab"] { padding: 8px 10px; font-size: 0.9rem; }
    [data-testid="stMetricValue"] { font-size: 1.1rem; }
  }
</style>
""", unsafe_allow_html=True)


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
    # ---- ランク絞り込み(画面トップに常時表示・スマホでサイドバーを開かず押せる) ----
    tier_opts = ["S", "A", "B", "C"]
    sel_tiers = st.pills("ランク絞り込み", tier_opts, selection_mode="multi",
                         default=["S", "A", "B"],
                         help="表示するランクを選ぶ。何も選ばなければ全ランク表示。")
    if not sel_tiers:
        sel_tiers = tier_opts  # 未選択=全ランク表示

    with st.sidebar:
        st.header("フィルタ")
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
        sort_mode = st.radio("並び替え",
                             ["🚀 ロケット度順", "🏆 ランク順(S→C)", "強さスコア順", "🐢 出遅れ度順"],
                             index=0,
                             help="ロケット=継続で1週間の大化けが出やすい(BT裏付け・主軸・要損切り)。"
                                  "ランク順=S→A→B→C(同ランク内は強さ降順)。強さ=もう強い銘柄。"
                                  "出遅れ=まだ走ってないバネ(参考軸・5日αの裏付けは無い)")
        compact = st.checkbox("📱 コンパクト表示", value=False,
                              help="スマホ向け。主要列(ティア/初動/ロケット/出遅れ/スコア/銘柄/テーマ)だけ表示し横スクロールを減らす")

    # ---- 🚀 今週の主役候補(ロケット度トップ: 継続で大化けしやすい銘柄) ----
    # ロケット度>=70(歴史的 top-decile)かつ大商い(vr>=1.5)を上位3件。
    # 注: 乖離大/伸びきり(🔥)は"燃料"なので除外しない(BTで大化けの源泉と確認)。
    cand = stocks[(stocks["potential"] >= 70)
                  & (stocks["vr"].fillna(0) >= 1.5)].copy()
    cand = cand.sort_values("potential", ascending=False).head(3)

    st.subheader("🚀 今週の主役候補")
    st.caption("超ホットなテーマ × もう上向きに走っている(乖離・20日高) × 大商い＝"
               "モメンタム継続で『1週間の大化け』が出やすい銘柄(ロケット度トップ)。"
               "バックテスト(2025-04〜2026-05)ではロケット度70+(歴史的top10%)の +20%/5日 確率は約5%＝全体1.4%の約3.5倍。"
               "※ただし大コケも増えるバーベル。損切り前提で、値上がり保証ではありません。")

    def _card(container, medal, r):
        tier = r["tier"]
        strong = TIER_COLOR.get(tier, "#333333")
        bg = TIER_BG.get(tier, "#ffffff")
        stars = {3: "★★★", 2: "★★☆", 1: "★☆☆"}.get(int(r["init_stars"]), "★★☆")
        dev = r["dev"] if pd.notna(r["dev"]) else 0.0
        vr = r["vr"] if pd.notna(r["vr"]) else 0.0
        heat = r["theme_heat"] if pd.notna(r["theme_heat"]) else 0.0
        pol = " 🏛国策" if r["policy"] else ""
        us = r["us_tailwind"]
        usb = f" 🇺🇸前夜+{us:.1f}%" if (us is not None and pd.notna(us) and us > 0) else ""
        container.markdown(
            f"<div style='border:2px solid {strong};border-radius:12px;padding:12px;"
            f"background:{bg};color:#111111'>"
            f"<div style='font-size:13px;color:#444'>{medal} ロケット度</div>"
            f"<div style='font-size:34px;font-weight:800;color:{strong};line-height:1.1'>{r['potential']:.0f}</div>"
            f"<div style='font-size:17px;font-weight:800;margin-top:6px'>{r['name']}</div>"
            f"<div style='font-size:12px;color:#555'>[{r['ticker']}]　{tier}　初動{stars}</div>"
            f"<div style='font-size:13px;margin-top:8px;font-weight:600'>🔥 {r['theme']}　heat{heat:.0f}</div>"
            f"<div style='font-size:13px;margin-top:4px'>📈 出来高{vr:.1f}x ・ 乖離{dev:+.1f}%{pol}{usb}</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    if cand.empty:
        st.info("ロケット度70+の継続銘柄が今日はありません(テーマが静か or 大商い未点火)。"
                "無理に追わず、テーマ熱が上がる日を待つのが吉。")
    else:
        medals = ["🥇", "🥈", "🥉"]
        rows = list(cand.iterrows())
        if compact:  # スマホ: 縦積み
            for medal, (_, r) in zip(medals, rows):
                _card(st, medal, r)
                st.write("")
        else:        # PC: 横並び
            for col, medal, (_, r) in zip(st.columns(len(rows)), medals, rows):
                _card(col, medal, r)

    st.divider()

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

    if sort_mode.startswith("🚀"):
        df = df.sort_values("potential", ascending=False)
        order_label = "ロケット度降順"
    elif sort_mode.startswith("🏆"):
        df = df.assign(_tr=df["tier"].map(TIER_ORDER).fillna(9))
        df = df.sort_values(["_tr", "score"], ascending=[True, False]).drop(columns="_tr")
        order_label = "ランク順(S→A→B→C)"
    elif sort_mode.startswith("🐢"):
        df = df.sort_values("laggard", ascending=False)
        order_label = "出遅れ度降順"
    else:
        df = df.sort_values("score", ascending=False)
        order_label = "強さスコア降順"
    st.markdown(f"**{len(df)} 銘柄**（{order_label}）")

    _stars = {3: "★★★", 2: "★★☆", 1: "★☆☆"}
    view = pd.DataFrame({
        "ティア": df["tier"],
        "ロケット": df["potential"],
        "出遅れ": df["laggard"],
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
        view = view[["ティア", "ロケット", "出遅れ", "初動", "伸", "スコア", "銘柄", "テーマ"]]

    def _row_style(row):
        tier = row["ティア"]
        bg = TIER_BG.get(tier, "#ffffff")
        strong = TIER_COLOR.get(tier, "#333333")
        styles = []
        for col in row.index:
            if col == "ティア":  # 色付きバッジ(濃色背景+白文字)
                styles.append(f"background-color:{strong};color:#ffffff;"
                              "font-weight:800;text-align:center")
            elif col == "ロケット":  # ロケット度(継続・大化け確率・主軸): 紫で強調
                styles.append(f"background-color:{bg};color:#6a1b9a;font-weight:800")
            elif col == "出遅れ":  # 出遅れ度(参考軸): 緑系で区別
                styles.append(f"background-color:{bg};color:#1b6a3a;font-weight:700")
            elif col == "スコア":  # キー指標: 太字の濃色文字で強調
                styles.append(f"background-color:{bg};color:#111111;font-weight:800")
            else:  # 文字色を濃色固定(ダークテーマでの白文字潰れ防止)
                styles.append(f"background-color:{bg};color:#1a1a1a")
        return styles

    fmt = {"ロケット": "{:.0f}", "出遅れ": "{:.0f}", "スコア": "{:.1f}", "テーマ熱": "{:.0f}", "出来高比": "{:.1f}x",
           "25MA乖離%": "{:+.1f}", "RSI": "{:.0f}", "5d%": "{:+.1f}",
           "20d%": "{:+.1f}", "米前夜%": "{:+.1f}"}
    fmt = {k: v for k, v in fmt.items() if k in view.columns}  # コンパクト時に欠ける列を除外
    styler = view.style.apply(_row_style, axis=1).format(fmt, na_rep="-")
    st.dataframe(styler, use_container_width=True, hide_index=True, height=620)

    st.caption("【3つの軸】"
               "①スコア(強さ)=資金流入が『もう強いか』。大相場銘柄は走った後もここが高い。／"
               "②ロケット度=『1週間で大化けしやすいか』(主軸)。超ホット熱×もう上向きに走っている(25MA乖離大×20日上昇大)×大商い＝"
               "BT(2025-04〜2026-05)で爆益の源泉と確定。70+=歴史的top10%で +20%/5日 が約5%(全体1.4%の約3.5倍)。ただしバーベル(大コケも増える)=要損切り。／"
               "③出遅れ度=『まだ走ってないバネの縮み』(参考軸)。テーマ点火中×資金流入中×まだ走ってない銘柄が高い。"
               "ただしBTでは5日先の爆益は出遅れからは出ず(=この軸に5日αの裏付けは無い)、『安く拾って待つ』発想の参考として残してある。／"
               "初動★=エントリーの入りやすさ。★★★=MA近辺(走り始め)/★☆☆=乖離大(もう走った)・🔥=伸びきり。"
               "🔥や乖離大はロケット度では『燃料』、出遅れ度では『減点要因』と逆に効く。"
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

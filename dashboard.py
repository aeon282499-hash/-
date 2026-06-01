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
                             ["🏆 ランク順(S→A→B→C)", "🔥 短期爆益度順(2-3日)", "🚀 ロケット度順",
                              "強さスコア順", "🐢 出遅れ度順"],
                             index=0,
                             help="ランク順=S→A→B→C(同ランク内は短期爆益度降順)。"
                                  "短期爆益=2-3日(特に3日)で+10〜15%の大ポップが出やすい"
                                  "(BT裏付け・🔥伸びきりも歓迎・要損切り)。"
                                  "ロケット=1週間の大化け(継続・主軸)。"
                                  "強さ=もう強い銘柄。出遅れ=まだ走ってないバネ(参考軸・5日αの裏付けは無い)")
        compact = st.checkbox("📱 コンパクト表示", value=False,
                              help="スマホ向け。主要列(ティア/初動/短期爆益/ロケット/スコア/銘柄/テーマ)だけ表示し横スクロールを減らす")

    # ---- 📋 推奨トレードプラン(出口ルールBT bt_exit.py の最適解) ----
    with st.expander("📋 推奨トレードプラン（バックテストで最も勝てた出口ルール）", expanded=False):
        st.markdown(
            "**エントリ**：シグナル翌営業日の寄り付き（成行）\n\n"
            "**保有**：約 **8営業日（≒1.5〜2週間）**。3日で降りるより期待値はほぼ2倍。"
            "モメンタム銘柄は走り続けるので**利は引っ張る**のが正解（出口ルールBTで確認）。\n\n"
            "**利確**：基本は**付けない（引っ張る）**。付けるなら **+25〜30%** で部分利確。"
            "タイトな利確（+10〜15%）は大化けの裾を切って逆効果。\n\n"
            "**損切り**：**-12% 目安**（事故防止の保険）。\n\n"
            "**実績（blast≥70 を8営業日保有・2025-04〜2026-05）**：平均 +3.0% / 勝率 54% / PF 1.84。\n\n"
            "**なぜ損切り -12% か**：損切りは“儲けを増やす”ものではなく**最悪ケースを抑える保険**。"
            "-12% を入れると平均は約 -0.5%pt 下がるが、最悪級の下位5%が **-23% → -12% へほぼ半減**。"
            "-8% など浅すぎる損切りは、モメンタム銘柄が一時的な押し目で約4割も振り落とされ逆効果（中央値マイナス）。"
        )
        st.caption("✅ 地合い別の再検証でも、この出口（8日保有・利を引っ張る・損切り-12%）が上げ／下げ局面の両方で頑健と確認。"
                   "そもそも『超ホット×大商い×走り出し』というシグナルは下げ相場ではほとんど点火しない＝"
                   "出動回数が自然に絞られる内蔵プロテクションが効きます。"
                   "短期(3日)で回したい場合は +10%/3日 が約2.8倍狙えますが、期待値は8日保有に劣ります。")

    # ---- 🔥 短期(モメンタム)主役候補(短期爆益度トップ: 大ポップが出やすい) ----
    # blast>=70 を上位3件。🔥伸びきりは"燃料"なので除外しない(BTで大ポップの源泉)。
    bcand = stocks[stocks["blast"] >= 70].sort_values("blast", ascending=False).head(3)

    st.subheader("🔥 短期(モメンタム)主役候補")
    st.caption("もう上向きに走っている(乖離大・🔥伸びきりも歓迎) × 超ホット熱 × 大商い＝"
               "短期で +10〜15% の大ポップが出やすい銘柄(短期爆益度トップ)。"
               "BT(2025-04〜2026-05)で短期爆益度70+の +10%/3日 確率は約9.9%＝全体3.5%の約2.8倍。"
               "ただし期待値は『約8営業日まで引っ張る』方が高い(平均+3.0%/PF1.84)→📋推奨トレードプラン参照。"
               "翌寄りで入り、利は引っ張る・損切り-12%目安。"
               "※平均・勝率は上がらないバーベル(大コケも増える)。損切り必須・値上がり保証ではありません。")

    def _blast_card(container, medal, r):
        tier = r["tier"]
        strong = TIER_COLOR.get(tier, "#333333")
        stars = {3: "★★★", 2: "★★☆", 1: "★☆☆"}.get(int(r["init_stars"]), "★★☆")
        dev = r["dev"] if pd.notna(r["dev"]) else 0.0
        vr = r["vr"] if pd.notna(r["vr"]) else 0.0
        heat = r["theme_heat"] if pd.notna(r["theme_heat"]) else 0.0
        r20 = (r["r20"] * 100) if pd.notna(r["r20"]) else 0.0
        oe = " 🔥伸びきり" if r["overextended"] else ""
        accent = "#c2185b"  # 短期爆益=ピンクで強調(ロケットの紫と区別)
        container.markdown(
            f"<div style='border:2px solid {accent};border-radius:12px;padding:12px;"
            f"background:#fff0f5;color:#111111'>"
            f"<div style='font-size:13px;color:#444'>{medal} 短期爆益度(3日)</div>"
            f"<div style='font-size:34px;font-weight:800;color:{accent};line-height:1.1'>{r['blast']:.0f}</div>"
            f"<div style='font-size:17px;font-weight:800;margin-top:6px'>{r['name']}</div>"
            f"<div style='font-size:12px;color:#555'>[{r['ticker']}]　{tier}　初動{stars}{oe}</div>"
            f"<div style='font-size:13px;margin-top:8px;font-weight:600'>🔥 {r['theme']}　heat{heat:.0f}</div>"
            f"<div style='font-size:13px;margin-top:4px'>📈 出来高{vr:.1f}x ・ 乖離{dev:+.1f}% ・ 20日{r20:+.0f}%</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    if bcand.empty:
        st.info("短期爆益度70+の銘柄が今日はありません(走ってる×超ホットが不在)。無理に追わないのが吉。")
    else:
        bmedals = ["🥇", "🥈", "🥉"]
        brows = list(bcand.iterrows())
        if compact:  # スマホ: 縦積み
            for medal, (_, r) in zip(bmedals, brows):
                _blast_card(st, medal, r)
                st.write("")
        else:        # PC: 横並び
            for col, medal, (_, r) in zip(st.columns(len(brows)), bmedals, brows):
                _blast_card(col, medal, r)

    st.divider()

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

    if sort_mode.startswith("🏆"):
        df = df.assign(_tr=df["tier"].map(TIER_ORDER).fillna(9))
        df = df.sort_values(["_tr", "blast"], ascending=[True, False]).drop(columns="_tr")
        order_label = "ランク順 S→A→B→C(同ランク内は短期爆益度降順)"
    elif sort_mode.startswith("🔥"):
        df = df.sort_values("blast", ascending=False)
        order_label = "短期爆益度降順(2-3日)"
    elif sort_mode.startswith("🚀"):
        df = df.sort_values("potential", ascending=False)
        order_label = "ロケット度降順"
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
        "短期爆益": df["blast"],
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

    # ティアは順序付きカテゴリにして列ヘッダクリックでも S→A→B→C で並ぶようにする
    # (文字列のままだとアルファベット順 A→B→C→S になり S が最後に来てしまう)
    view["ティア"] = pd.Categorical(view["ティア"], categories=["S", "A", "B", "C"], ordered=True)

    if compact:
        view = view[["ティア", "短期爆益", "ロケット", "初動", "伸", "スコア", "銘柄", "テーマ"]]

    def _row_style(row):
        tier = row["ティア"]
        bg = TIER_BG.get(tier, "#ffffff")
        strong = TIER_COLOR.get(tier, "#333333")
        styles = []
        for col in row.index:
            if col == "ティア":  # 色付きバッジ(濃色背景+白文字)
                styles.append(f"background-color:{strong};color:#ffffff;"
                              "font-weight:800;text-align:center")
            elif col == "短期爆益":  # 短期爆益度(2-3日・大ポップ確率): ピンクで強調
                styles.append(f"background-color:{bg};color:#c2185b;font-weight:800")
            elif col == "ロケット":  # ロケット度(継続・大化け確率・主軸): 紫で強調
                styles.append(f"background-color:{bg};color:#6a1b9a;font-weight:800")
            elif col == "出遅れ":  # 出遅れ度(参考軸): 緑系で区別
                styles.append(f"background-color:{bg};color:#1b6a3a;font-weight:700")
            elif col == "スコア":  # キー指標: 太字の濃色文字で強調
                styles.append(f"background-color:{bg};color:#111111;font-weight:800")
            else:  # 文字色を濃色固定(ダークテーマでの白文字潰れ防止)
                styles.append(f"background-color:{bg};color:#1a1a1a")
        return styles

    fmt = {"短期爆益": "{:.0f}", "ロケット": "{:.0f}", "出遅れ": "{:.0f}", "スコア": "{:.1f}", "テーマ熱": "{:.0f}", "出来高比": "{:.1f}x",
           "25MA乖離%": "{:+.1f}", "RSI": "{:.0f}", "5d%": "{:+.1f}",
           "20d%": "{:+.1f}", "米前夜%": "{:+.1f}"}
    fmt = {k: v for k, v in fmt.items() if k in view.columns}  # コンパクト時に欠ける列を除外
    styler = view.style.apply(_row_style, axis=1).format(fmt, na_rep="-")
    st.dataframe(styler, use_container_width=True, hide_index=True, height=620)

    st.caption("【4つの軸】"
               "①短期爆益度=『短期で +10〜15% の大ポップが出やすいか』。乖離大×超ホット熱×大商いで、🔥伸びきりも"
               "『燃料』として加点(除外しない)。BT(2025-04〜2026-05)で 70+ の +10%/3日 確率は約9.9%＝全体3.5%の約2.8倍。"
               "出口BTでは『翌寄り買い→約8営業日保有・利は引っ張る・損切り-12%』が最も期待値が高い(平均+3.0%/PF1.84・📋推奨トレードプラン)。"
               "バーベル(大コケも増える)=損切り必須。／"
               "②ロケット度=『1週間(5日)で大化けしやすいか』。同じく継続(乖離大×20日上昇×超ホット×大商い)。70+で +20%/5日 が約5%(全体1.4%の約3.5倍)。／"
               "③スコア(強さ)=資金流入が『もう強いか』。大相場銘柄は走った後もここが高い。／"
               "④出遅れ度=『まだ走ってないバネの縮み』(参考軸)。BTでは短期の爆益は出遅れからは出ず(=5日αの裏付けは無い)、『安く拾って待つ』発想の参考。／"
               "初動★=エントリーの入りやすさ。★★★=MA近辺(走り始め)/★☆☆=乖離大(もう走った)・🔥=伸びきり。"
               "🔥や乖離大は短期爆益度/ロケット度では『燃料』、出遅れ度では『減点要因』と逆に効く。"
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

// about.js — ❓使い方・データについて・免責（v8・2026-10-17 タブ7本・検証の数字は折りたたみ）
function viewAbout() {
  return `
  <h2>モメンタムチンパン v8 <span class="sub">タブ7本（2026-10-17 本人「タブ増やす・UIよくして・検証を簡素化」）</span></h2>
  <div class="card">
    <div style="line-height:1.95">
      <div>📰 <b>IR</b> … 今日の適時開示の 👍好材料 / 👎悪材料 / 🔍要精査（表題ルール＋Claude判定）と、翌日の答え合わせ。🔍開示検索は社名・表題・本文（PDF冒頭）を横断、チップは株探の人気テーマ30。</div>
      <div>🌙 <b>前夜</b>（ホーム） … 上髭逆指値の明日の銘柄（買い／売り・利確/損切り/株数つき）→ 土俵（人と注文が集まる順）→ 材料 → 明日の決算。</div>
      <div>💴 <b>資金</b> … 今日 資金が集まった業種/テーマ → 主役に連動する銘柄 → 夜間PTS。📚連動集（安定して連動するグループ・SBI登録用コピー）。🔥株探の人気テーマ30（母集団の中身つき）。</div>
      <div>🚩 <b>初動</b> … 初動ブレイク（10年検証・負け筋除外・🆕新高値）に「本命」ボタン → 📝本命の台帳（翌寄り建て・20日/−15%で自動答え合わせ・候補全体と比較）。🧪C/Dに材料が出た。その他の探検。</div>
      <div>📋 <b>作戦</b>（寄り前） … 実弾の注文（🩳フェード／👑極上）を書いてある通りに出す文面、余力→サイズ、保有の出口、✅チェック。</div>
      <div>📊 <b>データ</b> … 🔻売り（フェード・崩壊・モメンタム終了）・🐵EODランキング・🔍銘柄検索→詳細（日足・指標）。</div>
      <div>🧮 <b>電卓</b> … 建値 → 利確3段（1.5／3／5%）・損切り3段（1／2／3%）・株数（損失2万円）。計算だけ。</div>
    </div>
  </div>
  <div class="card note" style="line-height:1.9"><b>検証の数字</b>は各画面の1行の結論の横にある「検証の数字」を押すと開きます。どの画面も「上がる/下がる・勝てる」は言いません。勝てる型だけが📋作戦に載ります。</div>
  <h2>データについて</h2>
  <div class="card">
    <div class="chips"><span class="tag">EOD ${DATA.data_date || "—"}（${freshnessText()}）</span><span class="tag">更新 平日 朝7時・8:40・夕18:30・19:45ごろ＋配信完走に連動</span><span class="tag">対象 ${DATA.universe_scored ? DATA.universe_scored.toLocaleString() : "—"}銘柄</span></div>
    <div class="note" style="margin-top:10px">すべて<b>最新営業日の終値</b>ベースで場中は変わりません（J-Quants(JPX)の日足・貸借区分・規制銘柄情報を加工）。生の株価(OHLCV)は再配布せず、計算済みの指標と窓を限定した加工チャートのみ表示。IR は TDnet 直接、分類の基準は brisk のタグ体系（brisk のデータは載せない）。株探の人気テーマは PC から取得。</div>
  </div>
  <h2>アプリとして使う</h2>
  <div class="card note">📱 スマホの<b>「ホーム画面に追加」</b>でアプリのように使えます。右上 ◐ でダーク/ライト切替。旧画面は legacy_v5.html / legacy_v6.html に残置。</div>
  <h2>免責</h2>
  <div class="card"><ul style="margin:0;padding-left:18px;font-size:12px;line-height:1.8;color:var(--mut)">
    <li>本アプリは<b style="color:var(--tx)">参考情報</b>であり、特定銘柄の売買を推奨するものではありません。投資判断は利用者ご自身の責任で。</li>
    <li>候補・水準・倍率は機械的な計算で、将来の値動きや精度を保証しません。</li>
    <li>商用での一般公開にあたっては、J-Quants の利用条件を確認のうえ運用してください（現状は本人専用）。</li></ul></div>
  <p class="disc">${esc(DATA.disclaimer)}<br>生成: ${DATA.generated_at || "—"} / source: ${DATA.source || "—"} / app v${CFG.VERSION}</p>`;
}

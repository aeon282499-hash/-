// about.js — ❓使い方・データについて・免責（v5）
function viewAbout() {
  return `
  <h2>モメンタムチンパン v5 <span class="sub">デイトレ特化（2026-09-14 リニューアル）</span></h2>
  <div class="card">
    <div style="line-height:1.95">
      <div>🔥 <b>ライブ</b> … <b>いま資金が来ているテーマ／セクター／銘柄</b>。取引所リアルタイムの現在値・売買代金（立花証券API）を約1分で全銘柄一巡し、届いた瞬間に画面が更新されます（WebSocket）。平日8:58〜15:35の場中だけ流れます。</div>
      <div>🎯 <b>土俵</b> … 前夜に配信した「人と注文が集まる」銘柄15本。方向は付けません。ライブが動いている間は「土俵のいま」も出ます。</div>
      <div>🔻 <b>売り</b> … 🩳フェード（実弾の主力・急騰しすぎの当日空売り）と、📉上昇が崩れたサイン。</div>
      <div>🐵 <b>EOD</b> … 終値ベースのモメンタム指数ランキング（買い推奨ではない）・🏆勝ちやすい順張り・📒持ち株コーチ・⭐ウォッチ。</div>
      <div>🧭 <b>探検</b> … 初動・押し目・ストップ高などの探索用スクリーナー（優位性は未検証）。</div>
    </div>
  </div>
  <h2>🔥ライブの数字の読み方</h2>
  <div class="card">
    <ul style="margin:0;padding-left:18px;font-size:12.5px;line-height:1.9">
      <li><b>今の資金 ×N</b> … 直近5分の売買代金が、ふだんの5分（20日平均代金÷66）の何倍か。<b style="color:var(--hot)">×2以上＝資金が集中</b>、×1＝平常、×0.7未満＝閑散。テーマ/セクターは構成銘柄の平均代金で加重。</li>
      <li><b>当日資金 ×N</b> … 当日累計代金 ÷ (20日平均代金 × 時刻別の想定進捗)。寄り直後は分母が小さく荒れやすいので、5分の方を主に見る。</li>
      <li><b>本日 %</b> … 前日終値比。テーマ/セクターの「本日」は代金加重（大型に引っ張られる）・「上昇%」は構成銘柄のうち上がっている割合。</li>
      <li><b>5分 %</b> … 5分前との価格差。<b>🔺高値</b>＝現在値が当日高値。<b>VWAP</b>＝VWAPとの乖離。</li>
      <li>テーマは手作りの34テーマ（半導体5分類・フィジカルAI3分類・防衛・宇宙・…）。セクターは東証33業種。</li>
      <li><b>予測でも推奨でもありません。</b>「どこに資金が来ているか」の観測です。撃つかどうかは板と歩み値で。</li>
    </ul>
  </div>
  <h2>データについて</h2>
  <div class="card">
    <div class="chips"><span class="tag">EOD ${DATA.data_date || "—"}（${freshnessText()}）</span><span class="tag">EOD更新 平日 朝7時・8:40・夕18:30ごろ</span><span class="tag">対象 ${DATA.universe_scored ? DATA.universe_scored.toLocaleString() : "—"}銘柄</span></div>
    <div class="note" style="margin-top:10px">EODタブは<b>最新営業日の終値</b>ベースで場中は変わりません（J-Quants(JPX)の日足を加工）。🔥ライブだけがリアルタイム（立花証券e支店API・取引所の現在値）。生の株価(OHLCV)は再配布せず、計算済みの指標と窓を限定した加工チャートのみ表示。</div>
  </div>
  <h2>アプリとして使う</h2>
  <div class="card note">📱 スマホの<b>「ホーム画面に追加」</b>でアプリのように使えます。右上 ◐ でダーク/ライト切替。</div>
  <h2>免責</h2>
  <div class="card"><ul style="margin:0;padding-left:18px;font-size:12px;line-height:1.8;color:var(--mut)">
    <li>本アプリは<b style="color:var(--tx)">参考情報</b>であり、特定銘柄の売買を推奨するものではありません。投資判断は利用者ご自身の責任で。</li>
    <li>シグナル・期待値・倍率は推定値で、将来の値動きや精度を保証しません。</li>
    <li>商用での一般公開にあたっては、J-Quants／立花証券APIの利用条件を確認のうえ運用してください（現状は本人専用）。</li></ul></div>
  <p class="disc">${esc(DATA.disclaimer)}<br>生成: ${DATA.generated_at || "—"} / source: ${DATA.source || "—"} / app v${CFG.VERSION}</p>`;
}

// boot.js — ルート登録と起動（v7・2026-10-13 再設計）。ROUTESの並び＝前方一致の優先順。最後が既定(🌙前夜の準備)。
// v7 で外したもの: 🔥ライブ(live.js・立花取得停止中)・📐候補/📊紙トレ(フィボ・10/6削除)・✂️上ヒゲ刈り取り(oshime5・10/9停止)・✅チェック(📋作戦に吸収)
// v8.2 (2026-10-19 本人「やっぱりIRいらない」): #/ir のルートとタブを外した（ir.js は 🧪C/D材料の irChip 用に残置）
route("#/flow/hot", "shodo", viewFlowHot);  // v8: 💴資金 → 🔥株探の人気テーマ
route("#/flow", "shodo", viewFlow);    // v8.2: 💴資金は🚩初動の切替に（本人「資金もまとめる」「前夜いらない」）         // v8: 💴資金の流れ→連動株→連動集 は独立タブ
route("#/short", "short", viewShort);   // v8.4: 🩳空売り（本人「この空売りとモメンタムはタブわけて」2026-10-19）
route("#/search", "data", viewSearch);
route("#/detail/", "data", h => viewDetail(decodeURIComponent(h.split("/")[2])));
route("#/plan", "plan", viewPlan);
route("#/sell", "data", viewSell);
route("#/momentum", "momentum", viewMomentum);   // v8.2: 🐵ランキングは独立タブ（本人「あれ一番欲しい」）
route("#/explore/", "shodo", h => viewExploreList(decodeURIComponent(h.split("/")[2])));
route("#/explore", "shodo", viewExplore);   // v8: 🚩初動（探検）は独立タブ
route("#/calc", "calc", viewCalc);
route("#/coach", "coach", viewCoach);   // v8.3: 🧠AI調教（人格プロンプト・銘柄の解剖・言い訳チェッカー・任意でClaude直接）
route("#/about", "arena", viewAbout);
route("#/arena", "arena", viewArena);
route("#/", "momentum", viewMomentum);   // v8.2: ホーム＝🐵ランキング（本人「あれ一番欲しい」「前夜いらない」）。🌙前夜は #/arena に残置（タブなし）

window.addEventListener("hashchange", render);
try { if (typeof navigator !== "undefined" && navigator.serviceWorker) navigator.serviceWorker.register("sw.js"); } catch (e) { /* ignore */ }
applyTheme();
load().then(() => {
  const dp = $("#datepill");
  dp.textContent = `EOD ${DATA.data_date || "—"}・${freshnessText()}`;
  if (dp.classList && DATA.data_lag_days != null && DATA.data_lag_days <= 1) dp.classList.add("fresh");
  render();
}).catch(e => { $("#view").innerHTML = `<div class="card">${esc(e.message)}</div>`; });

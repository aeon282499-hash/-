// boot.js — ルート登録と起動（v7・2026-10-13 再設計）。ROUTESの並び＝前方一致の優先順。最後が既定(🌙前夜の準備)。
// v7 で外したもの: 🔥ライブ(live.js・立花取得停止中)・📐候補/📊紙トレ(フィボ・10/6削除)・✂️上ヒゲ刈り取り(oshime5・10/9停止)・✅チェック(📋作戦に吸収)
route("#/ir", "ir", viewIR);            // v8: 📰IR は独立タブ
route("#/flow/hot", "flow", viewFlowHot);  // v8: 💴資金 → 🔥株探の人気テーマ
route("#/flow", "flow", viewFlow);         // v8: 💴資金の流れ→連動株→連動集 は独立タブ
route("#/search", "data", viewSearch);
route("#/detail/", "data", h => viewDetail(decodeURIComponent(h.split("/")[2])));
route("#/plan", "plan", viewPlan);
route("#/sell", "data", viewSell);
route("#/momentum", "data", viewMomentum);
route("#/explore/", "shodo", h => viewExploreList(decodeURIComponent(h.split("/")[2])));
route("#/explore", "shodo", viewExplore);   // v8: 🚩初動（探検）は独立タブ
route("#/calc", "calc", viewCalc);
route("#/about", "arena", viewAbout);
route("#/arena", "arena", viewArena);
route("#/", "arena", viewArena);

window.addEventListener("hashchange", render);
try { if (typeof navigator !== "undefined" && navigator.serviceWorker) navigator.serviceWorker.register("sw.js"); } catch (e) { /* ignore */ }
applyTheme();
load().then(() => {
  const dp = $("#datepill");
  dp.textContent = `EOD ${DATA.data_date || "—"}・${freshnessText()}`;
  if (dp.classList && DATA.data_lag_days != null && DATA.data_lag_days <= 1) dp.classList.add("fresh");
  render();
}).catch(e => { $("#view").innerHTML = `<div class="card">${esc(e.message)}</div>`; });

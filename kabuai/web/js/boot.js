// boot.js — ルート登録と起動（v5）。ROUTESの並び＝前方一致の優先順。最後が既定(🔥ライブ)。
route("#/search", "momentum", viewSearch);
route("#/detail/", "momentum", h => viewDetail(decodeURIComponent(h.split("/")[2])));
route("#/plan", "plan", viewPlan);
route("#/sell", "sell", viewSell);
route("#/arena", "arena", viewArena);
route("#/momentum", "momentum", viewMomentum);
route("#/explore/", "explore", h => viewExploreList(decodeURIComponent(h.split("/")[2])));
route("#/explore", "explore", viewExplore);
route("#/about", "live", viewAbout);
route("#/live", "live", viewLive);
route("#/", "live", viewLive);

window.addEventListener("hashchange", render);
try { if (typeof navigator !== "undefined" && navigator.serviceWorker) navigator.serviceWorker.register("sw.js"); } catch (e) { /* ignore */ }
applyTheme();
load().then(() => {
  const dp = $("#datepill");
  dp.textContent = `EOD ${DATA.data_date || "—"}・${freshnessText()}`;
  if (dp.classList && DATA.data_lag_days != null && DATA.data_lag_days <= 1) dp.classList.add("fresh");
  render();
  liveInit();
}).catch(e => { $("#view").innerHTML = `<div class="card">${esc(e.message)}</div>`; });

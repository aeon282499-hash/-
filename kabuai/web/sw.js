// モメンタムチンパン Service Worker（PWAインストール要件用・キャッシュなし＝常に最新）
// データの鮮度が命のアプリなので、オフラインキャッシュはあえて行わない（古いシグナルを
// 見せて誤エントリーさせるリスク回避）。fetchはネットワーク素通し。
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));
self.addEventListener("fetch", () => {});

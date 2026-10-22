// モメンタムチンパン Service Worker（PWAインストール要件用・キャッシュなし＝常に最新）
// データの鮮度が命のアプリなので、オフラインキャッシュはあえて行わない（古いシグナルを
// 見せて誤エントリーさせるリスク回避）。fetchはネットワーク素通し。
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));
// v8.5（2026-10-22）: 例外は 📄事業内容の索引 biz/<k>.json だけ（有報の本文・月1更新・10本で約22MB）。
// シグナルではないので古くても害がない → キャッシュを先に返し、裏で新しいのを取って入れ替える。
self.addEventListener("fetch", (e) => {
  if (!/\/biz\/[0-9A-Z]\.json(\?|$)/.test(e.request.url)) return;
  e.respondWith(caches.open("kabuai-biz-v1").then(async (c) => {
    const hit = await c.match(e.request);
    const net = fetch(e.request).then((r) => { if (r && r.ok) c.put(e.request, r.clone()); return r; }).catch(() => hit);
    return hit || net;
  }));
});

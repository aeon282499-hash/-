// Cloudflare Worker: gapfade-signal-trigger
// 既存 swing-signal-trigger / close-signal-trigger と同じパターンで gapfade.yml を
// workflow_dispatch する。
//
// 用途: 平日 11:00 JST に cron-job.org からHTTP POSTで起動 → GitHub Actions の
//      "ギャップフェード 朝の候補配信" を発火 → gapfade.py 実行 → 候補をDiscordへ。
//
// なぜ必要か（2026-07-28）:
//   初日7/27は gapfade.yml の GitHub cron（9:25 JST設計）が **13:10 JST に起動** し、
//   エントリー時刻の後場寄り12:30に間に合わなかった。同リポジトリの実測では
//   GitHub cron は常時40〜50分遅延（8:20設計→9:08〜9:13着弾）する一方、
//   cron-job.org→Worker の dispatch は毎日8:05きっかりで1分もズレない。
//   ＝「12時までに必ず出す」にはこの経路が要る。
//
// 環境変数（Cloudflare ダッシュボード Settings → Variables and Secrets で設定）:
//   GITHUB_PAT       — fine-grained PAT（actions: read/write・対象リポジトリ aeon282499-hash/-）
//   GITHUB_OWNER     — "aeon282499-hash"
//   GITHUB_REPO      — "-"
//   WORKFLOW_FILE    — "gapfade.yml"
//   GIT_REF          — "main"
//
// 二重配信の心配は不要: gapfade.py が gapfade_last_send.json で同日ガードするので、
// GitHub cron 4本＋この外部トリガーの何本が当たっても配信は1日1回。

export default {
  async fetch(request, env, ctx) {
    return await dispatchWorkflow(env);
  },

  async scheduled(event, env, ctx) {
    // Cloudflareの内蔵cronも一応バックアップとして使えるようにしておく
    ctx.waitUntil(dispatchWorkflow(env));
  },
};

async function dispatchWorkflow(env) {
  const owner = env.GITHUB_OWNER || "aeon282499-hash";
  const repo  = env.GITHUB_REPO  || "-";
  const file  = env.WORKFLOW_FILE || "gapfade.yml";
  const ref   = env.GIT_REF || "main";

  if (!env.GITHUB_PAT) {
    return new Response("Missing GITHUB_PAT", { status: 500 });
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${file}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${env.GITHUB_PAT}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "gapfade-signal-trigger",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref }),
  });

  const text = await res.text();
  // GitHub workflow_dispatch は成功時 204 No Content
  if (res.status === 204) {
    return new Response(`OK: dispatched ${file} on ${ref}`, { status: 200 });
  }
  return new Response(
    `dispatch failed: status=${res.status} body=${text}`,
    { status: 502 }
  );
}

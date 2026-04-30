// Cloudflare Worker: close-signal-trigger
// 既存 swing-signal-trigger と同じパターンで schedule_close.yml を workflow_dispatch する。
//
// 用途: 平日 14:55 JST に cron-job.org からHTTP POSTで起動 → GitHub Actions の
//      "大引け前RSI判定" ワークフローを発火 → close_check.py 実行 → 大引け処分通知。
//
// 環境変数（Cloudflare ダッシュボード Settings → Variables and Secrets で設定）:
//   GITHUB_PAT       — fine-grained PAT（actions: read/write 権限・対象リポジトリ aeon282499-hash/-）
//   GITHUB_OWNER     — "aeon282499-hash"
//   GITHUB_REPO      — "-"
//   WORKFLOW_FILE    — "schedule_close.yml"
//   GIT_REF          — "main"

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
  const file  = env.WORKFLOW_FILE || "schedule_close.yml";
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
      "User-Agent": "close-signal-trigger",
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

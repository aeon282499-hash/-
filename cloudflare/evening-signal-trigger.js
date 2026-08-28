// Cloudflare Worker: evening-signal-trigger
// 既存 swing-signal-trigger / close-signal-trigger と同じパターンで
// schedule_evening.yml（スイング前夜配信）を workflow_dispatch する。
//
// 用途: 平日 18:05 JST に cron-job.org からHTTPで起動 → GitHub Actions の
//      "スイング前夜配信" ワークフローを発火 → main.py（スイング通常版＋極み）
//      → main_day.py（デイトレ売りフェード）。着弾はスイング≒18:35・フェード≒18:45-50
//      ＝19:00のSBIハイカラ(HYPER)在庫解禁前に売り禁玉を押さえられる。
//
// 経緯: 2026-08-27にGitHub cronが全本無発火（前夜配信3系統が丸ごと欠落）。
//      GitHub cronは実測40〜50分遅延・最悪スキップなので、朝8:05と同じ外部経路を本命化。
//
// 環境変数（Cloudflare ダッシュボード Settings → Variables and Secrets で設定）:
//   GITHUB_PAT       — fine-grained PAT（actions: read/write 権限・対象リポジトリ aeon282499-hash/-）
//   GITHUB_OWNER     — "aeon282499-hash"
//   GITHUB_REPO      — "-"
//   WORKFLOW_FILE    — "schedule_evening.yml"
//   GIT_REF          — "main"

export default {
  // 2026-08-28: 内蔵cronが鳴らない日があった（8/28は18:05/16:40とも無発火・Cloudflare側の
  // invocationsゼロ）ので、実績のある cron-job.org → URL 経路を鍵付きで復活。
  // ?key=<TRIGGER_KEY> か X-Trigger-Key ヘッダが一致した時だけ dispatch（botのGETは403で無害）。
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") || request.headers.get("X-Trigger-Key") || "";
    if (!env.TRIGGER_KEY || key !== env.TRIGGER_KEY) {
      return new Response("forbidden", { status: 403 });
    }
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
  const file  = env.WORKFLOW_FILE || "schedule_evening.yml";
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
      "User-Agent": "evening-signal-trigger",
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

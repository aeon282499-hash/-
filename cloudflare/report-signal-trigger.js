// Cloudflare Worker: report-signal-trigger
// evening-signal-trigger と同じパターンで report.yml（夕方の結果レポート＋金曜の週次）を
// workflow_dispatch する。
//
// 経緯: report.yml は GitHub cron 単独（15:40 JST設計・実測16:30前後着）だったが、
//      2026-08-28未明に11時間12分遅延して木曜便が金曜2:52に発火→週次レポートを
//      金曜の取引前に誤送信する実障害。時間窓ガード＋同日送信済みマーカーを入れた上で、
//      本命の発火をこのWorker（内蔵cron 16:40 JST）に移す。
//
// 16:40 の理由: J-Quants四本値の当日分は16:30頃公開（公式仕様）。15:40だと当日バーが
// 無く帳簿ドライランが不正確になる（従来はGitHub cronの遅延が偶然16:30以降に運んでいた）。
//
// 環境変数: GITHUB_PAT（Secret・他Workerと同じfine-grained PAT）/ GITHUB_OWNER /
//          GITHUB_REPO / WORKFLOW_FILE=report.yml / GIT_REF=main
// workers.dev URLは無効（botのGETで誤dispatchされるため）＝内蔵cron専用。

export default {
  async fetch(request, env, ctx) {
    return await dispatchWorkflow(env);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(dispatchWorkflow(env));
  },
};

async function dispatchWorkflow(env) {
  const owner = env.GITHUB_OWNER || "aeon282499-hash";
  const repo  = env.GITHUB_REPO  || "-";
  const file  = env.WORKFLOW_FILE || "report.yml";
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
      "User-Agent": "report-signal-trigger",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref }),
  });

  const text = await res.text();
  if (res.status === 204) {
    return new Response(`OK: dispatched ${file} on ${ref}`, { status: 200 });
  }
  return new Response(
    `dispatch failed: status=${res.status} body=${text}`,
    { status: 502 }
  );
}

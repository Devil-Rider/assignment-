/* ============================================================
   leaderboard.js — ranks all players on this device by XP
   (Auth is client-side, so the leaderboard reflects accounts
    created in this browser. Swap Auth for a backend to make it
    global — the page only reads Auth.listPlayers().)
   ============================================================ */
window.Pages = window.Pages || {};
window.Pages.leaderboard = function (view) {
  const players = window.Auth.listPlayers();
  const me = window.Auth.currentUser();

  const medal = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);

  const rows = players.length === 0
    ? `<tr><td colspan="5" class="muted" style="text-align:center;padding:2rem">No players yet — <a href="#/login?mode=signup">create an account</a> and start earning XP!</td></tr>`
    : players.map((p, i) => `
        <tr style="${p.isMe ? 'background:rgba(109,139,255,.12)' : ''}">
          <td style="font-weight:700">${medal(i)}</td>
          <td>
            <span class="avatar" style="display:inline-grid;vertical-align:middle;margin-right:.5rem">${p.name.split(/\s+/).map(s => s[0]).join('').slice(0, 2).toUpperCase()}</span>
            ${window.Components.escapeHtml(p.name)} ${p.isMe ? '<span class="chip" style="color:var(--accent)">you</span>' : ''}
          </td>
          <td style="color:var(--gold);font-weight:700">⭐ ${p.xp}</td>
          <td>Lv ${p.level}</td>
          <td>${p.lessons}/${window.COURSE_INDEX.total} · 🏅 ${p.badges}</td>
        </tr>`).join('');

  view.innerHTML = `
    <h1 class="section-title">🏆 Leaderboard</h1>
    <p class="muted" style="margin:.25rem 0 1.25rem">Top SQL questers ranked by XP. Climb the ranks by completing lessons!</p>

    ${!me ? `<div class="panel" style="padding:1rem 1.25rem;margin-bottom:1.25rem">
        <span class="muted">You're not logged in. <a href="#/login?mode=signup&next=learn">Create a free account</a> to appear here and start earning XP.</span>
      </div>` : ''}

    <div class="result-panel panel">
      <div class="table-scroll">
        <table class="result">
          <thead><tr><th>Rank</th><th>Player</th><th>XP</th><th>Level</th><th>Progress</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>

    ${window.Ads.slot('inline', 'ad-inline')}

    <div class="panel" style="padding:1.25rem;margin-top:1.25rem;text-align:center">
      <p class="muted" style="margin:0 0 .75rem">Want to climb higher?</p>
      <a class="btn btn-primary" href="#/learn">Continue the course →</a>
    </div>
  `;
};

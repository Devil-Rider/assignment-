/* ============================================================
   admin.js — Admin control panel (login-gated to admins).
   Manage accounts (ban / unban / delete), view & export the
   newsletter list, and maintain an IP blocklist.

   ⚠️ IMPORTANT: this is CLIENT-SIDE. Bans and IP blocks here only
   affect THIS browser's data / this visitor's session and are
   bypassable. For real enforcement across all users you need a
   backend or a CDN/WAF (e.g. Cloudflare). See web/README.md.
   ============================================================ */
window.Pages = window.Pages || {};
const IPBLOCK_KEY = 'sqlquest_ipblock';

window.Pages.admin = function (view) {
  if (!window.Auth.isAdmin()) {
    view.innerHTML = `<div class="gate panel"><div class="lock">🛡️</div><h2>Admins only</h2>
      <p class="muted">This area is restricted. Log in with an admin account.</p>
      <a class="btn btn-primary" href="#/login?next=admin">Admin log in</a></div>`;
    return;
  }

  const esc = window.Components.escapeHtml;
  const draw = () => {
    const users = window.Auth.getAllUsers();
    const subs = window.Newsletter.list();
    const blocklist = (localStorage.getItem(IPBLOCK_KEY) || '').trim();

    const userRows = users.map((u) => `
      <tr>
        <td>${esc(u.name)} ${u.isAdmin ? '<span class="chip" style="color:var(--accent)">admin</span>' : ''}</td>
        <td class="muted">${esc(u.email)}</td>
        <td>${u.xp}</td>
        <td>${u.banned ? '<span style="color:var(--red)">banned</span>' : '<span style="color:var(--green)">active</span>'}</td>
        <td>${u.newsletter ? '✉️' : '—'}</td>
        <td>${u.isAdmin ? '<span class="muted small">—</span>' : `
          <button class="btn btn-sm" data-act="${u.banned ? 'unban' : 'ban'}" data-email="${esc(u.email)}">${u.banned ? 'Unban' : 'Ban'}</button>
          <button class="btn btn-sm" data-act="del" data-email="${esc(u.email)}" style="color:var(--red)">Delete</button>`}</td>
      </tr>`).join('');

    const subRows = subs.length ? subs.map((s) => `
      <tr><td>${esc(s.email)}</td><td class="muted">${esc(s.name || '')}</td>
      <td class="muted small">${esc(s.source)}</td>
      <td><button class="btn btn-sm" data-sub="${esc(s.email)}" style="color:var(--red)">Remove</button></td></tr>`).join('')
      : `<tr><td colspan="4" class="muted" style="text-align:center;padding:1rem">No subscribers yet.</td></tr>`;

    view.innerHTML = `
      <h1 class="section-title">🛡️ Admin Panel</h1>
      <p class="muted" style="margin:.25rem 0 1.25rem">Manage users, the newsletter list and blocked IPs.</p>

      <div class="stats-row">
        <div class="stat"><div class="num">${users.length}</div><div class="lbl">Accounts</div></div>
        <div class="stat"><div class="num">${users.filter(u => u.banned).length}</div><div class="lbl">Banned</div></div>
        <div class="stat"><div class="num">${subs.length}</div><div class="lbl">Newsletter subscribers</div></div>
      </div>

      <h2 class="section-title" style="font-size:1.2rem;margin-top:1.5rem">Users</h2>
      <div class="result-panel panel"><div class="table-scroll">
        <table class="result"><thead><tr><th>Name</th><th>Email</th><th>XP</th><th>Status</th><th>News</th><th>Actions</th></tr></thead>
        <tbody>${userRows}</tbody></table>
      </div></div>

      <div class="row-between" style="margin:1.5rem 0 .5rem">
        <h2 class="section-title" style="font-size:1.2rem;margin:0">Newsletter subscribers</h2>
        <button class="btn btn-sm btn-primary" id="exportBtn">⬇ Export CSV</button>
      </div>
      <div class="result-panel panel"><div class="table-scroll">
        <table class="result"><thead><tr><th>Email</th><th>Name</th><th>Source</th><th></th></tr></thead>
        <tbody>${subRows}</tbody></table>
      </div></div>

      <h2 class="section-title" style="font-size:1.2rem;margin-top:1.5rem">IP blocklist</h2>
      <div class="panel" style="padding:1.25rem">
        <p class="muted small" style="margin-top:0">One IP per line. ⚠️ Client-side enforcement is best-effort and bypassable — for real blocking, push this list to a backend or Cloudflare WAF (see README).</p>
        <textarea class="ta" id="ipList" placeholder="203.0.113.5&#10;198.51.100.22">${esc(blocklist)}</textarea>
        <div style="margin-top:.75rem"><button class="btn btn-primary" id="saveIpBtn">Save blocklist</button></div>
      </div>
    `;

    // wire actions
    view.querySelectorAll('[data-act]').forEach((btn) => {
      btn.onclick = () => {
        const email = btn.dataset.email;
        if (btn.dataset.act === 'ban') window.Auth.setBanned(email, true);
        else if (btn.dataset.act === 'unban') window.Auth.setBanned(email, false);
        else if (btn.dataset.act === 'del' && confirm(`Delete account ${email}? This cannot be undone.`)) window.Auth.deleteUser(email);
        window.Components.toast('Updated', 'success'); draw();
      };
    });
    view.querySelectorAll('[data-sub]').forEach((btn) => {
      btn.onclick = () => { window.Newsletter.remove(btn.dataset.sub); window.Components.toast('Removed', 'success'); draw(); };
    });
    view.querySelector('#exportBtn').onclick = () => {
      const blob = new Blob([window.Newsletter.exportCsv()], { type: 'text/csv' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob); a.download = 'subscribers.csv'; a.click();
      URL.revokeObjectURL(a.href);
    };
    view.querySelector('#saveIpBtn').onclick = () => {
      localStorage.setItem(IPBLOCK_KEY, view.querySelector('#ipList').value.trim());
      window.Components.toast('Blocklist saved', 'success');
    };
  };

  draw();
};

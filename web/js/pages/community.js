/* ============================================================
   community.js — Community discussion.
   If CONFIG.community.enabled, embeds Giscus (real cross-user
   discussion backed by GitHub Discussions). Otherwise shows a
   local demo board so the page is functional out of the box.
   ============================================================ */
window.Pages = window.Pages || {};
const DISCUSS_KEY = 'sqlquest_discussion';

window.Pages.community = function (view) {
  const cfg = window.CONFIG.community;
  view.innerHTML = `
    <h1 class="section-title">💬 Community</h1>
    <p class="muted" style="margin:.25rem 0 1.25rem">Ask questions, share queries and help fellow learners.</p>
    <div id="communityHost"></div>
    ${window.Ads.slot('inline', 'ad-inline')}
  `;
  const host = view.querySelector('#communityHost');

  if (cfg.enabled && cfg.repoId && cfg.categoryId) {
    // Real discussion via Giscus
    const s = document.createElement('script');
    s.src = 'https://giscus.app/client.js';
    s.setAttribute('data-repo', cfg.repo);
    s.setAttribute('data-repo-id', cfg.repoId);
    s.setAttribute('data-category', cfg.category);
    s.setAttribute('data-category-id', cfg.categoryId);
    s.setAttribute('data-mapping', 'specific');
    s.setAttribute('data-term', 'SQLQuest Community');
    s.setAttribute('data-reactions-enabled', '1');
    s.setAttribute('data-theme', document.body.classList.contains('light') ? 'light' : 'dark');
    s.crossOrigin = 'anonymous'; s.async = true;
    const wrap = document.createElement('div'); wrap.className = 'panel'; wrap.style.padding = '1rem';
    wrap.appendChild(s); host.appendChild(wrap);
    return;
  }

  // ---- Local demo board ----
  renderLocalBoard(host);
};

function renderLocalBoard(host) {
  const esc = window.Components.escapeHtml;
  const user = window.Auth.currentUser();
  const load = () => { try { return JSON.parse(localStorage.getItem(DISCUSS_KEY)) || []; } catch { return []; } };
  const save = (l) => localStorage.setItem(DISCUSS_KEY, JSON.stringify(l));

  // seed a friendly first message once
  if (load().length === 0) {
    save([{ name: 'SQLQuest Team', text: 'Welcome to the community! 👋 Introduce yourself and share what you\'re learning.', date: new Date().toISOString() }]);
  }

  const draw = () => {
    const msgs = load().slice().reverse();
    host.innerHTML = `
      <div class="panel" style="padding:1.25rem;margin-bottom:1rem">
        ${user ? `
          <strong>Post a message</strong>
          <textarea class="ta" id="msgText" placeholder="Share something with the community…" style="margin-top:.5rem"></textarea>
          <div class="row-between" style="margin-top:.6rem">
            <span class="muted small">Posting as <b>${esc(user.name)}</b></span>
            <button class="btn btn-primary" id="postBtn">Post</button>
          </div>`
          : `<span class="muted">Please <a href="#/login?next=community">log in</a> to join the discussion.</span>`}
      </div>
      <div id="msgList">
        ${msgs.map((m) => `
          <div class="panel" style="padding:1rem;margin-bottom:.75rem">
            <div class="row-between">
              <strong>${esc(m.name)}</strong>
              <span class="muted small">${new Date(m.date).toLocaleString()}</span>
            </div>
            <p style="margin:.4rem 0 0;white-space:pre-wrap">${esc(m.text)}</p>
          </div>`).join('')}
      </div>
      <p class="muted small" style="margin-top:1rem">ℹ️ This is a local demo board (messages are stored in your browser). Enable <b>Giscus</b> in <code>js/config.js</code> for real cross-user discussion. See the README.</p>
    `;

    if (user) {
      const post = () => {
        const ta = host.querySelector('#msgText');
        const text = ta.value.trim();
        if (!text) return;
        const list = load(); list.push({ name: user.name, text, date: new Date().toISOString() }); save(list);
        draw();
        window.Components.toast('Posted!', 'success');
      };
      host.querySelector('#postBtn').onclick = post;
    }
  };
  draw();
}

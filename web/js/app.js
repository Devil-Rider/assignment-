/* ============================================================
   app.js — bootstrap
   ============================================================ */
(function () {
  // Apply the site name from CONFIG everywhere (rename in one place).
  function applyBranding() {
    const s = (window.CONFIG && window.CONFIG.site) || { name: 'SQLQuest', accent: '', logo: '🐘' };
    const brandHtml = s.accent && s.name.endsWith(s.accent)
      ? `${s.name.slice(0, -s.accent.length)}<span class="accent">${s.accent}</span>`
      : s.name;
    document.querySelectorAll('.brand-name').forEach((el) => { el.innerHTML = brandHtml; });
    document.querySelectorAll('.brand-logo').forEach((el) => { el.textContent = s.logo; });
    document.querySelectorAll('.footer-brand').forEach((el) => { el.textContent = s.name; });
    document.title = `${s.name} — ${s.tagline || 'Learn SQL by playing'}`;
  }

  // Best-effort IP block (client-side; bypassable — real blocking needs a backend).
  async function ipGuard() {
    const raw = (localStorage.getItem('sqlquest_ipblock') || '').trim();
    if (!raw) return;
    const blocked = raw.split(/\s+/).filter(Boolean);
    try {
      const res = await fetch('https://api.ipify.org?format=json');
      const { ip } = await res.json();
      if (blocked.includes(ip)) {
        document.body.innerHTML = `<div style="display:grid;place-items:center;min-height:100vh;text-align:center;font-family:Inter,sans-serif;color:#e6e9f2;background:#0f1117"><div><h1>Access blocked</h1><p style="color:#98a0b8">Your access to this site has been restricted.</p></div></div>`;
      }
    } catch { /* offline or blocked request — ignore */ }
  }

  function wireFooterNewsletter() {
    const form = document.getElementById('footerNews');
    if (!form) return;
    form.onsubmit = async (e) => {
      e.preventDefault();
      const email = document.getElementById('footerNewsEmail').value;
      const msg = document.getElementById('footerNewsMsg');
      const res = await window.Newsletter.subscribe(email, '', 'footer');
      if (!res.ok) { msg.style.color = 'var(--red)'; msg.textContent = res.error; return; }
      msg.style.color = 'var(--green)';
      msg.textContent = res.warning || '✓ Subscribed — thank you!';
      document.getElementById('footerNewsEmail').value = '';
    };
  }

  function boot() {
    // Footer year
    const yr = document.getElementById('year');
    if (yr) yr.textContent = new Date().getFullYear();

    applyBranding();

    // Ensure an admin account exists (from CONFIG)
    if (window.Auth.ensureAdmin) window.Auth.ensureAdmin();

    // Best-effort IP guard
    ipGuard();

    // Footer newsletter form
    wireFooterNewsletter();

    // Auth widget in navbar
    window.Auth.renderNav();

    // Theme (persisted)
    const themeBtn = document.getElementById('themeToggle');
    const applyTheme = (t) => {
      document.body.classList.toggle('light', t === 'light');
      if (themeBtn) themeBtn.textContent = t === 'light' ? '☀️' : '🌙';
    };
    applyTheme(localStorage.getItem('sqlquest_theme') || 'dark');
    if (themeBtn) themeBtn.onclick = () => {
      const next = document.body.classList.contains('light') ? 'dark' : 'light';
      localStorage.setItem('sqlquest_theme', next);
      applyTheme(next);
    };

    // Mobile menu toggle
    const hamburger = document.getElementById('hamburger');
    const navLinks = document.getElementById('navLinks');
    const navRight = document.getElementById('navRight');
    if (hamburger) hamburger.onclick = () => {
      navLinks.classList.toggle('open');
      navRight.classList.toggle('open');
    };

    // Warm up the SQL engine in the background (non-blocking)
    if (window.SQLEngine) window.SQLEngine.init().catch(() => {});

    // Start router
    window.Router.start();

    // Render top ad slot
    if (window.Ads) window.Ads.refresh();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();

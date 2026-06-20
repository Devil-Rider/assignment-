/* ============================================================
   app.js — bootstrap
   ============================================================ */
(function () {
  function boot() {
    // Footer year
    const yr = document.getElementById('year');
    if (yr) yr.textContent = new Date().getFullYear();

    // Auth widget in navbar
    window.Auth.renderNav();

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

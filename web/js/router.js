/* ============================================================
   router.js — tiny hash-based SPA router
   ============================================================ */
window.Router = (function () {
  const view = () => document.getElementById('view');

  // route key -> render function (registered by page modules)
  const routes = {
    home: (params) => window.Pages.home(view(), params),
    editor: (params) => window.Pages.editor(view(), params),
    learn: (params) => window.Pages.learn(view(), params),
    leaderboard: (params) => window.Pages.leaderboard(view(), params),
    community: (params) => window.Pages.community(view(), params),
    contact: (params) => window.Pages.contact(view(), params),
    login: (params) => window.Pages.login(view(), params),
    admin: (params) => window.Pages.admin(view(), params),
  };

  function parse() {
    let hash = location.hash.replace(/^#\/?/, '');     // strip "#/"
    let [path, query] = hash.split('?');
    const parts = path.split('/').filter(Boolean);     // e.g. ['learn','m1l2']
    const route = parts[0] || 'home';
    const params = { parts: parts.slice(1), query: {} };
    if (query) query.split('&').forEach((kv) => {
      const [k, v] = kv.split('=');
      params.query[decodeURIComponent(k)] = decodeURIComponent(v || '');
    });
    return { route, params };
  }

  function setActiveNav(route) {
    document.querySelectorAll('.nav-links a').forEach((a) => {
      a.classList.toggle('active', a.dataset.route === route);
    });
  }

  function render() {
    const { route, params } = parse();
    const fn = routes[route] || routes.home;
    setActiveNav(route);
    view().innerHTML = '';
    window.scrollTo(0, 0);
    try {
      fn(params);
    } catch (err) {
      view().innerHTML = `<div class="panel" style="padding:1.5rem"><h2>Something went wrong</h2><pre class="msg err">${(err && err.message) || err}</pre></div>`;
      console.error(err);
    }
    // Fill any ad slots the page added
    if (window.Ads) window.Ads.refresh();
    // close mobile menu
    document.getElementById('navLinks')?.classList.remove('open');
  }

  function start() {
    window.addEventListener('hashchange', render);
    if (!location.hash) location.hash = '#/home';
    else render();
  }

  return { start, refresh: render, parse };
})();

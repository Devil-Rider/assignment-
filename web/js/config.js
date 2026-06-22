/* ============================================================
   config.js — ALL the knobs in one place.
   Change the site name, newsletter endpoint, community widget
   and admin credentials here. Loaded before everything else.
   ============================================================ */
window.CONFIG = {
  site: {
    name: 'SQLQuest',     // <-- change the whole site's name here
    accent: 'Quest',      // the trailing part to highlight in the logo ('' for none)
    logo: '🐘',
    tagline: 'Learn SQL by playing.',
  },

  // ---- Newsletter (Formspree) --------------------------------
  // 1) Create a free form at https://formspree.io  ->  get an
  //    endpoint like https://formspree.io/f/abcdwxyz
  // 2) Paste it below. Until then, signups are stored locally so
  //    the admin can still see/export them.
  newsletter: {
    enabled: true,
    formspreeEndpoint: '', // e.g. 'https://formspree.io/f/xxxxxxxx'
    autoSubscribeOnSignup: true,
  },

  // ---- Community discussion (Giscus, backed by GitHub) -------
  // Enable GitHub Discussions on your repo, install the giscus
  // app (https://giscus.app), then fill these in. Until enabled,
  // a local demo discussion board is shown instead.
  community: {
    enabled: false,
    repo: 'Devil-Rider/assignment-',
    repoId: '',           // from giscus.app
    category: 'General',
    categoryId: '',       // from giscus.app
  },

  // ---- Admin -------------------------------------------------
  // The admin account is auto-created on first load. CHANGE the
  // password before going live. Admins get the Admin panel.
  admin: {
    email: 'admin@sqlquest.app',
    password: 'admin123',
    name: 'Site Admin',
  },
};

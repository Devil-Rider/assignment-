/* ============================================================
   auth.js — Client-side authentication & player progress
   ------------------------------------------------------------
   NOTE: This is a front-end-only auth system backed by
   localStorage, suitable for a static site / demo / MVP. It is
   NOT secure for real secrets — passwords are only lightly
   obfuscated. To productionize, swap these functions for a real
   backend (Firebase Auth, Supabase, Auth0, or your own API).
   The rest of the app only talks to Auth.* so swapping is easy.
   ============================================================ */
window.Auth = (function () {
  const USERS_KEY = 'sqlquest_users';
  const SESSION_KEY = 'sqlquest_session';

  function load(key, fallback) {
    try { return JSON.parse(localStorage.getItem(key)) ?? fallback; }
    catch { return fallback; }
  }
  function save(key, val) { localStorage.setItem(key, JSON.stringify(val)); }

  // Tiny non-cryptographic obfuscation (demo only).
  function obfuscate(str) {
    let h = 0;
    for (let i = 0; i < str.length; i++) { h = (h << 5) - h + str.charCodeAt(i); h |= 0; }
    return 'h' + (h >>> 0).toString(16);
  }

  function getUsers() { return load(USERS_KEY, {}); }

  function currentEmail() { return load(SESSION_KEY, null); }

  function currentUser() {
    const email = currentEmail();
    if (!email) return null;
    const u = getUsers()[email];
    if (!u) return null;
    return { email, name: u.name, xp: u.xp || 0, completed: u.completed || {}, badges: u.badges || [] };
  }

  function isLoggedIn() { return !!currentUser(); }

  // All accounts on this device, ranked for the leaderboard.
  function listPlayers() {
    const users = getUsers();
    const me = currentEmail();
    return Object.keys(users).map((email) => {
      const u = users[email];
      const completed = u.completed || {};
      const lessons = Object.keys(completed).filter((k) => window.COURSE_INDEX && window.COURSE_INDEX.byId[k]).length;
      return {
        email, name: u.name, xp: u.xp || 0,
        badges: (u.badges || []).length,
        lessons,
        level: levelInfo(u.xp || 0).level,
        isMe: email === me,
      };
    }).sort((a, b) => b.xp - a.xp || b.lessons - a.lessons || a.name.localeCompare(b.name));
  }

  function signup(name, email, password) {
    email = (email || '').trim().toLowerCase();
    if (!name || !name.trim()) return { ok: false, error: 'Please enter your name.' };
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return { ok: false, error: 'Please enter a valid email.' };
    if ((password || '').length < 4) return { ok: false, error: 'Password must be at least 4 characters.' };
    const users = getUsers();
    if (users[email]) return { ok: false, error: 'An account with this email already exists.' };
    users[email] = { name: name.trim(), pw: obfuscate(password), xp: 0, completed: {}, badges: [] };
    save(USERS_KEY, users);
    save(SESSION_KEY, email);
    return { ok: true };
  }

  function login(email, password) {
    email = (email || '').trim().toLowerCase();
    const users = getUsers();
    const u = users[email];
    if (!u || u.pw !== obfuscate(password)) return { ok: false, error: 'Invalid email or password.' };
    save(SESSION_KEY, email);
    return { ok: true };
  }

  function logout() { localStorage.removeItem(SESSION_KEY); }

  // ---- Progress / gamification ----
  function markLessonComplete(lessonId, xp) {
    const email = currentEmail();
    if (!email) return null;
    const users = getUsers();
    const u = users[email];
    if (!u.completed) u.completed = {};
    let awarded = 0;
    if (!u.completed[lessonId]) {        // first completion only
      u.completed[lessonId] = true;
      u.xp = (u.xp || 0) + xp;
      awarded = xp;
    }
    save(USERS_KEY, users);
    return { xp: u.xp, awarded };
  }

  function awardBadge(badgeId) {
    const email = currentEmail();
    if (!email) return;
    const users = getUsers();
    const u = users[email];
    if (!u.badges) u.badges = [];
    if (!u.badges.includes(badgeId)) { u.badges.push(badgeId); save(USERS_KEY, users); }
  }

  // Level from XP: simple curve.
  function levelInfo(xp) {
    const level = Math.floor(xp / 100) + 1;
    const into = xp % 100;
    return { level, into, next: 100, pct: into };
  }

  // ---- Render auth widget in navbar ----
  function renderNav() {
    const el = document.getElementById('navRight');
    if (!el) return;
    const u = currentUser();
    if (u) {
      const { level } = levelInfo(u.xp);
      const initials = u.name.split(/\s+/).map(s => s[0]).join('').slice(0, 2).toUpperCase();
      el.innerHTML = `
        <span class="xp-pill" title="Experience points">⭐ ${u.xp} XP · Lv ${level}</span>
        <span class="user-chip">
          <span class="avatar">${initials}</span>
          <span class="small">${u.name.split(/\s+/)[0]}</span>
        </span>
        <button class="btn btn-sm btn-ghost" id="logoutBtn">Logout</button>`;
      el.querySelector('#logoutBtn').onclick = () => {
        logout(); renderNav();
        if (location.hash.startsWith('#/learn')) location.hash = '#/home';
        else if (window.Router) window.Router.refresh();
      };
    } else {
      el.innerHTML = `
        <a class="btn btn-sm btn-ghost" href="#/login">Log in</a>
        <a class="btn btn-sm btn-primary" href="#/login?mode=signup">Sign up</a>`;
    }
  }

  return {
    signup, login, logout, currentUser, isLoggedIn,
    markLessonComplete, awardBadge, levelInfo, renderNav,
  };
})();

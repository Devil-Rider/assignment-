/* ============================================================
   login.js — login / signup (client-side auth)
   Query params: ?mode=signup  ?next=learn
   ============================================================ */
window.Pages = window.Pages || {};
window.Pages.login = function (view, params) {
  // already logged in? bounce to next/home
  if (window.Auth.isLoggedIn()) {
    location.hash = '#/' + (params.query.next || 'learn');
    return;
  }

  let mode = params.query.mode === 'signup' ? 'signup' : 'login';
  const next = params.query.next || 'learn';

  function draw() {
    const isSignup = mode === 'signup';
    view.innerHTML = `
      <div class="auth-wrap panel">
        <h2>${isSignup ? 'Create your free account' : 'Welcome back'}</h2>
        <p class="muted" style="margin-top:0">${isSignup ? 'Track your XP, levels and badges across the SQL course.' : 'Log in to continue your SQL quest.'}</p>
        <form id="authForm">
          ${isSignup ? `<div class="field"><label>Name</label><input id="aName" type="text" placeholder="Ada Lovelace" autocomplete="name"></div>` : ''}
          <div class="field"><label>Email</label><input id="aEmail" type="email" placeholder="you@example.com" autocomplete="email"></div>
          <div class="field"><label>Password</label><input id="aPass" type="password" placeholder="••••••" autocomplete="${isSignup ? 'new-password' : 'current-password'}"></div>
          <div class="form-err" id="aErr"></div>
          <button class="btn btn-primary" type="submit" style="width:100%">${isSignup ? 'Sign up & start' : 'Log in'}</button>
        </form>
        <div class="auth-switch">
          ${isSignup
            ? `Already have an account? <a href="#" id="toggleMode">Log in</a>`
            : `New here? <a href="#" id="toggleMode">Create an account</a>`}
        </div>
        <p class="muted small" style="text-align:center;margin-top:1rem">Accounts are stored locally in your browser for this demo.</p>
      </div>
    `;

    view.querySelector('#toggleMode').onclick = (e) => { e.preventDefault(); mode = isSignup ? 'login' : 'signup'; draw(); };

    view.querySelector('#authForm').onsubmit = (e) => {
      e.preventDefault();
      const err = view.querySelector('#aErr');
      const email = view.querySelector('#aEmail').value;
      const pass = view.querySelector('#aPass').value;
      let res;
      if (isSignup) {
        const name = view.querySelector('#aName').value;
        res = window.Auth.signup(name, email, pass);
      } else {
        res = window.Auth.login(email, pass);
      }
      if (!res.ok) { err.textContent = res.error; return; }
      window.Auth.renderNav();
      window.Components.toast(isSignup ? '🎉 Account created — let\'s go!' : '👋 Welcome back!', 'success');
      location.hash = '#/' + next;
    };
  }

  draw();
};

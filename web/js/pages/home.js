/* ============================================================
   home.js — landing page
   ============================================================ */
window.Pages = window.Pages || {};
window.Pages.home = function (view) {
  const total = window.COURSE_INDEX.total;
  view.innerHTML = `
    <section class="hero">
      <div>
        <h1>Learn SQL by <span class="grad">playing</span>.</h1>
        <p class="lead">Write real SQL right in your browser, level up through a gamified course, and go from <b>SELECT *</b> to window functions — no installs, no setup.</p>
        <div class="hero-cta">
          <a class="btn btn-primary" href="#/editor">⚡ Open SQL Editor</a>
          <a class="btn" href="#/learn">🎮 Start Learning</a>
        </div>
        <div class="tag-row">
          <span class="chip">Runs 100% in-browser</span>
          <span class="chip">Free forever</span>
          <span class="chip">${total} hands-on lessons</span>
        </div>
      </div>
      <div class="hero-card">
        <div class="cm-line"><span class="kw">SELECT</span> name, <span class="fn">SUM</span>(price * quantity) <span class="kw">AS</span> revenue</div>
        <div class="cm-line"><span class="kw">FROM</span> order_items oi</div>
        <div class="cm-line"><span class="kw">JOIN</span> products p <span class="kw">ON</span> p.id = oi.product_id</div>
        <div class="cm-line"><span class="kw">GROUP BY</span> name</div>
        <div class="cm-line"><span class="kw">ORDER BY</span> revenue <span class="kw">DESC</span>;</div>
        <div class="cm-line" style="margin-top:.6rem;color:#3ddc84">▸ 10 rows · 1.2 ms</div>
      </div>
    </section>

    <div class="stats-row">
      <div class="stat"><div class="num">6</div><div class="lbl">Worlds, beginner → advanced</div></div>
      <div class="stat"><div class="num">${total}</div><div class="lbl">Interactive challenges</div></div>
      <div class="stat"><div class="num">∞</div><div class="lbl">Free SQL queries</div></div>
      <div class="stat"><div class="num">XP</div><div class="lbl">Earn points & badges</div></div>
    </div>

    ${window.Ads.slot('inline', 'ad-inline')}

    <h2 class="section-title">Why SQLQuest?</h2>
    <div class="features">
      <div class="feature"><div class="ico">⚡</div><h3>Real in-browser SQL</h3><p>Powered by SQLite compiled to WebAssembly. Your queries actually run — instantly and privately.</p></div>
      <div class="feature"><div class="ico">🎮</div><h3>Gamified course</h3><p>Earn XP, level up, and collect badges as you master each SQL concept through bite-sized challenges.</p></div>
      <div class="feature"><div class="ico">📈</div><h3>Beginner to advanced</h3><p>Start with <code>SELECT</code> and finish on window functions, CTEs and self-joins. A complete path.</p></div>
      <div class="feature"><div class="ico">🧪</div><h3>Auto-graded</h3><p>Every challenge checks your result set automatically and tells you the moment you nail it.</p></div>
      <div class="feature"><div class="ico">🔒</div><h3>Private by design</h3><p>Nothing leaves your device. The database lives entirely in your browser tab.</p></div>
      <div class="feature"><div class="ico">💸</div><h3>Free to use</h3><p>The editor and the full course are free. Ads keep the lights on so learning stays open to all.</p></div>
    </div>

    <div class="panel" style="padding:2rem;margin-top:1.5rem;text-align:center">
      <h2 style="margin-top:0">Ready to write your first query?</h2>
      <p class="muted">Jump straight into the editor, or take the guided path through the course.</p>
      <div class="hero-cta" style="justify-content:center">
        <a class="btn btn-primary" href="#/editor">Open SQL Editor</a>
        <a class="btn" href="#/learn">Browse the course</a>
      </div>
    </div>
  `;
};

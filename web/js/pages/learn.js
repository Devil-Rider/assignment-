/* ============================================================
   learn.js — gamified course (login-gated)
   Routes:
     #/learn            -> course map
     #/learn/<lessonId> -> a single lesson challenge
   ============================================================ */
window.Pages = window.Pages || {};

window.Pages.learn = function (view, params) {
  const user = window.Auth.currentUser();

  // ---- Gate: must be logged in ----
  if (!user) {
    view.innerHTML = `
      <div class="gate panel">
        <div class="lock">🔒</div>
        <h2>Log in to start the course</h2>
        <p class="muted">The gamified course tracks your XP, levels and badges — so it needs an account. The <a href="#/editor">SQL Editor</a> is free to use without logging in.</p>
        <div class="hero-cta" style="justify-content:center;margin-top:1rem">
          <a class="btn btn-primary" href="#/login?mode=signup&next=learn">Create free account</a>
          <a class="btn" href="#/login?next=learn">Log in</a>
        </div>
      </div>`;
    return;
  }

  const lessonId = params.parts[0];
  if (lessonId && window.COURSE_INDEX.byId[lessonId]) {
    renderLesson(view, lessonId, user);
  } else {
    renderMap(view, user);
  }
};

// Is a lesson unlocked? First lesson always; otherwise previous must be done.
function isUnlocked(lessonId, completed) {
  const idx = window.COURSE_INDEX.byId[lessonId].order;
  if (idx === 0) return true;
  const prev = window.COURSE_INDEX.lessons[idx - 1];
  return !!completed[prev.id];
}

/* ---------------- COURSE MAP ---------------- */
function renderMap(view, user) {
  const completed = user.completed || {};
  const doneCount = Object.keys(completed).filter((k) => window.COURSE_INDEX.byId[k]).length;
  const total = window.COURSE_INDEX.total;
  const pct = Math.round((doneCount / total) * 100);
  const { level } = window.Auth.levelInfo(user.xp);

  const badgesHtml = window.BADGES.map((b) => {
    const earned = (user.badges || []).includes(b.id);
    return `<div class="badge ${earned ? 'earned' : ''}" title="${b.name}${earned ? ' (earned)' : ' (locked)'}">${b.icon}</div>`;
  }).join('');

  const modulesHtml = window.COURSE.map((m) => {
    const lessons = m.lessons.map((l) => {
      const done = !!completed[l.id];
      const unlocked = isUnlocked(l.id, completed);
      const state = done ? '✅' : (unlocked ? '' : '🔒');
      const cls = done ? 'done' : (unlocked ? '' : 'locked');
      const href = unlocked ? `#/learn/${l.id}` : '#';
      return `<a class="lesson-card ${cls}" href="${href}" ${unlocked ? '' : 'onclick="return false"'}>
          <span class="lstate">${state}</span>
          <div class="lnum">Lesson</div>
          <h4>${l.title}</h4>
          <span class="ltag">${l.tag}</span>
          <div class="tag-row"><span class="chip">⭐ ${l.xp} XP</span></div>
        </a>`;
    }).join('');
    const mdone = m.lessons.every((l) => completed[l.id]);
    return `<section class="module">
        <div class="module-head">
          <div class="mico">${m.icon}</div>
          <div><h3>${m.title} ${mdone ? '<span class="chip" style="color:var(--gold)">Complete</span>' : ''}</h3>
          <div class="mmeta">${m.subtitle}</div></div>
        </div>
        <div class="lesson-grid">${lessons}</div>
      </section>`;
  }).join(window.Ads.slot('inline', 'ad-inline'));

  view.innerHTML = `
    <div class="learn-header">
      <div>
        <h1 class="section-title">🎮 SQL Quest — Your Journey</h1>
        <p class="muted" style="margin:.25rem 0 0">Complete challenges to earn XP and unlock the next level.</p>
      </div>
      <div class="progress-ring">
        <div>
          <div class="muted small">Level ${level} · ${user.xp} XP</div>
          <div class="bigbar"><span style="width:${pct}%"></span></div>
          <div class="muted small" style="margin-top:.3rem">${doneCount}/${total} lessons · ${pct}% complete</div>
        </div>
      </div>
    </div>

    <div class="panel" style="padding:1rem 1.25rem;margin-bottom:1.5rem">
      <div class="row-between">
        <strong>🏅 Badges</strong>
        <span class="muted small">Earn a badge for finishing each world</span>
      </div>
      <div class="badges" style="margin-top:.75rem">${badgesHtml}</div>
    </div>

    ${modulesHtml}
  `;
}

/* ---------------- LESSON VIEW ---------------- */
function renderLesson(view, lessonId, user) {
  const lesson = window.COURSE_INDEX.byId[lessonId];
  const completed = user.completed || {};

  if (!isUnlocked(lessonId, completed)) {
    view.innerHTML = `<div class="gate panel"><div class="lock">🔒</div><h2>Locked</h2>
      <p class="muted">Finish the previous lesson to unlock this one.</p>
      <a class="btn btn-primary" href="#/learn">Back to course</a></div>`;
    return;
  }

  const idx = lesson.order;
  const next = window.COURSE_INDEX.lessons[idx + 1];
  const alreadyDone = !!completed[lessonId];

  view.innerHTML = `
    <div class="row-between" style="margin-bottom:1rem">
      <a class="btn btn-sm btn-ghost" href="#/learn">← Course</a>
      <span class="muted small">Lesson ${idx + 1} of ${window.COURSE_INDEX.total} · ${lesson.moduleTitle}</span>
    </div>

    <div class="lesson-layout">
      <aside class="lesson-theory panel">
        <span class="pill">${lesson.tag}</span>
        <h2>${lesson.title}</h2>
        ${lesson.theory}
        <div class="task-box">
          <div class="task-label">🎯 Your Task</div>
          <p style="margin:.4rem 0 0">${lesson.task}</p>
        </div>
        <div class="tag-row"><span class="chip">⭐ ${lesson.xp} XP</span>${alreadyDone ? '<span class="chip" style="color:var(--green)">✓ Completed</span>' : ''}</div>
      </aside>

      <div class="lesson-work">
        <div class="editor-toolbar">
          <button class="btn btn-green" id="checkBtn">✓ Check Answer <span class="muted small">(Ctrl/⌘+Enter)</span></button>
          <button class="btn btn-sm" id="runOnlyBtn">▶ Run</button>
          <button class="btn btn-sm" id="hintBtn">💡 Solution</button>
          <span class="spacer"></span>
          <span class="muted small" id="engineStatus">Loading…</span>
        </div>
        <div id="cmHost"></div>
        <div id="feedbackHost"></div>
        <div id="resultHost"></div>
      </div>
    </div>

    ${window.Ads.slot('inline', 'ad-inline')}
  `;

  const editor = window.Components.codeEditor(view.querySelector('#cmHost'), lesson.starter || '');
  const resultHost = view.querySelector('#resultHost');
  const feedbackHost = view.querySelector('#feedbackHost');
  const status = view.querySelector('#engineStatus');

  (async function ready() {
    try { await window.SQLEngine.init(); status.textContent = '● Ready'; status.style.color = 'var(--green)'; }
    catch (err) { status.textContent = '● Engine failed'; status.style.color = 'var(--red)'; resultHost.innerHTML = window.Components.errorBox(err.message); }
  })();

  function runUser() {
    const sql = editor.getValue().trim();
    if (!sql) return null;
    window.SQLEngine.reset();           // fresh DB every attempt
    return window.SQLEngine.run(sql);
  }

  view.querySelector('#runOnlyBtn').onclick = async () => {
    try {
      await window.SQLEngine.init();
      const out = runUser();
      if (out) resultHost.innerHTML = window.Components.renderResult(out.results, out.elapsedMs);
    } catch (err) { resultHost.innerHTML = window.Components.errorBox(err.message); }
  };

  view.querySelector('#hintBtn').onclick = () => {
    feedbackHost.innerHTML = `<div class="panel" style="padding:1rem;border-left:3px solid var(--gold)">
      <strong>Reference solution</strong>
      <pre style="background:#282a36;padding:.7rem;border-radius:8px;margin:.5rem 0 0;overflow:auto;font-family:'JetBrains Mono',monospace;font-size:.82rem;color:#f8f8f2">${window.Components.escapeHtml(lesson.solution)}</pre>
      <p class="muted small" style="margin:.5rem 0 0">You can copy this, but try it yourself first — that's how it sticks!</p>
    </div>`;
  };

  async function check() {
    try {
      await window.SQLEngine.init();
      const sql = editor.getValue().trim();
      if (!sql) { feedbackHost.innerHTML = fb('Write a query first.', 'err'); return; }

      // Run user query
      let userOut;
      try { userOut = runUser(); }
      catch (err) { resultHost.innerHTML = window.Components.errorBox(err.message); feedbackHost.innerHTML = fb('Your SQL has an error — see below.', 'err'); return; }

      // Run reference solution on a fresh DB
      window.SQLEngine.reset();
      const solOut = window.SQLEngine.run(lesson.solution);

      resultHost.innerHTML = window.Components.renderResult(userOut.results, userOut.elapsedMs);

      const userRes = userOut.results[userOut.results.length - 1];
      const solRes = solOut.results[solOut.results.length - 1];

      if (window.Components.resultsMatch(userRes, solRes, lesson.ordered)) {
        onSolved(lesson);
      } else {
        feedbackHost.innerHTML = fb('Not quite — your result doesn\'t match the expected output. Compare the columns and rows, then tweak your query. (Try 💡 Solution if stuck.)', 'err');
      }
    } catch (err) {
      resultHost.innerHTML = window.Components.errorBox(err.message);
    }
  }

  function onSolved(lesson) {
    const res = window.Auth.markLessonComplete(lesson.id, lesson.xp);
    // Badge if module now complete
    const mod = window.COURSE.find((m) => m.lessons.some((l) => l.id === lesson.id));
    const fresh = window.Auth.currentUser();
    const modDone = mod.lessons.every((l) => fresh.completed[l.id]);
    if (modDone) window.Auth.awardBadge(mod.id);
    window.Auth.renderNav();

    const xpMsg = res && res.awarded > 0 ? `+${res.awarded} XP earned!` : 'Already completed — nice review!';
    window.Components.toast(`🎉 Correct! ${xpMsg}`, 'success xp');

    feedbackHost.innerHTML = `<div class="panel" style="padding:1.25rem;border-left:3px solid var(--green)">
      <h3 style="margin:0">✅ Correct!</h3>
      <p class="muted" style="margin:.4rem 0 .8rem">${res && res.awarded > 0 ? `You earned <b style="color:var(--gold)">${res.awarded} XP</b>.` : 'You\'ve already completed this one.'} ${modDone ? `🏅 World complete — badge unlocked!` : ''}</p>
      <div class="hero-cta">
        ${next ? `<a class="btn btn-primary" href="#/learn/${next.id}">Next lesson →</a>` : `<a class="btn btn-primary" href="#/learn">🏆 Back to course</a>`}
        <a class="btn" href="#/learn">Course map</a>
      </div>
    </div>`;
  }

  function fb(msg, kind) {
    return `<div class="panel" style="padding:.9rem 1rem;border-left:3px solid var(--${kind === 'err' ? 'red' : 'green'})"><span class="${kind === 'err' ? 'msg err' : 'msg ok'}" style="padding:0">${msg}</span></div>`;
  }

  view.querySelector('#checkBtn').onclick = check;
  // bound to a page-local node so it's discarded on route change
  view.querySelector('.lesson-work').addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); check(); }
  });
}

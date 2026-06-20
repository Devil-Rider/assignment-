/* ============================================================
   components.js — shared UI pieces used by multiple pages:
   a SQL code editor (CodeMirror with <textarea> fallback),
   a result-table renderer, and a toast.
   ============================================================ */
window.Components = (function () {

  /* Mount a SQL editor into `container`. Returns an object with
     getValue()/setValue(). Falls back to a plain textarea if
     CodeMirror failed to load from the CDN. */
  function codeEditor(container, initial) {
    container.classList.add('code-area');
    if (window.CodeMirror) {
      const cm = CodeMirror(container, {
        value: initial || '',
        mode: 'text/x-sql',
        theme: 'dracula',
        lineNumbers: true,
        indentUnit: 2,
        smartIndent: true,
        matchBrackets: true,
      });
      // refresh after layout settles
      setTimeout(() => cm.refresh(), 30);
      return { getValue: () => cm.getValue(), setValue: (v) => cm.setValue(v), cm };
    }
    const ta = document.createElement('textarea');
    ta.className = 'code-fallback';
    ta.value = initial || '';
    ta.spellcheck = false;
    container.appendChild(ta);
    return { getValue: () => ta.value, setValue: (v) => { ta.value = v; }, ta };
  }

  /* Render sql.js result objects into HTML. `results` is the
     array returned by db.exec(). */
  function renderResult(results, elapsedMs) {
    if (!results || results.length === 0) {
      return `<div class="result-panel panel">
        <div class="result-head"><span class="meta">Statement executed</span>
        <span class="meta">${elapsedMs ?? 0} ms</span></div>
        <div class="msg ok">✓ Success. No rows returned.</div></div>`;
    }
    // Render the LAST result set (typical for multi-statement scripts)
    const r = results[results.length - 1];
    const head = r.columns.map((c) => `<th>${escapeHtml(c)}</th>`).join('');
    const body = r.values.map((row) =>
      `<tr>${row.map((cell) => `<td>${cell === null ? '<span class="muted">NULL</span>' : escapeHtml(String(cell))}</td>`).join('')}</tr>`
    ).join('');
    return `<div class="result-panel panel">
      <div class="result-head">
        <span class="meta">${r.values.length} row${r.values.length === 1 ? '' : 's'} · ${r.columns.length} column${r.columns.length === 1 ? '' : 's'}</span>
        <span class="meta">${elapsedMs ?? 0} ms</span>
      </div>
      <div class="table-scroll"><table class="result"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>
    </div>`;
  }

  function errorBox(message) {
    return `<div class="result-panel panel">
      <div class="result-head"><span class="meta">Error</span></div>
      <div class="msg err">✗ ${escapeHtml(message)}</div></div>`;
  }

  let toastTimer = null;
  function toast(msg, kind = '') {
    let t = document.getElementById('toast');
    if (!t) { t = document.createElement('div'); t.id = 'toast'; t.className = 'toast'; document.body.appendChild(t); }
    t.className = 'toast ' + kind;
    t.innerHTML = msg;
    // force reflow then show
    void t.offsetWidth;
    t.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.classList.remove('show'), 2600);
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  /* Compare two sql.js result sets for the Learn validator.
     ordered=false sorts rows before comparing. */
  function resultsMatch(a, b, ordered) {
    if (!a || !b) return false;
    if (a.columns.length !== b.columns.length) return false;
    if (a.values.length !== b.values.length) return false;
    const norm = (rows) => rows.map((r) => r.map((c) => (c === null ? '∅' : String(c))));
    let av = norm(a.values), bv = norm(b.values);
    if (!ordered) {
      const key = (r) => r.join('');
      av = av.map(key).sort();
      bv = bv.map(key).sort();
      return av.every((v, i) => v === bv[i]);
    }
    return av.every((r, i) => r.length === bv[i].length && r.every((c, j) => c === bv[i][j]));
  }

  return { codeEditor, renderResult, errorBox, toast, escapeHtml, resultsMatch };
})();

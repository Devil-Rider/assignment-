/* ============================================================
   editor.js — public SQL Editor (no login required)
   ============================================================ */
window.Pages = window.Pages || {};
window.Pages.editor = function (view) {
  view.innerHTML = `
    <div class="row-between" style="margin-bottom:1rem">
      <div>
        <h1 class="section-title">SQL Editor</h1>
        <p class="muted" style="margin:.25rem 0 0">Run SQL against a sample shop database. 100% in your browser — free for everyone.</p>
      </div>
    </div>

    <div class="editor-wrap">
      <aside class="schema-rail panel">
        <h3>📁 Tables</h3>
        <div id="schemaList"></div>
        <hr style="border-color:var(--border);margin:1rem 0">
        <h3>💡 Try</h3>
        <ul class="cols" style="list-style:none;padding:0;margin:0;font-size:.8rem;line-height:1.7">
          <li><a href="#" data-snippet="SELECT * FROM products;">All products</a></li>
          <li><a href="#" data-snippet="SELECT category, COUNT(*) AS n\nFROM products GROUP BY category;">Count by category</a></li>
          <li><a href="#" data-snippet="SELECT c.name, COUNT(o.id) AS orders\nFROM customers c\nLEFT JOIN orders o ON o.customer_id=c.id\nGROUP BY c.name\nORDER BY orders DESC;">Orders per customer</a></li>
        </ul>
      </aside>

      <div class="editor-main">
        <div class="editor-toolbar">
          <button class="btn btn-green" id="runBtn">▶ Run <span class="muted small">(Ctrl/⌘+Enter)</span></button>
          <button class="btn btn-sm" id="resetDbBtn" title="Restore the sample data">↺ Reset DB</button>
          <button class="btn btn-sm" id="clearBtn">Clear</button>
          <span class="spacer"></span>
          <span class="muted small" id="engineStatus">Loading SQL engine…</span>
        </div>
        <div id="cmHost"></div>
        <div id="resultHost"><div class="panel" style="padding:1rem"><span class="muted">Run a query to see results.</span></div></div>
        ${window.Ads.slot('inline', 'ad-inline')}
      </div>
    </div>
  `;

  // Render schema sidebar
  const schemaList = view.querySelector('#schemaList');
  schemaList.innerHTML = window.SQLEngine.getSchema().map((t) => `
    <div class="schema-table">
      <div class="tname" data-table="${t.name}">▸ ${t.name}</div>
      <ul class="cols">${t.columns.map((c) => {
        const isPk = /PK/.test(c);
        return `<li class="${isPk ? 'pk' : ''}">${c.replace(/ (PK|FK)/, m => ` <span class="muted">${m.trim()}</span>`)}</li>`;
      }).join('')}</ul>
    </div>`).join('');

  // Mount editor
  const editor = window.Components.codeEditor(view.querySelector('#cmHost'), 'SELECT * FROM products;');
  const resultHost = view.querySelector('#resultHost');
  const status = view.querySelector('#engineStatus');

  // Clicking a table name inserts a SELECT
  schemaList.querySelectorAll('.tname').forEach((el) => {
    el.onclick = () => editor.setValue(`SELECT * FROM ${el.dataset.table};`);
  });
  view.querySelectorAll('[data-snippet]').forEach((a) => {
    a.onclick = (e) => { e.preventDefault(); editor.setValue(a.dataset.snippet); };
  });

  async function ensureEngine() {
    try {
      status.textContent = 'Loading SQL engine…';
      await window.SQLEngine.init();
      status.textContent = '● Engine ready';
      status.style.color = 'var(--green)';
      view.querySelector('#runBtn').disabled = false;
    } catch (err) {
      status.textContent = '● Engine failed to load';
      status.style.color = 'var(--red)';
      resultHost.innerHTML = window.Components.errorBox(err.message);
    }
  }

  async function run() {
    try {
      await window.SQLEngine.init();
      const sql = editor.getValue().trim();
      if (!sql) return;
      const { results, elapsedMs } = window.SQLEngine.run(sql);
      resultHost.innerHTML = window.Components.renderResult(results, elapsedMs);
    } catch (err) {
      resultHost.innerHTML = window.Components.errorBox(err.message);
    }
  }

  view.querySelector('#runBtn').onclick = run;
  view.querySelector('#clearBtn').onclick = () => { editor.setValue(''); };
  view.querySelector('#resetDbBtn').onclick = async () => {
    await window.SQLEngine.init();
    window.SQLEngine.reset();
    window.Components.toast('Database reset to sample data', 'success');
  };

  // Keyboard shortcut — bound to a page-local node so it's discarded on route change
  view.querySelector('.editor-main').addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); }
  });

  ensureEngine();
};

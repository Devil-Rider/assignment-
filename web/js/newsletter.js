/* ============================================================
   newsletter.js — email collection + newsletter subscription.
   Posts to Formspree if configured (so emails reach YOU), and
   always keeps a local copy so the admin panel can list/export.
   ============================================================ */
window.Newsletter = (function () {
  const KEY = 'sqlquest_subscribers';

  function load() { try { return JSON.parse(localStorage.getItem(KEY)) || []; } catch { return []; } }
  function save(list) { localStorage.setItem(KEY, JSON.stringify(list)); }

  function list() { return load(); }

  function isValidEmail(e) { return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(e || ''); }

  // Store locally (dedup by email)
  function storeLocal(email, name, source) {
    const subs = load();
    email = email.trim().toLowerCase();
    if (!subs.some((s) => s.email === email)) {
      subs.push({ email, name: name || '', source: source || 'form', date: new Date().toISOString() });
      save(subs);
    }
  }

  // Returns a promise. Always stores locally; also POSTs to
  // Formspree when an endpoint is configured.
  async function subscribe(email, name, source) {
    const cfg = window.CONFIG.newsletter;
    if (!cfg.enabled) return { ok: false, error: 'Newsletter is disabled.' };
    if (!isValidEmail(email)) return { ok: false, error: 'Please enter a valid email.' };

    storeLocal(email, name, source);

    if (cfg.formspreeEndpoint) {
      try {
        const res = await fetch(cfg.formspreeEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ email, name: name || '', _subject: 'New SQLQuest newsletter signup', source: source || 'form' }),
        });
        if (!res.ok) return { ok: true, warning: 'Saved locally, but the email service returned an error.' };
        return { ok: true, remote: true };
      } catch {
        return { ok: true, warning: 'Saved locally; could not reach the email service (offline?).' };
      }
    }
    return { ok: true, remote: false };
  }

  function exportCsv() {
    const subs = load();
    const rows = [['email', 'name', 'source', 'date'], ...subs.map((s) => [s.email, s.name, s.source, s.date])];
    return rows.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
  }

  function remove(email) { save(load().filter((s) => s.email !== email)); }

  return { subscribe, list, exportCsv, remove, isValidEmail, storeLocal };
})();

/* ============================================================
   ads.js — Monetization layer
   ------------------------------------------------------------
   This is where your earnings come from. The site reserves
   dedicated ad slots. By default they render a labelled
   placeholder so the layout is correct. To go live:

   1. Create a Google AdSense account: https://adsense.google.com
   2. Put your publisher id below (ca-pub-XXXXXXXXXXXXXXXX).
   3. Uncomment the adsbygoogle <script> in index.html.
   4. Create ad units in AdSense and map their slot ids in
      AD_SLOTS below.

   Until ADSENSE.enabled is true, placeholders are shown so the
   site is fully usable and you can demo it without ads.
   ============================================================ */
window.Ads = (function () {
  const ADSENSE = {
    enabled: false,                         // flip to true when live
    client: 'ca-pub-XXXXXXXXXXXXXXXX',       // <-- your publisher id
  };

  // Map logical slot -> AdSense ad-unit slot id
  const AD_SLOTS = {
    top: '0000000001',     // leaderboard 728x90
    inline: '0000000002',  // in-content
    rail: '0000000003',    // sidebar / rail 300x250
  };

  // Render all ad slots currently on the page.
  function refresh() {
    document.querySelectorAll('.ad-slot[data-ad]').forEach((el) => {
      if (el.dataset.rendered === '1') return;
      const kind = el.dataset.ad;
      if (ADSENSE.enabled) {
        el.innerHTML = '';
        const ins = document.createElement('ins');
        ins.className = 'adsbygoogle';
        ins.style.display = 'block';
        ins.setAttribute('data-ad-client', ADSENSE.client);
        ins.setAttribute('data-ad-slot', AD_SLOTS[kind] || AD_SLOTS.inline);
        ins.setAttribute('data-ad-format', 'auto');
        ins.setAttribute('data-full-width-responsive', 'true');
        el.appendChild(ins);
        try { (window.adsbygoogle = window.adsbygoogle || []).push({}); } catch (e) {}
      } else {
        el.innerHTML = `<span style="opacity:.6;font-size:.85rem">Ad space · ${kind}</span>`;
      }
      el.dataset.rendered = '1';
    });
  }

  // Build a fresh ad slot element of a given kind/class.
  function slot(kind, extraClass = 'ad-inline') {
    return `<div class="ad-slot ${extraClass}" data-ad="${kind}"></div>`;
  }

  return { refresh, slot, config: ADSENSE };
})();

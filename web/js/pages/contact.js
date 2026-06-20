/* ============================================================
   contact.js — Contact Us page with basic details
   ============================================================ */
window.Pages = window.Pages || {};
window.Pages.contact = function (view) {
  view.innerHTML = `
    <h1 class="section-title">Contact Us</h1>
    <p class="muted" style="margin:.25rem 0 1.5rem">Questions, feedback, partnership or advertising enquiries — we'd love to hear from you.</p>

    <div class="contact-grid">
      <div class="contact-info panel">
        <h3 style="margin-top:0">Get in touch</h3>
        <div class="row"><span class="ico">📧</span><div><strong>Email</strong><br><a href="mailto:hello@sqlquest.app">hello@sqlquest.app</a></div></div>
        <div class="row"><span class="ico">💬</span><div><strong>Support</strong><br><a href="mailto:support@sqlquest.app">support@sqlquest.app</a></div></div>
        <div class="row"><span class="ico">📣</span><div><strong>Advertising</strong><br><a href="mailto:ads@sqlquest.app">ads@sqlquest.app</a></div></div>
        <div class="row"><span class="ico">📍</span><div><strong>Address</strong><br>SQLQuest, Bengaluru, India</div></div>
        <div class="row"><span class="ico">🕘</span><div><strong>Hours</strong><br>Mon–Fri, 9:00–18:00 IST</div></div>
        <div class="tag-row" style="margin-top:1rem">
          <a class="chip" href="#" onclick="return false">Twitter / X</a>
          <a class="chip" href="#" onclick="return false">GitHub</a>
          <a class="chip" href="#" onclick="return false">LinkedIn</a>
        </div>
      </div>

      <div class="contact-form panel">
        <h3 style="margin-top:0">Send a message</h3>
        <form id="contactForm">
          <div class="field"><label>Name</label><input type="text" id="cName" required placeholder="Your name" /></div>
          <div class="field"><label>Email</label><input type="email" id="cEmail" required placeholder="you@example.com" /></div>
          <div class="field"><label>Subject</label><input type="text" id="cSubject" placeholder="How can we help?" /></div>
          <div class="field"><label>Message</label><textarea class="ta" id="cMessage" required placeholder="Write your message…"></textarea></div>
          <div class="form-err" id="cErr"></div>
          <button class="btn btn-primary" type="submit" style="width:100%">Send message</button>
          <p class="muted small" style="margin:.75rem 0 0">This demo form opens your email client. Wire it to a service like Formspree or your own API to receive messages.</p>
        </form>
      </div>
    </div>

    ${window.Ads.slot('inline', 'ad-inline')}
  `;

  view.querySelector('#contactForm').onsubmit = (e) => {
    e.preventDefault();
    const name = view.querySelector('#cName').value.trim();
    const email = view.querySelector('#cEmail').value.trim();
    const subject = view.querySelector('#cSubject').value.trim() || 'SQLQuest enquiry';
    const message = view.querySelector('#cMessage').value.trim();
    const err = view.querySelector('#cErr');
    if (!name || !email || !message) { err.textContent = 'Please fill in name, email and message.'; return; }
    err.textContent = '';
    const body = encodeURIComponent(`From: ${name} <${email}>\n\n${message}`);
    window.location.href = `mailto:hello@sqlquest.app?subject=${encodeURIComponent(subject)}&body=${body}`;
    window.Components.toast('Opening your email client…', 'success');
  };
};

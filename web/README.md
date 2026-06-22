# 🐘 SQLQuest — Learn SQL by Playing

A free, **gamified SQL learning platform** with an **in-browser SQL editor**.
Everything runs 100% client-side (SQLite compiled to WebAssembly via
[sql.js](https://sql.js.org)) — there is **no backend to host**, so you can
deploy it as static files anywhere and monetize it with ads.

## ✨ Features

| Area | What it does | Login required? |
|------|--------------|-----------------|
| **SQL Editor** (`#/editor`) | Write and run real SQL against a sample shop database, right in the browser. Schema sidebar, Run (Ctrl/⌘+Enter), reset DB. | ❌ Open to everyone |
| **Learn** (`#/learn`) | A gamified course from beginner → advanced. 6 "worlds", 25 auto-graded challenges, XP, levels, badges, and locked/unlocked progression. | ✅ Yes |
| **Leaderboard** (`#/leaderboard`) | Ranks all players by XP, level, lessons completed & badges. Top 5 also featured on the home page. | ❌ |
| **Community** (`#/community`) | Discussion board. Real cross-user discussion via Giscus when configured; local demo board otherwise. | ❌ (post: ✅) |
| **Contact** (`#/contact`) | Contact-us page with details and a message form. | ❌ |
| **Login / Sign up** (`#/login`) | Client-side accounts with newsletter opt-in (auto-subscribe). | — |
| **Admin** (`#/admin`) | Admin-only panel: ban/unban/delete users, view & export newsletter list, IP blocklist. | 🛡️ Admin |
| **Newsletter** | Email capture on signup + footer/home forms, sent to Formspree, with local backup. | — |
| **Ads** | Dedicated, AdSense-ready ad slots throughout the site. **This is the revenue source.** | — |

### 🔧 Everything configurable lives in `js/config.js`

```js
site:       { name, accent, logo, tagline }   // rename the whole site in ONE place
newsletter: { enabled, formspreeEndpoint }    // paste your Formspree form URL
community:  { enabled, repo, repoId, ... }     // Giscus / GitHub Discussions
admin:      { email, password, name }          // CHANGE the password before going live
```

**Rename the site:** change `site.name` (and optionally `site.accent`, the
highlighted trailing part of the logo). The navbar, footer and page title all
update automatically.

**Collect newsletter emails for real:** create a free form at
[formspree.io](https://formspree.io), paste the endpoint into
`newsletter.formspreeEndpoint`. New signups (and footer/home subscribers) are
POSTed there **and** kept locally so the admin panel can list/export them as CSV.

**Real community discussion:** enable GitHub Discussions on your repo, set up
[giscus.app](https://giscus.app), fill in `community.*` and set `enabled: true`.

**Admin login:** an admin account is auto-created from `config.admin` on first
load (default `admin@sqlquest.app` / `admin123` — **change it!**). Logging in as
admin reveals the 🛡️ Admin panel.

> ⚠️ **Honest limits of a static site:** user bans and the IP blocklist in the
> admin panel are enforced **client-side**, so they only affect this browser /
> this visitor and are bypassable. For real, server-enforced blocking you need a
> backend or a CDN/WAF such as Cloudflare. The admin UI is ready to feed such a
> backend — the app talks only to `Auth.*` and `Newsletter.*`, so swapping in a
> real API is a contained change.

**Extras:** 🌙/☀️ **dark/light theme toggle** (remembers your choice) and a
🔗 **Share** button in the editor that copies a link which reopens your exact
query (`#/editor?q=…`) — great for sharing snippets or asking for help.

## 🧠 The Course (beginner → advanced)

1. 🌱 **SQL Basics** — SELECT, columns, WHERE, ORDER BY, LIMIT
2. 🔍 **Filtering & Functions** — DISTINCT, AND/OR, IN/BETWEEN, LIKE, aggregates
3. 📊 **Grouping & Aggregation** — GROUP BY, AS, HAVING, SUM per group
4. 🔗 **Joins** — INNER, multi-table, LEFT JOIN, join + group
5. 🧩 **Subqueries & CTEs** — subqueries, IN (subquery), WITH
6. 🏆 **Advanced** — ROW_NUMBER, RANK + PARTITION BY, self-joins, capstone

Each challenge runs the learner's query **and** a reference solution against a
fresh copy of the database, then compares the result sets to grade it.

## 🚀 Run locally

It's just static files — use any static server:

```bash
cd web
npx serve .          # or: python3 -m http.server 8000
```

Then open the printed URL. (An internet connection is needed the first time so
the browser can fetch sql.js / CodeMirror from their CDNs.)

## 🌐 Deploy (free options)

### GitHub Pages — automated (recommended)
A workflow at `.github/workflows/deploy-pages.yml` validates the lessons and
publishes the `web/` folder on every push. **One-time setup:**

1. In the repo, go to **Settings → Pages → Build and deployment**.
2. Set **Source** to **GitHub Actions**.
3. Push to `main` (or the feature branch) — the **Deploy SQLQuest to GitHub
   Pages** action runs and prints the live URL in its summary.

Live URL (project page): **https://devil-rider.github.io/assignment-/**

> Note: on a project page the site is served under `/assignment-/`. All asset
> paths are relative, so it works. For AdSense, `ads.txt` must sit at the domain
> *root*, so use a custom domain or a user/org Pages site when you go live with ads.

### Netlify / Vercel / Cloudflare Pages (no build step)
Point them at the `web/` directory — that's it.

## 💸 Monetization — how you earn

Ads are wired through `js/ads.js`. By default the slots show a labelled
placeholder so the layout is correct. To go live with **Google AdSense**:

1. Create an account at <https://adsense.google.com> and get approved.
2. In `index.html`, uncomment the `adsbygoogle` `<script>` and add your
   publisher id (`ca-pub-XXXXXXXXXXXXXXXX`).
3. In `js/ads.js`, set `ADSENSE.enabled = true`, fill in `ADSENSE.client`, and
   map your ad-unit slot ids in `AD_SLOTS`.
4. Add an `ads.txt` at the site root (template provided) with your publisher id.

Ad slots are placed in: the top leaderboard, the home page, inside the editor,
between course worlds, on lessons, and on the contact page.

## 🔐 A note on authentication

Login is **front-end only** (localStorage) — perfect for a static MVP/demo, but
**not secure for real secrets**. The whole app talks only to `Auth.*`, so to
productionize you can drop in Firebase Auth, Supabase, Auth0, or your own API
without touching the rest of the code.

## ✅ Tests

`test/run-solutions.mjs` executes every lesson's reference solution against the
seed database (via Node's built-in `node:sqlite`) to guarantee they all run:

```bash
node web/test/run-solutions.mjs
```

## 🗂 Project structure

```
web/
├── index.html              app shell, nav, footer, CDN scripts
├── css/styles.css          all styling (dark, gamified theme)
├── js/
│   ├── sql-engine.js       sql.js wrapper + sample "shop" database
│   ├── auth.js             client-side auth + XP/badges/progress
│   ├── ads.js              AdSense-ready ad slots
│   ├── course-data.js      the full curriculum (modules & lessons)
│   ├── components.js       editor widget, result table, toast, validator
│   ├── router.js           hash-based SPA router
│   ├── app.js              bootstrap
│   └── pages/              home, editor, learn, contact, login
└── test/run-solutions.mjs  validates every lesson solution
```

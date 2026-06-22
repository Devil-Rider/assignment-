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
| **Leaderboard** (`#/leaderboard`) | Ranks all players by XP, level, lessons completed & badges. | ❌ |
| **Contact** (`#/contact`) | Contact-us page with details and a message form. | ❌ |
| **Login / Sign up** (`#/login`) | Client-side accounts that store your XP & progress. | — |
| **Ads** | Dedicated, AdSense-ready ad slots throughout the site. **This is the revenue source.** | — |

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

- **GitHub Pages**: push and enable Pages on the `/web` folder (or move files to root).
- **Netlify / Vercel / Cloudflare Pages**: point them at the `web/` directory, no build step.

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

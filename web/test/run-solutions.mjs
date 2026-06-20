// Validates every lesson solution against the seed DB using node:sqlite.
// Run: node web/test/run-solutions.mjs
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(dir, '..');

// --- Load SEED_SQL from sql-engine.js by extracting the template literal ---
const engineSrc = readFileSync(path.join(root, 'js/sql-engine.js'), 'utf8');
const seedMatch = engineSrc.match(/const SEED_SQL = `([\s\S]*?)`;/);
if (!seedMatch) throw new Error('Could not find SEED_SQL');
const SEED_SQL = seedMatch[1];

// --- Load COURSE by evaluating course-data.js in a sandbox with a window stub ---
const courseSrc = readFileSync(path.join(root, 'js/course-data.js'), 'utf8');
const sandbox = {};
sandbox.window = sandbox;        // mimic browser: window.X also defines global X
vm.createContext(sandbox);
vm.runInContext(courseSrc, sandbox);
const COURSE = sandbox.window.COURSE;

function freshDb() {
  const db = new DatabaseSync(':memory:');
  db.exec(SEED_SQL);
  return db;
}

let pass = 0, fail = 0;
for (const m of COURSE) {
  for (const l of m.lessons) {
    const db = freshDb();
    try {
      const rows = db.prepare(l.solution).all();
      if (rows.length === 0 && !/INSERT|UPDATE|DELETE|CREATE/i.test(l.solution)) {
        console.log(`⚠️  ${l.id} (${l.title}): solution returned 0 rows`);
      }
      pass++;
      // sanity: the starter, if it is a complete statement, should error or differ — not asserted here
    } catch (e) {
      fail++;
      console.log(`❌ ${l.id} (${l.title}): ${e.message}`);
    } finally {
      db.close();
    }
  }
}
console.log(`\n${pass} solutions ran OK, ${fail} failed, across ${COURSE.length} modules.`);
process.exit(fail ? 1 : 0);

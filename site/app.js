// issuescope: browse and relate a GitHub tracker snapshot with DuckDB-wasm.
// Everything runs in the browser. Data comes from ./data/*.parquet (built by
// build/build_data.py); "Refresh from GitHub" merges in changes since the
// snapshot using the public REST API.

const DUCKDB_VERSION = "1.32.0";
const CDN = `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_VERSION}`;
// For offline testing a page can set window.ISSUESCOPE_DUCKDB = "<base url of duckdb-wasm dist>".
const LOCAL_BASE = window.ISSUESCOPE_DUCKDB || null;
// "parquet" (default; DuckDB loads its parquet extension on first use) or "arrow" (Arrow IPC files, no extension needed)
const FORMAT = window.ISSUESCOPE_FORMAT || "parquet";
const DATA = "data/";
const K1 = 1.2, B = 0.75;
const MARKS_KEY = "issuescope.marks.v1";
const WEIGHTS_KEY = "issuescope.weights.v2";
// Wish list from a downstream tracker (scripts/wishes.py in mdsumner/osgeo-upstream):
// {repo, generated, wishes: [{number, title, url, priority, status, search, ...}]}.
// Set window.ISSUESCOPE_WISHES to another URL, or to "" to turn the picker off.
const WISHES_URL = window.ISSUESCOPE_WISHES !== undefined ? window.ISSUESCOPE_WISHES
  : "https://raw.githubusercontent.com/mdsumner/osgeo-upstream/main/wishes.json";

const $ = (s) => document.querySelector(s);
const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const sqlstr = (s) => "'" + String(s).replace(/'/g, "''") + "'";

let db, conn, meta, mod;
let lazyLoaded = {};
let lastResults = [];
let lastQuery = "";
let liveCount = 0;
let wishes = [];

// ---------------------------------------------------------------- storage
function load(key, dflt) { try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : dflt; } catch (e) { return dflt; } }
function save(key, v) { try { localStorage.setItem(key, JSON.stringify(v)); } catch (e) { /* storage unavailable */ } }

const FEATURES = [
  { key: "bm25", label: "text match", dflt: 1.0 },
  { key: "recent", label: "recently active", dflt: 0.3 },
  { key: "xref", label: "cross-referenced", dflt: 0.2 },
  { key: "talk", label: "discussion size", dflt: 0.1 },
  { key: "open", label: "is open", dflt: 0.0 },
];
let weights = Object.assign(Object.fromEntries(FEATURES.map((f) => [f.key, f.dflt])), load(WEIGHTS_KEY, {}));
let marks = load(MARKS_KEY, {});

// ---------------------------------------------------------------- tokenizer
// Must match build/build_data.py
let STOP = new Set();
function tokenize(s) {
  const out = [];
  const m = String(s || "").toLowerCase().match(/[a-z0-9_]{2,}/g) || [];
  for (const t of m) {
    if (/^\d+$/.test(t)) { if (t.length >= 3) out.push(t); continue; }
    if (!STOP.has(t)) out.push(t);
  }
  return out;
}

// ---------------------------------------------------------------- duckdb
async function q(sql) {
  const res = await conn.query(sql);
  return res.toArray().map((r) => r.toJSON());
}

function status(msg) { $("#status").textContent = msg; }

async function initDuckDB() {
  mod = await import(LOCAL_BASE ? new URL("duckdb.mjs", new URL(LOCAL_BASE, location.href)).href : `${CDN}/+esm`);
  let bundle;
  if (LOCAL_BASE) {
    const abs = new URL(LOCAL_BASE, location.href).href;
    bundle = await mod.selectBundle({
      mvp: { mainModule: abs + "duckdb-mvp.wasm", mainWorker: abs + "duckdb-browser-mvp.worker.js" },
      eh: { mainModule: abs + "duckdb-eh.wasm", mainWorker: abs + "duckdb-browser-eh.worker.js" },
    });
  } else {
    bundle = await mod.selectBundle(mod.getJsDelivrBundles());
  }
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  const worker = new Worker(workerUrl);
  db = new mod.AsyncDuckDB(new mod.ConsoleLogger(mod.LogLevel.WARNING), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();
}

async function fetchBytes(name) {
  const r = await fetch(DATA + name + (FORMAT === "arrow" ? ".arrow" : ".parquet"));
  if (!r.ok) throw new Error(`fetch ${name}: HTTP ${r.status}`);
  return new Uint8Array(await r.arrayBuffer());
}

// Load data file <name> into table <name>; create = false appends to an existing table.
async function loadTable(name, create = true) {
  const bytes = await fetchBytes(name);
  if (FORMAT === "arrow") {
    await conn.insertArrowFromIPCStream(bytes, { name, create });
    return;
  }
  await db.registerFileBuffer(name + ".parquet", bytes);
  await conn.query(create ? `CREATE TABLE ${name} AS SELECT * FROM '${name}.parquet'` : `INSERT INTO ${name} SELECT * FROM '${name}.parquet'`);
}

async function loadCore() {
  meta = await (await fetch(DATA + "meta.json")).json();
  STOP = new Set(meta.stopwords || []);
  $("#repo-name").textContent = `${meta.repo}  |  snapshot ${meta.snapshot_utc.slice(0, 16).replace("T", " ")} UTC`;
  status("Fetching data...");
  await setNow();
  for (const t of ["items", "vocab", "postings", "links", "xrefs"]) await loadTable(t);
  await conn.query(`CREATE TABLE bodies (number INT, body VARCHAR)`);
  await conn.query(`CREATE TABLE comments (number INT, author VARCHAR, author_association VARCHAR, created_at TIMESTAMP, body VARCHAR)`);
  await conn.query(`CREATE TABLE live_log (number INT, kind VARCHAR, seen_at TIMESTAMP)`);
  await conn.query(`CREATE OR REPLACE VIEW xin AS SELECT dst AS number, count(DISTINCT src)::INT AS n_xin FROM xrefs GROUP BY dst`);
  await conn.query(`CREATE OR REPLACE VIEW corpus_stats AS SELECT count(*)::INT AS n, avg(dl)::DOUBLE AS avgdl FROM items`);
  const [c] = await q(`SELECT count(*)::INT n, count(*) FILTER (WHERE state='open' AND NOT is_pr)::INT oi, count(*) FILTER (WHERE state='open' AND is_pr)::INT op FROM items`);
  status(`${c.n.toLocaleString()} threads | ${c.oi} open issues | ${c.op} open PRs`);
}

// Plain UTC timestamps throughout: the browser build cannot fetch the ICU extension that time zones need.
async function setNow() {
  const iso = new Date().toISOString().slice(0, 19).replace("T", " ");
  await conn.query(`CREATE OR REPLACE MACRO now_utc() AS TIMESTAMP '${iso}'`);
}

async function ensureLazy(name) {
  if (lazyLoaded[name]) return;
  status(`Loading ${name}...`);
  await loadTable(name, false);
  lazyLoaded[name] = true;
  status("Ready");
}

// ---------------------------------------------------------------- search
function bm25SQL(terms, where, limit) {
  const vals = terms.map((t) => `(${sqlstr(t)})`).join(",");
  return `
WITH qq(term) AS (VALUES ${vals}),
qt AS (SELECT DISTINCT v.term_id, v.df FROM vocab v JOIN qq USING (term)),
st AS (SELECT * FROM corpus_stats),
s AS (
  SELECT p.number,
         sum(ln(1 + (st.n - qt.df + 0.5) / (qt.df + 0.5)) * (p.tf * (${K1} + 1)) /
             (p.tf + ${K1} * (1 - ${B} + ${B} * i.dl / st.avgdl)))::DOUBLE AS bm25,
         count(*)::INT AS nterms
  FROM postings p JOIN qt USING (term_id) JOIN items i USING (number), st
  GROUP BY p.number)
SELECT i.number, i.is_pr, i.state, i.merged, i.draft, i.title,
       array_to_string(i.labels, ', ') AS labels, i.author,
       strftime(i.updated_at, '%Y-%m-%d') AS updated, strftime(i.created_at, '%Y-%m-%d') AS created,
       date_diff('day', i.updated_at, now_utc())::INT AS idle_days,
       i.n_comments, coalesce(x.n_xin, 0) AS n_xin, s.bm25, s.nterms, i.snippet
FROM s JOIN items i USING (number) LEFT JOIN xin x USING (number)
WHERE ${where}
ORDER BY s.bm25 DESC LIMIT ${limit}`;
}

function filterSQL() {
  const w = ["TRUE"];
  const t = $("#f-type").value, st = $("#f-state").value, lab = $("#f-label").value.trim(), since = $("#f-since").value;
  if (t === "issue") w.push("NOT i.is_pr");
  if (t === "pr") w.push("i.is_pr");
  if (st === "open") w.push("i.state = 'open'");
  if (st === "closed") w.push("i.state = 'closed'");
  if (st === "merged") w.push("i.merged");
  if (lab) w.push(`lower(array_to_string(i.labels, ',')) LIKE ${sqlstr("%" + lab.toLowerCase() + "%")}`);
  if (since) w.push(`i.updated_at >= ${sqlstr(since)}::DATE`);
  return w.join(" AND ");
}

function featuresOf(r, maxbm) {
  return {
    bm25: maxbm > 0 ? r.bm25 / maxbm : 0,
    recent: Math.exp(-r.idle_days / 365),
    xref: Math.min(1, Math.log1p(r.n_xin) / Math.log1p(10)),
    talk: Math.min(1, Math.log1p(r.n_comments) / Math.log1p(50)),
    open: r.state === "open" ? 1 : 0,
  };
}
// Text match gates everything: the other signals scale it up, so they can re-order
// matching threads but never lift a thread that does not match.
const fitX = (f) => FEATURES.map((F) => (F.key === "bm25" ? f.bm25 : f.bm25 * f[F.key]));
const scoreOf = (f) => fitX(f).reduce((a, v, j) => a + weights[FEATURES[j].key] * v, 0);

async function runSearch(text, opts = {}) {
  const terms = [...new Set(tokenize(text))];
  lastQuery = opts.label || text.trim();
  if (!terms.length) { $("#results").innerHTML = `<p class="muted">No searchable words in that query.</p>`; return; }
  const t0 = performance.now();
  const rows = await q(bm25SQL(terms, opts.where || filterSQL(), 400));
  const maxbm = rows.reduce((m, r) => Math.max(m, r.bm25), 0);
  for (const r of rows) { r.f = featuresOf(r, maxbm); r.score = scoreOf(r.f); }
  rows.sort((a, b) => b.score - a.score);
  lastResults = rows.filter((r) => r.number !== opts.exclude).slice(0, 60);
  renderResults(performance.now() - t0, terms.length);
  setHash(opts.hash || { q: text.trim() });
}

// ---------------------------------------------------------------- deep links
// #q=<text> runs a search, #wish=<tracker issue number> runs that wish, #live refreshes.
function hashParams() {
  const h = location.hash.slice(1);
  return h === "live" ? new URLSearchParams("live=1") : new URLSearchParams(h);
}
function setHash(obj) {
  const p = new URLSearchParams(obj);
  try { history.replaceState(null, "", "#" + p.toString()); } catch (e) { /* ignore */ }
}

// ---------------------------------------------------------------- tracker wishes
async function loadWishes() {
  if (!WISHES_URL) return;
  try {
    const r = await fetch(WISHES_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error(r.status + " " + r.statusText);
    const j = await r.json();
    wishes = j.wishes || [];
    const sel = $("#wish");
    for (const w of wishes) {
      const o = document.createElement("option");
      o.value = String(w.number);
      o.textContent = `#${w.number} ${w.priority || "inbox"} ${w.status}: ${w.title}`;
      sel.appendChild(o);
    }
    sel.title = `${wishes.length} open items from ${j.repo || "tracker"}, generated ${j.generated || "?"}`;
    $("#wish-bar").hidden = !wishes.length;
  } catch (e) {
    console.warn("wish list not loaded:", e.message);
  }
}
function runWish(n) {
  const w = wishes.find((x) => String(x.number) === String(n));
  if (!w) return false;
  $("#wish").value = String(w.number);
  $("#wish-link").href = w.url;
  $("#wish-link").textContent = `tracker #${w.number}` + (w.search_source === "title" ? " (searching its title; add Search words for better matches)" : "");
  $("#q").value = w.search;
  runSearch(w.search, { label: `tracker #${w.number}`, hash: { wish: w.number } });
  return true;
}

function pills(r) {
  const s = r.is_pr ? (r.merged ? "merged" : r.state) : r.state;
  return `<span class="pill ${s}">${r.is_pr ? "PR " : ""}${s}${r.draft ? " draft" : ""}</span>`;
}

function markKey(query, n) { return query + "||" + n; }

function renderResults(ms, nterms) {
  const el = $("#results");
  if (!lastResults.length) { el.innerHTML = `<p class="muted">No matches.</p>`; return; }
  el.innerHTML = `<p class="muted">${lastResults.length} shown, ${nterms} query terms, ${ms.toFixed(0)} ms. Query: <b>${esc(lastQuery.slice(0, 120))}</b></p>` +
    lastResults.map((r) => {
      const m = marks[markKey(lastQuery, r.number)];
      return `<div class="row" data-n="${r.number}">
        <div class="num">#${r.number}</div>
        <div><div>${pills(r)}${esc(r.title)}</div>
          <div class="meta">score ${r.score.toFixed(2)} | text ${r.f.bm25.toFixed(2)} | updated ${r.updated} | ${r.n_comments} comments | ${r.n_xin} xrefs${r.labels ? " | " + esc(r.labels) : ""}</div></div>
        <div class="marks"><button class="plus ${m && m.y === 1 ? "on" : ""}" data-mark="1" title="relevant">+</button><button class="minus ${m && m.y === 0 ? "on" : ""}" data-mark="0" title="not relevant">-</button></div>
      </div>`;
    }).join("");
}

$("#results").addEventListener("click", (ev) => {
  const row = ev.target.closest(".row"); if (!row) return;
  const n = +row.dataset.n;
  const mb = ev.target.closest("button[data-mark]");
  if (mb) {
    const r = lastResults.find((x) => x.number === n);
    const y = +mb.dataset.mark, k = markKey(lastQuery, n);
    if (marks[k] && marks[k].y === y) delete marks[k];
    else marks[k] = { q: lastQuery, n, y, f: r.f, t: r.title };
    save(MARKS_KEY, marks);
    renderResults(0, 0);
    return;
  }
  document.querySelectorAll(".row.sel").forEach((x) => x.classList.remove("sel"));
  row.classList.add("sel");
  showDetail(n);
});

// ---------------------------------------------------------------- detail
const ghUrl = (n, isPr) => `https://github.com/${meta.repo}/${isPr ? "pull" : "issues"}/${n}`;
function jumpList(rows, empty) {
  if (!rows.length) return `<p class="muted">${empty}</p>`;
  return "<ul>" + rows.map((r) => `<li><a class="jump" data-n="${r.number}">#${r.number}</a> ${pills(r)}${esc(r.title)}${r.why ? ` <span class="muted">(${esc(r.why)})</span>` : ""}</li>`).join("") + "</ul>";
}

async function showDetail(n) {
  const el = $("#detail");
  const [it] = await q(`SELECT number, is_pr, state, merged, draft, title, array_to_string(labels, ', ') labels, author, author_association,
      strftime(created_at, '%Y-%m-%d') created, strftime(updated_at, '%Y-%m-%d') updated, n_comments FROM items WHERE number = ${n}`);
  if (!it) { el.innerHTML = `<p class="muted">#${n} is not in the corpus.</p>`; return; }
  const cols = `i.number, i.is_pr, i.state, i.merged, i.draft, i.title`;
  const links = await q(`
    SELECT ${cols}, CASE WHEN l.kind='fixes' THEN 'claims to fix this' ELSE 'marked duplicate of this' END why FROM links l JOIN items i ON i.number = l.src WHERE l.dst = ${n}
    UNION ALL SELECT ${cols}, CASE WHEN l.kind='fixes' THEN 'this claims to fix it' ELSE 'this is a duplicate of it' END FROM links l JOIN items i ON i.number = l.dst WHERE l.src = ${n}`);
  const xin = await q(`SELECT ${cols} FROM xrefs x JOIN items i ON i.number = x.src WHERE x.dst = ${n} ORDER BY i.updated_at DESC LIMIT 40`);
  const xout = await q(`SELECT ${cols} FROM xrefs x JOIN items i ON i.number = x.dst WHERE x.src = ${n} ORDER BY i.number DESC LIMIT 40`);
  // "more like this": the thread's own most distinctive terms, fed back through BM25
  const top = await q(`
    WITH st AS (SELECT * FROM corpus_stats)
    SELECT v.term FROM postings p JOIN vocab v USING (term_id), st WHERE p.number = ${n}
    ORDER BY p.tf * ln(st.n / v.df) DESC LIMIT 25`);
  let similar = [];
  if (top.length) {
    const rows = await q(bm25SQL(top.map((t) => t.term), `i.number <> ${n}`, 12));
    similar = rows.map((r) => Object.assign(r, { why: `text ${r.bm25.toFixed(1)}` }));
  }
  el.innerHTML = `
    <h3><a href="${ghUrl(n, it.is_pr)}" target="_blank" rel="noopener">#${n}</a> ${pills(it)}${esc(it.title)}</h3>
    <div class="muted">by ${esc(it.author)} (${esc(it.author_association)}) | created ${it.created} | updated ${it.updated} | ${it.n_comments} comments${it.labels ? " | " + esc(it.labels) : ""}</div>
    <p><button id="d-search">Search with this thread's key terms</button> <button id="d-thread">Show conversation</button></p>
    <h4>Explicit links (fixes / duplicate of)</h4>${jumpList(links, "None parsed.")}
    <h4>Mentioned by</h4>${jumpList(xin, "No other thread mentions it.")}
    <h4>Mentions</h4>${jumpList(xout, "It mentions no other thread.")}
    <h4>Similar threads</h4>${jumpList(similar, "No similar threads.")}
    <div id="d-conv"></div>`;
  $("#d-search").onclick = () => {
    $("#q").value = top.map((t) => t.term).join(" ");
    runSearch($("#q").value, { label: `like #${n}`, exclude: n });
  };
  $("#d-thread").onclick = () => showThread(n);
}

$("#detail").addEventListener("click", (ev) => {
  const a = ev.target.closest("a.jump"); if (a) showDetail(+a.dataset.n);
});

async function showThread(n) {
  await ensureLazy("bodies"); await ensureLazy("comments");
  const [b] = await q(`SELECT body FROM bodies WHERE number = ${n}`);
  const cs = await q(`SELECT author, author_association, strftime(created_at, '%Y-%m-%d') d, body FROM comments WHERE number = ${n} ORDER BY created_at`);
  $("#d-conv").innerHTML = `<h4>Conversation</h4><div class="comment">${esc((b && b.body) || "(no body)")}</div>` +
    cs.map((c) => `<div class="comment"><div class="who">${esc(c.author)} (${esc(c.author_association)}) ${c.d}</div>${esc(c.body)}</div>`).join("");
}

// ---------------------------------------------------------------- weights UI
function renderWeights() {
  $("#weights").innerHTML = "Ranking weights:" + FEATURES.map((F) =>
    `<label>${F.label} <input type="range" min="0" max="1.5" step="0.05" value="${weights[F.key]}" data-w="${F.key}"> <span>${(+weights[F.key]).toFixed(2)}</span></label>`).join("") +
    ` <button id="w-reset">reset</button>`;
}
$("#weights").addEventListener("input", (ev) => {
  const k = ev.target.dataset.w; if (!k) return;
  weights[k] = +ev.target.value; ev.target.nextElementSibling.textContent = weights[k].toFixed(2);
  save(WEIGHTS_KEY, weights);
  for (const r of lastResults) r.score = scoreOf(r.f);
  lastResults.sort((a, b) => b.score - a.score); renderResults(0, 0);
});
$("#weights").addEventListener("click", (ev) => {
  if (ev.target.id !== "w-reset") return;
  weights = Object.fromEntries(FEATURES.map((f) => [f.key, f.dflt])); save(WEIGHTS_KEY, weights); renderWeights();
});

// ---------------------------------------------------------------- calibration
function fitLogistic(rows) {
  // rows: [{x:[...], y}] -> non-negative weights by projected gradient descent
  const d = rows[0].x.length; let w = new Array(d).fill(0.1), b = 0;
  for (let it = 0; it < 3000; it++) {
    const gw = new Array(d).fill(0); let gb = 0;
    for (const r of rows) {
      const z = b + r.x.reduce((a, v, j) => a + v * w[j], 0);
      const p = 1 / (1 + Math.exp(-z)), e = p - r.y;
      for (let j = 0; j < d; j++) gw[j] += e * r.x[j]; gb += e;
    }
    for (let j = 0; j < d; j++) w[j] = Math.max(0, w[j] - 0.5 * (gw[j] / rows.length + 0.01 * w[j]));
    b -= 0.5 * gb / rows.length;
  }
  return w;
}

function precisionAt(k) {
  // over all marked queries: of the top-k (by current weights) marked items, share marked relevant
  const byQ = {};
  for (const m of Object.values(marks)) (byQ[m.q] = byQ[m.q] || []).push(m);
  let hit = 0, tot = 0;
  for (const ms of Object.values(byQ)) {
    ms.sort((a, b) => scoreOf(b.f) - scoreOf(a.f));
    for (const m of ms.slice(0, k)) { tot++; hit += m.y; }
  }
  return tot ? hit / tot : null;
}

function renderCalib() {
  const ms = Object.values(marks);
  const pos = ms.filter((m) => m.y === 1).length;
  const p5 = precisionAt(5);
  const byQ = {}; for (const m of ms) byQ[m.q] = (byQ[m.q] || 0) + 1;
  $("#calib").innerHTML = `
    <div class="card"><b>${ms.length}</b> marks (${pos} relevant, ${ms.length - pos} not) over <b>${Object.keys(byQ).length}</b> queries.
      Precision of the top 5 marked results per query under the current weights: <b>${p5 == null ? "n/a" : (100 * p5).toFixed(0) + "%"}</b>.
      <p class="muted">Marks live in this browser only. Export them to share, for example as a pull request to a shared labels file that the nightly job can learn from.</p>
      <p><button id="c-fit" ${ms.length < 6 || pos === 0 || pos === ms.length ? "disabled" : ""}>Fit weights to my marks</button>
         <button id="c-csv">Export marks CSV</button> <button id="c-clear">Clear marks</button> <span id="c-msg" class="muted"></span></p>
      <p class="muted">Fitting needs at least 6 marks with both kinds. It learns non-negative weights from your marks, then rescales so text match is 1. Text match always gates the score: the other signals only re-order threads that match.</p>
    </div>
    <div class="card"><b>Marks by query</b>${ms.length ? "<table><tr><th>query</th><th>#</th><th>title</th><th>mark</th></tr>" +
      ms.slice(-200).reverse().map((m) => `<tr><td>${esc(m.q.slice(0, 60))}</td><td>${m.n}</td><td>${esc((m.t || "").slice(0, 70))}</td><td>${m.y ? "+" : "-"}</td></tr>`).join("") + "</table>" : `<p class="muted">No marks yet.</p>`}</div>`;
  const fit = $("#c-fit");
  if (fit) fit.onclick = () => {
    const rows = ms.map((m) => ({ x: fitX(m.f), y: m.y }));
    const before = precisionAt(5);
    const w = fitLogistic(rows); const s = w[0] > 0 ? w[0] : 1;
    FEATURES.forEach((F, j) => { weights[F.key] = Math.min(1.5, +(w[j] / s).toFixed(2)); });
    save(WEIGHTS_KEY, weights); renderWeights(); renderCalib();
    $("#c-msg").textContent = `precision@5 ${before == null ? "n/a" : (100 * before).toFixed(0) + "%"} -> ${(100 * precisionAt(5)).toFixed(0)}% (on the same marks, so optimistic)`;
  };
  $("#c-csv").onclick = () => download("issuescope-marks.csv", toCSV(ms.map((m) => ({ query: m.q, number: m.n, relevant: m.y, title: m.t, ...m.f }))));
  $("#c-clear").onclick = () => { if ($("#c-clear").dataset.armed) { marks = {}; save(MARKS_KEY, marks); renderCalib(); } else { $("#c-clear").dataset.armed = 1; $("#c-clear").textContent = "Click again to clear"; } };
}

// ---------------------------------------------------------------- export
function toCSV(rows) {
  if (!rows.length) return "";
  const cols = Object.keys(rows[0]);
  const cell = (v) => { const s = v == null ? "" : String(v); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => cell(r[c])).join(","))].join("\n");
}
function download(name, text, type = "text/plain") {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type })); a.download = name;
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
}

$("#x-csv").onclick = () => {
  const n = +$("#x-n").value || 10;
  download("issuescope-results.csv", toCSV(lastResults.slice(0, n).map((r) => ({ number: r.number, url: ghUrl(r.number, r.is_pr), is_pr: r.is_pr, state: r.state, title: r.title, labels: r.labels, updated: r.updated, score: r.score.toFixed(3), text_match: r.f.bm25.toFixed(3) }))));
};
$("#x-md").onclick = async () => {
  const n = +$("#x-n").value || 10; const rs = lastResults.slice(0, n);
  if (!rs.length) return;
  await ensureLazy("bodies"); await ensureLazy("comments");
  const nums = rs.map((r) => r.number).join(",");
  const bodies = Object.fromEntries((await q(`SELECT number, body FROM bodies WHERE number IN (${nums})`)).map((r) => [r.number, r.body]));
  const cs = await q(`SELECT number, author, author_association, strftime(created_at, '%Y-%m-%d') d, body FROM comments WHERE number IN (${nums}) ORDER BY number, created_at`);
  let md = `# ${meta.repo}: threads related to "${lastQuery}"\n\nSnapshot ${meta.snapshot_utc} plus live updates. ${rs.length} threads, ranked by issuescope.\n\n`;
  for (const r of rs) {
    md += `\n---\n\n## #${r.number} ${r.title}\n\n${ghUrl(r.number, r.is_pr)} | ${r.is_pr ? "PR" : "issue"} | ${r.state}${r.merged ? " (merged)" : ""} | labels: ${r.labels || "none"} | updated ${r.updated}\n\n${bodies[r.number] || ""}\n`;
    for (const c of cs.filter((c) => c.number === r.number)) md += `\n**${c.author}** (${c.author_association}, ${c.d}):\n\n${c.body}\n`;
  }
  download(`issuescope-${rs.length}-threads.md`, md, "text/markdown");
  try { await navigator.clipboard.writeText(md); $("#x-msg").textContent = "Downloaded, and copied to the clipboard."; }
  catch (e) { $("#x-msg").textContent = "Downloaded."; }
};

// ---------------------------------------------------------------- SQL tab
const PRESETS = [
  ["Tables and columns", `SELECT table_name, column_name, data_type FROM information_schema.columns ORDER BY table_name, ordinal_position`],
  ["Open issues with an abandoned fix attempt", `SELECT l.dst AS issue, list(DISTINCT l.src ORDER BY l.src) AS closed_unmerged_prs, any_value(i.title) AS title
FROM links l JOIN items p ON p.number = l.src JOIN items i ON i.number = l.dst
WHERE l.kind = 'fixes' AND p.is_pr AND p.state = 'closed' AND NOT p.merged AND NOT i.is_pr AND i.state = 'open'
GROUP BY l.dst ORDER BY l.dst DESC`],
  ["Most cross-referenced open issues", `SELECT i.number, x.n_xin, i.title, strftime(i.updated_at, '%Y-%m-%d') AS updated
FROM xin x JOIN items i USING (number) WHERE i.state = 'open' AND NOT i.is_pr ORDER BY x.n_xin DESC LIMIT 50`],
  ["Open issues per label, with median idle days", `SELECT l AS label, count(*) AS open_issues, median(date_diff('day', updated_at, now_utc()))::INT AS median_idle_days
FROM items, unnest(labels) t(l) WHERE state = 'open' AND NOT is_pr GROUP BY l ORDER BY open_issues DESC`],
  ["Never answered, oldest first", `SELECT number, strftime(created_at, '%Y-%m-%d') AS created, title, array_to_string(labels, ', ') AS labels
FROM items WHERE state = 'open' AND NOT is_pr AND n_comments = 0 ORDER BY created_at LIMIT 200`],
  ["BM25 by hand (edit the terms)", `WITH qq(term) AS (VALUES ('vrt'), ('connection'), ('outsize'), ('empty')),
qt AS (SELECT v.term_id, v.df FROM vocab v JOIN qq USING (term)), st AS (SELECT * FROM corpus_stats)
SELECT i.number, i.state, i.is_pr, i.title,
  sum(ln(1 + (st.n - qt.df + 0.5) / (qt.df + 0.5)) * (p.tf * 2.2) / (p.tf + 1.2 * (0.25 + 0.75 * i.dl / st.avgdl)))::DOUBLE AS bm25
FROM postings p JOIN qt USING (term_id) JOIN items i USING (number), st
GROUP BY ALL ORDER BY bm25 DESC LIMIT 30`],
  ["Threads touched since the snapshot (after a live refresh)", `SELECT l.number, l.kind, strftime(l.seen_at, '%Y-%m-%d %H:%M') AS seen_at, i.state, i.title
FROM live_log l LEFT JOIN items i USING (number) ORDER BY l.seen_at DESC`],
  ["Comment volume by author association and year (loads comments)", `SELECT year(created_at) AS yr, author_association, count(*) AS n FROM comments GROUP BY ALL ORDER BY yr, n DESC`],
];
let sqlRows = [];
function initSQL() {
  $("#sql-preset").innerHTML = PRESETS.map((p, i) => `<option value="${i}">${esc(p[0])}</option>`).join("");
  $("#sql").value = PRESETS[1][1];
  $("#sql-preset").value = 1;
  $("#sql-preset").onchange = () => { $("#sql").value = PRESETS[+$("#sql-preset").value][1]; };
  $("#sql-run").onclick = runSQL;
  $("#sql-csv").onclick = () => download("issuescope-query.csv", toCSV(sqlRows));
  $("#sql").addEventListener("keydown", (e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") runSQL(); });
}
async function runSQL() {
  const s = $("#sql").value;
  try {
    if (/\b(comments)\b/i.test(s)) await ensureLazy("comments");
    if (/\b(bodies)\b/i.test(s)) await ensureLazy("bodies");
    const t0 = performance.now();
    sqlRows = (await q(s)).map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : (v && typeof v === "object" && v.toArray ? JSON.stringify(v.toArray()) : v)])));
    $("#sql-msg").textContent = `${sqlRows.length} rows, ${(performance.now() - t0).toFixed(0)} ms`;
    const show = sqlRows.slice(0, 500);
    if (!show.length) { $("#sql-out").innerHTML = `<p class="muted" style="padding:8px">No rows.</p>`; return; }
    const cols = Object.keys(show[0]);
    $("#sql-out").innerHTML = `<table><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>` +
      show.map((r) => `<tr>${cols.map((c) => {
        const v = r[c];
        if ((c === "number" || c === "issue") && Number.isInteger(v)) return `<td><a href="${ghUrl(v, false)}" target="_blank" rel="noopener">${v}</a></td>`;
        return `<td>${esc(v instanceof Date ? v.toISOString() : v)}</td>`;
      }).join("")}</tr>`).join("") + `</table>`;
  } catch (e) {
    $("#sql-msg").textContent = "Error: " + e.message;
    $("#sql-out").innerHTML = "";
  }
}

// ---------------------------------------------------------------- live refresh
async function ghFetch(path) {
  const tok = $("#gh-token").value.trim();
  const headers = { Accept: "application/vnd.github+json" };
  if (tok) headers.Authorization = `Bearer ${tok}`;
  const r = await fetch(`https://api.github.com/${path}`, { headers });
  if (!r.ok) throw new Error(`GitHub API ${r.status}${r.status === 403 ? " (rate limit? add a token)" : ""}`);
  return { data: await r.json(), remaining: r.headers.get("x-ratelimit-remaining") };
}

async function ghPages(path, maxPages) {
  const out = []; let rem = null;
  for (let p = 1; p <= maxPages; p++) {
    const { data, remaining } = await ghFetch(`${path}&per_page=100&page=${p}`);
    rem = remaining; out.push(...data);
    if (data.length < 100) break;
  }
  return { items: out, remaining: rem };
}

const XREF_RE = /#(\d{2,6})\b/g;
async function liveRefresh() {
  const btn = $("#live-btn"); btn.disabled = true;
  try {
    await setNow();
    const [{ since }] = await q(`SELECT strftime(greatest(max(updated_at), ${sqlstr(meta.snapshot_utc.slice(0, 19))}::TIMESTAMP) - INTERVAL 1 MINUTE, '%Y-%m-%dT%H:%M:%SZ') AS since FROM items`);
    $("#live-msg").textContent = `Fetching changes since ${since}...`;
    const iss = await ghPages(`repos/${meta.repo}/issues?state=all&sort=updated&direction=asc&since=${since}`, 10);
    const com = await ghPages(`repos/${meta.repo}/issues/comments?sort=updated&direction=asc&since=${since}`, 10);
    const known = new Set((await q(`SELECT number FROM items WHERE number IN (${[0, ...iss.items.map((x) => x.number)].join(",")})`)).map((r) => r.number));
    const vocab = new Map((await q(`SELECT term, term_id FROM vocab`)).map((r) => [r.term, r.term_id]));
    let nextId = vocab.size ? Math.max(...vocab.values()) + 1 : 0;
    const newTerms = [], postRows = [], xrefRows = [], logRows = [];
    const addTokens = (number, text) => {
      const tf = {};
      for (const t of tokenize(text)) tf[t] = (tf[t] || 0) + 1;
      for (const [t, c] of Object.entries(tf)) {
        let id = vocab.get(t);
        if (id === undefined) { id = nextId++; vocab.set(t, id); newTerms.push([id, t]); }
        postRows.push([id, number, c]);
      }
      return Object.values(tf).reduce((a, b) => a + b, 0);
    };
    const now = new Date().toISOString().replace("Z", "");
    for (const it of iss.items) {
      const n = it.number, isPr = !!it.pull_request;
      const labels = (it.labels || []).map((l) => sqlstr(l.name)).join(",");
      const ts = (v) => (v ? `${sqlstr(v.replace("Z", ""))}::TIMESTAMP` : "NULL");
      let dl = "dl";
      if (!known.has(n)) {
        const len = addTokens(n, `${(it.title + " ").repeat(meta.title_weight || 3)} ${it.body || ""}`);
        dl = String(len);
        await conn.query(`INSERT INTO items VALUES (${n}, ${isPr}, ${sqlstr(it.state)}, ${it.state_reason ? sqlstr(it.state_reason) : "NULL"},
          ${!!(it.pull_request && it.pull_request.merged_at)}, ${!!it.draft}, ${sqlstr(it.title)}, [${labels}]::VARCHAR[],
          ${sqlstr(it.user ? it.user.login : "")}, ${sqlstr(it.author_association || "")}, ${ts(it.created_at)}, ${ts(it.updated_at)}, ${ts(it.closed_at)},
          ${it.comments || 0}, ${len}, ${sqlstr((it.body || "").replace(/\s+/g, " ").slice(0, 400))})`);
        logRows.push([n, "new " + (isPr ? "PR" : "issue")]);
      } else {
        await conn.query(`UPDATE items SET state = ${sqlstr(it.state)}, state_reason = ${it.state_reason ? sqlstr(it.state_reason) : "NULL"},
          merged = ${!!(it.pull_request && it.pull_request.merged_at)}, title = ${sqlstr(it.title)}, labels = [${labels}]::VARCHAR[],
          updated_at = ${ts(it.updated_at)}, closed_at = ${ts(it.closed_at)}, n_comments = ${it.comments || 0} WHERE number = ${n}`);
        logRows.push([n, "updated"]);
      }
      if (lazyLoaded.bodies) { await conn.query(`DELETE FROM bodies WHERE number = ${n}`); await conn.query(`INSERT INTO bodies VALUES (${n}, ${sqlstr(it.body || "")})`); }
      for (const m of (it.body || "").matchAll(XREF_RE)) xrefRows.push([n, +m[1]]);
    }
    const seenComments = new Set();
    for (const c of com.items) {
      const m = /\/issues\/(\d+)$/.exec(c.issue_url || ""); if (!m) continue;
      const n = +m[1];
      if (seenComments.has(c.id)) continue; seenComments.add(c.id);
      addTokens(n, c.body || "");
      for (const x of (c.body || "").matchAll(XREF_RE)) xrefRows.push([n, +x[1]]);
      if (lazyLoaded.comments) await conn.query(`INSERT INTO comments VALUES (${n}, ${sqlstr(c.user ? c.user.login : "")}, ${sqlstr(c.author_association || "")}, ${sqlstr((c.created_at || "").replace("Z", ""))}::TIMESTAMP, ${sqlstr(c.body || "")})`);
      logRows.push([n, "comment"]);
    }
    const batch = async (table, rows, fmt) => {
      for (let i = 0; i < rows.length; i += 500) {
        const chunk = rows.slice(i, i + 500); if (!chunk.length) continue;
        await conn.query(`INSERT INTO ${table} VALUES ${chunk.map(fmt).join(",")}`);
      }
    };
    await batch("vocab", newTerms, ([id, t]) => `(${id}, ${sqlstr(t)}, 1)`);
    await batch("postings", postRows, ([id, n, c]) => `(${id}, ${n}, ${c})`);
    await batch("xrefs", xrefRows, ([s, d]) => `(${s}, ${d})`);
    await batch("live_log", logRows, ([n, k]) => `(${n}, ${sqlstr(k)}, ${sqlstr(now)}::TIMESTAMP)`);
    // keep xrefs to threads we know about, deduplicated
    await conn.query(`CREATE OR REPLACE TABLE xrefs AS SELECT DISTINCT x.src, x.dst FROM xrefs x JOIN items i ON i.number = x.dst WHERE x.src <> x.dst`);
    liveCount += iss.items.length;
    $("#live-msg").textContent = `Live: ${iss.items.length} threads and ${com.items.length} comments merged (changed since ${since}). API calls left this hour: ${com.remaining ?? iss.remaining ?? "unknown"}.`;
  } catch (e) {
    $("#live-msg").textContent = "Live refresh failed: " + e.message;
  } finally { btn.disabled = false; }
}
$("#live-btn").onclick = liveRefresh;

// ---------------------------------------------------------------- tabs + about
document.querySelectorAll(".tabs button").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".tabs button").forEach((x) => x.classList.toggle("active", x === b));
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.id === "tab-" + b.dataset.tab));
  if (b.dataset.tab === "calib") renderCalib();
}));

function renderAbout() {
  $("#about").innerHTML = `<div class="card">
  <p><b>What this is.</b> A read-only lens on the <code>${esc(meta.repo)}</code> tracker. Every issue and PR thread is in one DuckDB database in your browser, with full-text ranking (BM25), parsed links ("fixes #N", "duplicate of #N") and every "#N" cross-reference.</p>
  <p><b>Freshness.</b> The data files come from a snapshot taken ${esc(meta.snapshot_utc)}. "Refresh from GitHub" merges in whatever changed since, using the public API: 60 calls an hour without a token, which is plenty for a day's changes.</p>
  <p><b>Search.</b> Describe the feature or bug in your own words. Results are ranked by text match, boosted by recency, cross-references, discussion size and open state (the boosts multiply the text match, so they never lift a non-matching thread). Change the weights with the sliders, or mark results and let the Calibration tab fit them.</p>
  <p><b>SQL.</b> Every table is queryable: <code>items</code>, <code>postings</code>, <code>vocab</code>, <code>links</code>, <code>xrefs</code>, <code>xin</code>, <code>corpus_stats</code>, <code>live_log</code>, plus <code>bodies</code> and <code>comments</code>, which load on first use.</p>
  <p><b>Agents.</b> "Export ... as Markdown" bundles the top results with full bodies and conversations, ready to give to an agent.</p>
  <p class="muted">Corpus: ${meta.n_docs.toLocaleString()} threads, ${meta.n_terms.toLocaleString()} terms, ${meta.n_postings.toLocaleString()} postings. Text matching is lexical: rephrased duplicates can be missed.</p></div>`;
}

// ---------------------------------------------------------------- boot
$("#search-form").addEventListener("submit", (e) => { e.preventDefault(); runSearch($("#q").value); });
$("#wish").addEventListener("change", () => { if ($("#wish").value) runWish($("#wish").value); });
$("#q").addEventListener("input", () => { $("#wish").value = ""; $("#wish-link").textContent = ""; $("#wish-link").removeAttribute("href"); });
$("#q").addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); runSearch($("#q").value); } });
try { $("#gh-token").value = sessionStorage.getItem("issuescope.token") || ""; } catch (e) { /* ignore */ }
$("#gh-token").addEventListener("change", () => { try { sessionStorage.setItem("issuescope.token", $("#gh-token").value); } catch (e) { /* ignore */ } });

(async function boot() {
  try {
    await initDuckDB();
    await loadCore();
    renderWeights(); initSQL(); renderAbout();
    $("#live-msg").textContent = `Snapshot ${meta.snapshot_utc.slice(0, 16).replace("T", " ")} UTC. Refresh to merge in later changes.`;
    await loadWishes();
    const hp = hashParams();
    if (hp.get("live")) liveRefresh();
    else if (hp.get("wish")) runWish(hp.get("wish"));
    else if (hp.get("q")) { $("#q").value = hp.get("q"); runSearch(hp.get("q")); }
  } catch (e) {
    status("Failed to start: " + e.message);
    console.error(e);
  }
})();

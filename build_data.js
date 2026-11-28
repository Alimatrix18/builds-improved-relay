// Builds the prebuilt data files the relay page serves. Run daily by .github/workflows/build-data.yml
// (and by hand after a big patch: Actions > Build data > Run workflow).
//
// For every hero: the builds the top 1000 players of each region and the hero's top 50 stand
// behind (see core.js for the answer's shape):
//   - builds they ran in ranked games over the last 45 days (from deadlock-api's data lake, one scan:
//     pro_usage.py), with recency-weighted games and how much of each build they actually bought
//   - builds they wrote, edited in the last 90 days (deadlock-api's builds search, per author)
// If the data lake can't be read, per-group totals from the REST analytics endpoint are used instead.
//
// Usage: node build_data.js <output folder> [cache folder]      (Node 18+; python3 + duckdb for the lake)
"use strict";
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const core = require("./core.js");

const OUT = path.resolve(process.argv[2] || "_site");
const CACHE = path.resolve(process.argv[3] || "cache");
const C = core.config;
const BUILD_REFETCH_S = 20 * 3600;  // cached builds are re-read once a day (they can be edited)
const MISSING_RETRY_S = 3 * 86400;  // a build the API didn't return is asked again after this
const AUTHORS_REFETCH_S = 20 * 3600;
const MAX_FETCHES = 8000;           // build and author requests per run; the rest wait for the next run
const LAKE_TIMEOUT_MS = 40 * 60 * 1000;

// Night Shift regulars from dlns-stats.co.uk's public API (https://dlns-stats.co.uk/api/docs): everyone
// who played in at least NS_MIN_WEEKS of the last NS_WEEKS weekly events, with their region. One
// request a second, once a day; the last good list is kept in the cache in case the site is down.
const NS_SITE = "https://dlns-stats.co.uk";
const NS_WEEKS = 20, NS_MIN_WEEKS = 3;
const UA = "builds-improved-relay/1.0 (Deadlock mod data; +https://github.com/alimatrix18/builds-improved-relay)";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function nsGet(p) {
    const r = await fetch(NS_SITE + p, { headers: { "User-Agent": UA, Accept: "application/json" } });
    if (!r.ok) throw new Error(p + " -> HTTP " + r.status);
    return r.json();
}
async function nightShiftRegulars() {
    if (process.env.BI_NO_DLNS) return { players: [], matches: [] };                    // tests
    const saved = path.join(CACHE, "nightshift_auto.json");
    try {
        const index = await nsGet("/db/nightshift/index?event_title=Night%20Shift");
        const weeks = (index.weeks || []).map((w) => w.week).filter((w) => w > 0).sort((a, b) => a - b).slice(-NS_WEEKS);
        const seen = {}, matches = [];                                                  // account -> { name, region, weeks: {} }
        for (const w of weeks) {
            try {
                const d = await nsGet("/db/nightshift/" + w + "?event_title=Night%20Shift");
                (d.matches || []).forEach((m) => {
                    if (!m.match_id) return;
                    const st = typeof m.start_time === "number" ? m.start_time : Math.floor(Date.parse(m.start_time || "") / 1000) || 0;
                    matches.push({ match_id: m.match_id, start: st });
                });
                (d.matches || []).forEach((m) => (m.players || []).forEach((p) => {
                    if (!p.account_id) return;
                    const x = seen[p.account_id] || (seen[p.account_id] = { name: p.persona_name, region: m.event_region || null, weeks: {} });
                    x.weeks[w] = true;
                    x.name = p.persona_name || x.name;
                    if (m.event_region) x.region = m.event_region;
                }));
            } catch (e) { console.warn("Night Shift week " + w + " skipped: " + e.message); }
            await sleep(1000);
        }
        const list = Object.keys(seen).map((id) => ({ id: Number(id), name: seen[id].name, region: seen[id].region, weeks: Object.keys(seen[id].weeks).length }))
            .filter((x) => x.weeks >= NS_MIN_WEEKS);
        const result = { players: list, matches };
        if (list.length) fs.writeFileSync(saved, JSON.stringify(result));
        console.log(`${list.length} Night Shift regulars (${NS_MIN_WEEKS}+ of the last ${weeks.length} weeks, ${matches.length} matches, dlns-stats.co.uk)`);
        return result;
    } catch (e) {
        let old = readJson(saved, { players: [], matches: [] });
        if (Array.isArray(old)) old = { players: old, matches: [] };                 // older cache format
        console.warn(`dlns-stats.co.uk unavailable (${e.message}); using the last list (${old.players.length} players)`);
        return old;
    }
}

function write(name, value) { fs.writeFileSync(path.join(OUT, "data", name + ".json"), JSON.stringify(value)); }
function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch (e) { return fallback; } }
const nowS = () => Math.floor(Date.now() / 1000);

function python(args) {
    execFileSync("python3", [path.join(__dirname, "pro_usage.py")].concat(args), { stdio: "inherit", timeout: LAKE_TIMEOUT_MS });
}

// Who ran what: { rows: [[hero, account, build, games, last, weighted x100], ...] } from the data lake, or null.
function lakeUsage(accounts, nsMatches) {
    if (process.env.BI_USAGE_FILE) return readJson(process.env.BI_USAGE_FILE, null);   // tests
    const tracked = path.join(CACHE, "tracked.json"), out = path.join(CACHE, "usage.json");
    fs.writeFileSync(tracked, JSON.stringify(accounts));
    const nsFile = path.join(CACHE, "ns_matches.json");
    fs.writeFileSync(nsFile, JSON.stringify(nsMatches || []));
    try {
        python(["usage", tracked, out, path.join(CACHE, "matches.parquet"), String(C.DAYS), String(C.HALF_LIFE_DAYS), nsFile]);
        return readJson(out, null);
    } catch (e) {
        console.warn("data lake scan failed, using per-group totals instead: " + e.message);
        return null;
    }
}

// Built %: { "account:build": pct } from the matches the usage step saved, or {}.
function lakeBuilt(buildItems, shopIds) {
    if (process.env.BI_BUILT_FILE) {                                                      // tests
        const out = {};
        (readJson(process.env.BI_BUILT_FILE, { rows: [] }).rows || []).forEach((r) => { out[r[0] + ":" + r[1]] = r[2]; });
        return out;
    }
    const bi = path.join(CACHE, "build_items.json"), shop = path.join(CACHE, "shop_items.json"), out = path.join(CACHE, "built.json");
    fs.writeFileSync(bi, JSON.stringify(buildItems));
    fs.writeFileSync(shop, JSON.stringify(shopIds));
    try {
        python(["built", path.join(CACHE, "matches.parquet"), bi, shop, out]);
        const res = {};
        (readJson(out, { rows: [] }).rows || []).forEach((r) => { res[r[0] + ":" + r[1]] = r[2]; });
        return res;
    } catch (e) {
        console.warn("built % skipped: " + e.message);
        return {};
    }
}

// Requests whose answers are kept between runs (actions/cache): builds by id, and each author's builds.
function store() {
    const file = path.join(CACHE, "store.json"), data = readJson(file, { builds: {}, authors: {} });
    let fetched = 0;
    const t = nowS();
    function cachedCall(table, key, maxAge, emptyMaxAge, call) {
        const hit = data[table][key];
        if (hit && t - hit.at < (hit.v ? maxAge : emptyMaxAge)) return Promise.resolve(hit.v || null);
        if (fetched >= MAX_FETCHES) return Promise.resolve(hit ? hit.v || null : null);
        fetched++;
        return call().then((v) => { data[table][key] = { at: t, v: v || null }; return v || null; },
                           () => (hit ? hit.v || null : null));
    }
    return {
        build: (id) => cachedCall("builds", id, BUILD_REFETCH_S, MISSING_RETRY_S, () => core.fetchBuild(id)),
        authored: (id) => cachedCall("authors", id, AUTHORS_REFETCH_S, AUTHORS_REFETCH_S,
                                     () => core.authoredBuilds(id).then((l) => (l.length ? l : null))),
        save() { fs.writeFileSync(file, JSON.stringify(data)); return fetched; }
    };
}

(async () => {
    fs.mkdirSync(path.join(OUT, "data"), { recursive: true });
    fs.mkdirSync(CACHE, { recursive: true });
    const started = Date.now(), generatedAt = Math.floor(started / 1000);

    const heroes = await core.heroes();                  // no heroes = nothing to build: fail the run
    write("heroes", { generatedAt, data: heroes });
    let items = {};
    try { items = await core.items(); write("items", { generatedAt, data: items }); }
    catch (e) { console.warn("items skipped: " + e.message); }

    const [boards, patchAt] = await Promise.all([core.overallBoards(), core.latestPatch()]);
    const heroBoardsById = {};
    await core.pool(heroes, 4, async (h) => { heroBoardsById[h.id] = await core.heroBoards(h.id); });
    const players = core.roster(boards, heroBoardsById);
    // Night Shift players, listed by hand in nightshift.json: { "players": [{ "id": <Steam account id>, "name": "..." }] }
    // Night Shift players: regulars from dlns-stats.co.uk, plus any listed by hand in nightshift.json
    // ({ "players": [{ "id": <Steam account id>, "name": "..." }] })
    const nsById = {}, ns = await nightShiftRegulars();
    ns.players.forEach((x) => { nsById[x.id] = x; });
    (readJson(process.env.BI_NIGHTSHIFT_FILE || path.join(__dirname, "nightshift.json"), { players: [] }).players || [])
        .forEach((x) => { const id = Number(x.id); if (id > 0) nsById[id] = Object.assign({ id, region: null }, nsById[id] || {}, { name: x.name || (nsById[id] || {}).name || null }); });
    const nsList = Object.values(nsById);
    nsList.forEach((x) => players.push({ key: "NS|" + x.id, name: x.name, r: x.region || null, p: 0, m: {}, ids: [x.id], ns: true }));
    if (nsList.length) console.log(`${nsList.length} Night Shift players in all`);
    console.log(`${players.length} leaderboard players (top ${C.TOP} per region + top ${C.TOP_HERO} per hero)`);

    // 1. who ran what, and who the tracked players are
    const lake = lakeUsage(core.unique([].concat(...players.map((x) => x.ids))), ns.matches);
    const people = {};                                   // account -> leaderboard player
    const games = {};
    if (lake) lake.rows.forEach((r) => { games[r[1]] = (games[r[1]] || 0) + r[3]; });
    players.forEach((x) => {
        // a name matching several accounts: the account that played (most games) is the player;
        // without match data such names are skipped
        const id = x.ids.length === 1 ? x.ids[0] : x.ids.slice().sort((a, b) => (games[b] || 0) - (games[a] || 0))[0];
        if (x.ids.length > 1 && !games[id]) return;
        if (x.ns) { if (people[id]) people[id].ns = true; else people[id] = x; return; }   // a listed player who is also ranked
        const ns = people[id] && people[id].ns;
        if (!people[id] || people[id].key.startsWith("NS|") || (x.p && (!people[id].p || x.p < people[id].p))) people[id] = Object.assign({}, x, ns ? { ns: true } : {});
    });
    const tracked = (x, heroId) => x.p || x.m[heroId] || x.ns;
    const person = (id, heroId) => {
        const x = people[id], o = { n: x.name, av: null, r: x.r, p: x.p || 0, m: x.m[heroId] || 0 };
        if (x.ns) o.ns = 1;
        return o;
    };

    // 2. builds the tracked players wrote, by hero
    const st = store(), written = {};                    // heroId -> [build]
    const authorIds = Object.keys(people);
    const lists = await core.pool(authorIds, 6, (id) => st.authored(id));
    lists.forEach((list) => (list || []).forEach((b) => { (written[b.h] = written[b.h] || []).push(b); }));

    // 3. per hero: the answer before folding, and its builds
    const drafts = [];
    for (const h of heroes) {
        try {
            let answer;
            if (lake) {
                const ppl = {}, uses = [];
                lake.rows.forEach((r) => {
                    const x = people[r[1]];
                    if (r[0] !== h.id || !x || !tracked(x, h.id)) return;
                    uses.push([r[1], r[2], r[3], r[4], r[5], -1]);
                    ppl[r[1]] = person(r[1], h.id);
                });
                // the builds Night Shift players loaded in Night Shift matches: [[account, build, games, last], ...]
                const nsUses = [];
                (lake.ns_rows || []).forEach((r) => {
                    const x = people[r[1]];
                    if (r[0] !== h.id || !x || !x.ns) return;
                    nsUses.push([r[1], r[2], r[3], r[4]]);
                    ppl[r[1]] = person(r[1], h.id);
                });
                answer = { v: 3, hero: h.id, generatedAt, patchAt, days: C.DAYS, halfLife: C.HALF_LIFE_DAYS, source: "players",
                           nightShift: nsList.length, people: ppl, uses, nsUses, builds: {} };
            } else {
                answer = await core.heroGroups(h.id, { boards, heroBoards: heroBoardsById[h.id], patchAt });
            }
            // written builds: authors placed best (overall, then on this hero) first
            const rank = (a) => { const x = people[a]; return x.p ? x.p : x.ns ? 1500 : 2000 + (x.m[h.id] || 999); };
            const mine = (written[h.id] || []).filter((b) => people[b.a] && tracked(people[b.a], h.id))
                .sort((x, y) => rank(x.a) - rank(y.a)).slice(0, C.MAX_AUTHORED);
            const known = {};
            mine.forEach((b) => { known[b.id] = Object.assign({}, b, { w: 1 }); answer.people[b.a] = answer.people[b.a] || person(b.a, h.id); });
            const builds = await core.fetchBuilds(core.candidates(answer, mine.map((b) => b.id)), h.id, st.build, known);
            drafts.push({ h, answer, builds });
        } catch (e) {
            drafts.push({ h, error: e.message });
        }
    }

    // 4. built %: how much of each build its players actually bought
    if (lake) {
        const buildItems = {};
        drafts.forEach((d) => d.builds && Object.values(d.builds).forEach((b) => {
            buildItems[b.id] = core.unique([].concat(...(b.c || []).map((c) => c.m)));
        }));
        const built = lakeBuilt(buildItems, Object.keys(items).map(Number));
        drafts.forEach((d) => d.answer && d.answer.uses && d.answer.uses.forEach((u) => {
            const p = built[u[0] + ":" + u[1]];
            if (p !== undefined) u[5] = p;
        }));
    }

    // 5. fold copies, add names and avatars, write
    let ok = 0;
    const failed = [], known = {};
    for (const d of drafts) {
        if (d.error) { failed.push(d.h.name + ": " + d.error); continue; }
        try {
            const answer = core.fold(d.answer, d.builds);
            // tracked authors of the kept builds carry their places and tags
            Object.values(answer.builds).forEach((b) => {
                if (people[b.a] && tracked(people[b.a], d.h.id)) answer.people[b.a] = Object.assign(answer.people[b.a] || {}, person(b.a, d.h.id));
            });
            await core.addProfiles(answer, known);
            write("hero_" + d.h.id, answer);
            ok++;
        } catch (e) {
            failed.push(d.h.name + ": " + e.message);
        }
    }
    const fetched = st.save();

    write("meta", { generatedAt, heroes: ok, failed, source: lake ? "players" : "groups", requests: fetched,
                    seconds: Math.round((Date.now() - started) / 1000) });
    console.log(`built ${ok}/${heroes.length} heroes (${lake ? "per player" : "per group"}), ${fetched} build/author requests, ` +
                `${Math.round((Date.now() - started) / 1000)}s`);
    failed.forEach((f) => console.warn("failed " + f));
    if (!ok) process.exit(1);                            // keep the previous site rather than an empty one
})().catch((e) => { console.error(e); process.exit(1); });

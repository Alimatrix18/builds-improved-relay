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

function write(name, value) { fs.writeFileSync(path.join(OUT, "data", name + ".json"), JSON.stringify(value)); }
function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch (e) { return fallback; } }
const nowS = () => Math.floor(Date.now() / 1000);

function python(args) {
    execFileSync("python3", [path.join(__dirname, "pro_usage.py")].concat(args), { stdio: "inherit", timeout: LAKE_TIMEOUT_MS });
}

// Who ran what: { rows: [[hero, account, build, games, last, weighted x100], ...] } from the data lake, or null.
function lakeUsage(accounts) {
    if (process.env.BI_USAGE_FILE) return readJson(process.env.BI_USAGE_FILE, null);   // tests
    const tracked = path.join(CACHE, "tracked.json"), out = path.join(CACHE, "usage.json");
    fs.writeFileSync(tracked, JSON.stringify(accounts));
    try {
        python(["usage", tracked, out, path.join(CACHE, "matches.parquet"), String(C.DAYS), String(C.HALF_LIFE_DAYS)]);
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
    console.log(`${players.length} leaderboard players (top ${C.TOP} per region + top ${C.TOP_HERO} per hero)`);

    // 1. who ran what, and who the tracked players are
    const lake = lakeUsage(core.unique([].concat(...players.map((x) => x.ids))));
    const people = {};                                   // account -> leaderboard player
    const games = {};
    if (lake) lake.rows.forEach((r) => { games[r[1]] = (games[r[1]] || 0) + r[3]; });
    players.forEach((x) => {
        // a name matching several accounts: the account that played (most games) is the player;
        // without match data such names are skipped
        const id = x.ids.length === 1 ? x.ids[0] : x.ids.slice().sort((a, b) => (games[b] || 0) - (games[a] || 0))[0];
        if (x.ids.length > 1 && !games[id]) return;
        if (!people[id] || (x.p && (!people[id].p || x.p < people[id].p))) people[id] = x;
    });
    const person = (id, heroId) => { const x = people[id]; return { n: x.name, av: null, r: x.r, p: x.p || 0, m: x.m[heroId] || 0 }; };

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
                    if (r[0] !== h.id || !x || !(x.p || x.m[h.id])) return;
                    uses.push([r[1], r[2], r[3], r[4], r[5], -1]);
                    ppl[r[1]] = person(r[1], h.id);
                });
                answer = { v: 3, hero: h.id, generatedAt, patchAt, days: C.DAYS, halfLife: C.HALF_LIFE_DAYS, source: "players",
                           people: ppl, uses, builds: {} };
            } else {
                answer = await core.heroGroups(h.id, { boards, heroBoards: heroBoardsById[h.id], patchAt });
            }
            // written builds: authors placed best (overall, then on this hero) first
            const rank = (a) => { const x = people[a]; return x.p ? x.p : 2000 + (x.m[h.id] || 999); };
            const mine = (written[h.id] || []).filter((b) => people[b.a] && (people[b.a].p || people[b.a].m[h.id]))
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
            const answer = await core.addProfiles(core.fold(d.answer, d.builds), known);
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

// Builds the prebuilt data files the relay page serves. Run hourly by .github/workflows/build-data.yml.
//
// For every hero: the builds the top 1000 players of each region (and the hero's top 50 mains)
// actually played in ranked games over the last 14 days, one row per player and build
// (see core.js for the answer's shape). Who played what comes from deadlock-api's data lake in
// one scan (pro_usage.py); if that fails, per-group totals from the REST analytics endpoint.
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
const BUILD_REFETCH_S = 6 * 3600;   // cached builds are re-read this often (they can be edited)
const MISSING_RETRY_S = 24 * 3600;  // a build the API didn't return is asked again after this
const MAX_BUILD_FETCHES = 2500;     // per run; the rest wait for the next run
const LAKE_TIMEOUT_MS = 25 * 60 * 1000;

function write(name, value) { fs.writeFileSync(path.join(OUT, "data", name + ".json"), JSON.stringify(value)); }
function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch (e) { return fallback; } }

// Who played what: [[hero, account, build, games, last], ...] from the data lake, or null.
function lakeUsage(accounts) {
    const tracked = path.join(CACHE, "tracked.json"), out = path.join(CACHE, "usage.json");
    fs.writeFileSync(tracked, JSON.stringify(accounts));
    if (process.env.BI_USAGE_FILE) return readJson(process.env.BI_USAGE_FILE, null);   // tests
    try {
        execFileSync("python3", [path.join(__dirname, "pro_usage.py"), tracked, out, String(C.DAYS)],
                     { stdio: "inherit", timeout: LAKE_TIMEOUT_MS });
        return readJson(out, null);
    } catch (e) {
        console.warn("data lake scan failed, using per-group totals instead: " + e.message);
        return null;
    }
}

// Builds are fetched one by one, so they are kept between runs (actions/cache).
function buildStore() {
    const file = path.join(CACHE, "builds.json"), store = readJson(file, {});
    let fetched = 0;
    const t = Math.floor(Date.now() / 1000);
    return {
        get(id) {
            const hit = store[id];
            if (hit && (hit.b ? t - hit.at < BUILD_REFETCH_S : t - hit.at < MISSING_RETRY_S)) return Promise.resolve(hit.b || null);
            if (fetched >= MAX_BUILD_FETCHES) return Promise.resolve(hit ? hit.b || null : null);
            fetched++;
            return core.fetchBuild(id).then(
                (b) => { store[id] = { at: t, b: b || null }; return b; },
                () => (hit ? hit.b || null : null));
        },
        save() { fs.writeFileSync(file, JSON.stringify(store)); return fetched; }
    };
}

(async () => {
    fs.mkdirSync(path.join(OUT, "data"), { recursive: true });
    fs.mkdirSync(CACHE, { recursive: true });
    const started = Date.now(), generatedAt = Math.floor(started / 1000);

    const heroes = await core.heroes();                  // no heroes = nothing to build: fail the run
    write("heroes", { generatedAt, data: heroes });
    try { write("items", { generatedAt, data: await core.items() }); }
    catch (e) { console.warn("items skipped: " + e.message); }

    const [boards, patchAt] = await Promise.all([core.overallBoards(), core.latestPatch()]);
    const heroBoardsById = {};
    await core.pool(heroes, 4, async (h) => { heroBoardsById[h.id] = await core.heroBoards(h.id); });
    const players = core.roster(boards, heroBoardsById);
    console.log(`${players.length} leaderboard players (top ${C.TOP} per region + top ${C.TOP_HERO} per hero)`);

    // 1. who played what
    const lake = lakeUsage(core.unique([].concat(...players.map((x) => x.ids))));
    const people = {};                                   // account -> leaderboard player
    if (lake) {
        // a name matching several accounts: the account that played (most games) is the player
        const games = {};
        lake.rows.forEach((r) => { games[r[1]] = (games[r[1]] || 0) + r[3]; });
        players.forEach((x) => {
            const id = x.ids.length === 1 ? x.ids[0] : x.ids.slice().sort((a, b) => (games[b] || 0) - (games[a] || 0))[0];
            if (x.ids.length > 1 && !games[id]) return;
            if (!people[id] || (x.p && (!people[id].p || x.p < people[id].p))) people[id] = x;
        });
    }

    const builds = buildStore(), known = {};
    let ok = 0;
    const failed = [];
    for (const h of heroes) {
        try {
            let answer;
            if (lake) {
                const ppl = {}, uses = [];
                lake.rows.forEach((r) => {
                    const x = people[r[1]];
                    if (r[0] !== h.id || !x || !(x.p || x.m[h.id])) return;
                    uses.push([r[1], r[2], r[3], r[4]]);
                    ppl[r[1]] = { n: x.name, av: null, r: x.r, p: x.p || 0, m: x.m[h.id] || 0 };
                });
                answer = { v: 2, hero: h.id, generatedAt, patchAt, days: C.DAYS, source: "players", people: ppl, uses, builds: {} };
            } else {
                answer = await core.heroGroups(h.id, { boards, heroBoards: heroBoardsById[h.id], patchAt });
            }
            answer = await core.finishAnswer(answer, (id) => builds.get(id));
            answer = await core.addProfiles(answer, known);
            write("hero_" + h.id, answer);
            ok++;
        } catch (e) {
            failed.push(h.name + ": " + e.message);
        }
    }
    const fetched = builds.save();

    write("meta", { generatedAt, heroes: ok, failed, source: lake ? "players" : "groups", buildsFetched: fetched,
                    seconds: Math.round((Date.now() - started) / 1000) });
    console.log(`built ${ok}/${heroes.length} heroes (${lake ? "per player" : "per group"}), ${fetched} builds fetched, ` +
                `${Math.round((Date.now() - started) / 1000)}s`);
    failed.forEach((f) => console.warn("failed " + f));
    if (!ok) process.exit(1);                            // keep the previous site rather than an empty one
})().catch((e) => { console.error(e); process.exit(1); });

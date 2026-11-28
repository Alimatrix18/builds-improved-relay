// Builds the prebuilt data files the relay page serves. Run daily by .github/workflows/build-data.yml
// (and by hand after a big patch: Actions > Build data > Run workflow).
//
// For every hero: the builds the top 1000 players of each region and the hero's top 50 stand
// behind (see core.js for the answer's shape):
//   - builds they ran in ranked games over the last 45 days (from deadlock-api's data lake, one scan:
//     pro_usage.py), with recency-weighted games, USAGE totals, off-build items, swaps and skipped items
//   - builds they wrote, edited in the last 90 days (deadlock-api's builds search, per author)
//   - Night Shift regulars (dlns-stats.co.uk) and the builds they loaded in Night Shift matches
// If the data lake can't be read, the run fails, so the site keeps yesterday's (per-player) files
// rather than publishing much thinner per-group ones. BI_ALLOW_GROUPS=1 publishes those instead
// (e.g. for a first run).
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
// Cached builds are re-read every few days (they can be edited, and favourites change). Builds by
// tracked players are refreshed daily anyway, from their authors' lists.
const BUILD_REFETCH_S = 3 * 86400;
const STORE_DROP_S = 30 * 86400;    // cached answers not used for this long are dropped
const MISSING_RETRY_S = 3 * 86400;  // a build the API didn't return is asked again after this
const AUTHORS_REFETCH_S = 20 * 3600;
const MAX_FETCHES = 8000;           // build and author requests per run; the rest wait for the next run
const LAKE_TIMEOUT_MS = 60 * 60 * 1000;  // a full rescan reads ~600 files in batches
// After a big patch, a player's numbers on a hero count only games since the patch once they have
// this many there; until then their whole window counts (so a fresh patch doesn't empty the list).
const PATCH_MIN_GAMES = 5;
// An author whose builds for this many heroes were all updated within BULK_SPAN_S is a bulk
// publisher (a site or tool generating builds): their builds rank with untrusted ones.
const BULK_HEROES = 20, BULK_SPAN_S = 3 * 86400;
// An author's build counts as a top player's build on a hero only if they play that hero: 20+ ranked
// games on it in the window (about what unlocking a hero for ranked takes: 15 wins and 5 losses),
// or 3+ Night Shift games on it, or a place in its top 50. Players on trusted.json are exempt.
const AUTHOR_MIN_GAMES = 20, AUTHOR_MIN_NS_GAMES = 3;

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
                    if (!(Number(m.match_id) > 0)) return;                                 // real match ids only
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

// Who ran what: { rows: [[hero, account, build, games, last, weighted x100, games since patch,
// weighted since patch x100], ...], totals: [[hero, account, games, games since patch], ...] }
// from the data lake, or null.
function lakeUsage(accounts, nsMatches, patchAt) {
    if (process.env.BI_USAGE_FILE) return readJson(process.env.BI_USAGE_FILE, null);   // tests
    const tracked = path.join(CACHE, "tracked.json"), out = path.join(CACHE, "usage.json");
    fs.writeFileSync(tracked, JSON.stringify(accounts));
    const nsFile = path.join(CACHE, "ns_matches.json");
    fs.writeFileSync(nsFile, JSON.stringify(nsMatches || []));
    try {
        python(["usage", tracked, out, path.join(CACHE, "matches.parquet"), String(C.DAYS), String(C.HALF_LIFE_DAYS), nsFile, String(patchAt || 0)]);
        return readJson(out, null);
    } catch (e) {
        console.warn("data lake scan failed: " + e.message);
        return null;
    }
}

// Built %, off-build items, skipped build items, swaps and finished items:
// { "account:build": [pct, off per game x10, [[item, % of games], ...], [[item, %], ...], [[item, instead, %], ...],
//                     [[item, % of games, in build 0/1], ...]] }
// from the matches the usage step saved, or {}.
function lakeBuilt(buildItems, shopIds) {
    const keyed = (rows) => {
        const out = {};
        (rows || []).forEach((r) => { out[r[0] + ":" + r[1]] = [r[2], r[3] !== undefined ? r[3] : -1, r[4] || [], r[5] || [], r[6] || [], r[7] || []]; });
        return out;
    };
    if (process.env.BI_BUILT_FILE) return keyed(readJson(process.env.BI_BUILT_FILE, { rows: [] }).rows);   // tests
    const bi = path.join(CACHE, "build_items.json"), shop = path.join(CACHE, "shop_items.json"), out = path.join(CACHE, "built.json");
    fs.writeFileSync(bi, JSON.stringify(buildItems));
    fs.writeFileSync(shop, JSON.stringify(shopIds));
    try {
        python(["built", path.join(CACHE, "matches.parquet"), bi, shop, out]);
        return keyed(readJson(out, { rows: [] }).rows);
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
        if (hit) hit.used = t;
        if (hit && t - hit.at < (hit.v ? maxAge : emptyMaxAge)) return Promise.resolve(hit.v || null);
        if (fetched >= MAX_FETCHES) return Promise.resolve(hit ? hit.v || null : null);
        fetched++;
        return call().then((v) => { data[table][key] = { at: t, v: v || null, used: t }; return v || null; },
                           () => (hit ? hit.v || null : null));
    }
    return {
        build: (id) => cachedCall("builds", id, BUILD_REFETCH_S, MISSING_RETRY_S, () => core.fetchBuild(id)),
        authored: (id) => cachedCall("authors", id, AUTHORS_REFETCH_S, AUTHORS_REFETCH_S,
                                     () => core.authoredBuilds(id).then((l) => (l.length ? l : null))),
        // a build seen in an author's list is the latest version: cache it as fetched now
        remember(b) { if (b && b.id) data.builds[b.id] = { at: t, v: b, used: t }; },
        save() {
            // entries no run has used for STORE_DROP_S: their players or builds are gone
            ["builds", "authors"].forEach((table) => Object.keys(data[table]).forEach((k) => {
                const e = data[table][k];
                if (t - (e.used || e.at) > STORE_DROP_S) delete data[table][k];
            }));
            fs.writeFileSync(file, JSON.stringify(data));
            return fetched;
        }
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
    // trusted.json: { "players": [{ "id", "name", "note" }], "blocked": [{ "id", "note" }] }
    // players: trusted by hand (coaches, known creators), ranked just below top players;
    // blocked: builds by these accounts always rank with untrusted ones.
    const trustedFile = readJson(process.env.BI_TRUSTED_FILE || path.join(__dirname, "trusted.json"), {});
    const trustedById = {}, blocked = {};
    (trustedFile.players || []).forEach((x) => { const id = Number(x.id); if (id > 0) trustedById[id] = x; });
    (trustedFile.blocked || []).forEach((x) => { const id = Number(x.id || x); if (id > 0) blocked[id] = x.note || "blocked by hand"; });
    Object.values(trustedById).forEach((x) => players.push({ key: "TR|" + x.id, name: x.name || null, r: null, p: 0, m: {}, ids: [Number(x.id)], t: true }));
    if (Object.keys(trustedById).length || Object.keys(blocked).length)
        console.log(`${Object.keys(trustedById).length} trusted and ${Object.keys(blocked).length} blocked accounts (trusted.json)`);
    nsList.forEach((x) => players.push({ key: "NS|" + x.id, name: x.name, r: x.region || null, p: 0, m: {}, ids: [x.id], ns: true }));
    if (nsList.length) console.log(`${nsList.length} Night Shift players in all`);
    console.log(`${players.length} leaderboard players (top ${C.TOP} per region + top ${C.TOP_HERO} per hero)`);

    // 1. who ran what, and who the tracked players are
    const lake = lakeUsage(core.unique([].concat(...players.map((x) => x.ids))), ns.matches, patchAt);
    if (!lake && !process.env.BI_ALLOW_GROUPS) {
        console.error("no match data: stopping so the site keeps the last good files (BI_ALLOW_GROUPS=1 to publish per-group data)");
        return process.exit(1);
    }
    const people = {};                                   // account -> leaderboard player
    const games = {};
    // every ranked game counts, build loaded or not (older data: games with a build)
    if (lake) (lake.totals && lake.totals.length ? lake.totals.map((r) => [r[0], r[1], 0, r[2]]) : lake.rows)
        .forEach((r) => { games[r[1]] = (games[r[1]] || 0) + r[3]; });
    players.forEach((x) => {
        // a name matching several accounts: the account that played (most games) is the player;
        // without match data such names are skipped
        const id = x.ids.length === 1 ? x.ids[0] : x.ids.slice().sort((a, b) => (games[b] || 0) - (games[a] || 0))[0];
        if (x.ids.length > 1 && !games[id]) return;
        if (x.ns || x.t) {                                // a listed player who may also be ranked
            if (people[id]) { if (x.ns) people[id].ns = true; if (x.t) people[id].t = true; }
            else people[id] = Object.assign({}, x);
            return;
        }
        const prev = people[id] || {};
        // the same account under two leaderboard names or regions: keep the better overall place, and
        // every hero place from both
        if (!people[id] || /^(NS|TR)\|/.test(prev.key) || (x.p && (!prev.p || x.p < prev.p)))
            people[id] = Object.assign({}, x, prev.ns ? { ns: true } : {}, prev.t ? { t: true } : {});
        const m = Object.assign({}, x.m);
        Object.keys(prev.m || {}).forEach((h) => { if (!m[h] || prev.m[h] < m[h]) m[h] = prev.m[h]; });
        people[id].m = m;
    });
    const tracked = (x, heroId) => x.p || x.m[heroId] || x.ns || x.t;
    const flags = {};                                    // account -> why their builds aren't trusted
    const playedOn = {};                                 // heroId -> { account: [ranked games, Night Shift games] }
    const person = (id, heroId) => {
        const x = people[id], o = { n: x.name, av: null, r: x.r, p: x.p || 0, m: x.m[heroId] || 0 };
        if (x.ns) o.ns = 1;
        if (x.t) o.t = 1;
        if (flags[id]) o.x = flags[id];
        // q: plays this hero enough for their builds of it to count (hg / nsg: their games on it)
        const g = (playedOn[heroId] && playedOn[heroId][id]) || [0, 0];
        o.hg = g[0]; o.nsg = g[1];
        o.q = !lakeKnown || o.m || x.t || g[0] >= AUTHOR_MIN_GAMES || g[1] >= AUTHOR_MIN_NS_GAMES ? 1 : 0;
        return o;
    };
    let lakeKnown = false;

    // 2. builds the tracked players wrote, by hero
    const st = store(), written = {};                    // heroId -> [build]
    const authorIds = Object.keys(people);
    const lists = await core.pool(authorIds, 6, (id) => st.authored(id));
    lists.forEach((list) => (list || []).forEach((b) => { (written[b.h] = written[b.h] || []).push(b); st.remember(b); }));
    // bulk publishers: builds for BULK_HEROES+ heroes all updated within a few days
    authorIds.forEach((id, i) => {
        if (people[id].t) return;
        const byHero = {};
        (lists[i] || []).forEach((b) => { byHero[b.h] = Math.max(byHero[b.h] || 0, b.u || 0); });
        const times = Object.values(byHero).filter(Boolean).sort((a, b) => a - b);
        for (let lo = 0, hi = 0; hi < times.length; hi++) {
            while (times[hi] - times[lo] > BULK_SPAN_S) lo++;
            if (hi - lo + 1 >= BULK_HEROES) { flags[id] = "bulk:" + times.length; break; }
        }
    });
    Object.keys(blocked).forEach((id) => { flags[id] = "blocked:" + blocked[id]; });
    if (Object.keys(flags).length) console.log(`${Object.keys(flags).length} authors not trusted (bulk publishers or blocked)`);

    // 3. per hero: the answer before folding, and its builds
    // the match data's rows, grouped by hero once (instead of scanning every row for every hero)
    const lakeBy = {};
    ["rows", "totals", "ns_rows", "ns_totals"].forEach((k) => {
        lakeBy[k] = {};
        ((lake && lake[k]) || []).forEach((r) => { (lakeBy[k][r[0]] = lakeBy[k][r[0]] || []).push(r); });
    });
    // every tracked player's games per hero in the window (build loaded or not), for authors' "q"
    lakeKnown = !!(lake && lake.totals && lake.totals.length);
    (lake && lake.totals || []).forEach((r) => { ((playedOn[r[0]] = playedOn[r[0]] || {})[r[1]] = playedOn[r[0]][r[1]] || [0, 0])[0] = r[2]; });
    (lake && lake.ns_totals || []).forEach((r) => { ((playedOn[r[0]] = playedOn[r[0]] || {})[r[1]] = playedOn[r[0]][r[1]] || [0, 0])[1] = r[2]; });
    // a few heroes at a time (their builds are fetched in parallel too); drafts keep the heroes' order
    const drafts = await core.pool(heroes, 3, async (h) => {
        try {
            let answer;
            if (lake) {
                const ppl = {}, uses = [];
                // players with PATCH_MIN_GAMES+ games on the hero since the patch: only those games count
                const sincePatch = {};
                (lakeBy.totals[h.id] || []).forEach((r) => { if (patchAt && r[3] !== undefined && r[3] >= PATCH_MIN_GAMES) sincePatch[r[1]] = true; });
                (lakeBy.rows[h.id] || []).forEach((r) => {
                    const x = people[r[1]];
                    if (!x || !tracked(x, h.id)) return;
                    if (sincePatch[r[1]]) {
                        if (!r[6]) return;                       // not played since the patch
                        uses.push([r[1], r[2], r[6], r[4], r[7], -1, -1, [], [], [], []]);
                    } else uses.push([r[1], r[2], r[3], r[4], r[5], -1, -1, [], [], [], []]);
                    ppl[r[1]] = person(r[1], h.id);
                    if (sincePatch[r[1]]) ppl[r[1]].sp = 1;
                });
                // the builds Night Shift players loaded in Night Shift matches: [[account, build, games, last, weighted x100], ...]
                const nsUses = [];
                (lakeBy.ns_rows[h.id] || []).forEach((r) => {
                    const x = people[r[1]];
                    if (!x || !x.ns) return;
                    nsUses.push([r[1], r[2], r[3], r[4], r[5] !== undefined ? r[5] : r[3] * 100]);
                    ppl[r[1]] = person(r[1], h.id);
                });
                // every ranked / Night Shift game they played on the hero, build loaded or not (USAGE)
                const heroGames = {}, nsHeroGames = {};
                (lakeBy.totals[h.id] || []).forEach((r) => { if (ppl[r[1]]) heroGames[r[1]] = sincePatch[r[1]] ? r[3] : r[2]; });
                (lakeBy.ns_totals[h.id] || []).forEach((r) => { if (ppl[r[1]]) nsHeroGames[r[1]] = r[2]; });
                answer = { v: 3, hero: h.id, generatedAt, patchAt, days: C.DAYS, halfLife: C.HALF_LIFE_DAYS, source: "players",
                           nightShift: nsList.length, people: ppl, uses, nsUses, heroGames, nsHeroGames, builds: {} };
            } else {
                answer = await core.heroGroups(h.id, { boards, heroBoards: heroBoardsById[h.id], patchAt });
            }
            // written builds: authors placed best (overall, then on this hero) first
            const rank = (a) => { const x = people[a]; return x.p ? x.p : x.ns ? 1500 : 2000 + (x.m[h.id] || 999); };
            // (not bulk publishers' or blocked accounts': those are never shown unless someone runs them)
            // (only authors who play this hero: a build of a hero they rarely play is listed only if someone runs it)
            const mine = (written[h.id] || []).filter((b) => people[b.a] && tracked(people[b.a], h.id) && !flags[b.a] && person(b.a, h.id).q)
                .sort((x, y) => rank(x.a) - rank(y.a)).slice(0, C.MAX_AUTHORED);
            const known = {};
            mine.forEach((b) => { known[b.id] = Object.assign({}, b, { w: 1 }); answer.people[b.a] = answer.people[b.a] || person(b.a, h.id); });
            const builds = await core.fetchBuilds(core.candidates(answer, mine.map((b) => b.id)), h.id, st.build, known);
            return { h, answer, builds };
        } catch (e) {
            return { h, error: e.message };
        }
    });

    // 4. built % and off-build items: what of each build its players bought, and what else
    if (lake) {
        const buildItems = {};
        drafts.forEach((d) => d.builds && Object.values(d.builds).forEach((b) => {
            // core: items outside optional categories (only those can be "skipped")
            buildItems[b.id] = { all: core.unique([].concat(...(b.c || []).map((c) => c.m))),
                                 core: core.unique([].concat(...(b.c || []).filter((c) => !c.o).map((c) => c.m))) };
        }));
        const shop = {};
        Object.keys(items).forEach((id) => { shop[id] = [items[id][2] || "", items[id][3] || 0]; });
        const built = lakeBuilt(buildItems, shop);
        drafts.forEach((d) => d.answer && d.answer.uses && d.answer.uses.forEach((u) => {
            const p = built[u[0] + ":" + u[1]];
            if (p === undefined) return;
            u[5] = p[0]; u[6] = p[1]; u[7] = p[2]; u[8] = p[3]; u[9] = p[4]; u[10] = p[5];
            d.answer.itemNames = d.answer.itemNames || {};
            const name = (i) => { if (items[i]) d.answer.itemNames[i] = items[i][0]; };
            p[2].forEach((x) => name(x[0])); p[3].forEach((x) => name(x[0])); p[4].forEach((x) => { name(x[0]); name(x[1]); }); p[5].forEach((x) => name(x[0]));
        }));
    }

    // 5. fold copies, add names and avatars, write
    let ok = 0;
    const failed = [], known = {};
    for (const d of drafts) {
        if (d.error) { failed.push(d.h.name + ": " + d.error); continue; }
        try {
            const answer = core.trim(core.fold(d.answer, d.builds));
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

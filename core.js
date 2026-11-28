// Builds Improved: shared data logic.
// Runs in the relay page (browser) and in build_data.js (Node, GitHub Action).
//
// What PROS shows for a hero: the builds top players actually played in ranked games. Each row of
// the answer says who played which build, how many games and when last, so the mod can filter by
// region (ALL / NA / EU / ...) and by player group (top 1000, top 100, the hero's mains) and sort
// (best, last played, most played, newest) without asking again.
//
// Hero answer (v2):
//   { v: 2, hero, generatedAt, patchAt, days, source,
//     people: { accountId: { n: name, av: avatar, r: region, p: leaderboard place (0 = not top 1000),
//                            m: this hero's leaderboard place (0 = none) } },
//     uses:   [[accountId, buildId, games, lastPlayed], ...]     source "players": one row per player and build
//     groups: [[buildId, region, group, players, games], ...]    source "groups" (live fallback, no per-player data):
//                                                                group 1 = top 100, 2 = top 1000, 3 = hero mains
//     builds: { buildId: build } }
// The hourly Action builds "players" answers from deadlock-api's data lake (pro_usage.py); when it
// can't, and in the relay's live fallback, "groups" answers come from the REST analytics endpoint.
(function (root) {
"use strict";

var C = {
    API: "https://api.deadlock-api.com",
    FETCH_TIMEOUT_MS: 15000,
    LB_REGIONS: ["Europe", "NAmerica", "SAmerica", "Asia", "Oceania"],
    LB_SHORT: { Europe: "EU", NAmerica: "NA", SAmerica: "SA", Asia: "ASIA", Oceania: "OCE" },
    TOP: 1000,              // leaderboard places tracked per region
    TOP_SMALL: 100,         // the "TOP 100" group
    TOP_HERO: 50,           // hero leaderboard places that count as the hero's mains ("MAIN")
    DAYS: 14,               // ranked games this recent
    MAX_BUILDS: 120,        // builds kept per hero (most played first)...
    MAX_BUILDS_REGION: 15,  // ...plus each region's most played, so region filters are never empty
    MAX_CANDIDATES: 4,      // a leaderboard name matching more Steam accounts than this is skipped
    ID_CHUNK: 300,          // account ids per analytics request
    SAME_ITEMS_MIN: 6       // builds sharing an item set this big are the same build
};

// ---------- fetching ----------

function getJson(path) {
    var ctrl = typeof AbortController === "function" ? new AbortController() : null;
    var timer = ctrl ? setTimeout(function () { ctrl.abort(); }, C.FETCH_TIMEOUT_MS) : null;
    return root.fetch(C.API + path, ctrl ? { signal: ctrl.signal } : undefined).then(function (r) {
        if (timer) clearTimeout(timer);
        if (!r.ok) throw new Error(path.split("?")[0] + " -> HTTP " + r.status);
        return r.json();
    }, function (e) {
        if (timer) clearTimeout(timer);
        throw new Error(path.split("?")[0] + " -> " + (e && e.name === "AbortError" ? "timed out" : (e && e.message)));
    });
}

// Runs fn over items with at most `limit` in flight; failed items become null.
function pool(items, limit, fn) {
    var out = new Array(items.length), next = 0;
    function worker() {
        if (next >= items.length) return Promise.resolve();
        var i = next++;
        return fn(items[i], i).then(function (v) { out[i] = v; }, function () { out[i] = null; }).then(worker);
    }
    var workers = [];
    for (var w = 0; w < Math.min(limit, items.length); w++) workers.push(worker());
    return Promise.all(workers).then(function () { return out; });
}

function chunks(list, size) {
    var out = [];
    for (var i = 0; i < list.length; i += size) out.push(list.slice(i, i + size));
    return out;
}
function unique(list) { var seen = {}; return list.filter(function (x) { return seen[x] ? false : (seen[x] = true); }); }
function now() { return Math.floor(Date.now() / 1000); }

// ---------- shapes ----------

// A build as the mod needs it. c: categories [{ n: name, d: description, o: optional, m: [item ids] }]
function slimBuild(b) {
    var hb = b.hero_build, det = hb.details || {};
    return {
        id: hb.hero_build_id, h: hb.hero_id, n: hb.name, v: hb.version, a: hb.author_account_id,
        u: hb.last_updated_timestamp || hb.publish_timestamp || 0,
        c: (det.mod_categories || []).map(function (cat) {
            var o = { n: cat.name || "", m: (cat.mods || []).map(function (m) { return m.ability_id; }) };
            if (cat.description) o.d = cat.description;
            if (cat.optional) o.o = 1;
            return o;
        })
    };
}

function fetchBuild(id) {
    return getJson("/v1/builds?build_id=" + id + "&only_latest=true&limit=1").then(function (l) {
        return l && l[0] ? slimBuild(l[0]) : null;
    });
}

// Steam name and avatar per account, in batch calls.
function profiles(ids) {
    var out = {};
    return pool(chunks(unique(ids), 100), 3, function (part) {
        return getJson("/v1/players/steam?account_ids=" + part.join(","));
    }).then(function (res) {
        res.forEach(function (list) {
            (list || []).forEach(function (p) { out[p.account_id] = { n: p.personaname || null, av: p.avatarmedium || null }; });
        });
        return out;
    });
}

// ---------- assets ----------

function heroes() {
    return getJson("/v1/assets/heroes").then(function (list) {
        return list.filter(function (h) { return h.player_selectable !== false; })
                   .map(function (h) { return { id: h.id, cls: h.class_name, name: h.name }; });
    });
}

// Shop items: id -> [name, image URL, slot ("weapon" | "vitality" | "spirit"), tier]
function items() {
    return getJson("/v1/assets/items/by-type/upgrade").then(function (list) {
        var out = {};
        (list || []).forEach(function (i) {
            out[i.id] = [i.name, i.shop_image_small || i.shop_image || i.image || "", i.item_slot_type || "", i.item_tier || 0];
        });
        return out;
    });
}

// Unix time of the latest big patch (0 if unknown). Builds edited before it are "old".
function latestPatch() {
    return getJson("/v1/patches/big-days").then(function (days) {
        return (days || []).reduce(function (best, d) {
            var t = Math.floor(Date.parse(d) / 1000);
            return t > best ? t : best;
        }, 0);
    }).catch(function () { return 0; });
}

// ---------- who is tracked ----------

function overallBoards() {
    return Promise.all(C.LB_REGIONS.map(function (r) {
        return getJson("/v1/leaderboard/" + r).catch(function () { return { entries: [] }; });
    }));
}

function heroBoards(heroId) {
    return Promise.all(C.LB_REGIONS.map(function (r) {
        return getJson("/v1/leaderboard/" + r + "/" + heroId).catch(function () { return { entries: [] }; });
    }));
}

// Leaderboard entries -> players: { key, name, r: region, p: place (0 = not in the top 1000),
// m: { heroId: hero place }, ids: possible Steam accounts }. The leaderboard only has names, so a
// name can match several accounts; build_data.js picks the one that played, the live path skips them.
// heroBoardsById: { heroId: heroBoards(heroId) result }
function roster(overall, heroBoardsById) {
    var byKey = {}, list = [];
    function add(regionIndex, e, heroId) {
        var ids = (e.possible_account_ids || []).slice();
        if (!ids.length || ids.length > C.MAX_CANDIDATES || !e.account_name) return;
        var r = C.LB_SHORT[C.LB_REGIONS[regionIndex]], key = r + "|" + e.account_name;
        var x = byKey[key];
        if (!x) { x = byKey[key] = { key: key, name: e.account_name, r: r, p: 0, m: {}, ids: ids }; list.push(x); }
        if (heroId) x.m[heroId] = e.rank; else x.p = e.rank;
    }
    (overall || []).forEach(function (board, ri) {
        ((board && board.entries) || []).forEach(function (e) { if (e.rank && e.rank <= C.TOP) add(ri, e, 0); });
    });
    Object.keys(heroBoardsById || {}).forEach(function (h) {
        (heroBoardsById[h] || []).forEach(function (board, ri) {
            ((board && board.entries) || []).forEach(function (e) { if (e.rank && e.rank <= C.TOP_HERO) add(ri, e, Number(h)); });
        });
    });
    return list;
}

// ---------- builds and copies ----------

var COPY_RE = /^\s*copy of\s*\[?(.*?)\]?\s*$/i;
function baseName(name) { var m = COPY_RE.exec(name || ""); return m ? m[1] : null; }
function itemKey(b) {
    var ids = [];
    (b.c || []).forEach(function (cat) { cat.m.forEach(function (id) { if (ids.indexOf(id) < 0) ids.push(id); }); });
    return ids.length >= C.SAME_ITEMS_MIN ? ids.sort(function (x, y) { return x - y; }).join(",") : null;
}

// ids (most played first) -> { id: id it is a copy of }. Same build = same item set, or named
// "Copy of [X]" where X is another build's name. The original is kept (not a "Copy of" name),
// else the most played.
function copyMap(ids, builds) {
    var groups = [], byKey = {}, byName = {};
    ids.forEach(function (id) {
        var b = builds[id]; if (!b) return;
        var key = itemKey(b), g = key && byKey[key];
        var base = baseName(b.n);
        if (!g && base !== null) g = byName[base.toLowerCase()];
        if (!g) { g = { ids: [] }; groups.push(g); }
        g.ids.push(id);
        if (key) byKey[key] = g;
        if (base === null && !byName[(b.n || "").toLowerCase()]) byName[(b.n || "").toLowerCase()] = g;
    });
    // a copy seen before its original: attach it to the original's group
    groups.forEach(function (g) {
        if (!g.ids.length || !g.ids.every(function (id) { return baseName(builds[id].n) !== null; })) return;
        var target = byName[baseName(builds[g.ids[0]].n).toLowerCase()];
        if (target && target !== g) { target.ids = target.ids.concat(g.ids); g.ids = []; }
    });
    var map = {};
    groups.forEach(function (g) {
        if (!g.ids.length) return;
        var keep = g.ids.filter(function (id) { return baseName(builds[id].n) === null; })[0] || g.ids[0];
        g.ids.forEach(function (id) { map[id] = keep; });
    });
    return map;
}

// Per build: distinct players and games, for picking which builds to keep.
function usage(answer) {
    var by = {};
    function at(b) { return by[b] || (by[b] = { id: b, players: 0, games: 0, regions: {} }); }
    (answer.uses || []).forEach(function (u) {
        var x = at(u[1]), r = (answer.people[u[0]] || {}).r;
        x.players++; x.games += u[2];
        if (r) x.regions[r] = (x.regions[r] || 0) + 1;
    });
    (answer.groups || []).forEach(function (g) {
        if (g[2] !== 2) return;                          // count each player once: the top-1000 rows
        var x = at(g[0]); x.players += g[3]; x.games += g[4]; x.regions[g[1]] = (x.regions[g[1]] || 0) + g[3];
    });
    return Object.keys(by).map(function (k) { return by[k]; })
        .sort(function (x, y) { return (y.players - x.players) || (y.games - x.games); });
}

// Keeps the most played builds (and each region's), fetches them, folds copies into their original
// and drops rows for builds that are gone or are for another hero.
// getBuild(id) -> Promise<build | null>
function finishAnswer(answer, getBuild) {
    var ranked = usage(answer), keep = ranked.slice(0, C.MAX_BUILDS).map(function (x) { return x.id; });
    C.LB_REGIONS.forEach(function (lr) {
        var r = C.LB_SHORT[lr];
        ranked.filter(function (x) { return x.regions[r]; })
              .sort(function (x, y) { return y.regions[r] - x.regions[r]; })
              .slice(0, C.MAX_BUILDS_REGION).forEach(function (x) { keep.push(x.id); });
    });
    keep = unique(keep);
    var builds = {};
    return pool(keep, 6, function (id) { return getBuild(id); }).then(function (list) {
        list.forEach(function (b) { if (b && b.h === answer.hero) builds[b.id] = b; });
        var map = copyMap(keep.filter(function (id) { return builds[id]; }), builds);
        var copies = {};
        Object.keys(map).forEach(function (id) { if (map[id] !== Number(id)) copies[map[id]] = (copies[map[id]] || 0) + 1; });
        if (answer.uses) {
            var merged = {};                             // a player who played a build and its copy: one row
            answer.uses.forEach(function (u) {
                var b = map[u[1]]; if (!b) return;
                var k = u[0] + ":" + b, m = merged[k];
                if (!m) merged[k] = [u[0], b, u[2], u[3]];
                else { m[2] += u[2]; m[3] = Math.max(m[3], u[3]); }
            });
            answer.uses = Object.keys(merged).map(function (k) { return merged[k]; });
            var used = {};
            answer.uses.forEach(function (u) { used[u[0]] = true; });
            Object.keys(answer.people).forEach(function (a) { if (!used[a]) delete answer.people[a]; });
        }
        if (answer.groups) {
            var g = {};                                   // copies add their players and games (an estimate)
            answer.groups.forEach(function (x) {
                var b = map[x[0]]; if (!b) return;
                var k = [b, x[1], x[2]].join(":"), m = g[k];
                if (!m) g[k] = [b, x[1], x[2], x[3], x[4]];
                else { m[3] += x[3]; m[4] += x[4]; }
            });
            answer.groups = Object.keys(g).map(function (k) { return g[k]; });
        }
        var out = {};
        Object.keys(builds).forEach(function (id) {
            if (map[id] !== Number(id)) return;
            if (copies[id]) builds[id].k = copies[id];
            out[id] = builds[id];
        });
        answer.builds = out;
        return answer;
    });
}

// Names and avatars for the players and the builds' authors (people without a leaderboard entry
// get r: null, p: 0, m: 0).
function addProfiles(answer, known) {
    var ids = Object.keys(answer.people).concat(Object.keys(answer.builds).map(function (k) { return String(answer.builds[k].a); }));
    ids = unique(ids).filter(function (id) { return !(known && known[id]); });
    return profiles(ids).then(function (prof) {
        Object.keys(prof).forEach(function (id) { if (known) known[id] = prof[id]; });
        function profileOf(id) { return (known && known[id]) || prof[id] || {}; }
        Object.keys(answer.people).forEach(function (id) {
            var p = profileOf(id), x = answer.people[id];
            if (p.n) x.n = p.n;
            x.av = p.av || null;
        });
        Object.keys(answer.builds).forEach(function (k) {
            var a = answer.builds[k].a;
            if (answer.people[a]) return;
            var p = profileOf(a);
            answer.people[a] = { n: p.n || null, av: p.av || null, r: null, p: 0, m: 0 };
        });
        return answer;
    });
}

// ---------- the live answer (relay fallback, and the Action when the data lake fails) ----------

// "groups" answer from the analytics endpoint: per region and player group, how many tracked
// players picked each build and their games. Only leaderboard names with one Steam account count.
function heroGroups(heroId, opts) {
    opts = opts || {};
    heroId = Number(heroId);
    var t = now(), since = t - C.DAYS * 86400;
    return Promise.all([
        opts.boards ? Promise.resolve(opts.boards) : overallBoards(),
        opts.heroBoards ? Promise.resolve(opts.heroBoards) : heroBoards(heroId),
        opts.patchAt !== undefined ? Promise.resolve(opts.patchAt) : latestPatch()
    ]).then(function (res) {
        var hb = {}; hb[heroId] = res[1];
        var players = roster(res[0], hb).filter(function (x) { return x.ids.length === 1; });
        var asks = [];                                    // [region, group, ids]
        C.LB_REGIONS.forEach(function (lr) {
            var r = C.LB_SHORT[lr], here = players.filter(function (x) { return x.r === r; });
            var sets = [
                here.filter(function (x) { return x.p && x.p <= C.TOP_SMALL; }),
                here.filter(function (x) { return x.p; }),
                here.filter(function (x) { return x.m[heroId]; })
            ];
            sets.forEach(function (set, gi) {
                chunks(set.map(function (x) { return x.ids[0]; }), C.ID_CHUNK).forEach(function (ids) { asks.push([r, gi + 1, ids]); });
            });
        });
        return pool(asks, 4, function (a) {
            return getJson("/v1/analytics/hero-build-stats?hero_ids=" + heroId + "&match_mode=ranked&min_unix_timestamp=" + since +
                           "&account_ids=" + a[2].join(","));
        }).then(function (lists) {
            var sum = {};
            lists.forEach(function (list, i) {
                (list || []).forEach(function (x) {
                    if (!x.hero_build_id) return;
                    var k = [x.hero_build_id, asks[i][0], asks[i][1]].join(":"), s = sum[k];
                    if (!s) sum[k] = [x.hero_build_id, asks[i][0], asks[i][1], x.players, x.matches];
                    else { s[3] += x.players; s[4] += x.matches; }
                });
            });
            return { v: 2, hero: heroId, generatedAt: t, patchAt: res[2] || 0, days: C.DAYS, source: "groups",
                     people: {}, groups: Object.keys(sum).map(function (k) { return sum[k]; }), builds: {} };
        });
    });
}

// The relay's live answer for one hero.
function hero(heroId, opts) {
    return heroGroups(heroId, opts).then(function (answer) {
        return finishAnswer(answer, function (id) { return fetchBuild(id).catch(function () { return null; }); });
    }).then(function (answer) { return addProfiles(answer); });
}

var BI_CORE = {
    config: C, getJson: getJson, pool: pool, chunks: chunks, unique: unique, slimBuild: slimBuild, fetchBuild: fetchBuild,
    profiles: profiles, heroes: heroes, items: items, latestPatch: latestPatch,
    overallBoards: overallBoards, heroBoards: heroBoards, roster: roster,
    copyMap: copyMap, usage: usage, finishAnswer: finishAnswer, addProfiles: addProfiles,
    heroGroups: heroGroups, hero: hero
};
root.BI_CORE = BI_CORE;
if (typeof module !== "undefined" && module.exports) module.exports = BI_CORE;
})(typeof globalThis !== "undefined" ? globalThis : this);

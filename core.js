// Builds Improved: shared data logic.
// Runs in the relay page (browser) and in build_data.js (Node, GitHub Action), so the hourly
// prebuilt files and the live fallback always produce the same data.
(function (root) {
"use strict";

var C = {
    API: "https://api.deadlock-api.com",
    FETCH_TIMEOUT_MS: 10000,
    LB_REGIONS: ["Europe", "NAmerica", "SAmerica", "Asia", "Oceania"],
    LB_SHORT: { Europe: "EU", NAmerica: "NA", SAmerica: "SA", Asia: "ASIA", Oceania: "OCE" },
    TOP_OVERALL: 50,        // top N of each regional leaderboard count as pros
    TOP_HERO: 10,           // top N of each hero leaderboard count as that hero's specialists
    TOP_BUILDS: 12,         // rows per list
    SPEC_BUILDS: 2,         // latest builds shown per specialist
    USED_DAYS: 21,          // "Used by pros": ranked matches this recent...
    USED_WIDE_DAYS: 60,     // ...or this recent when that gives fewer than MIN_ROWS
    POPULAR_DAYS: 14,       // "Eternus meta", same idea
    POPULAR_WIDE_DAYS: 45,
    MIN_ROWS: 3,
    ETERNUS_BADGE: 111,     // Eternus I
    SAME_ITEMS_MIN: 6       // builds sharing an item set this big are treated as the same build
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

function unique(list) { return list.filter(function (x, i) { return list.indexOf(x) === i; }); }
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

// Steam name, avatar and rank badge for each account, in two batch calls.
function profiles(ids) {
    var out = {};
    ids.forEach(function (id) { out[id] = { name: null, avatar: null, badge: 0 }; });
    if (!ids.length) return Promise.resolve(out);
    var q = ids.join(",");
    return Promise.all([
        getJson("/v1/players/steam?account_ids=" + q).catch(function () { return []; }),
        getJson("/v1/players/rank?account_ids=" + q).catch(function () { return []; })
    ]).then(function (res) {
        (res[0] || []).forEach(function (p) { if (out[p.account_id]) { out[p.account_id].name = p.personaname; out[p.account_id].avatar = p.avatarmedium; } });
        (res[1] || []).forEach(function (r) { if (out[r.account_id]) out[r.account_id].badge = r.badge || 0; });
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

function overallBoards() {
    return Promise.all(C.LB_REGIONS.map(function (r) {
        return getJson("/v1/leaderboard/" + r).catch(function () { return { entries: [] }; });
    }));
}

// ---------- quality ----------

var COPY_RE = /^\s*copy of\s*\[?(.*?)\]?\s*$/i;
function baseName(name) { var m = COPY_RE.exec(name || ""); return m ? m[1] : null; }
function itemKey(b) {
    var ids = [];
    (b.c || []).forEach(function (cat) { cat.m.forEach(function (id) { if (ids.indexOf(id) < 0) ids.push(id); }); });
    return ids.length >= C.SAME_ITEMS_MIN ? ids.sort(function (x, y) { return x - y; }).join(",") : null;
}

// Rows [{ id, players, matches }] -> the same with copies folded into the build they copy.
// Same build = same item set, or named "Copy of [X]" where X is another row's name.
// The kept row is the original (not a "Copy of" name), else the most used; counts are summed.
function groupCopies(rows, builds) {
    var groups = [], byKey = {}, byName = {};
    rows.forEach(function (r) {
        var b = builds[r.id]; if (!b) return;
        var key = itemKey(b), g = key && byKey[key];
        if (!g) {
            var base = baseName(b.n);
            if (base !== null) g = byName[base.toLowerCase()];
        }
        if (!g) { g = { rows: [] }; groups.push(g); }
        g.rows.push(r);
        if (key) byKey[key] = g;
        if (baseName(b.n) === null) byName[(b.n || "").toLowerCase()] = g;
    });
    // a copy listed before its original: attach it once the original is known
    groups.forEach(function (g) {
        if (!g.rows.length) return;
        var b = builds[g.rows[0].id], base = baseName(b.n), target = base !== null && byName[base.toLowerCase()];
        if (target && target !== g && g.rows.every(function (r) { return baseName(builds[r.id].n) !== null; })) {
            target.rows = target.rows.concat(g.rows); g.rows = [];
        }
    });
    return groups.filter(function (g) { return g.rows.length; }).map(function (g) {
        var keep = g.rows.slice().sort(function (x, y) {
            var cx = baseName(builds[x.id].n) !== null, cy = baseName(builds[y.id].n) !== null;
            return (cx - cy) || (y.players - x.players) || (y.matches - x.matches);
        })[0];
        var out = { id: keep.id, players: 0, matches: 0, copies: g.rows.length - 1 };
        g.rows.forEach(function (r) { out.players += r.players; out.matches += r.matches; });
        return out;
    }).sort(function (x, y) { return (y.players - x.players) || (y.matches - x.matches); });
}

// Drops one-off rows (1 player, 1 game) unless that would leave fewer than MIN_ROWS.
function dropNoise(rows) {
    var solid = rows.filter(function (r) { return r.players > 1 || r.matches > 1; });
    return solid.length >= C.MIN_ROWS ? solid : rows;
}

// ---------- the hero answer ----------

// Everything PROS shows for one hero:
//   used:        builds the tracked pros picked in ranked matches (last usedDays)
//   popular:     builds most picked in Eternus-average ranked lobbies (last popularDays)
//   specialists: the hero's top players per region and their latest builds
// Tracked pros = top TOP_OVERALL of each regional leaderboard + top TOP_HERO of each hero leaderboard.
// opts: { boards: overallBoards() result, patchAt } to share work across heroes.
function hero(heroId, opts) {
    opts = opts || {};
    heroId = Number(heroId);
    var t = now();
    return Promise.all([
        opts.boards ? Promise.resolve(opts.boards) : overallBoards(),
        Promise.all(C.LB_REGIONS.map(function (r) {
            return getJson("/v1/leaderboard/" + r + "/" + heroId).catch(function () { return { entries: [] }; });
        })),
        opts.patchAt !== undefined ? Promise.resolve(opts.patchAt) : latestPatch()
    ]).then(function (res) {
        var lb = {};      // accountId -> { region, name, place, heroPlace }; ambiguous names skipped
        function take(boards, top, key) {
            boards.forEach(function (board, ri) {
                ((board && board.entries) || []).slice(0, top).forEach(function (e) {
                    var ids = e.possible_account_ids || [];
                    if (ids.length !== 1) return;
                    var x = lb[ids[0]] || (lb[ids[0]] = { region: C.LB_SHORT[C.LB_REGIONS[ri]], name: e.account_name });
                    x[key] = e.rank;
                });
            });
        }
        take(res[0], C.TOP_OVERALL, "place");
        take(res[1], C.TOP_HERO, "heroPlace");
        var patchAt = res[2] || 0;
        var pros = Object.keys(lb).map(Number);
        var specialists = pros.filter(function (id) { return lb[id].heroPlace; })
                              .sort(function (a, b) { return lb[a].heroPlace - lb[b].heroPlace; });
        var base = "/v1/analytics/hero-build-stats?hero_ids=" + heroId + "&match_mode=ranked";

        function stats(query, days) {
            return getJson(base + query + "&min_unix_timestamp=" + (t - days * 86400)).then(function (list) {
                return (list || []).filter(function (x) { return x.hero_build_id; })
                    .map(function (x) { return { id: x.hero_build_id, players: x.players, matches: x.matches }; });
            }).catch(function () { return []; });
        }
        var usedQ = "&account_ids=" + pros.join(","), popQ = "&min_average_badge=" + C.ETERNUS_BADGE + "&min_matches=3";

        return Promise.all([
            pros.length ? stats(usedQ, C.USED_DAYS) : Promise.resolve([]),
            stats(popQ, C.POPULAR_DAYS),
            pool(specialists, 6, function (id) {
                return getJson("/v1/builds?author_id=" + id + "&hero_id=" + heroId + "&only_latest=true&sort_by=updated_at&limit=" + C.SPEC_BUILDS);
            })
        ]).then(function (r) {
            var builds = {};
            specialists.forEach(function (id, i) { (r[2][i] || []).forEach(function (b) { b = slimBuild(b); builds[b.id] = b; }); });

            // Fetches the builds behind stat rows, groups copies and drops noise.
            function finish(rows) {
                var need = unique(rows.map(function (x) { return x.id; })).filter(function (id) { return !builds[id]; });
                return pool(need, 6, function (id) {
                    return getJson("/v1/builds?build_id=" + id + "&only_latest=true&limit=1");
                }).then(function (lists) {
                    lists.forEach(function (l) { if (l && l[0]) { var b = slimBuild(l[0]); builds[b.id] = b; } });
                    return dropNoise(groupCopies(rows.filter(function (x) { return builds[x.id] && builds[x.id].h === heroId; }), builds))
                        .slice(0, C.TOP_BUILDS);
                });
            }
            // too little recent data (rare heroes): look further back once
            function section(rows, query, days, wideDays) {
                return finish(rows).then(function (list) {
                    if (list.length >= C.MIN_ROWS || (query === usedQ && !pros.length)) return { rows: list, days: days };
                    return stats(query, wideDays).then(finish).then(function (wide) {
                        return wide.length > list.length ? { rows: wide, days: wideDays } : { rows: list, days: days };
                    });
                });
            }
            return section(r[0], usedQ, C.USED_DAYS, C.USED_WIDE_DAYS).then(function (used) {
                return section(r[1], popQ, C.POPULAR_DAYS, C.POPULAR_WIDE_DAYS).then(function (popular) {
                    // keep only builds something refers to, so the answer stays small
                    var keep = {};
                    used.rows.concat(popular.rows).forEach(function (x) { keep[x.id] = builds[x.id]; });
                    specialists.forEach(function (id) { Object.keys(builds).forEach(function (k) { if (builds[k].a === id) keep[k] = builds[k]; }); });
                    var authors = unique(Object.keys(keep).map(function (k) { return keep[k].a; }).concat(specialists));
                    return profiles(authors).then(function (prof) {
                        var people = {};
                        authors.forEach(function (id) {
                            var p = prof[id] || {}, l = lb[id] || {};
                            people[id] = { name: p.name || l.name || ("Player " + id), avatar: p.avatar || null, badge: p.badge || 0,
                                           region: l.region || null, place: l.place || null, heroPlace: l.heroPlace || null };
                        });
                        return {
                            hero: heroId, generatedAt: t, patchAt: patchAt, tracked: pros.length,
                            usedDays: used.days, popularDays: popular.days,
                            used: used.rows, popular: popular.rows,
                            specialists: specialists.map(function (id) {
                                return { id: id, builds: Object.keys(keep).map(function (k) { return keep[k]; })
                                    .filter(function (b) { return b.a === id && b.h === heroId; })
                                    .sort(function (a, b) { return b.u - a.u; }).map(function (b) { return b.id; }) };
                            }),
                            builds: keep, people: people
                        };
                    });
                });
            });
        });
    });
}

// Latest builds for one hero from followed players.
function following(ids, heroId) {
    ids = unique((ids || []).map(Number).filter(Boolean)).slice(0, 40);
    heroId = Number(heroId);
    return Promise.all([
        pool(ids, 6, function (id) {
            return getJson("/v1/builds?author_id=" + id + "&hero_id=" + heroId + "&only_latest=true&sort_by=updated_at&limit=5");
        }),
        profiles(ids)
    ]).then(function (res) {
        var builds = [];
        res[0].forEach(function (l) { (l || []).forEach(function (b) { builds.push(slimBuild(b)); }); });
        builds.sort(function (x, y) { return y.u - x.u; });
        return { players: res[1], builds: builds.slice(0, 60) };
    });
}

function search(text) {
    return getJson("/v1/players/steam-search?search_query=" + encodeURIComponent(text || "")).then(function (list) {
        return (list || []).slice(0, 10).map(function (p) { return { id: p.account_id, name: p.personaname, avatar: p.avatarmedium }; });
    });
}

var BI_CORE = {
    config: C, getJson: getJson, pool: pool, slimBuild: slimBuild, profiles: profiles,
    heroes: heroes, items: items, latestPatch: latestPatch, overallBoards: overallBoards,
    groupCopies: groupCopies, dropNoise: dropNoise, hero: hero, following: following, search: search
};
root.BI_CORE = BI_CORE;
if (typeof module !== "undefined" && module.exports) module.exports = BI_CORE;
})(typeof globalThis !== "undefined" ? globalThis : this);

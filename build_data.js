// Builds the prebuilt data files the relay page serves, so PROS loads one small file per hero
// instead of ~15 live API calls. Run hourly by .github/workflows/build-data.yml.
// Usage: node build_data.js <output folder>      (needs Node 18+ for fetch)
"use strict";
const fs = require("fs");
const path = require("path");
const core = require("./core.js");

const OUT = path.resolve(process.argv[2] || "_site");
const HERO_CONCURRENCY = 2;     // heroes built at once; each runs its own small pool of requests

function write(name, value) {
    fs.writeFileSync(path.join(OUT, "data", name + ".json"), JSON.stringify(value));
}

(async () => {
    fs.mkdirSync(path.join(OUT, "data"), { recursive: true });
    const started = Date.now(), generatedAt = Math.floor(started / 1000);

    const heroes = await core.heroes();                  // no heroes = nothing to build: fail the run
    write("heroes", { generatedAt, data: heroes });
    try { write("items", { generatedAt, data: await core.items() }); }
    catch (e) { console.warn("items skipped: " + e.message); }

    const [boards, patchAt] = await Promise.all([core.overallBoards(), core.latestPatch()]);
    let ok = 0;
    const failed = [];
    await core.pool(heroes, HERO_CONCURRENCY, async (h) => {
        try {
            write("hero_" + h.id, await core.hero(h.id, { boards, patchAt }));
            ok++;
        } catch (e) {
            failed.push(h.name + ": " + e.message);
        }
    });

    write("meta", { generatedAt, heroes: ok, failed, seconds: Math.round((Date.now() - started) / 1000) });
    console.log(`built ${ok}/${heroes.length} heroes in ${Math.round((Date.now() - started) / 1000)}s`);
    failed.forEach((f) => console.warn("failed " + f));
    if (!ok) process.exit(1);                            // keep the previous site rather than an empty one
})().catch((e) => { console.error(e); process.exit(1); });

"""Builds Improved: which build each tracked player actually played, from deadlock-api's data lake.

Reads match_player from the public parquet snapshots (https://deadlock-api.com/data-dumps): one scan
per run instead of one API call per player.

  python3 pro_usage.py usage <tracked.json> <out.json> <matches.parquet> [days] [half_life_days] [ns_matches.json] [patch_unix]
      tracked.json: [account_id, ...]
      out.json:     {"generatedAt", "days", "halfLife",
                     "rows": [[hero, account, build, games, last_unix, weighted_games_x100,
                               games_since_patch, weighted_since_patch_x100], ...],
                     "totals": [[hero, account, games, games_since_patch], ...]}
                    weighted: each game counts 0.5 ** (age / half_life), so recent games count more
                    totals: all their ranked games on the hero, with or without a build loaded (for USAGE)
      matches.parquet: per (match, player) the build and the shop items bought, for the second step
      ns_matches.json: [{"match_id", "start"}] Night Shift matches; out.json then also has
                    "ns_rows": [[hero, account, build, games, last_unix, weighted_games_x100], ...] (the builds loaded in them)
                    and "ns_totals": [[hero, account, games], ...]

  python3 pro_usage.py built <matches.parquet> <build_items.json> <shop_items.json> <out.json>
      build_items.json: {build_id: [item ids]} or {build_id: {"all": [item ids], "core": [non-optional item ids]}}
      shop_items.json:  [item ids] or {item id: [slot, tier]}
      out.json: {"rows": [[account, build, built_pct, off_per_game_x10, [[item, pct_of_games], ...],
                           skipped [[item, pct], ...], swaps [[build item, bought instead, pct], ...],
                           core [[item, pct_of_games, in_build 0/1], ...]], ...]}
                built %: of the shop items the player bought in their games with the build, the share
                that are in the build (items of the build they never buy don't lower it)
                off-build: items bought that are not in the build, per game, and the most common ones
                skipped: core build items they rarely buy (pct: of the games where they bought anything of
                         that tier or higher, so short games don't count against late items)
                swaps: a skipped item and the off-build item of the same slot and tier bought in its place
                core: the late items (tier 3+) they finish most often with the build, in it or not: what
                      the build is really aiming for

Needs: pip install duckdb
"""
import json
import os
import sys
import time
import urllib.request

import duckdb

MANIFEST_URL = "https://data.deadlock-api.com/v1/manifest.json"
CATALOG_URL = "https://data.deadlock-api.com/v1/catalog.ducklake"
# Python's default User-Agent ("Python-urllib/3.x") is refused by the host's firewall (HTTP 403)
USER_AGENT = "builds-improved-relay/1.0 (Deadlock mod data; +https://github.com/alimatrix18/builds-improved-relay)"


def manifest():
    src = os.environ.get("BI_MANIFEST", MANIFEST_URL)        # a local file for tests
    if not src.startswith("http"):
        return json.load(open(src))
    req = urllib.request.Request(src, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def match_player_files(m, since):
    """[(key, url)] of match_player's current generation that can hold matches started since `since`,
    and the generation. Files are immutable and uniquely named, so a key read once never needs reading again."""
    t = m["tables"]["match_player"]
    gen, base = t.get("generation", 0), m["public_url"].rstrip("/")
    out = []
    for f in t["files"]:
        if f.get("generation", 0) != gen:
            continue
        if f.get("max_start_time") is not None:            # base files carry start-time bounds
            if f["max_start_time"] < since:
                continue
        elif f.get("hi") is not None and f["hi"] < since:  # deltas: rows created up to hi
            continue
        out.append((f["key"], base + "/" + f["key"] if base.startswith("http") else os.path.join(base, f["key"])))
    return out, gen


def connect():
    try:
        con = duckdb.connect(config={"custom_user_agent": USER_AGENT})   # data file requests say who they are
    except duckdb.Error:
        con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")                 # ClickHouse DateTime values are UTC
    except duckdb.Error:
        pass
    return con


def patient_http(con):
    """Longer HTTP timeouts and more retries for the data lake's files (httpfs must be loaded). The
    timeout's unit differs between DuckDB versions, so the current value is multiplied."""
    for name, factor in (("http_timeout", 4), ("http_retries", 3)):
        try:
            cur = con.execute(f"SELECT current_setting('{name}')").fetchone()[0]
            con.execute(f"SET {name} = {int(cur) * factor}")
        except duckdb.Error:
            pass
    try:
        con.execute("SET http_retry_wait_ms = 2000")
    except duckdb.Error:
        pass


SCAN_BATCH = 25       # match_player files read per query (one query over 600 remote files can time out)
SCAN_TRIES = 3        # tries per batch before its files are left for the next run
SCAN_MAX_FAILED = 0.2 # more than this share of files unread: the scan fails
SCAN_FAIL_STREAK = 6  # ...as does this many unreadable files in a row (the host is down)
FULL_SCAN_DAYS = 7     # a full rescan this often picks up the history of players new to the leaderboards
NEW_PLAYERS_FULL_SCAN = 50   # ...or as soon as more players than this are new to the tracked list
NS_HALF_LIFE_DAYS = 28   # Night Shift is weekly: its games fade over a month, not a week
SCHEMA = 2   # saved-rows format; 2 keeps games without a build (build 0) for USAGE. A change forces a full scan.
OFF_TOP, OFF_MIN_PCT = 3, 20  # off-build items listed: the 3 most common, bought in at least 20% of games
SKIP_MAX_PCT, SKIP_MIN_GAMES = 15, 3   # skipped: bought in under 15% of 3+ games that reached its tier
SWAP_MIN_PCT = 30                      # swap: the replacement bought in 30%+ of the games the item was skipped
CORE_TIER, CORE_TOP, CORE_MIN_PCT = 3, 4, 30   # finished items: tier 3+, the 4 most bought, in 30%+ of games


def usage(tracked_path, out_path, matches_path, days, half_life, ns_path=None, patch_at=0):
    """Incremental: the (match, player) rows already read are kept in <cache>/lake/ with the list of
    files they came from; a run reads only the files added since, unless a full rescan is due (weekly,
    a new export generation, or no saved rows)."""
    tracked = sorted({int(x) for x in json.load(open(tracked_path))})
    now = int(time.time())
    since = now - days * 86400
    lake_dir = os.path.join(os.path.dirname(os.path.abspath(matches_path)), "lake")
    os.makedirs(lake_dir, exist_ok=True)
    store_path, state_path = os.path.join(lake_dir, "rows.parquet"), os.path.join(lake_dir, "state.json")
    state = json.load(open(state_path)) if os.path.exists(state_path) else {}
    con = connect()

    # the file list, so only files that can hold the window's matches are read; only if the file
    # list itself is refused, the DuckLake catalog (a full scan, slower)
    m = None
    try:
        m = manifest()
        listing, gen = match_player_files(m, since)
    except Exception as e:
        if os.environ.get("BI_MANIFEST"):
            raise
        listing, gen = None, None
        print(f"manifest unavailable ({e}); reading through the DuckLake catalog instead", flush=True)

    # many players new to the tracked list (e.g. a new group such as Night Shift): their history is
    # only in files already read, so read everything again
    new_players = len(set(tracked) - set(state.get("watch", [])))
    full = (listing is None or not os.path.exists(store_path) or state.get("generation") != gen
            or now - state.get("full_at", 0) > FULL_SCAN_DAYS * 86400 or new_players > NEW_PLAYERS_FULL_SCAN
            or state.get("schema") != SCHEMA)
    if new_players:
        print(f"{new_players} players new to the tracked list", flush=True)
    watch = sorted(set(tracked) if full else set(tracked) | set(state.get("watch", [])))
    if listing is not None:
        if not listing:
            raise SystemExit("no match_player files for the window")
        done = set() if full else set(state.get("keys", []))
        todo = [(k, u) for k, u in listing if k not in done]
        print(f"{'full scan' if full else 'incremental'}: reading {len(todo)} of {len(listing)} match_player files", flush=True)
        if todo and todo[0][1].startswith("http"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
            patient_http(con)
        urls = [u for _, u in todo]
        source = f"{len(todo)} new files" if not full else f"{len(todo)} files (full scan)"
    else:
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
        patient_http(con)
        con.execute(f"ATTACH 'ducklake:{CATALOG_URL}' AS lake (READ_ONLY)")
        urls, source = None, "the DuckLake catalog (full scan)"

    con.execute("CREATE TABLE watch(id UBIGINT)")
    con.executemany("INSERT INTO watch VALUES (?)", [(i,) for i in watch])
    con.execute("CREATE TABLE n (match_id UBIGINT, account_id UBIGINT, hero_id UBIGINT, build_id UBIGINT, started BIGINT, items UBIGINT[])")
    t0 = time.time()

    def scan(source_sql):
        con.execute(f"CREATE OR REPLACE VIEW mp AS {source_sql}")
        # Column types differ by export (Enum as text or number, DateTime as timestamp or seconds).
        types = {r[0]: r[1].upper() for r in con.execute("DESCRIBE mp").fetchall()}
        st = "epoch(start_time)" if ("TIMESTAMP" in types.get("start_time", "") or "DATE" in types.get("start_time", "")) \
            else "CAST(start_time AS BIGINT)"
        mm = types.get("match_mode", "")
        ranked = "CAST(match_mode AS VARCHAR) = 'Ranked'" if ("VARCHAR" in mm or "ENUM" in mm) else "CAST(match_mode AS INTEGER) = 4"
        items = 'CAST("items.item_id" AS UBIGINT[])' if "items.item_id" in types else "CAST([] AS UBIGINT[])"
        # One row per (match, player) per batch (duplicates across batches are removed below).
        # Games with no build loaded are kept as build 0: they count towards USAGE's total only.
        con.execute(f"""
            INSERT INTO n
            SELECT match_id, account_id, any_value(hero_id), coalesce(max(hero_build_id), 0), max({st}), arg_max({items}, len({items}))
            FROM mp
            WHERE {st} >= {since} AND {ranked} AND account_id IN (SELECT id FROM watch)
            GROUP BY match_id, account_id
        """)

    failed_keys = set()
    if urls is None:
        print("scanning ranked games of tracked players…", flush=True)
        scan("SELECT * FROM lake.match_player")
    elif urls:
        # a few files per query, each batch tried a few times: one slow file no longer sinks the scan,
        # and a batch that keeps failing is left for the next run (its files aren't marked as read)
        batches = [todo[i:i + SCAN_BATCH] for i in range(0, len(todo), SCAN_BATCH)]
        pause = (lambda n: None) if os.environ.get("BI_MANIFEST") else (lambda n: time.sleep(10 * n))

        def read(files, label, tries):
            for attempt in range(1, tries + 1):
                try:
                    scan(f"SELECT * FROM read_parquet({[u for _, u in files]!r}, union_by_name = true)")
                    return True
                except duckdb.Error as e:
                    print(f"{label} try {attempt} failed: {str(e).splitlines()[0][:160]}", flush=True)
                    if attempt < tries:
                        pause(attempt)
            return False

        streak = 0
        for bi, batch in enumerate(batches):
            if not read(batch, f"batch {bi + 1}/{len(batches)}", SCAN_TRIES):
                # one bad or slow file shouldn't cost the whole batch: read its files one by one
                for key, url in batch:
                    if read([(key, url)], key, 2):
                        streak = 0
                        continue
                    failed_keys.add(key)
                    streak += 1
                    if streak >= SCAN_FAIL_STREAK:      # the host is down, not one bad file: stop now
                        raise SystemExit(f"{streak} match_player files in a row could not be read")
            else:
                streak = 0
            if (bi + 1) % 4 == 0 or bi + 1 == len(batches):
                print(f"  {min((bi + 1) * SCAN_BATCH, len(todo))}/{len(todo)} files, {round(time.time() - t0)}s", flush=True)
        if len(failed_keys) > SCAN_MAX_FAILED * len(todo):
            raise SystemExit(f"{len(failed_keys)} of {len(todo)} match_player files could not be read")
        if failed_keys:
            print(f"{len(failed_keys)} files left for the next run", flush=True)
    print(f"scan done in {round(time.time() - t0)}s: {con.execute('SELECT count(*) FROM n').fetchone()[0]} new (match, player) rows", flush=True)

    # saved rows + new rows, one per (match, player), inside the window
    old = f"SELECT * FROM read_parquet('{store_path}') WHERE started >= {since}" if not full else "SELECT * FROM n WHERE false"
    con.execute(f"""
        CREATE TABLE m AS
        SELECT * FROM ({old} UNION ALL SELECT * FROM n)
        QUALIFY row_number() OVER (PARTITION BY match_id, account_id ORDER BY build_id DESC, len(items) DESC) = 1
    """)
    con.execute(f"COPY m TO '{store_path}' (FORMAT parquet)")
    keys = [k for k, _ in listing if k not in failed_keys] if listing is not None else []
    json.dump({"schema": SCHEMA, "generation": gen, "keys": keys, "watch": watch,
               "full_at": now if full else state.get("full_at", now)}, open(state_path, "w"))

    con.execute("CREATE TABLE tracked(id UBIGINT)")
    con.executemany("INSERT INTO tracked VALUES (?)", [(i,) for i in tracked])
    con.execute("CREATE TABLE t AS SELECT * FROM m WHERE account_id IN (SELECT id FROM tracked)")
    rows = con.execute(f"""
        SELECT hero_id, account_id, build_id, count(*) AS games, max(started) AS last,
               round(100 * sum(pow(0.5, ({now} - started) / ({half_life} * 86400.0)))) AS weighted,
               count(*) FILTER (WHERE started >= {patch_at}),
               round(100 * coalesce(sum(pow(0.5, ({now} - started) / ({half_life} * 86400.0))) FILTER (WHERE started >= {patch_at}), 0))
        FROM t WHERE build_id > 0 GROUP BY ALL
    """).fetchall()
    totals = con.execute(f"SELECT hero_id, account_id, count(*), count(*) FILTER (WHERE started >= {patch_at}) FROM t GROUP BY ALL").fetchall()
    con.execute(f"COPY (SELECT account_id, build_id, items FROM t WHERE build_id > 0) TO '{matches_path}' (FORMAT parquet)")

    ns_rows, ns_totals = [], []
    if ns_path:
        try:
            ns_rows, ns_totals = night_shift(con, m, json.load(open(ns_path)), lake_dir, NS_HALF_LIFE_DAYS)
        except Exception as e:                              # Night Shift builds are extra: never fail the run
            print(f"Night Shift builds skipped: {e}", flush=True)

    json.dump({"generatedAt": now, "days": days, "halfLife": half_life,
               "patchAt": patch_at,
               "rows": [[int(h), int(a), int(b), int(g), int(t), int(w), int(gp), int(wp)] for h, a, b, g, t, w, gp, wp in rows],
               "totals": [[int(h), int(a), int(g), int(gp)] for h, a, g, gp in totals],
               "ns_rows": ns_rows, "ns_totals": ns_totals},
              open(out_path, "w"))
    print(f"{len(rows)} (hero, player, build) rows from {source}, {len(tracked)} players tracked")


def night_shift(con, m, ns_matches, lake_dir, half_life=7):
    """The build each player loaded in Night Shift matches: ([[hero, account, build, games, last, weighted x100], ...],
    [[hero, account, all Night Shift games on the hero], ...]).
    ns_matches: [{"match_id", "start"}] from dlns-stats.co.uk. Matches already read are kept in
    <cache>/lake/ns_rows.parquet; only new ones are looked up, in the files whose match id range
    can hold them."""
    store, done_path = os.path.join(lake_dir, "ns_rows.parquet"), os.path.join(lake_dir, "ns_done.json")
    saved = json.load(open(done_path)) if os.path.exists(done_path) and os.path.exists(store) else {}
    # an older format (a bare list) has no build-0 rows: read those matches again
    done = set(saved.get("done", [])) if isinstance(saved, dict) and saved.get("schema") == SCHEMA else set()
    if not done and os.path.exists(store):
        os.remove(store)
    # only real match ids (the site also lists entries without one, e.g. negative ids)
    wanted = {int(x["match_id"]): int(x.get("start") or 0) for x in ns_matches
              if str(x.get("match_id", "")).lstrip("-").isdigit() and int(x["match_id"]) > 0}
    todo = sorted(set(wanted) - done)
    con.execute("CREATE TABLE ns (match_id UBIGINT, account_id UBIGINT, hero_id UBIGINT, build_id UBIGINT, started BIGINT)")
    if todo:
        if m is not None:
            t = m["tables"]["match_player"]
            gen, base = t.get("generation", 0), m["public_url"].rstrip("/")
            first = min(wanted[i] for i in todo if wanted[i]) if any(wanted[i] for i in todo) else 0
            urls = []
            for f in t["files"]:
                if f.get("generation", 0) != gen:
                    continue
                lo, hi = f.get("min_match_id"), f.get("max_match_id")
                if lo is not None and hi is not None:
                    if not any(lo <= i <= hi for i in todo):
                        continue
                elif f.get("hi") is not None and first and f["hi"] < first:
                    continue
                urls.append(base + "/" + f["key"] if base.startswith("http") else os.path.join(base, f["key"]))
            src = f"read_parquet({urls!r}, union_by_name = true)" if urls else None
        else:
            src = "lake.match_player"
        if src:
            con.execute(f"CREATE OR REPLACE VIEW nsmp AS SELECT * FROM {src}")
            types = {r[0]: r[1].upper() for r in con.execute("DESCRIBE nsmp").fetchall()}
            st = "epoch(start_time)" if ("TIMESTAMP" in types.get("start_time", "") or "DATE" in types.get("start_time", "")) \
                else "CAST(start_time AS BIGINT)"
            con.execute("CREATE TABLE nsids(id UBIGINT)")
            con.executemany("INSERT INTO nsids VALUES (?)", [(i,) for i in todo])
            con.execute(f"""
                INSERT INTO ns
                SELECT match_id, account_id, any_value(hero_id), coalesce(max(hero_build_id), 0), max({st})
                FROM nsmp WHERE match_id IN (SELECT id FROM nsids)
                GROUP BY match_id, account_id
            """)
        found = {r[0] for r in con.execute("SELECT DISTINCT match_id FROM ns").fetchall()}
        print(f"Night Shift: {len(found)} of {len(todo)} new matches found in the match data", flush=True)
        # a match not in the data yet is asked again on later runs, for a week
        week_ago = int(time.time()) - 7 * 86400
        done |= found | {i for i in todo if wanted[i] and wanted[i] < week_ago}
    old = f"SELECT * FROM read_parquet('{store}')" if os.path.exists(store) else "SELECT * FROM ns WHERE false"
    keep = ",".join(str(i) for i in wanted) or "0"
    con.execute(f"CREATE TABLE nsall AS SELECT * FROM ({old} UNION ALL SELECT * FROM ns) WHERE match_id IN ({keep}) "
                "QUALIFY row_number() OVER (PARTITION BY match_id, account_id ORDER BY build_id DESC) = 1")
    con.execute(f"COPY nsall TO '{store}' (FORMAT parquet)")
    json.dump({"schema": SCHEMA, "done": sorted(i for i in done if i in wanted)}, open(done_path, "w"))
    now = int(time.time())
    rows = con.execute(f"""
        SELECT hero_id, account_id, build_id, count(*), max(started),
               round(100 * sum(pow(0.5, greatest({now} - started, 0) / ({half_life} * 86400.0))))
        FROM nsall WHERE build_id > 0 GROUP BY ALL
    """).fetchall()
    totals = con.execute("SELECT hero_id, account_id, count(*) FROM nsall GROUP BY ALL").fetchall()
    return ([[int(h), int(a), int(b), int(g), int(t), int(w)] for h, a, b, g, t, w in rows],
            [[int(h), int(a), int(g)] for h, a, g in totals])


def built(matches_path, build_items_path, shop_path, out_path):
    build_items = json.load(open(build_items_path))
    shop_raw = json.load(open(shop_path))
    shop = {int(i): (v if isinstance(v, list) else ["", 0]) for i, v in
            (shop_raw.items() if isinstance(shop_raw, dict) else ((i, None) for i in shop_raw))}
    con = connect()
    con.execute("CREATE TABLE bi(build_id UBIGINT, item_id UBIGINT, core BOOLEAN)")
    rows_bi = []
    for b, v in build_items.items():
        every, core = (v.get("all", []), v.get("core", v.get("all", []))) if isinstance(v, dict) else (v, v)
        core = {int(i) for i in core}
        rows_bi += [(int(b), int(i), int(i) in core) for i in {int(i) for i in every}]
    con.executemany("INSERT INTO bi VALUES (?, ?, ?)", rows_bi)
    con.execute("CREATE TABLE shop(item_id UBIGINT, slot VARCHAR, tier INTEGER)")
    con.executemany("INSERT INTO shop VALUES (?, ?, ?)", [(i, str(v[0] or ""), int(v[1] or 0)) for i, v in shop.items()])
    # one row per (player, build, game, shop item bought), marked in or out of the build
    con.execute(f"""
        CREATE TABLE b AS
        WITH games AS (
            SELECT account_id, build_id, row_number() OVER () AS game, items
            FROM read_parquet('{matches_path}')
            WHERE build_id IN (SELECT DISTINCT build_id FROM bi)
        ), bought AS (
            SELECT DISTINCT account_id, build_id, game, CAST(unnest(items) AS UBIGINT) AS item_id FROM games
        )
        SELECT x.account_id, x.build_id, x.game, x.item_id, bi.item_id IS NOT NULL AS inside, s.slot, s.tier
        FROM bought x JOIN shop s ON s.item_id = x.item_id
        LEFT JOIN bi ON bi.build_id = x.build_id AND bi.item_id = x.item_id
    """)
    rows = con.execute("""
        SELECT account_id, build_id, round(100.0 * count(*) FILTER (WHERE inside) / count(*)),
               round(10.0 * count(*) FILTER (WHERE NOT inside) / count(DISTINCT game)), count(DISTINCT game)
        FROM b GROUP BY ALL
    """).fetchall()
    off = {}
    for a, bid, item, games in con.execute("""
        SELECT account_id, build_id, item_id, count(DISTINCT game) AS games FROM b WHERE NOT inside
        GROUP BY ALL ORDER BY games DESC, item_id
    """).fetchall():
        off.setdefault((a, bid), []).append((int(item), int(games)))

    # skipped core items: of the games where the player reached the item's tier, how many had it
    con.execute("CREATE TABLE reach AS SELECT account_id, build_id, game, max(tier) AS top FROM b GROUP BY ALL")
    con.execute("""
        CREATE TABLE skip AS
        SELECT r.account_id, r.build_id, bi.item_id, s.slot, s.tier,
               count(*) AS reached, count(h.item_id) AS had
        FROM reach r JOIN bi ON bi.build_id = r.build_id AND bi.core JOIN shop s ON s.item_id = bi.item_id
        LEFT JOIN b h ON h.account_id = r.account_id AND h.build_id = r.build_id AND h.game = r.game AND h.item_id = bi.item_id
        WHERE r.top >= s.tier AND s.tier > 0
        GROUP BY ALL
    """)
    skipped = {}
    for a, bid, item, reached, had in con.execute(f"""
        SELECT account_id, build_id, item_id, reached, had FROM skip
        WHERE reached >= {SKIP_MIN_GAMES} AND 100.0 * had / reached < {SKIP_MAX_PCT}
        ORDER BY had * 1.0 / reached, item_id
    """).fetchall():
        skipped.setdefault((a, bid), []).append([int(item), round(100 * had / reached)])
    # swaps: in the games that reached a skipped item's tier without it, the off-build item of the
    # same slot and tier bought most often
    swaps = {}
    for a, bid, item, repl, pct in con.execute(f"""
        WITH miss AS (
            SELECT k.account_id, k.build_id, k.item_id, k.slot, k.tier, r.game
            FROM skip k JOIN reach r ON r.account_id = k.account_id AND r.build_id = k.build_id AND r.top >= k.tier
            LEFT JOIN b h ON h.account_id = r.account_id AND h.build_id = r.build_id AND h.game = r.game AND h.item_id = k.item_id
            WHERE k.reached >= {SKIP_MIN_GAMES} AND 100.0 * k.had / k.reached < {SKIP_MAX_PCT} AND h.item_id IS NULL
        ), n AS (SELECT account_id, build_id, item_id, count(*) AS games FROM miss GROUP BY ALL),
        c AS (
            SELECT m.account_id, m.build_id, m.item_id, b.item_id AS repl, count(DISTINCT m.game) AS games
            FROM miss m JOIN b ON b.account_id = m.account_id AND b.build_id = m.build_id AND b.game = m.game
                              AND NOT b.inside AND b.slot = m.slot AND b.tier = m.tier
            GROUP BY ALL
        )
        SELECT c.account_id, c.build_id, c.item_id, c.repl, round(100.0 * c.games / n.games) AS pct
        FROM c JOIN n USING (account_id, build_id, item_id)
        WHERE 100.0 * c.games / n.games >= {SWAP_MIN_PCT}
        QUALIFY row_number() OVER (PARTITION BY c.account_id, c.build_id, c.item_id ORDER BY c.games DESC, c.repl) = 1
        ORDER BY pct DESC
    """).fetchall():
        swaps.setdefault((a, bid), []).append([int(item), int(repl), int(pct)])

    core = {}
    for a, bid, item, inside, pct in con.execute(f"""
        SELECT account_id, build_id, item_id, any_value(inside), round(100.0 * count(DISTINCT game) / any_value(n)) AS pct
        FROM b JOIN (SELECT account_id, build_id, count(DISTINCT game) AS n FROM b GROUP BY ALL) USING (account_id, build_id)
        WHERE tier >= {CORE_TIER}
        GROUP BY account_id, build_id, item_id
        HAVING pct >= {CORE_MIN_PCT}
        ORDER BY pct DESC, item_id
    """).fetchall():
        core.setdefault((a, bid), []).append([int(item), int(pct), 1 if inside else 0])

    out = []
    for a, bid, pct, per_game, games in rows:
        common = [[i, round(100 * g / games)] for i, g in off.get((a, bid), []) if 100 * g / games >= OFF_MIN_PCT][:OFF_TOP]
        out.append([int(a), int(bid), int(pct), int(per_game), common,
                    skipped.get((a, bid), [])[:OFF_TOP], swaps.get((a, bid), [])[:OFF_TOP], core.get((a, bid), [])[:CORE_TOP]])
    json.dump({"rows": out}, open(out_path, "w"))
    print(f"built %, off-build, skipped, swapped and finished items for {len(out)} (player, build) pairs", flush=True)


def main():
    mode = sys.argv[1]
    if mode == "usage":
        usage(sys.argv[2], sys.argv[3], sys.argv[4],
              int(sys.argv[5]) if len(sys.argv) > 5 else 45, float(sys.argv[6]) if len(sys.argv) > 6 else 7,
              sys.argv[7] if len(sys.argv) > 7 and sys.argv[7] else None, int(sys.argv[8]) if len(sys.argv) > 8 else 0)
    elif mode == "built":
        built(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        raise SystemExit("usage: pro_usage.py usage|built ...")


if __name__ == "__main__":
    main()

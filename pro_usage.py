"""Builds Improved: which build each tracked player actually played, from deadlock-api's data lake.

Reads match_player from the public parquet snapshots (https://deadlock-api.com/data-dumps): one scan
per run instead of one API call per player.

  python3 pro_usage.py usage <tracked.json> <out.json> <matches.parquet> [days] [half_life_days] [ns_matches.json]
      tracked.json: [account_id, ...]
      out.json:     {"generatedAt", "days", "halfLife",
                     "rows": [[hero, account, build, games, last_unix, weighted_games_x100], ...]}
                    weighted: each game counts 0.5 ** (age / half_life), so recent games count more
      matches.parquet: per (match, player) the build and the shop items bought, for the second step
      ns_matches.json: [{"match_id", "start"}] Night Shift matches; out.json then also has
                    "ns_rows": [[hero, account, build, games, last_unix], ...] (the builds loaded in them)

  python3 pro_usage.py built <matches.parquet> <build_items.json> <shop_items.json> <out.json>
      build_items.json: {build_id: [item ids]}   shop_items.json: [item ids]
      out.json: {"rows": [[account, build, built_pct], ...]}
                built %: of the shop items the player bought in their games with the build, the share
                that are in the build (did they follow it, or load it and buy something else)

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


FULL_SCAN_DAYS = 7     # a full rescan this often picks up the history of players new to the leaderboards
NEW_PLAYERS_FULL_SCAN = 50   # ...or as soon as more players than this are new to the tracked list


def usage(tracked_path, out_path, matches_path, days, half_life, ns_path=None):
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
            or now - state.get("full_at", 0) > FULL_SCAN_DAYS * 86400 or new_players > NEW_PLAYERS_FULL_SCAN)
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
        urls = [u for _, u in todo]
        source = f"{len(todo)} new files" if not full else f"{len(todo)} files (full scan)"
    else:
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
        con.execute(f"ATTACH 'ducklake:{CATALOG_URL}' AS lake (READ_ONLY)")
        urls, source = None, "the DuckLake catalog (full scan)"

    con.execute("CREATE TABLE watch(id UBIGINT)")
    con.executemany("INSERT INTO watch VALUES (?)", [(i,) for i in watch])
    con.execute("CREATE TABLE n (match_id UBIGINT, account_id UBIGINT, hero_id UBIGINT, build_id UBIGINT, started BIGINT, items UBIGINT[])")
    t0 = time.time()
    if urls is None or urls:
        if urls is None:
            con.execute("CREATE VIEW mp AS SELECT * FROM lake.match_player")
        else:
            con.execute(f"CREATE VIEW mp AS SELECT * FROM read_parquet({urls!r}, union_by_name = true)")
        # Column types differ by export (Enum as text or number, DateTime as timestamp or seconds).
        types = {r[0]: r[1].upper() for r in con.execute("DESCRIBE mp").fetchall()}
        st = "epoch(start_time)" if ("TIMESTAMP" in types.get("start_time", "") or "DATE" in types.get("start_time", "")) \
            else "CAST(start_time AS BIGINT)"
        mm = types.get("match_mode", "")
        ranked = "CAST(match_mode AS VARCHAR) = 'Ranked'" if ("VARCHAR" in mm or "ENUM" in mm) else "CAST(match_mode AS INTEGER) = 4"
        items = 'CAST("items.item_id" AS UBIGINT[])' if "items.item_id" in types else "CAST([] AS UBIGINT[])"
        print("scanning ranked games of tracked players…", flush=True)
        # One row per (match, player): the export can hold a match twice, one copy without its build.
        con.execute(f"""
            INSERT INTO n
            SELECT match_id, account_id, any_value(hero_id), max(hero_build_id), max({st}), arg_max({items}, len({items}))
            FROM mp
            WHERE {st} >= {since} AND {ranked} AND account_id IN (SELECT id FROM watch)
            GROUP BY match_id, account_id
            HAVING max(hero_build_id) > 0
        """)
    print(f"scan done in {round(time.time() - t0)}s: {con.execute('SELECT count(*) FROM n').fetchone()[0]} new (match, player) rows", flush=True)

    # saved rows + new rows, one per (match, player), inside the window
    old = f"SELECT * FROM read_parquet('{store_path}') WHERE started >= {since}" if not full else "SELECT * FROM n WHERE false"
    con.execute(f"""
        CREATE TABLE m AS
        SELECT * FROM ({old} UNION ALL SELECT * FROM n)
        QUALIFY row_number() OVER (PARTITION BY match_id, account_id ORDER BY len(items) DESC) = 1
    """)
    con.execute(f"COPY m TO '{store_path}' (FORMAT parquet)")
    keys = [k for k, _ in listing] if listing is not None else []
    json.dump({"generation": gen, "keys": keys, "watch": watch,
               "full_at": now if full else state.get("full_at", now)}, open(state_path, "w"))

    con.execute("CREATE TABLE tracked(id UBIGINT)")
    con.executemany("INSERT INTO tracked VALUES (?)", [(i,) for i in tracked])
    con.execute("CREATE TABLE t AS SELECT * FROM m WHERE account_id IN (SELECT id FROM tracked)")
    rows = con.execute(f"""
        SELECT hero_id, account_id, build_id, count(*) AS games, max(started) AS last,
               round(100 * sum(pow(0.5, ({now} - started) / ({half_life} * 86400.0)))) AS weighted
        FROM t GROUP BY ALL
    """).fetchall()
    con.execute(f"COPY (SELECT account_id, build_id, items FROM t) TO '{matches_path}' (FORMAT parquet)")

    ns_rows = []
    if ns_path:
        try:
            ns_rows = night_shift(con, m, json.load(open(ns_path)), lake_dir)
        except Exception as e:                              # Night Shift builds are extra: never fail the run
            print(f"Night Shift builds skipped: {e}", flush=True)

    json.dump({"generatedAt": now, "days": days, "halfLife": half_life,
               "rows": [[int(h), int(a), int(b), int(g), int(t), int(w)] for h, a, b, g, t, w in rows],
               "ns_rows": ns_rows},
              open(out_path, "w"))
    print(f"{len(rows)} (hero, player, build) rows from {source}, {len(tracked)} players tracked")


def night_shift(con, m, ns_matches, lake_dir):
    """The build each player loaded in Night Shift matches: [[hero, account, build, games, last], ...].
    ns_matches: [{"match_id", "start"}] from dlns-stats.co.uk. Matches already read are kept in
    <cache>/lake/ns_rows.parquet; only new ones are looked up, in the files whose match id range
    can hold them."""
    store, done_path = os.path.join(lake_dir, "ns_rows.parquet"), os.path.join(lake_dir, "ns_done.json")
    done = set(json.load(open(done_path))) if os.path.exists(done_path) and os.path.exists(store) else set()
    wanted = {int(x["match_id"]): int(x.get("start") or 0) for x in ns_matches if x.get("match_id")}
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
                SELECT match_id, account_id, any_value(hero_id), max(hero_build_id), max({st})
                FROM nsmp WHERE match_id IN (SELECT id FROM nsids)
                GROUP BY match_id, account_id HAVING max(hero_build_id) > 0
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
    json.dump(sorted(i for i in done if i in wanted), open(done_path, "w"))
    rows = con.execute("SELECT hero_id, account_id, build_id, count(*), max(started) FROM nsall GROUP BY ALL").fetchall()
    return [[int(h), int(a), int(b), int(g), int(t)] for h, a, b, g, t in rows]


def built(matches_path, build_items_path, shop_path, out_path):
    build_items = json.load(open(build_items_path))
    shop = sorted({int(i) for i in json.load(open(shop_path))})
    con = connect()
    con.execute("CREATE TABLE bi(build_id UBIGINT, item_id UBIGINT)")
    con.executemany("INSERT INTO bi VALUES (?, ?)", [(int(b), int(i)) for b, ids in build_items.items() for i in set(ids)])
    con.execute("CREATE TABLE shop(item_id UBIGINT)")
    con.executemany("INSERT INTO shop VALUES (?)", [(i,) for i in shop])
    rows = con.execute(f"""
        WITH games AS (
            SELECT account_id, build_id, row_number() OVER () AS game, items
            FROM read_parquet('{matches_path}')
            WHERE build_id IN (SELECT DISTINCT build_id FROM bi)
        ), bought AS (
            SELECT account_id, build_id, game, CAST(unnest(items) AS UBIGINT) AS item_id FROM games
        )
        SELECT b.account_id, b.build_id,
               round(100.0 * count(bi.item_id) / count(*)) AS pct
        FROM (SELECT DISTINCT account_id, build_id, game, item_id FROM bought
              WHERE item_id IN (SELECT item_id FROM shop)) b
        LEFT JOIN bi ON bi.build_id = b.build_id AND bi.item_id = b.item_id
        GROUP BY ALL
    """).fetchall()
    json.dump({"rows": [[int(a), int(b), int(p)] for a, b, p in rows]}, open(out_path, "w"))
    print(f"built % for {len(rows)} (player, build) pairs")


def main():
    mode = sys.argv[1]
    if mode == "usage":
        usage(sys.argv[2], sys.argv[3], sys.argv[4],
              int(sys.argv[5]) if len(sys.argv) > 5 else 45, float(sys.argv[6]) if len(sys.argv) > 6 else 7,
              sys.argv[7] if len(sys.argv) > 7 else None)
    elif mode == "built":
        built(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        raise SystemExit("usage: pro_usage.py usage|built ...")


if __name__ == "__main__":
    main()

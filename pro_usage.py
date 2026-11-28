"""Builds Improved: which build each tracked player actually played, from deadlock-api's data lake.

Reads match_player from the public parquet snapshots (https://deadlock-api.com/data-dumps): one scan
per run instead of one API call per player.

  python3 pro_usage.py usage <tracked.json> <out.json> <matches.parquet> [days] [half_life_days]
      tracked.json: [account_id, ...]
      out.json:     {"generatedAt", "days", "halfLife",
                     "rows": [[hero, account, build, games, last_unix, weighted_games_x100], ...]}
                    weighted: each game counts 0.5 ** (age / half_life), so recent games count more
      matches.parquet: per (match, player) the build and the shop items bought, for the second step

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


def manifest():
    src = os.environ.get("BI_MANIFEST", MANIFEST_URL)        # a local file for tests
    if not src.startswith("http"):
        return json.load(open(src))
    with urllib.request.urlopen(src, timeout=60) as r:
        return json.load(r)


def match_player_urls(m, since):
    """Parquet files of match_player's current generation that can hold matches started since `since`."""
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
        out.append(base + "/" + f["key"] if base.startswith("http") else os.path.join(base, f["key"]))
    return out


def connect():
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")                 # ClickHouse DateTime values are UTC
    except duckdb.Error:
        pass
    return con


def usage(tracked_path, out_path, matches_path, days, half_life):
    tracked = sorted({int(x) for x in json.load(open(tracked_path))})
    now = int(time.time())
    since = now - days * 86400

    urls = match_player_urls(manifest(), since)
    if not urls:
        raise SystemExit("no match_player files for the window")

    con = connect()
    if urls[0].startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE TABLE tracked(id UBIGINT)")
    con.executemany("INSERT INTO tracked VALUES (?)", [(i,) for i in tracked])
    con.execute(f"CREATE VIEW mp AS SELECT * FROM read_parquet({urls!r}, union_by_name = true)")

    # Column types differ by export (Enum as text or number, DateTime as timestamp or seconds).
    types = {r[0]: r[1].upper() for r in con.execute("DESCRIBE mp").fetchall()}
    st = "epoch(start_time)" if ("TIMESTAMP" in types.get("start_time", "") or "DATE" in types.get("start_time", "")) \
        else "CAST(start_time AS BIGINT)"
    mm = types.get("match_mode", "")
    ranked = "CAST(match_mode AS VARCHAR) = 'Ranked'" if ("VARCHAR" in mm or "ENUM" in mm) else "CAST(match_mode AS INTEGER) = 4"
    items = '"items.item_id"' if "items.item_id" in types else "CAST([] AS UINTEGER[])"

    # One row per (match, player): the export can hold a match twice, one copy without its build.
    con.execute(f"""
        CREATE TABLE m AS
        SELECT match_id, account_id, any_value(hero_id) AS hero_id, max(hero_build_id) AS build_id,
               max({st}) AS started, arg_max({items}, len({items})) AS items
        FROM mp
        WHERE {st} >= {since} AND {ranked}
          AND account_id IN (SELECT id FROM tracked)
        GROUP BY match_id, account_id
        HAVING max(hero_build_id) > 0
    """)
    rows = con.execute(f"""
        SELECT hero_id, account_id, build_id, count(*) AS games, max(started) AS last,
               round(100 * sum(pow(0.5, ({now} - started) / ({half_life} * 86400.0)))) AS weighted
        FROM m GROUP BY ALL
    """).fetchall()
    con.execute(f"COPY (SELECT account_id, build_id, items FROM m) TO '{matches_path}' (FORMAT parquet)")

    json.dump({"generatedAt": now, "days": days, "halfLife": half_life,
               "rows": [[int(h), int(a), int(b), int(g), int(t), int(w)] for h, a, b, g, t, w in rows]},
              open(out_path, "w"))
    print(f"{len(rows)} (hero, player, build) rows from {len(urls)} files, {len(tracked)} players tracked")


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
              int(sys.argv[5]) if len(sys.argv) > 5 else 45, float(sys.argv[6]) if len(sys.argv) > 6 else 7)
    elif mode == "built":
        built(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        raise SystemExit("usage: pro_usage.py usage|built ...")


if __name__ == "__main__":
    main()

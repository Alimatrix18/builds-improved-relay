"""Builds Improved: which build each tracked player actually played, from deadlock-api's data lake.

Reads match_player from the public parquet snapshots (https://deadlock-api.com/data-dumps), one
scan per run instead of one API call per player. For every tracked account it returns, per hero
and build, the ranked games played with it and when it was last played.

Usage: python3 pro_usage.py <tracked.json> <out.json> [days]
  tracked.json: [account_id, ...]
  out.json:     {"generatedAt": unix, "days": N, "rows": [[hero_id, account_id, build_id, games, last_unix], ...]}
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


def main():
    tracked_path, out_path = sys.argv[1], sys.argv[2]
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 14
    tracked = sorted({int(x) for x in json.load(open(tracked_path))})
    now = int(time.time())
    since = now - days * 86400

    urls = match_player_urls(manifest(), since)
    if not urls:
        raise SystemExit("no match_player files for the window")

    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")                 # ClickHouse DateTime values are UTC
    except duckdb.Error:
        pass
    if urls[0].startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE TABLE tracked(id UBIGINT)")
    con.executemany("INSERT INTO tracked VALUES (?)", [(i,) for i in tracked])
    con.execute(f"CREATE VIEW mp AS SELECT * FROM read_parquet({urls!r}, union_by_name = true)")

    # Column types differ by export (Enum as text or number, DateTime as timestamp or seconds).
    types = {r[0]: r[1].upper() for r in con.execute("DESCRIBE mp").fetchall()}
    st = "epoch(start_time)" if "TIMESTAMP" in types.get("start_time", "") or "DATE" in types.get("start_time", "") \
        else "CAST(start_time AS BIGINT)"
    mm = types.get("match_mode", "")
    ranked = "CAST(match_mode AS VARCHAR) = 'Ranked'" if ("VARCHAR" in mm or "ENUM" in mm) else "CAST(match_mode AS INTEGER) = 4"

    # One row per (match, player): the export can hold a match twice, one copy without its build.
    rows = con.execute(f"""
        WITH m AS (
            SELECT match_id, account_id, any_value(hero_id) AS hero_id,
                   max(hero_build_id) AS build_id, max({st}) AS started
            FROM mp
            WHERE {st} >= {since} AND {ranked}
              AND account_id IN (SELECT id FROM tracked)
            GROUP BY match_id, account_id
        )
        SELECT hero_id, account_id, build_id, count(*) AS games, max(started) AS last
        FROM m
        WHERE build_id IS NOT NULL AND build_id > 0
        GROUP BY ALL
    """).fetchall()

    json.dump({"generatedAt": now, "days": days,
               "rows": [[int(h), int(a), int(b), int(g), int(t)] for h, a, b, g, t in rows]},
              open(out_path, "w"))
    print(f"{len(rows)} (hero, player, build) rows from {len(urls)} files, {len(tracked)} players tracked")


if __name__ == "__main__":
    main()

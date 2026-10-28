import json

import duckdb

# endpoint folder → raw table, glob under data/raw/, natural key, and the record's timestamp
# commits/reviews/releases have no updated_at, so the closest real date stands in for it
ENDPOINTS = {
    "commits":  {"table": "raw_commits",       "glob": "*/commits/page_*.json",   "id": "sha",
                 "updated": lambda r: r["commit"]["author"]["date"]},
    "pulls":    {"table": "raw_pull_requests", "glob": "*/pulls/page_*.json",     "id": "number",
                 "updated": lambda r: r["updated_at"]},
    "reviews":  {"table": "raw_pr_reviews",    "glob": "*/reviews/*/page_*.json", "id": "id",
                 "updated": lambda r: r.get("submitted_at")},   # null for PENDING reviews
    "releases": {"table": "raw_releases",      "glob": "*/releases/page_*.json",  "id": "id",
                 "updated": lambda r: r.get("published_at")},   # null for drafts
}


# create the raw tables (one row per record) and the load-state table, if they don't exist
def create_tables(con):
    for cfg in ENDPOINTS.values():
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {cfg['table']} (
                repo        VARCHAR,
                id          VARCHAR,
                updated_at  TIMESTAMPTZ,
                fetched_at  TIMESTAMPTZ,
                source_file VARCHAR,
                payload     JSON,
                PRIMARY KEY (repo, id)
            )
        """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _load_state (
            repo           VARCHAR,
            endpoint       VARCHAR,
            max_updated_at TIMESTAMPTZ,
            loaded_at      TIMESTAMPTZ,
            PRIMARY KEY (repo, endpoint)
        )
    """)


# unwrap every page file for one endpoint into one row per record, then upsert on (repo, id)
def load_endpoint(con, raw_dir, cfg):
    rows = {}   # keyed by (repo, id), so a record that landed on two pages is kept once
    for path in sorted(raw_dir.glob(cfg["glob"])):
        page = json.loads(path.read_text())
        for rec in page["data"]:
            row = {
                "repo": page["repo"],
                "id": str(rec[cfg["id"]]),
                "updated_at": cfg["updated"](rec),
                "fetched_at": page["fetched_at"],
                "source_file": str(path),
                "payload": json.dumps(rec),
            }
            key = (row["repo"], row["id"])
            # keep the most recently fetched copy
            if key in rows and rows[key]["fetched_at"] > row["fetched_at"]:
                continue
            rows[key] = row

    if rows:
        con.executemany(f"""
            INSERT OR REPLACE INTO {cfg['table']}
            VALUES ($repo, $id, $updated_at, $fetched_at, $source_file, $payload)
        """, list(rows.values()))
    return len(rows)


# record the newest updated_at per repo and endpoint; Task 1 reads this as its watermark
def update_load_state(con):
    for endpoint, cfg in ENDPOINTS.items():
        con.execute(f"""
            INSERT OR REPLACE INTO _load_state
            SELECT repo, '{endpoint}', max(updated_at), now()
            FROM {cfg['table']}
            GROUP BY repo
        """)


def row_counts(con):
    return {cfg["table"]: con.execute(f"SELECT count(*) FROM {cfg['table']}").fetchone()[0]
            for cfg in ENDPOINTS.values()}


# load all landed files, then update _load_state, then touch _LOADED (in that order)
def load_all(raw_dir, db_path):
    with duckdb.connect(str(db_path)) as con:
        create_tables(con)
        before = row_counts(con)
        for cfg in ENDPOINTS.values():
            load_endpoint(con, raw_dir, cfg)
        update_load_state(con)
        after = row_counts(con)
    (raw_dir / "_LOADED").touch()
    return before, after

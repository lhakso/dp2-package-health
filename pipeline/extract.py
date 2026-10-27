import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

PER_PAGE = 100               # GitHub max is 100
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"   # project root / data / raw

TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}


# from 3022/04_apis/09_github_pr_vals.py
# GET a URL and check quota; if exhausted, sleep until reset and retry
def get_and_check_quota(url, params=None):
    while True:
        try:
            r = httpx.get(url, headers=HEADERS, params=params)
            remaining = int(r.headers.get("x-ratelimit-remaining", 0))
            reset = int(r.headers.get("x-ratelimit-reset", time.time()))

            # GitHub returns 403/429 once the quota is exhausted
            if r.status_code in (403, 429) and remaining == 0:
                wait = max(reset - time.time(), 0) + 1
                print(f"Quota exhausted, sleeping {wait:.0f}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json(), r.links
        except httpx.HTTPError as e:
            print(f"GET {url} failed: {e}")
            return None


# from 3022/04_apis/05_pagination.py
# use the "Link" header with the URL of the next page. httpx parses it into
# response.links. The "next" URL already includes per_page and page, so params are only
# needed on the first request. The loop ends when there is no "next" link (last page).
# stop_before: for pulls (sorted by updated desc), stop once a page ends before this timestamp
def paginate(repo, endpoint, url, params=None, stop_before=None):
    params = {"per_page": PER_PAGE, **(params or {})}
    page = 1
    total = 0
    while url:
        response = get_and_check_quota(url, params)
        if response is None:
            break
        records, links = response

        write_page(repo, endpoint, page, str(httpx.URL(url, params=params)), records)
        total += len(records)
        print(f"--- {repo} {endpoint} page {page}: {len(records)} records ---")

        # ISO 8601 UTC strings compare correctly as plain strings
        if stop_before and records and records[-1]["updated_at"] < stop_before:
            break

        url = links.get("next", {}).get("url")   # None on the last page
        params = None
        page += 1
    return total


# read the landed pulls pages and fetch reviews for every PR created since `since`
# each PR gets its own folder (reviews/<number>/) so page numbers don't collide
def fetch_reviews(repo, base_url, since):
    pulls_dir = RAW_DIR / repo.replace("/", "__") / "pulls"
    numbers = set()
    for path in sorted(pulls_dir.glob("page_*.json")):
        for pr in json.loads(path.read_text())["data"]:
            if pr["created_at"] >= since:
                numbers.add(pr["number"])

    total = 0
    for number in sorted(numbers):
        total += paginate(repo, f"reviews/{number}", f"{base_url}/{repo}/pulls/{number}/reviews")
    return len(numbers), total


# write one response page to data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json
# writes to a .tmp file first, then renames, so a crash never leaves a half-written page
def write_page(repo, endpoint, page, url, records):
    out_dir = RAW_DIR / repo.replace("/", "__") / endpoint
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"page_{page:04d}.json"
    tmp = path.with_suffix(".tmp")

    tmp.write_text(json.dumps({
        "repo": repo,
        "endpoint": endpoint,
        "url": url,
        "page": page,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": records,
    }))
    tmp.rename(path)
    return path

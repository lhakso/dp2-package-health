import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipeline.extract import fetch_reviews, paginate

PROJECT_DIR = Path(__file__).parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
PLOTS_DIR = PROJECT_DIR / "plots"
DB_PATH = PROJECT_DIR / "packages.duckdb"
LOADED_MARKER = RAW_DIR / "_LOADED"

REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]   # package set B
ENDPOINTS = ["commits", "pulls", "reviews", "releases"]                  # one data/raw/ subfolder each
SINCE = "2026-01-01T00:00:00Z"                                           # DEV window; real run: 2024-01-01T00:00:00Z

TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
BASE_URL = "https://api.github.com/repos"


@task
def extract():
    """Task 1: fetch all endpoints for all repos into data/raw/."""
    logger = get_run_logger()
    for repo in REPOS:
        n = paginate(repo, "commits", f"{BASE_URL}/{repo}/commits", {"since": SINCE})
        logger.info(f"{repo} commits: {n} records")

        n = paginate(repo, "pulls", f"{BASE_URL}/{repo}/pulls",
                     {"state": "all", "sort": "updated", "direction": "desc"}, stop_before=SINCE)
        logger.info(f"{repo} pulls: {n} records")

        prs, n = fetch_reviews(repo, BASE_URL, SINCE)
        logger.info(f"{repo} reviews: {n} records across {prs} PRs")

        n = paginate(repo, "releases", f"{BASE_URL}/{repo}/releases")
        logger.info(f"{repo} releases: {n} records")


@task
def load():
    """Task 2: upsert landed JSON into raw_* tables, update _load_state, touch _LOADED."""
    logger = get_run_logger()


@task
def dbt_build():
    """Task 3: run dbt build; fail on any model or test failure."""
    logger = get_run_logger()


@task
def validate():
    """Task 4: completeness checks, outlier detection, descriptive stats (no dbt)."""
    logger = get_run_logger()


@task
def model_and_rank():
    """Task 5: score metrics from mart_health_inputs, rank repos, write results to DB."""
    logger = get_run_logger()


@task
def plot():
    """Task 6: save dimension profile, monthly trends, and relationship plots to plots/."""
    logger = get_run_logger()


@flow(name="package-health")
def package_health_flow():
    extract()
    load()
    dbt_build()
    validate()
    model_and_rank()
    plot()


if __name__ == "__main__":
    package_health_flow()

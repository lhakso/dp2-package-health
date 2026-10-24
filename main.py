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

PROJECT_DIR = Path(__file__).parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
PLOTS_DIR = PROJECT_DIR / "plots"
DB_PATH = PROJECT_DIR / "packages.duckdb"
LOADED_MARKER = RAW_DIR / "_LOADED"

GITHUB_API = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GH_PAT")
WINDOW_START = "2024-01-01T00:00:00Z"

# TODO: set to your package set (A, B, or C)
REPOS = [
    "owner/large_repo",
    "owner/small_repo_1",
    "owner/small_repo_2",
]

ENDPOINTS = ["commits", "pulls", "reviews", "releases"]


@task
def extract():
    """Task 1: fetch all endpoints for all repos into data/raw/."""
    logger = get_run_logger()


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

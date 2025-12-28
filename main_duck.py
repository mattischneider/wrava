"""Set up Strava activities data in Motherduck DuckDB."""

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
LOGGER = logging.getLogger(__name__)

STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
MOTHERDUCK_TOKEN = os.getenv("MOTHER_DUCK_API_KEY", "")
HTTP_STATUS_OK = 200


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        type=int,
        help="Which year of activities to download from Strava.",
    )
    return parser.parse_args()


def get_access_token() -> str:
    """Fetch a new access token from Strava using the refresh token."""
    LOGGER.info("Fetching new access token from Strava.")
    url_endpoint = "https://www.strava.com/oauth/token"
    response = requests.post(
        url_endpoint,
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "refresh_token": STRAVA_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        },
        timeout=10,
    )
    if response.status_code != HTTP_STATUS_OK:
        error_message = f"Error fetching access token: {response.text}"
        LOGGER.error(error_message)
        raise RuntimeError
    response_data = response.json()
    return response_data["access_token"]


def year_to_unix_range(year: int) -> tuple[int, int]:
    """Convert a year to a tuple of (start_unix, end_unix) timestamps."""
    start = datetime.datetime(year, 1, 1, tzinfo=datetime.UTC)
    end = datetime.datetime(year + 1, 1, 1, tzinfo=datetime.UTC)
    return int(start.timestamp()), int(end.timestamp())


def fetch_activities(access_token: str, year: int | None = None) -> pd.DataFrame:
    """Fetch all activities for a given year or last 7 days from Strava API."""
    url_endpoint = "https://www.strava.com/api/v3/athlete/activities"
    if year is not None:
        after, before = year_to_unix_range(year)
    else:
        # Default to last 7 days
        now = datetime.datetime.now(datetime.UTC)
        after = int((now - datetime.timedelta(days=7)).timestamp())
        before = int(now.timestamp())
    all_activities = []
    page = 1
    per_page = 200

    while True:
        params = {"after": after, "before": before, "page": page, "per_page": per_page}
        response = requests.get(
            url_endpoint,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        activities = response.json()
        if not activities:
            break

        all_activities.extend(activities)
        page += 1

    return pd.DataFrame(all_activities)


def setup_duckdb() -> None:
    """Set up DuckDB table."""
    conn = duckdb.connect("md:my_db", config={"motherduck_token": MOTHERDUCK_TOKEN})
    conn.execute("create database if not exists strava;")
    conn.execute("use strava;")
    conn.execute("""
    create table if not exists activities_raw (
        id bigint primary key,
        name varchar,
        start_date_local timestamp,
        type varchar,
        distance double,
        moving_time int
    );
    """)
    conn.execute("""
    create or replace view activities as select
        id,
        name,
        start_date_local as start_date,
        type,
        case when type = 'Workout'
            then regexp_extract(name, '^(.*)\\s+with\\s+(.+)$', 1)
            end as workout_type,
        case when type in ('Workout', 'VirtualRide')
            then regexp_extract(name, '^(.*)\\s+with\\s+(.+)$', 2)
            end as coach,
        round(distance // 1000, 1) as distance_km,
        round(moving_time // 60, 1) as moving_time_min
    from activities_raw;
    """)
    conn.close()


def upsert_duckdb_activities() -> None:
    """Merge local activities csv files into DuckDB table."""
    conn = duckdb.connect("md:strava", config={"motherduck_token": MOTHERDUCK_TOKEN})
    for csv_file in Path().glob("*.csv"):
        conn.execute(f"""
            create temp table activities_staging as
            select * from read_csv_auto('{csv_file}');
        """)
        conn.execute("""
            merge into activities_raw
            using (select * from activities_staging) as upserts
            on (upserts.id = activities_raw.id)
            when matched then update
            when not matched then insert;
        """)
        conn.execute("drop table activities_staging;")
        LOGGER.info("Uploaded %s to DuckDB.", csv_file)
    conn.close()


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select only the specified columns from the DataFrame."""
    strava_columns_used = [
        "id",
        "name",
        "start_date_local",
        "type",
        "distance",
        "moving_time",
    ]
    return df[[c for c in strava_columns_used if c in df.columns]]


def main() -> None:
    """Entrypoint to set up the data in Motherduck from Strava."""
    args = parse_args()
    if args.year:
        LOGGER.info("Downloading activities for year: %s", args.year)
        access_token = get_access_token()
        activities_df = fetch_activities(access_token=access_token, year=args.year)
        df_clean = select_columns(activities_df)
        LOGGER.info("Downloaded %d activities", len(df_clean))
        df_clean.to_csv(f"activities_{args.year}.csv", index=False)
    else:
        LOGGER.info("No year specified, download activities from last 7 days only.")
        access_token = get_access_token()
        activities_df = fetch_activities(access_token=access_token)
        df_clean = select_columns(activities_df)
        LOGGER.info("Downloaded %d activities", len(df_clean))
        df_clean.to_csv("activities_last_7_days.csv", index=False)

    setup_duckdb()
    upsert_duckdb_activities()


if __name__ == "__main__":
    main()

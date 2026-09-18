"""
DATA 226 - Homework #3: Porting Homework #2 (Weather ETL) to Airflow

Flow:  extract (Open-Meteo archive API)
         -> transform (JSON -> list of rows, last 60 complete days)
         -> load (Snowflake full refresh inside a SQL transaction)

Airflow setup required (NOT stored in this file):
  Admin -> Variables   : latitude, longitude
  Admin -> Connections : snowflake_conn  (type: Snowflake)
"""
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ---- Settings -------------------------------------------------------------
# TODO: change to the table you used in HW2 if it was named differently
TARGET_TABLE = "dev.raw.weather_data"
NUM_DAYS = 60        # HW2 requirement: past 60 days
BUFFER_DAYS = 10     # archive API lags a few days; fetch extra, keep 60 complete
TIMEZONE = "Asia/Shanghai"


def return_snowflake_conn():
    """Return a cursor using the Airflow connection 'snowflake_conn'.
    Credentials live in Airflow's metadata DB, never in code."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude: float, longitude: float) -> dict:
    """Call the Open-Meteo historical (archive) API and return the 'daily' block."""
    end_date = datetime.now().date() - timedelta(days=1)                 # yesterday
    start_date = end_date - timedelta(days=NUM_DAYS + BUFFER_DAYS - 1)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code",
        "timezone": TIMEZONE,
    }
    resp = requests.get("https://archive-api.open-meteo.com/v1/archive",
                        params=params, timeout=30)
    resp.raise_for_status()                      # non-200 -> task fails -> retry
    data = resp.json()
    if "daily" not in data:
        raise ValueError(f"Unexpected API response: {str(data)[:300]}")
    print(f"Fetched {len(data['daily']['time'])} days: {start_date} ~ {end_date}")
    return data["daily"]                         # small dict -> safe for XCom


@task
def transform(daily: dict, latitude: float, longitude: float) -> list:
    """Column arrays -> one row per day. Drop incomplete days, keep last 60."""
    records = []
    for d, tmax, tmin, prcp, code in zip(daily["time"],
                                         daily["temperature_2m_max"],
                                         daily["temperature_2m_min"],
                                         daily["precipitation_sum"],
                                         daily["weather_code"]):
        if None in (tmax, tmin, prcp, code):
            continue                             # "all required columns populated"
        records.append([latitude, longitude, d, tmax, tmin, prcp, int(code)])

    records = records[-NUM_DAYS:]
    if len(records) != NUM_DAYS:
        raise ValueError(f"Expected {NUM_DAYS} complete days, got {len(records)}")
    if len({r[2] for r in records}) != NUM_DAYS:
        raise ValueError("Duplicate dates found")
    print(f"Transformed {len(records)} rows: {records[0][2]} ~ {records[-1][2]}")
    return records


@task
def load(records: list, target_table: str) -> None:
    """Full refresh: CREATE (DDL, auto-commits) then BEGIN -> DELETE -> INSERT -> COMMIT.
    Any error -> ROLLBACK, so the table keeps its previous 60 rows."""
    cur = return_snowflake_conn()
    try:
        # DDL commits immediately in Snowflake, so run it BEFORE BEGIN
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude      FLOAT,
                longitude     FLOAT,
                date          DATE,
                temp_max      FLOAT,
                temp_min      FLOAT,
                precipitation FLOAT,
                weather_code  INT,
                PRIMARY KEY (latitude, longitude, date)
            )""")

        cur.execute("BEGIN")
        cur.execute(f"DELETE FROM {target_table}")
        cur.executemany(
            f"""INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            records,
        )
        cur.execute("COMMIT")

        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        print(f"Loaded into {target_table}. Row count now = {cur.fetchone()[0]}")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"ROLLBACK executed because of: {e}")
        raise                                    # let Airflow mark the task failed
    finally:
        cur.close()


with DAG(
    dag_id="WeatherData_ETL",
    start_date=datetime(2026, 9, 1),
    schedule="30 2 * * *",                        # daily at 02:30 UTC
    catchup=False,
    tags=["ETL", "HW3"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
) as dag:
    # Airflow Variables (Admin -> Variables)
    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))

    daily = extract(latitude, longitude)
    records = transform(daily, latitude, longitude)
    load(records, TARGET_TABLE)
    # dependency is inferred from data passing: extract >> transform >> load

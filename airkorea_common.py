import os
import re
import unicodedata
from datetime import datetime
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import psycopg2
import requests


AIRKOREA_BASE_URL = (
    "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc"
)
AIRKOREA_STATION_BASE_URL = (
    "https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc"
)
TARGET_REGIONS = (
    "서울",
    "부산",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산",
    "세종",
)
REGION_ADDRESS_PREFIXES = {
    "서울": ("서울특별시", "서울 "),
    "부산": ("부산광역시", "부산 "),
    "대구": ("대구광역시", "대구 "),
    "인천": ("인천광역시", "인천 "),
    "광주": ("광주광역시", "광주 "),
    "대전": ("대전광역시", "대전 "),
    "울산": ("울산광역시", "울산 "),
    "세종": ("세종특별자치시", "세종 "),
}
DAILY_CALL_HARD_CAP = 400
KST = ZoneInfo("Asia/Seoul")


def get_db_connection():
    return psycopg2.connect(
        host=os.environ["DBHOST"],
        dbname=os.environ["DBNAME"],
        user=os.environ["DBUSER"],
        password=os.environ["DBPASS"],
    )


def normalize_station_name(value):
    normalized = unicodedata.normalize("NFKC", value or "")
    return re.sub(r"\s+", "", normalized).strip()


def station_external_code(region, station_name):
    return (
        f"AIRKOREA_{normalize_station_name(region)}_"
        f"{normalize_station_name(station_name)}"
    )


def station_belongs_to_region(region, item):
    address = unicodedata.normalize(
        "NFKC", str((item or {}).get("addr") or "")
    ).strip()
    prefixes = REGION_ADDRESS_PREFIXES.get(region, ())
    return any(address.startswith(prefix) for prefix in prefixes)


def ensure_usage_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS air.api_call_usage (
                provider text NOT NULL,
                usage_date date NOT NULL,
                call_count integer NOT NULL DEFAULT 0,
                updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (provider, usage_date)
            )
            """
        )
    conn.commit()


def reserve_api_call(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO air.api_call_usage(
                provider, usage_date, call_count, updated_at
            )
            VALUES (
                'AIRKOREA',
                (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date,
                1,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (provider, usage_date) DO UPDATE
            SET call_count=air.api_call_usage.call_count + 1,
                updated_at=CURRENT_TIMESTAMP
            WHERE air.api_call_usage.call_count < %s
            RETURNING call_count
            """,
            (DAILY_CALL_HARD_CAP,),
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        raise RuntimeError(
            f"AirKorea daily API call hard cap reached: "
            f"{DAILY_CALL_HARD_CAP}"
        )
    return row[0]


def is_auth_error(payload):
    header = (payload or {}).get("response", {}).get("header", {})
    code = str(header.get("resultCode") or "")
    message = str(header.get("resultMsg") or "").upper()
    return code in {"20", "22", "30", "31"} or any(
        marker in message
        for marker in ("SERVICE KEY", "AUTH", "UNREGISTERED", "NOT REGISTERED")
    )


def request_json(conn, url, params, timeout=25, decode_key=False):
    key = os.environ.get("AIRKOREA_KEY")
    if not key:
        raise RuntimeError("AIRKOREA_KEY is not configured")
    candidate = unquote(key) if decode_key else key
    reserve_api_call(conn)
    response = requests.get(
        url,
        params={**params, "serviceKey": candidate},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if is_auth_error(payload):
        raise RuntimeError("AirKorea authentication failed")
    return payload


def parse_observed_at(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=KST)


def to_int(value):
    if value in (None, "", "-"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None

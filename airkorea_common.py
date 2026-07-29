import os
import re
import unicodedata
from datetime import datetime, timedelta
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
REGION_TIERS = {
    "A": ("서울", "경기", "인천", "부산", "대구"),
    "B": ("대전", "광주", "울산", "경남", "경북", "충남", "충북"),
    "C": ("강원", "전북", "전남", "제주", "세종"),
}
TIER_RUNS_PER_DAY = {"A": 12, "B": 8, "C": 6}
TARGET_REGIONS = tuple(
    region
    for tier in ("A", "B", "C")
    for region in REGION_TIERS[tier]
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
    "경기": ("경기도", "경기 "),
    "강원": ("강원특별자치도", "강원도", "강원 "),
    "충북": ("충청북도", "충북 "),
    "충남": ("충청남도", "충남 "),
    "전북": ("전북특별자치도", "전라북도", "전북 "),
    "전남": ("전라남도", "전남 "),
    "경북": ("경상북도", "경북 "),
    "경남": ("경상남도", "경남 "),
    "제주": ("제주특별자치도", "제주 "),
}
GWANGJU_CORE_DISTRICTS = {"동구", "서구", "남구", "북구", "광산구"}
DAILY_CALL_HARD_CAP = 400
EXPECTED_DAILY_REALTIME_CALLS = sum(
    len(REGION_TIERS[tier]) * TIER_RUNS_PER_DAY[tier]
    for tier in REGION_TIERS
)
WORST_CASE_WEEKLY_SYNC_DAY_CALLS = (
    EXPECTED_DAILY_REALTIME_CALLS * 2 + len(TARGET_REGIONS) * 2
)
KST = ZoneInfo("Asia/Seoul")


def configured_regions():
    tier = (os.environ.get("AIRKOREA_TIER") or "").strip().upper()
    if tier:
        try:
            return REGION_TIERS[tier]
        except KeyError as exc:
            raise RuntimeError(f"Unknown AIRKOREA_TIER: {tier}") from exc
    requested = [
        value.strip()
        for value in (os.environ.get("AIRKOREA_REGIONS") or "").split(",")
        if value.strip()
    ]
    if requested:
        unknown = sorted(set(requested) - set(TARGET_REGIONS))
        if unknown:
            raise RuntimeError(
                f"Unknown AIRKOREA_REGIONS: {','.join(unknown)}"
            )
        return tuple(requested)
    return TARGET_REGIONS


def get_db_connection():
    return psycopg2.connect(
        host=os.environ["DBHOST"],
        port=int(os.environ.get("DBPORT", "5432")),
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
    if address.startswith("전남광주통합특별시 "):
        address_parts = address.split()
        district = address_parts[1] if len(address_parts) > 1 else ""
        if region == "광주":
            return district in GWANGJU_CORE_DISTRICTS
        if region == "전남":
            return district not in GWANGJU_CORE_DISTRICTS
        return False
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
    midnight_match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}) 24:(\d{2})", value or ""
    )
    if midnight_match:
        observed_date = datetime.strptime(
            midnight_match.group(1), "%Y-%m-%d"
        )
        return (
            observed_date
            + timedelta(days=1, minutes=int(midnight_match.group(2)))
        ).replace(tzinfo=KST)
    return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=KST)


def to_int(value):
    if value in (None, "", "-"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None

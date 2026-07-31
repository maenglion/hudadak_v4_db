"""Provider-neutral air observation selection queries."""

from typing import Optional


VALID_LOOKUP_MODES = {"current", "search"}
VALID_REGION_LEVELS = {"sido", "sigungu"}


def validate_search_scope(
    lookup_mode: str,
    region_level: Optional[str],
    region_code: Optional[str],
):
    if lookup_mode not in VALID_LOOKUP_MODES:
        raise ValueError("lookup_mode must be current or search")
    if lookup_mode == "current":
        return
    if region_level not in VALID_REGION_LEVELS:
        raise ValueError("search requires region_level=sido or sigungu")
    expected_length = 2 if region_level == "sido" else 5
    if not region_code or not region_code.isdigit():
        raise ValueError("search requires a numeric region_code")
    if len(region_code) != expected_length:
        raise ValueError(
            f"{region_level} region_code must be {expected_length} digits"
        )


def no_data_reason(lookup_mode: str) -> str:
    return (
        "NO_DATA_IN_REGION"
        if lookup_mode == "search"
        else "NO_OBSERVATION_WITHIN_RADIUS"
    )


def build_pm_query(
    lookup_mode: str,
    region_level: Optional[str],
    include_gases: bool = False,
) -> str:
    if lookup_mode == "search":
        scope_predicate = """
        EXISTS (
          SELECT 1
          FROM air.admin_regions requested_region
          WHERE requested_region.code = %s
            AND requested_region.level = 'REGION_LEVEL'
            AND ST_Covers(
              requested_region.geom,
              s.geom::geometry
            )
        )
        """.replace("REGION_LEVEL", region_level)
        scope_order = "display_ts DESC, distance_m ASC"
    else:
        scope_predicate = "ST_DWithin(s.geom, target.g, 50000)"
        scope_order = "distance_band ASC, display_ts DESC, distance_m ASC"

    def pollutant_ctes(pollutant: str) -> str:
        value_column = "pm10" if pollutant == "pm10" else "pm25"
        unit_column = "unit_pm10" if pollutant == "pm10" else "unit_pm25"
        return f"""
        {pollutant}_nearby AS (
          SELECT
            s.id AS {pollutant}_station_id,
            s.name AS {pollutant}_station,
            s.provider AS {pollutant}_provider,
            s.kind AS {pollutant}_source_kind,
            s.lat AS {pollutant}_lat,
            s.lon AS {pollutant}_lon,
            s.sido_code AS {pollutant}_sido_code,
            s.sigungu_code AS {pollutant}_sigungu_code,
            ST_Distance(s.geom, target.g) AS {pollutant}_distance_m,
            CASE
              WHEN ST_Distance(s.geom, target.g) <= 10000 THEN 1
              WHEN ST_Distance(s.geom, target.g) <= 25000 THEN 2
              WHEN ST_Distance(s.geom, target.g) <= 50000 THEN 3
              ELSE NULL
            END AS {pollutant}_distance_band,
            current_pm.{value_column} AS {pollutant},
            current_pm.{unit_column} AS {pollutant}_unit,
            current_pm.display_ts AS {pollutant}_display_ts
          FROM air.stations s
          CROSS JOIN target
          JOIN LATERAL (
            SELECT
              m.{value_column},
              m.{unit_column},
              m.ts AS display_ts
            FROM air.measurements m
            WHERE m.station_id = s.id
              AND m.ts <= CURRENT_TIMESTAMP
              AND m.ts >= CURRENT_TIMESTAMP - INTERVAL '3 hours'
              AND m.source_quality = 'observed'
              AND m.{value_column} IS NOT NULL
            ORDER BY m.ts DESC
            LIMIT 1
          ) current_pm ON TRUE
          WHERE s.geom IS NOT NULL
            AND {scope_predicate}
        ),
        {pollutant}_selected AS (
          SELECT *
          FROM {pollutant}_nearby
          ORDER BY
            {scope_order.replace("display_ts", f"{pollutant}_display_ts").replace("distance_m", f"{pollutant}_distance_m").replace("distance_band", f"{pollutant}_distance_band")}
          LIMIT 1
        )
        """

    def gas_ctes(pollutant: str) -> str:
        return f"""
        {pollutant}_nearby AS (
          SELECT
            s.id AS {pollutant}_station_id,
            s.name AS {pollutant}_station,
            s.provider AS {pollutant}_provider,
            s.lat AS {pollutant}_lat,
            s.lon AS {pollutant}_lon,
            ST_Distance(s.geom, target.g) AS {pollutant}_distance_m,
            CASE
              WHEN ST_Distance(s.geom, target.g) <= 10000 THEN 1
              WHEN ST_Distance(s.geom, target.g) <= 25000 THEN 2
              WHEN ST_Distance(s.geom, target.g) <= 50000 THEN 3
              ELSE NULL
            END AS {pollutant}_distance_band,
            current_gas.{pollutant} AS {pollutant},
            current_gas.display_ts AS {pollutant}_display_ts
          FROM air.stations s
          CROSS JOIN target
          JOIN LATERAL (
            SELECT
              m.{pollutant},
              m.ts AS display_ts
            FROM air.measurements m
            WHERE m.station_id = s.id
              AND m.ts <= CURRENT_TIMESTAMP
              AND m.ts >= CURRENT_TIMESTAMP - INTERVAL '12 hours'
              AND m.source_quality = 'observed'
              AND m.{pollutant} IS NOT NULL
            ORDER BY m.ts DESC
            LIMIT 1
          ) current_gas ON TRUE
          WHERE s.geom IS NOT NULL
            AND {scope_predicate}
        ),
        {pollutant}_selected AS (
          SELECT *
          FROM {pollutant}_nearby
          ORDER BY
            {scope_order.replace("display_ts", f"{pollutant}_display_ts").replace("distance_m", f"{pollutant}_distance_m").replace("distance_band", f"{pollutant}_distance_band")}
          LIMIT 1
        )
        """

    gas_pollutants = ("o3", "no2", "so2", "co") if include_gases else ()
    gas_cte_sql = "".join(
        f",\n{gas_ctes(pollutant)}" for pollutant in gas_pollutants
    )
    gas_join_sql = "".join(
        f"\nFULL OUTER JOIN {pollutant}_selected ON TRUE"
        for pollutant in gas_pollutants
    )
    return f"""
    WITH target AS (
      SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS g
    ),
    {pollutant_ctes("pm10")},
    {pollutant_ctes("pm25")}
    {gas_cte_sql}
    SELECT *
    FROM pm10_selected
    FULL OUTER JOIN pm25_selected ON TRUE
    {gas_join_sql}
    """


def query_params(
    lookup_mode: str,
    lon: float,
    lat: float,
    region_code: Optional[str],
    include_gases: bool = False,
):
    params = [lon, lat]
    if lookup_mode == "search":
        pollutant_count = 6 if include_gases else 2
        params.extend([region_code] * pollutant_count)
    return tuple(params)

import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = ROOT_DIR / "app"
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(APP_DIR))

import cleanup_measurements
import main
from fastapi import HTTPException


class FakeCursor:
    description = None

    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, row):
        self.row = row
        self.closed = False

    def cursor(self):
        return FakeCursor(self.row)

    def close(self):
        self.closed = True


class RetentionCursor:
    def __init__(self, rowcount):
        self.rowcount = rowcount
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.query = " ".join(query.split())
        self.params = params


class RetentionConnection:
    def __init__(self, deleted):
        self.cursor_instance = RetentionCursor(deleted)
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True


class MeasurementCursor(FakeCursor):
    def __init__(self, station, measurements, current_time):
        super().__init__(None)
        self.station = station
        self.measurements = measurements
        self.current_time = current_time

    def execute(self, query, params):
        super().execute(query, params)
        normalized_query = " ".join(query.split())
        assert "FROM air.measurements m" in normalized_query
        assert "JOIN LATERAL" in normalized_query
        assert "pm10_nearby AS" in normalized_query
        assert "pm25_nearby AS" in normalized_query
        assert "m.ts <= CURRENT_TIMESTAMP" in normalized_query
        assert "m.source_quality = 'observed'" in normalized_query
        assert "m.pm10 IS NOT NULL" in normalized_query
        assert "m.pm25 IS NOT NULL" in normalized_query
        assert "ORDER BY m.ts DESC" in normalized_query

        eligible = {
            pollutant: [
                measurement
                for measurement in self.measurements
                if measurement["ts"] <= self.current_time
                and measurement["ts"] >= (
                    self.current_time - timedelta(hours=3)
                )
                and measurement.get("source_quality") != "model"
                and measurement.get(pollutant) is not None
            ]
            for pollutant in ("pm10", "pm25")
        }
        if not any(eligible.values()):
            self.row = None
            return
        self.row = {}
        for pollutant in ("pm10", "pm25"):
            if not eligible[pollutant]:
                self.row[pollutant] = None
                continue
            latest = max(eligible[pollutant], key=lambda item: item["ts"])
            self.row.update({
                pollutant: latest[pollutant],
                f"{pollutant}_unit": latest.get(f"unit_{pollutant}"),
                f"{pollutant}_display_ts": latest["ts"],
                f"{pollutant}_station_id": self.station["station_id"],
                f"{pollutant}_station": self.station["name"],
                f"{pollutant}_provider": self.station["provider"],
                f"{pollutant}_source_kind": self.station["kind"],
                f"{pollutant}_lat": self.station["lat"],
                f"{pollutant}_lon": self.station["lon"],
                f"{pollutant}_distance_m": self.station["distance_m"],
            })


class MeasurementConnection(FakeConnection):
    def __init__(self, station, measurements, current_time):
        super().__init__(None)
        self.station = station
        self.measurements = measurements
        self.current_time = current_time

    def cursor(self):
        return MeasurementCursor(
            self.station, self.measurements, self.current_time
        )


class CandidateCursor(FakeCursor):
    def __init__(self, stations, measurements, current_time):
        super().__init__(None)
        self.stations = stations
        self.measurements = measurements
        self.current_time = current_time

    def execute(self, query, params):
        super().execute(query, params)
        normalized_query = " ".join(query.split())
        assert "pm10_nearby AS" in normalized_query
        assert "pm25_nearby AS" in normalized_query
        assert "m.ts <= CURRENT_TIMESTAMP" in normalized_query
        assert "m.source_quality = 'observed'" in normalized_query
        assert "ST_DWithin(s.geom, target.g, 50000)" in normalized_query
        self.row = {}
        for pollutant in ("pm10", "pm25"):
            candidates = []
            for station in self.stations:
                eligible = [
                    measurement
                    for measurement in self.measurements
                    if measurement["station_id"] == station["station_id"]
                    and measurement["ts"] <= self.current_time
                    and measurement.get("source_quality") != "model"
                    and measurement.get(pollutant) is not None
                ]
                if eligible:
                    latest = max(eligible, key=lambda item: item["ts"])
                    candidates.append((station, latest))
            candidates = [
                item
                for item in candidates
                if item[0]["distance_m"] <= 50000
            ]
            if not candidates:
                self.row[pollutant] = None
                continue
            station, latest = min(
                candidates,
                key=lambda item: (
                    1 if item[0]["distance_m"] <= 10000
                    else 2 if item[0]["distance_m"] <= 25000
                    else 3,
                    -item[1]["ts"].timestamp(),
                    item[0]["distance_m"],
                ),
            )
            self.row.update({
                pollutant: latest[pollutant],
                f"{pollutant}_unit": latest.get(f"unit_{pollutant}"),
                f"{pollutant}_display_ts": latest["ts"],
                f"{pollutant}_station_id": station["station_id"],
                f"{pollutant}_station": station["name"],
                f"{pollutant}_provider": station["provider"],
                f"{pollutant}_source_kind": station["kind"],
                f"{pollutant}_lat": station["lat"],
                f"{pollutant}_lon": station["lon"],
                f"{pollutant}_distance_m": station["distance_m"],
            })
        if not any(self.row.get(p) is not None for p in ("pm10", "pm25")):
            self.row = None


class CandidateConnection(FakeConnection):
    def __init__(self, stations, measurements, current_time):
        super().__init__(None)
        self.stations = stations
        self.measurements = measurements
        self.current_time = current_time

    def cursor(self):
        return CandidateCursor(
            self.stations, self.measurements, self.current_time
        )


class NearestTests(unittest.TestCase):
    def setUp(self):
        self.stored_row = {
            "station_id": 17,
            "name": "Widget station",
            "provider": "WAQI",
            "kind": "waqi_station",
            "lat": 37.5,
            "lon": 127.0,
            "distance_m": 125.0,
            "pm10": 31.0,
            "pm25": 14.0,
            "unit_pm10": "µg/m³",
            "unit_pm25": "µg/m³",
            "display_ts": "2026-07-23T12:00:00+09:00",
        }

    def test_retention_deletes_only_measurements_older_than_72_hours(self):
        connection = RetentionConnection(deleted=4)

        deleted = cleanup_measurements.delete_expired_measurements(connection)

        self.assertEqual(deleted, 4)
        self.assertTrue(connection.committed)
        self.assertIn(
            "DELETE FROM air.measurements",
            connection.cursor_instance.query,
        )
        self.assertIn(
            "ts < CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')",
            connection.cursor_instance.query,
        )
        self.assertEqual(connection.cursor_instance.params, (72,))

    def test_source_db_returns_stored_pm_without_calling_openmeteo(self):
        connection = FakeConnection(self.stored_row)

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(main, "cached_fetch_openmeteo", new=AsyncMock()) as openmeteo,
            patch.object(main, "_fetch_owm_gas_backup") as owm_backup,
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )

        openmeteo.assert_not_awaited()
        owm_backup.assert_not_called()
        self.assertTrue(connection.closed)
        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["source"], "db")
        self.assertEqual(response["source_kind"], "waqi_station")
        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["pm25"], 14.0)
        for gas_key in (
            "o3",
            "no2",
            "so2",
            "co",
            "gas_provider",
            "gas_source_kind",
            "gas_display_ts",
            "gas_station",
            "gas_meta",
        ):
            self.assertIsNone(response[gas_key])
        self.assertEqual(
            response["display_ts"], "2026-07-23T12:00:00+09:00"
        )

    def test_source_auto_separates_pm_and_gas_providers(self):
        current_time = datetime(2026, 7, 23, 18, 27, tzinfo=timezone(
            timedelta(hours=9)
        ))
        station = {
            key: value
            for key, value in self.stored_row.items()
            if key not in {
                "pm10", "pm25", "unit_pm10", "unit_pm25", "display_ts"
            }
        }
        past_observation = {
            "ts": current_time - timedelta(hours=1),
            "pm10": 31.0,
            "pm25": 14.0,
            "unit_pm10": "µg/m³",
            "unit_pm25": "µg/m³",
            "source_quality": "observed",
        }
        future_forecast = {
            "ts": current_time + timedelta(hours=6),
            "pm10": 101.0,
            "pm25": 81.0,
            "unit_pm10": "µg/m³",
            "unit_pm25": "µg/m³",
            "source_quality": "model",
        }
        connection = MeasurementConnection(
            station,
            [past_observation, future_forecast],
            current_time,
        )
        gas_payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "ozone": [45.0],
                "nitrogen_dioxide": [12.0],
                "sulphur_dioxide": [3.0],
                "carbon_monoxide": [210.0],
            }
        }

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(return_value=gas_payload),
            ) as openmeteo,
            patch.object(main, "_now_kst_floor_hour", return_value=main.datetime(
                2026, 7, 23, 12, 0
            )),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        openmeteo.assert_awaited_once()
        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["gas_provider"], "OPENMETEO")
        self.assertEqual(response["source_kind"], "waqi_station")
        self.assertEqual(response["gas_source_kind"], "model")
        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["o3"], 45.0)
        for key in ("o3", "no2", "so2", "co"):
            self.assertEqual(
                response["gas_meta"][key]["provider"], "OPENMETEO"
            )
            self.assertEqual(
                response["gas_meta"][key]["source_kind"], "model"
            )
            self.assertNotEqual(
                response["gas_meta"][key]["provider"], response["provider"]
            )
        self.assertNotEqual(
            response["gas_display_ts"], response["display_ts"]
        )
        self.assertLessEqual(response["display_ts"], current_time)

    def test_eight_hour_observed_o3_is_kept_and_stale_no2_falls_back(self):
        row = {
            **self.stored_row,
            "o3": 52.0,
            "o3_provider": "WAQI",
            "o3_station": "Nearby gas station",
            "o3_station_id": 91,
            "o3_display_ts": "2026-07-23T04:00:00+09:00",
            "o3_lat": 37.49,
            "o3_lon": 127.01,
            "o3_distance_m": 1500.0,
            # A 13-hour-old NO2 row is excluded by the SQL and therefore
            # reaches the response row as missing.
            "no2": None,
            "so2": None,
            "co": None,
        }
        model_payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "nitrogen_dioxide": [18.0],
                "sulphur_dioxide": [4.0],
                "carbon_monoxide": [220.0],
            }
        }
        with (
            patch.object(
                main,
                "get_db_connection",
                return_value=FakeConnection(row),
            ),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(return_value=model_payload),
            ),
            patch.object(
                main,
                "_now_kst_floor_hour",
                return_value=datetime(
                    2026, 7, 23, 12, 0, tzinfo=main.SEOUL_TZ
                ),
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["o3"], 52.0)
        self.assertEqual(
            response["gas_meta"]["o3"]["source_kind"], "observed"
        )
        self.assertEqual(
            response["gas_meta"]["o3"]["provider"], "WAQI"
        )
        self.assertEqual(response["no2"], 18.0)
        self.assertEqual(
            response["gas_meta"]["no2"]["provider"], "OPENMETEO"
        )
        self.assertEqual(
            response["gas_meta"]["no2"]["source_kind"], "model"
        )
        self.assertEqual(response["gas_source_kind"], "mixed")

    def test_observed_gases_keep_independent_stations_and_timestamps(self):
        row = {
            **self.stored_row,
            "o3": 41.0,
            "o3_provider": "WAQI",
            "o3_station": "Ozone station",
            "o3_station_id": 31,
            "o3_display_ts": "2026-07-23T10:00:00+09:00",
            "no2": 15.0,
            "no2_provider": "AIRKOREA",
            "no2_station": "Nitrogen station",
            "no2_station_id": 32,
            "no2_display_ts": "2026-07-23T11:00:00+09:00",
            "so2": 3.0,
            "so2_provider": "WAQI",
            "so2_station": "Sulfur station",
            "so2_station_id": 33,
            "so2_display_ts": "2026-07-23T09:00:00+09:00",
            "co": 205.0,
            "co_provider": "AIRKOREA",
            "co_station": "Carbon station",
            "co_station_id": 34,
            "co_display_ts": "2026-07-23T08:00:00+09:00",
        }
        with (
            patch.object(
                main,
                "get_db_connection",
                return_value=FakeConnection(row),
            ),
            patch.object(
                main, "cached_fetch_openmeteo", new=AsyncMock()
            ) as openmeteo,
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        openmeteo.assert_not_awaited()
        self.assertEqual(response["gas_provider"], "AIRKOREA+WAQI")
        self.assertEqual(response["gas_source_kind"], "observed")
        self.assertIsNone(response["gas_display_ts"])
        self.assertIsNone(response["gas_station"])
        self.assertEqual(
            response["gas_meta"]["o3"]["station"], "Ozone station"
        )
        self.assertEqual(
            response["gas_meta"]["no2"]["station"], "Nitrogen station"
        )
        self.assertNotEqual(
            response["gas_meta"]["o3"]["display_ts"],
            response["gas_meta"]["no2"]["display_ts"],
        )

    def test_future_openmeteo_gas_uses_latest_past_item_value(self):
        payload = {
            "hourly": {
                "time": [
                    "2026-07-23T11:00",
                    "2026-07-23T13:00",
                ],
                "ozone": [42.0, 999.0],
            }
        }
        selected = main._pick_latest_value(
            payload,
            "ozone",
            now=datetime(
                2026, 7, 23, 12, 0, tzinfo=main.SEOUL_TZ
            ),
        )
        self.assertEqual(selected["value"], 42.0)
        self.assertEqual(
            selected["display_ts"], "2026-07-23T11:00"
        )

    def test_openmeteo_gas_failure_leaves_only_failed_items_null(self):
        row = {
            **self.stored_row,
            "provider": "AIRKOREA",
            "name": "AirKorea station",
            "kind": "airkorea_station",
        }
        connection = FakeConnection(row)
        gas_payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "ozone": [None],
                "nitrogen_dioxide": [None],
                "sulphur_dioxide": [None],
                "carbon_monoxide": [None],
            }
        }
        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(return_value=gas_payload),
            ),
            patch.object(main, "_fetch_owm_gas_backup") as owm_backup,
            patch.object(main, "_now_kst_floor_hour", return_value=main.datetime(
                2026, 7, 23, 12, 0
            )),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["provider"], "AIRKOREA")
        self.assertIsNone(response["gas_provider"])
        self.assertIsNone(response["gas_station"])
        self.assertIsNone(response["gas_display_ts"])
        for key in ("o3", "no2", "so2", "co"):
            self.assertIsNone(response[key])
            self.assertIsNone(response["gas_meta"][key])
        owm_backup.assert_not_called()

    def test_partial_openmeteo_gases_do_not_discard_valid_items(self):
        connection = FakeConnection(self.stored_row)
        gas_payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "ozone": [45.0],
                "nitrogen_dioxide": [None],
                "sulphur_dioxide": [3.0],
                "carbon_monoxide": [None],
            }
        }
        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(return_value=gas_payload),
            ),
            patch.object(main, "_fetch_owm_gas_backup") as owm_backup,
            patch.object(main, "_now_kst_floor_hour", return_value=main.datetime(
                2026, 7, 23, 12, 0
            )),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["gas_provider"], "OPENMETEO")
        self.assertEqual(
            response["gas_display_ts"], "2026-07-23T12:00:00"
        )
        self.assertIsNone(response["gas_station"])
        self.assertEqual(response["gas_meta"]["o3"]["provider"], "OPENMETEO")
        self.assertEqual(response["gas_meta"]["so2"]["provider"], "OPENMETEO")
        self.assertIsNone(response["gas_meta"]["no2"])
        self.assertIsNone(response["gas_meta"]["co"])
        self.assertEqual(
            response["gas_meta"]["o3"]["display_ts"],
            "2026-07-23T12:00:00",
        )
        self.assertEqual(response["o3"], 45.0)
        self.assertIsNone(response["no2"])
        owm_backup.assert_not_called()

    def test_source_db_returns_explicit_radius_error_when_no_row_exists(self):
        connection = FakeConnection(None)

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(main, "cached_fetch_openmeteo", new=AsyncMock()) as openmeteo,
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(
                    main.nearest(lat=37.5, lon=127.0, source="db")
                )

        openmeteo.assert_not_awaited()
        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(
            raised.exception.detail["code"],
            "NO_OBSERVATION_WITHIN_RADIUS",
        )

    def test_source_db_uses_past_observation_instead_of_future_forecast(self):
        current_time = datetime(2026, 7, 23, 18, 27, tzinfo=timezone(
            timedelta(hours=9)
        ))
        station = {
            "station_id": 17,
            "name": "Mixed timeline station",
            "provider": "WAQI",
            "kind": "waqi_station",
            "lat": 37.5,
            "lon": 127.0,
            "distance_m": 125.0,
        }
        past_observation = {
            "ts": current_time - timedelta(hours=1),
            "pm10": 28.0,
            "pm25": 11.0,
            "unit_pm10": "µg/m³",
            "unit_pm25": "µg/m³",
            "source_quality": "observed",
        }
        future_forecast = {
            "ts": current_time + timedelta(hours=6),
            "pm10": 99.0,
            "pm25": 77.0,
            "unit_pm10": "µg/m³",
            "unit_pm25": "µg/m³",
            "source_quality": "model",
        }
        connection = MeasurementConnection(
            station,
            [past_observation, future_forecast],
            current_time,
        )

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(main, "cached_fetch_openmeteo", new=AsyncMock()) as openmeteo,
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )

        openmeteo.assert_not_awaited()
        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["pm10"], 28.0)
        self.assertEqual(response["pm25"], 11.0)
        self.assertEqual(response["display_ts"], past_observation["ts"])
        self.assertLessEqual(response["display_ts"], current_time)

    def test_newer_airkorea_beats_closer_stale_waqi(self):
        current_time = datetime(
            2026, 7, 23, 18, 30, tzinfo=timezone(timedelta(hours=9))
        )
        stations = [
            {
                "station_id": 1,
                "name": "Closer WAQI",
                "provider": "WAQI",
                "kind": "waqi_station",
                "lat": 37.5,
                "lon": 127.0,
                "distance_m": 100.0,
            },
            {
                "station_id": 2,
                "name": "Fresher AirKorea",
                "provider": "AIRKOREA",
                "kind": "airkorea_station",
                "lat": 37.51,
                "lon": 127.01,
                "distance_m": 350.0,
            },
        ]
        measurements = [
            {
                "station_id": 1,
                "ts": current_time - timedelta(hours=3),
                "pm10": 41.0,
                "pm25": 21.0,
                "source_quality": "observed",
            },
            {
                "station_id": 2,
                "ts": current_time - timedelta(minutes=30),
                "pm10": 22.0,
                "pm25": 10.0,
                "source_quality": "observed",
            },
        ]
        connection = CandidateConnection(
            stations, measurements, current_time
        )

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(main, "cached_fetch_openmeteo", new=AsyncMock()) as openmeteo,
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )

        openmeteo.assert_not_awaited()
        self.assertEqual(response["provider"], "AIRKOREA")
        self.assertEqual(response["name"], "Fresher AirKorea")
        self.assertEqual(response["source_kind"], "airkorea_station")
        self.assertEqual(response["display_ts"], measurements[1]["ts"])
        self.assertEqual(response["pm10"], 22.0)

    def test_equal_timestamp_chooses_nearer_station(self):
        current_time = datetime(
            2026, 7, 23, 18, 30, tzinfo=timezone(timedelta(hours=9))
        )
        shared_ts = current_time - timedelta(minutes=30)
        stations = [
            {
                "station_id": 1,
                "name": "Near WAQI",
                "provider": "WAQI",
                "kind": "waqi_station",
                "lat": 37.5,
                "lon": 127.0,
                "distance_m": 100.0,
            },
            {
                "station_id": 2,
                "name": "Far AirKorea",
                "provider": "AIRKOREA",
                "kind": "airkorea_station",
                "lat": 37.51,
                "lon": 127.01,
                "distance_m": 350.0,
            },
        ]
        measurements = [
            {
                "station_id": 1,
                "ts": shared_ts,
                "pm10": 31.0,
                "pm25": 14.0,
                "source_quality": "observed",
            },
            {
                "station_id": 2,
                "ts": shared_ts,
                "pm10": 22.0,
                "pm25": 10.0,
                "source_quality": "observed",
            },
        ]
        connection = CandidateConnection(
            stations, measurements, current_time
        )

        with (
            patch.object(main, "get_db_connection", return_value=connection),
            patch.object(main, "cached_fetch_openmeteo", new=AsyncMock()) as openmeteo,
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )

        openmeteo.assert_not_awaited()
        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["name"], "Near WAQI")
        self.assertEqual(response["display_ts"], shared_ts)

    def test_fresher_outer_band_cannot_displace_nearer_band(self):
        current_time = datetime(
            2026, 7, 23, 18, 30,
            tzinfo=timezone(timedelta(hours=9)),
        )
        stations = [
            {
                "station_id": 1,
                "name": "Near band WAQI",
                "provider": "WAQI",
                "kind": "waqi_station",
                "lat": 37.5,
                "lon": 127.0,
                "distance_m": 9000.0,
            },
            {
                "station_id": 2,
                "name": "Outer band AirKorea",
                "provider": "AIRKOREA",
                "kind": "airkorea_station",
                "lat": 37.6,
                "lon": 127.1,
                "distance_m": 11000.0,
            },
        ]
        measurements = [
            {
                "station_id": 1,
                "ts": current_time - timedelta(hours=3),
                "pm10": 31.0,
                "pm25": 14.0,
                "source_quality": "observed",
            },
            {
                "station_id": 2,
                "ts": current_time - timedelta(minutes=10),
                "pm10": 19.0,
                "pm25": 8.0,
                "source_quality": "observed",
            },
        ]
        connection = CandidateConnection(
            stations, measurements, current_time
        )

        with patch.object(
            main, "get_db_connection", return_value=connection
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )

        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["name"], "Near band WAQI")

    def test_model_only_stations_do_not_displace_observed_candidate(self):
        current_time = datetime(
            2026, 7, 24, 21, 0, tzinfo=timezone(timedelta(hours=9))
        )
        stations = [
            {
                "station_id": index,
                "name": f"Model {index}",
                "provider": "OWM",
                "kind": "model",
                "lat": 37.3925,
                "lon": 126.6399,
                "distance_m": float(index * 10),
            }
            for index in range(1, 12)
        ]
        stations.append(
            {
                "station_id": 99,
                "name": "Aam, Incheon",
                "provider": "WAQI",
                "kind": "station",
                "lat": 37.40508,
                "lon": 126.63227,
                "distance_m": 1500.0,
            }
        )
        measurements = [
            {
                "station_id": station["station_id"],
                "ts": current_time,
                "pm10": 15.0,
                "pm25": 10.0,
                "source_quality": "model",
            }
            for station in stations[:-1]
        ]
        measurements.append(
            {
                "station_id": 99,
                "ts": current_time - timedelta(hours=1),
                "pm10": 34.0,
                "pm25": 78.0,
                "source_quality": "observed",
            }
        )
        connection = CandidateConnection(
            stations, measurements, current_time
        )

        with patch.object(
            main, "get_db_connection", return_value=connection
        ):
            response = asyncio.run(
                main.nearest(lat=37.3925, lon=126.6399, source="db")
            )

        self.assertEqual(response["provider"], "WAQI")
        self.assertEqual(response["name"], "Aam, Incheon")

    def test_pm10_and_pm25_return_independent_metadata(self):
        row = {
            "pm10": 36.0,
            "pm10_unit": "ug/m3",
            "pm10_provider": "AIRKOREA",
            "pm10_station": "인천 신흥",
            "pm10_station_id": 91412,
            "pm10_display_ts": "2026-07-28T14:00:00+09:00",
            "pm10_source_kind": "airkorea_station",
            "pm10_lat": 37.4,
            "pm10_lon": 126.62,
            "pm10_distance_m": 6362.6,
            "pm25": 72.0,
            "pm25_unit": "ug/m3",
            "pm25_provider": "WAQI",
            "pm25_station": "Aam, Incheon",
            "pm25_station_id": 91221,
            "pm25_display_ts": "2026-07-28T13:00:00+09:00",
            "pm25_source_kind": "waqi_station",
            "pm25_lat": 37.40508,
            "pm25_lon": 126.63227,
            "pm25_distance_m": 1412.2,
        }
        with patch.object(
            main, "get_db_connection", return_value=FakeConnection(row)
        ):
            response = asyncio.run(
                main.nearest(lat=37.4102, lon=126.6177, source="db")
            )

        self.assertEqual(response["pm10_meta"]["provider"], "AIRKOREA")
        self.assertEqual(response["pm25_meta"]["provider"], "WAQI")
        self.assertNotEqual(
            response["pm10_meta"]["display_ts"],
            response["pm25_meta"]["display_ts"],
        )
        self.assertEqual(response["provider"], "AIRKOREA")
        self.assertEqual(response["name"], "인천 신흥")

    def test_pm10_only_returns_200_shape_with_null_pm25_meta(self):
        row = {
            **self.stored_row,
            "pm25": None,
        }
        with patch.object(
            main, "get_db_connection", return_value=FakeConnection(row)
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )
        self.assertEqual(response["pm10"], 31.0)
        self.assertIsNone(response["pm25"])
        self.assertIsNotNone(response["pm10_meta"])
        self.assertIsNone(response["pm25_meta"])

    def test_pm25_only_returns_200_shape_with_null_pm10_meta(self):
        row = {
            **self.stored_row,
            "pm10": None,
        }
        with patch.object(
            main, "get_db_connection", return_value=FakeConnection(row)
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="db")
            )
        self.assertIsNone(response["pm10"])
        self.assertEqual(response["pm25"], 14.0)
        self.assertIsNone(response["pm10_meta"])
        self.assertIsNotNone(response["pm25_meta"])
        self.assertEqual(response["provider"], "WAQI")

    def test_source_auto_keeps_same_pm_metadata_as_db(self):
        row = {
            **self.stored_row,
            "pm10_provider": "AIRKOREA",
            "pm10_station": "신흥",
            "pm10_station_id": 10,
            "pm10_display_ts": "2026-07-28T14:00:00+09:00",
            "pm10_source_kind": "airkorea_station",
            "pm25_provider": "WAQI",
            "pm25_station": "Aam",
            "pm25_station_id": 20,
            "pm25_display_ts": "2026-07-28T13:00:00+09:00",
            "pm25_source_kind": "waqi_station",
        }
        empty_gas = {"hourly": {"time": [], "ozone": [], "nitrogen_dioxide": [],
                                "sulphur_dioxide": [], "carbon_monoxide": []}}
        with (
            patch.object(
                main, "get_db_connection", return_value=FakeConnection(row)
            ),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(return_value=empty_gas),
            ),
            patch.object(main, "_fetch_owm_gas_backup", return_value={}),
        ):
            auto = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )
        self.assertEqual(auto["pm10_meta"]["provider"], "AIRKOREA")
        self.assertEqual(auto["pm25_meta"]["provider"], "WAQI")

    def test_future_openmeteo_hour_is_never_selected(self):
        now = datetime(
            2026, 7, 30, 12, 30, tzinfo=main.SEOUL_TZ
        )
        payload = {
            "hourly": {
                "time": [
                    "2026-07-30T11:00",
                    "2026-07-30T13:00",
                ],
                "pm10": [18.0, 999.0],
            }
        }

        selected = main._pick_latest_value(
            payload, "pm10", now=now
        )

        self.assertEqual(selected["value"], 18.0)
        self.assertEqual(selected["display_ts"], "2026-07-30T11:00")

    def test_observation_exactly_three_hours_old_is_used(self):
        current_time = datetime(
            2026, 7, 30, 12, 0, tzinfo=main.SEOUL_TZ
        )
        connection = MeasurementConnection(
            self.stored_row,
            [{
                "ts": current_time - timedelta(hours=3),
                "pm10": 31.0,
                "pm25": 14.0,
                "source_quality": "observed",
            }],
            current_time,
        )
        payload = {"hourly": {"time": []}}
        with (
            patch.object(
                main, "get_db_connection", return_value=connection
            ),
            patch.object(
                main, "cached_fetch_openmeteo",
                new=AsyncMock(return_value=payload),
            ),
            patch.object(
                main, "_fetch_owm_gas_backup", return_value={}
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["pm25"], 14.0)
        self.assertEqual(response["pm10_meta"]["provider"], "WAQI")
        self.assertEqual(response["pm25_meta"]["provider"], "WAQI")

    def test_observation_older_than_three_hours_uses_model(self):
        current_time = datetime(
            2026, 7, 30, 12, 0, tzinfo=main.SEOUL_TZ
        )
        connection = MeasurementConnection(
            self.stored_row,
            [{
                "ts": current_time - timedelta(
                    hours=3, seconds=1
                ),
                "pm10": 31.0,
                "pm25": 14.0,
                "source_quality": "observed",
            }],
            current_time,
        )
        payload = {
            "hourly": {
                "time": ["2026-07-30T12:00"],
                "pm10": [17.0],
                "pm2_5": [9.0],
            }
        }
        with (
            patch.object(
                main, "get_db_connection", return_value=connection
            ),
            patch.object(
                main, "cached_fetch_openmeteo",
                new=AsyncMock(return_value=payload),
            ),
            patch.object(
                main,
                "_now_kst_floor_hour",
                return_value=current_time,
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["pm10"], 17.0)
        self.assertEqual(response["pm25"], 9.0)
        self.assertEqual(
            response["pm10_meta"]["provider"], "OPENMETEO"
        )
        self.assertEqual(
            response["pm25_meta"]["source_kind"], "model"
        )

    def test_only_future_openmeteo_hours_returns_no_value(self):
        now = datetime(
            2026, 7, 30, 12, 30, tzinfo=main.SEOUL_TZ
        )
        payload = {
            "hourly": {
                "time": ["2026-07-30T13:00"],
                "pm2_5": [999.0],
            }
        }

        selected = main._pick_latest_value(
            payload, "pm2_5", now=now
        )

        self.assertIsNone(selected["value"])
        self.assertIsNone(selected["display_ts"])

    def test_only_future_model_response_is_not_returned_as_current(self):
        payload = {
            "hourly": {
                "time": ["2026-07-30T13:00"],
                "pm10": [999.0],
                "pm2_5": [999.0],
            }
        }
        with (
            patch.object(
                main, "get_db_connection", return_value=None
            ),
            patch.object(
                main, "cached_fetch_openmeteo",
                new=AsyncMock(return_value=payload),
            ),
            patch.object(
                main,
                "_now_kst_floor_hour",
                return_value=datetime(
                    2026, 7, 30, 12, 0,
                    tzinfo=main.SEOUL_TZ,
                ),
            ),
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(
                    main.nearest(
                        lat=37.5, lon=127.0, source="auto"
                    )
                )

        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(
            raised.exception.detail["code"], "MODEL_PM_UNAVAILABLE"
        )

    def test_pm10_observed_pm25_model_fallback_are_independent(self):
        row = {**self.stored_row, "pm25": None}
        payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "pm2_5": [22.0],
            }
        }
        with (
            patch.object(
                main, "get_db_connection",
                return_value=FakeConnection(row),
            ),
            patch.object(
                main, "cached_fetch_openmeteo",
                new=AsyncMock(return_value=payload),
            ),
            patch.object(
                main, "_fetch_owm_gas_backup", return_value={}
            ),
            patch.object(
                main,
                "_now_kst_floor_hour",
                return_value=datetime(
                    2026, 7, 23, 12, 0, tzinfo=main.SEOUL_TZ
                ),
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["pm10_meta"]["provider"], "WAQI")
        self.assertEqual(response["pm25"], 22.0)
        self.assertEqual(
            response["pm25_meta"]["provider"], "OPENMETEO"
        )
        self.assertEqual(
            response["pm25_meta"]["source_kind"], "model"
        )
        self.assertEqual(response["pm25_meta"]["lat"], 37.5)
        self.assertEqual(response["pm25_meta"]["lon"], 127.0)
        self.assertIsNone(response["pm25_meta"]["station"])

    def test_pm25_observed_pm10_model_fallback_are_independent(self):
        row = {**self.stored_row, "pm10": None}
        payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "pm10": [44.0],
            }
        }
        with (
            patch.object(
                main, "get_db_connection",
                return_value=FakeConnection(row),
            ),
            patch.object(
                main, "cached_fetch_openmeteo",
                new=AsyncMock(return_value=payload),
            ),
            patch.object(
                main, "_fetch_owm_gas_backup", return_value={}
            ),
            patch.object(
                main,
                "_now_kst_floor_hour",
                return_value=datetime(
                    2026, 7, 23, 12, 0, tzinfo=main.SEOUL_TZ
                ),
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["pm10"], 44.0)
        self.assertEqual(
            response["pm10_meta"]["provider"], "OPENMETEO"
        )
        self.assertEqual(response["pm25"], 14.0)
        self.assertEqual(response["pm25_meta"]["provider"], "WAQI")

    def test_widget_pm_fallback_matches_app_without_gases(self):
        row = {**self.stored_row, "pm25": None}
        payload = {
            "hourly": {
                "time": ["2026-07-23T12:00"],
                "pm2_5": [22.0],
            }
        }

        async def request(source, pm_fallback=False):
            with (
                patch.object(
                    main, "get_db_connection",
                    return_value=FakeConnection(row),
                ),
                patch.object(
                    main, "cached_fetch_openmeteo",
                    new=AsyncMock(return_value=payload),
                ),
                patch.object(
                    main, "_fetch_owm_gas_backup", return_value={}
                ),
                patch.object(
                    main,
                    "_now_kst_floor_hour",
                    return_value=datetime(
                        2026, 7, 23, 12, 0,
                        tzinfo=main.SEOUL_TZ,
                    ),
                ),
            ):
                return await main.nearest(
                    lat=37.5,
                    lon=127.0,
                    source=source,
                    pm_fallback=pm_fallback,
                )

        app_response = asyncio.run(request("auto"))
        widget_response = asyncio.run(request("db", True))

        for key in ("pm10", "pm25", "pm10_meta", "pm25_meta"):
            self.assertEqual(widget_response[key], app_response[key])
        self.assertIsNone(widget_response["gas_provider"])
        self.assertIsNone(widget_response["gas_meta"])

    def test_openmeteo_failure_keeps_valid_observed_pollutant(self):
        row = {**self.stored_row, "pm25": None}
        with (
            patch.object(
                main, "get_db_connection",
                return_value=FakeConnection(row),
            ),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(side_effect=RuntimeError("offline")),
            ),
            patch.object(
                main, "_fetch_owm_gas_backup", return_value={}
            ),
        ):
            response = asyncio.run(
                main.nearest(lat=37.5, lon=127.0, source="auto")
            )

        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["pm10_meta"]["provider"], "WAQI")
        self.assertIsNone(response["pm25"])
        self.assertIsNone(response["pm25_meta"])

    def test_observation_and_openmeteo_failure_propagates_error(self):
        with (
            patch.object(
                main, "get_db_connection",
                return_value=FakeConnection(None),
            ),
            patch.object(
                main,
                "cached_fetch_openmeteo",
                new=AsyncMock(side_effect=RuntimeError("offline")),
            ),
        ):
            with self.assertRaises(RuntimeError):
                asyncio.run(
                    main.nearest(
                        lat=37.5, lon=127.0, source="auto"
                    )
                )


if __name__ == "__main__":
    unittest.main()

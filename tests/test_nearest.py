import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

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
        assert "m.ts <= CURRENT_TIMESTAMP" in normalized_query
        assert "m.source_quality IS DISTINCT FROM 'model'" in normalized_query
        assert "(m.pm10 IS NOT NULL OR m.pm25 IS NOT NULL)" in normalized_query
        assert "ORDER BY m.ts DESC" in normalized_query
        assert normalized_query.index(
            "ORDER BY m.ts DESC"
        ) < normalized_query.index("ORDER BY ST_Distance")

        eligible = [
            measurement
            for measurement in self.measurements
            if measurement["ts"] <= self.current_time
            and measurement.get("source_quality") != "model"
            and (
                measurement.get("pm10") is not None
                or measurement.get("pm25") is not None
            )
        ]
        if not eligible:
            self.row = None
            return

        latest = max(eligible, key=lambda measurement: measurement["ts"])
        self.row = {
            **self.station,
            "pm10": latest.get("pm10"),
            "pm25": latest.get("pm25"),
            "unit_pm10": latest.get("unit_pm10"),
            "unit_pm25": latest.get("unit_pm25"),
            "display_ts": latest["ts"],
        }


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
        self.assertIsNone(response["gas_provider"])
        self.assertIsNone(response["gas_source_kind"])
        self.assertEqual(response["pm10"], 31.0)
        self.assertEqual(response["pm25"], 14.0)
        self.assertIsNone(response["o3"])
        self.assertIsNone(response["no2"])
        self.assertIsNone(response["so2"])
        self.assertIsNone(response["co"])
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
        self.assertLessEqual(response["display_ts"], current_time)

    def test_source_db_returns_204_when_no_row_exists(self):
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
        self.assertEqual(raised.exception.status_code, 204)

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


if __name__ == "__main__":
    unittest.main()

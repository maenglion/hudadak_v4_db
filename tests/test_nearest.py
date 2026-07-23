import asyncio
import sys
import unittest
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
        connection = FakeConnection(self.stored_row)
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


if __name__ == "__main__":
    unittest.main()

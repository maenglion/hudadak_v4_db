import sys
import unittest
from unittest.mock import patch
from datetime import timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import ingest_waqi
import airkorea_common
import ingest_airkorea
import sync_airkorea_stations


class IngesterTests(unittest.TestCase):
    def test_waqi_time_prefers_timezone_aware_iso_over_epoch(self):
        parsed = ingest_waqi.parse_waqi_ts(
            {
                "v": 1784923200,
                "s": "2026-07-24 20:00:00",
                "tz": "+09:00",
                "iso": "2026-07-24T20:00:00+09:00",
            }
        )

        self.assertEqual(parsed.isoformat(), "2026-07-24T20:00:00+09:00")
        self.assertEqual(parsed.utcoffset(), timedelta(hours=9))

    def test_job_keeps_firms_optional_and_runs_cleanup_last(self):
        script = (ROOT_DIR / "ingest-all.sh").read_text(encoding="utf-8")

        firms = script.index('run_optional "FIRMS"')
        cleanup = script.index("python /app/cleanup_measurements.py")
        success_check = script.index("if (( core_success == 0 ))")
        self.assertLess(firms, cleanup)
        self.assertLess(cleanup, success_check)
        self.assertNotIn('run_core "FIRMS"', script)

    def test_combined_job_keeps_waqi_on_its_own_schedule(self):
        script = (ROOT_DIR / "ingest-all.sh").read_text(encoding="utf-8")

        self.assertIn('run_core "WAQI" "ingest_waqi.py"', script)
        self.assertNotIn('run_core "AIRKOREA"', script)

    def test_airkorea_targets_all_regions_in_configured_tiers(self):
        self.assertEqual(
            airkorea_common.REGION_TIERS,
            {
                "A": ("서울", "경기", "인천", "부산", "대구"),
                "B": (
                    "대전", "광주", "울산", "경남",
                    "경북", "충남", "충북",
                ),
                "C": ("강원", "전북", "전남", "제주", "세종"),
            },
        )
        self.assertEqual(airkorea_common.DAILY_CALL_HARD_CAP, 400)
        self.assertEqual(
            airkorea_common.EXPECTED_DAILY_REALTIME_CALLS, 146
        )
        self.assertEqual(
            airkorea_common.WORST_CASE_WEEKLY_SYNC_DAY_CALLS, 326
        )

    def test_airkorea_tier_limits_each_execution_to_its_regions(self):
        with patch.dict("os.environ", {"AIRKOREA_TIER": "B"}, clear=False):
            self.assertEqual(
                airkorea_common.configured_regions(),
                airkorea_common.REGION_TIERS["B"],
            )

    def test_airkorea_uses_one_regional_realtime_endpoint(self):
        self.assertTrue(
            ingest_airkorea.REALTIME_ENDPOINT.endswith(
                "/getCtprvnRltmMesureDnsty"
            )
        )
        self.assertNotIn(
            "getMsrstnAcctoRltmMesureDnsty",
            (ROOT_DIR / "ingest_airkorea.py").read_text(encoding="utf-8"),
        )

    def test_station_sync_uses_official_station_list_endpoint(self):
        self.assertTrue(
            sync_airkorea_stations.STATION_ENDPOINT.endswith(
                "/getMsrstnList"
            )
        )

    def test_station_external_code_normalizes_whitespace(self):
        self.assertEqual(
            airkorea_common.station_external_code("인천", " 송도  "),
            "AIRKOREA_인천_송도",
        )

    def test_airkorea_midnight_24_hour_timestamp_rolls_to_next_day(self):
        observed_at = airkorea_common.parse_observed_at(
            "2026-07-24 24:00"
        )
        self.assertEqual(observed_at.isoformat(), "2026-07-25T00:00:00+09:00")

    def test_station_list_filters_to_requested_metropolitan_region(self):
        self.assertTrue(
            airkorea_common.station_belongs_to_region(
                "광주", {"addr": "광주광역시 북구 첨단과기로"}
            )
        )
        self.assertTrue(
            airkorea_common.station_belongs_to_region(
                "광주", {"addr": "광주 북구 첨단과기로"}
            )
        )
        self.assertFalse(
            airkorea_common.station_belongs_to_region(
                "광주", {"addr": "경기도 광주시 중앙로"}
            )
        )
        self.assertFalse(
            airkorea_common.station_belongs_to_region(
                "광주", {"addr": "전라남도 광양시 중마로"}
            )
        )
        self.assertTrue(
            airkorea_common.station_belongs_to_region(
                "광주",
                {"addr": "전남광주통합특별시 광산구 하남산단로"},
            )
        )
        self.assertFalse(
            airkorea_common.station_belongs_to_region(
                "광주",
                {"addr": "전남광주통합특별시 광양시 중마로"},
            )
        )
        self.assertTrue(
            airkorea_common.station_belongs_to_region(
                "전남",
                {"addr": "전남광주통합특별시 광양시 중마로"},
            )
        )
        self.assertFalse(
            airkorea_common.station_belongs_to_region(
                "전남",
                {"addr": "전남광주통합특별시 북구 첨단과기로"},
            )
        )
        self.assertFalse(
            airkorea_common.station_belongs_to_region(
                "광주",
                {"addr": "전남광주통합특별시 광주시 중앙로"},
            )
        )

    def test_airkorea_hourly_runs_retention_after_collection(self):
        script = (ROOT_DIR / "airkorea-hourly.sh").read_text(
            encoding="utf-8"
        )
        self.assertLess(
            script.index("python /app/ingest_airkorea.py"),
            script.index("python /app/cleanup_measurements.py"),
        )
        self.assertIn("RUN_RETENTION_CLEANUP", script)

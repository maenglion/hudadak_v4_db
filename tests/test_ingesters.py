import sys
import unittest
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

    def test_airkorea_targets_only_eight_metropolitan_regions(self):
        self.assertEqual(
            airkorea_common.TARGET_REGIONS,
            ("서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종"),
        )
        self.assertEqual(airkorea_common.DAILY_CALL_HARD_CAP, 400)

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

    def test_airkorea_hourly_runs_retention_after_collection(self):
        script = (ROOT_DIR / "airkorea-hourly.sh").read_text(
            encoding="utf-8"
        )
        self.assertLess(
            script.index("python /app/ingest_airkorea.py"),
            script.index("python /app/cleanup_measurements.py"),
        )

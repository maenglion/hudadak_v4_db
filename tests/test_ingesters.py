import sys
import unittest
from datetime import timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import ingest_waqi


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

    def test_job_runs_waqi_and_airkorea_as_core_providers(self):
        script = (ROOT_DIR / "ingest-all.sh").read_text(encoding="utf-8")

        self.assertIn('run_core "WAQI" "ingest_waqi.py"', script)
        self.assertIn(
            'run_core "AIRKOREA" "ingest_airkorea.py"', script
        )

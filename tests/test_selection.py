import sys
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

from selection import (
    build_pm_query,
    no_data_reason,
    query_params,
    validate_search_scope,
)


class SelectionPolicyTests(unittest.TestCase):
    def test_current_uses_distance_bands_before_freshness(self):
        query = " ".join(build_pm_query("current", None).split())

        self.assertIn("ST_DWithin(s.geom, target.g, 50000)", query)
        self.assertIn(
            "ORDER BY pm10_distance_band ASC, "
            "pm10_display_ts DESC, pm10_distance_m ASC",
            query,
        )
        self.assertIn(
            "ORDER BY pm25_distance_band ASC, "
            "pm25_display_ts DESC, pm25_distance_m ASC",
            query,
        )
        self.assertNotIn("provider ASC", query)

    def test_search_filters_by_sigungu_code(self):
        query = " ".join(build_pm_query("search", "sigungu").split())

        self.assertEqual(
            query.count("requested_region.code = %s"), 2
        )
        self.assertEqual(
            query.count("requested_region.level = 'sigungu'"), 2
        )
        self.assertEqual(query.count("ST_Covers("), 2)
        self.assertNotIn("ST_DWithin(s.geom, target.g, 50000)", query)
        self.assertIn(
            "ORDER BY pm10_display_ts DESC, pm10_distance_m ASC",
            query,
        )

    def test_search_filters_by_sido_code(self):
        query = build_pm_query("search", "sido")
        self.assertEqual(query.count("requested_region.code = %s"), 2)
        self.assertEqual(
            query.count("requested_region.level = 'sido'"), 2
        )

    def test_pm_candidates_require_observed_rows(self):
        query = build_pm_query("current", None)
        self.assertEqual(
            query.count("m.source_quality = 'observed'"), 2
        )
        self.assertIn("m.pm10 IS NOT NULL", query)
        self.assertIn("m.pm25 IS NOT NULL", query)

    def test_search_requires_a_level_specific_numeric_code(self):
        validate_search_scope("search", "sido", "41")
        validate_search_scope("search", "sigungu", "41110")
        with self.assertRaises(ValueError):
            validate_search_scope("search", "sigungu", "41")
        with self.assertRaises(ValueError):
            validate_search_scope("search", "sido", "경기")

    def test_search_params_repeat_region_code_for_each_pollutant(self):
        self.assertEqual(
            query_params("search", 127.0, 37.5, "41110"),
            (127.0, 37.5, "41110", "41110"),
        )

    def test_no_data_reasons_are_mode_specific(self):
        self.assertEqual(
            no_data_reason("search"), "NO_DATA_IN_REGION"
        )
        self.assertEqual(
            no_data_reason("current"),
            "NO_OBSERVATION_WITHIN_RADIUS",
        )


if __name__ == "__main__":
    unittest.main()

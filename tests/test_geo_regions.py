import sys
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

from routers.geo import _choose_sigungu_row, _legal_sigungu_code


class GeoRegionTests(unittest.TestCase):
    def test_city_search_prefers_named_city_over_overlapping_district(self):
        district = ("41", "경기도", "41115", "경기도 팔달구", "팔달구", 1)
        city = ("41", "경기도", "41110", "경기도 수원시", "수원시", 4)

        selected = _choose_sigungu_row(
            [district, city], "경기 수원시"
        )

        self.assertEqual(selected[2], "41110")

    def test_address_without_region_name_uses_smallest_polygon(self):
        district = ("28", "인천광역시", "28185", "인천광역시 연수구", "연수구", 1)
        city = ("28", "인천광역시", "28000", "인천광역시", "인천", 4)

        selected = _choose_sigungu_row(
            [district, city], "랜드마크로 113"
        )

        self.assertEqual(selected[2], "28185")

    def test_kakao_legal_district_code_supplies_sigungu_fallback(self):
        document = {"address": {"b_code": "2818510600"}}
        self.assertEqual(_legal_sigungu_code(document), "28185")

    def test_missing_kakao_legal_district_code_is_ignored(self):
        self.assertIsNone(_legal_sigungu_code({"address": {}}))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.weather_models import _parse_mav_or_met_block, _parse_mex_block


MAV_SAMPLE = """KSEA   GFS MOS GUIDANCE    3/09/2026  1800 UTC
 DT /MAR  10                  /MAR  11                /MAR  12
 HR   00 03 06 09 12 15 18 21 00 03 06 09 12 15 18 21 00 03 06 12 18
 N/X              33          43          39          50       44
 TMP  43 40 38 36 35 34 38 41 41 41 42 41 41 42 45 48 50 50 49 46 43
"""

MEX_SAMPLE = """KSEA   GFSX MOS GUIDANCE   3/09/2026  1200 UTC
 FHR  24  36| 48  60| 72  84| 96 108|120 132|144 156|168 180|192
      TUE 10| WED 11| THU 12| FRI 13| SAT 14| SUN 15| MON 16|TUE CLIMO
 N/X  34  44| 39  50| 44  49| 44  53| 37  49| 35  48| 43  58| 42 39 54
 TMP  36  43| 41  49| 47  48| 45  51| 38  48| 36  47| 45  56| 45
"""


class WeatherModelsMOSTests(unittest.TestCase):
    def test_parse_mav_block_extracts_daily_highs(self) -> None:
        rows = _parse_mav_or_met_block(MAV_SAMPLE)
        self.assertEqual(rows["2026-03-10"], (33.0, 43.0))
        self.assertEqual(rows["2026-03-11"], (39.0, 50.0))
        self.assertEqual(rows["2026-03-12"], (44.0, 44.0))

    def test_parse_mex_block_extracts_daily_pairs(self) -> None:
        rows = _parse_mex_block(MEX_SAMPLE)
        self.assertEqual(rows["2026-03-10"], (34.0, 44.0))
        self.assertEqual(rows["2026-03-11"], (39.0, 50.0))
        self.assertEqual(rows["2026-03-16"], (43.0, 58.0))
        self.assertNotIn("2026-03-17", rows)


if __name__ == "__main__":
    unittest.main()

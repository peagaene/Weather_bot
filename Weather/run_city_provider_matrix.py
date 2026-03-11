from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS


PROVIDER_CAPABILITIES: dict[str, dict[str, str]] = {
    "open_meteo": {
        "kind": "forecast",
        "scope": "global",
        "status": "active",
        "reference": "https://open-meteo.com/en/docs",
    },
    "nws": {
        "kind": "forecast_and_observation",
        "scope": "us_only",
        "status": "active",
        "reference": "https://www.weather.gov/documentation/services-web-api",
    },
    "mos": {
        "kind": "forecast",
        "scope": "us_station_only",
        "status": "active",
        "reference": "https://vlab.noaa.gov/web/mdl/mos-documentation",
    },
    "hrrr": {
        "kind": "forecast",
        "scope": "us_domain_only",
        "status": "active",
        "reference": "https://rapidrefresh.noaa.gov/hrrr/",
    },
    "tomorrow": {
        "kind": "forecast",
        "scope": "global",
        "status": "candidate",
        "reference": "https://docs.tomorrow.io/reference/weather-forecast/",
    },
    "weatherapi": {
        "kind": "forecast",
        "scope": "global",
        "status": "candidate",
        "reference": "https://www.weatherapi.com/docs/",
    },
    "visualcrossing": {
        "kind": "forecast_and_history",
        "scope": "global",
        "status": "candidate",
        "reference": "https://www2.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/",
    },
    "openweather": {
        "kind": "forecast",
        "scope": "global",
        "status": "candidate",
        "reference": "https://openweathermap.org/api/one-call-api",
    },
    "meteostat": {
        "kind": "truth_observation",
        "scope": "global_station_or_point",
        "status": "candidate",
        "reference": "https://dev.meteostat.net/python",
    },
    "isd": {
        "kind": "truth_observation",
        "scope": "global_station",
        "status": "candidate",
        "reference": "https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database",
    },
}


def main() -> int:
    rows: list[dict[str, object]] = []
    for city in CITY_CONFIGS:
        active = ["open_meteo"]
        if city.supports_nws:
            active.append("nws")
        if city.supports_mos:
            active.append("mos")
        if city.supports_hrrr:
            active.append("hrrr")
        candidates = ["tomorrow", "weatherapi", "visualcrossing", "openweather", "meteostat", "isd"]
        rows.append(
            {
                "city_key": city.key,
                "display_name": city.display_name,
                "market_city": city.market_city,
                "timezone_name": city.timezone_name,
                "market_temp_unit": city.market_temp_unit,
                "live_enabled": city.live_enabled,
                "official_station_code": city.official_station_code,
                "active_provider_support": active,
                "candidate_provider_support": candidates,
                "regime_tags": list(city.regime_tags),
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider_capabilities": PROVIDER_CAPABILITIES,
        "cities": rows,
    }
    output_path = ROOT / "export" / "analysis" / "city_provider_matrix.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

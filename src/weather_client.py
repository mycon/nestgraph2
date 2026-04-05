"""
Outdoor weather data for Nest R-value and delta-T computations.

Two backends are supported (select via ``config.json → weather.outdoor_source``):

  open_meteo  (default)
    Free, no API key required; uses lat/lon from config.
    https://open-meteo.com/

  nest
    Uses Nest's built-in forecast endpoint — same data the Nest app shows.
    Requires ``nest.postal_code`` (and optionally ``nest.country_code``) in
    config.json.  The request is authenticated with the Nest session token.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_NEST_WEATHER_URL = "https://home.nest.com/api/0.1/weather/forecast/{postal}"
_TIMEOUT = 15


class WeatherClient:
    """Open-Meteo weather client (lat/lon based)."""

    def __init__(self, lat: float, lon: float) -> None:
        self.lat = lat
        self.lon = lon

    def get_current(self) -> dict[str, Any]:
        """
        Return current weather as::

            {
                "outdoor_temp_c": float,
                "outdoor_humidity": float,
                "wind_speed_mph": float,
                "wind_gusts_mph": float,
                "solar_irradiance_wm2": float,
                "cloud_cover_pct": float,
                "source": "open_meteo",
            }

        Returns None values on failure.
        """
        params = {
            "latitude": self.lat,
            "longitude": self.lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,shortwave_radiation,cloud_cover",
            "temperature_unit": "celsius",
            "wind_speed_unit": "mph",
            "timezone": "auto",
        }
        try:
            resp = requests.get(_OPEN_METEO_URL, params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            current = data.get("current", {})
            return {
                "outdoor_temp_c": current.get("temperature_2m"),
                "outdoor_humidity": current.get("relative_humidity_2m"),
                "wind_speed_mph": current.get("wind_speed_10m"),
                "wind_gusts_mph": current.get("wind_gusts_10m"),
                "solar_irradiance_wm2": current.get("shortwave_radiation"),
                "cloud_cover_pct": current.get("cloud_cover"),
                "source": "open_meteo",
            }
        except Exception as exc:
            log.warning("Open-Meteo fetch failed: %s", exc)
            return {
                "outdoor_temp_c": None,
                "outdoor_humidity": None,
                "wind_speed_mph": None,
                "wind_gusts_mph": None,
                "solar_irradiance_wm2": None,
                "cloud_cover_pct": None,
                "source": "open_meteo",
            }


class NestWeatherClient:
    """
    Nest built-in weather client.

    Mirrors PHP Nest::getWeather() — hits the home.nest.com forecast endpoint
    that the Nest thermostat itself uses to display outdoor temperature.
    """

    def __init__(self, postal_code: str, country_code: str = "", access_token: str = "") -> None:
        self.postal_code = postal_code
        self.country_code = country_code
        self._access_token = access_token

    def update_token(self, access_token: str) -> None:
        self._access_token = access_token

    def get_current(self) -> dict[str, Any]:
        """
        Return current weather as::

            {
                "outdoor_temp_c": float,
                "outdoor_humidity": float,
                "wind_speed_mph": float,
                "wind_gusts_mph": float,
                "solar_irradiance_wm2": float,
                "cloud_cover_pct": float,
                "source": "nest",
            }
        """
        url = _NEST_WEATHER_URL.format(postal=self.postal_code)
        if self.country_code:
            url += f",{self.country_code}"
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Basic {self._access_token}"
        try:
            resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            now = data.get("now", {})
            temp_c = now.get("current_temperature")
            humidity = now.get("current_humidity")
            return {
                "outdoor_temp_c": float(temp_c) if temp_c is not None else None,
                "outdoor_humidity": float(humidity) if humidity is not None else None,
                "wind_speed_mph": None,
                "wind_gusts_mph": None,
                "solar_irradiance_wm2": None,
                "cloud_cover_pct": None,
                "source": "nest",
            }
        except Exception as exc:
            log.warning("Nest weather fetch failed: %s", exc)
            return {
                "outdoor_temp_c": None,
                "outdoor_humidity": None,
                "wind_speed_mph": None,
                "wind_gusts_mph": None,
                "solar_irradiance_wm2": None,
                "cloud_cover_pct": None,
                "source": "nest",
            }


def make_weather_client(cfg: dict[str, Any], access_token: str = "") -> WeatherClient | NestWeatherClient:
    """
    Factory function: returns the appropriate weather client for the given config.

    ``cfg`` should be the ``weather`` section of config.json::

        {
            "outdoor_source": "open_meteo",   # or "nest"
            "lat": 40.7128,
            "lon": -74.0060
        }

    plus ``nest.postal_code`` and ``nest.country_code`` from the ``nest`` section
    (pass them merged in, or pass the full config dict and let the factory look them up).
    """
    source = cfg.get("outdoor_source", "open_meteo").lower()
    if source == "nest":
        postal = cfg.get("postal_code") or cfg.get("nest", {}).get("postal_code", "")
        country = cfg.get("country_code") or cfg.get("nest", {}).get("country_code", "US")
        if not postal:
            log.warning("weather.outdoor_source=nest but nest.postal_code is empty; falling back to open_meteo")
            source = "open_meteo"
        else:
            return NestWeatherClient(postal, country, access_token)
    # open_meteo (default)
    lat = cfg.get("lat")
    lon = cfg.get("lon")
    if lat is None or lon is None:
        log.warning("weather.lat/lon not set; weather will return None values")
        lat = lat or 0.0
        lon = lon or 0.0
    return WeatherClient(float(lat), float(lon))

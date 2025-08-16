from datetime import datetime

import httpx
from loguru import logger

from berlin_departure_board.config import settings
from berlin_departure_board.models import Departure, DepartureParsedInfo, TransportMode


class BVGAPIClient:
    def __init__(self) -> None:
        self.base_url = settings.BVG_BASE_URL
        self.client = httpx.AsyncClient(
            timeout=settings.BVG_REQUEST_TIMEOUT,
            headers={"User-Agent": "BVG-Departure-Board/1.0"},
        )

    async def close(self):
        await self.client.aclose()

    async def get_departure_info(self, station_id: str, duration_minutes: int = 5):
        try:
            url = f"{self.base_url}/stops/{station_id}/departures"
            params = {"duration": duration_minutes}

            logger.info(f"Fetching departures: {url} with params {params}")

            response = await self.client.get(url, params=params)
            response.raise_for_status()

            response_data = response.json()
            raw_departures = response_data["departures"]

            logger.info(
                f"Found {len(raw_departures)} raw departures for station {station_id}"
            )

            fetch_time = datetime.now()

            process_departures = []
            for raw_dep in raw_departures:
                try:
                    departures = Departure(**raw_dep)
                    departure_info = self._get_departure_parsed(departures, fetch_time)
                    process_departures.append(departure_info)
                except Exception as e:
                    logger.warning(f"Failed to parse departure: {e}")
                    continue

            logger.info(f"Successfully processed {len(process_departures)} departures")
            return process_departures
        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error fetching departures for {station_id}: {e.response.status_code}"
            )
            return []
        except httpx.TimeoutException:
            logger.error(f"Timeout fetching departures for {station_id}")
            return []

    def _get_departure_parsed(
        self, departure_info: Departure, fetch_time: datetime
    ) -> DepartureParsedInfo:
        delay = departure_info.delay_minutes
        transport_mode = self._map_transport_mode(departure_info.line.product)

        return DepartureParsedInfo(
            trip_id=departure_info.tripId,
            station_id=departure_info.stop.id,
            station_name=departure_info.stop.name,
            line_id=departure_info.line.id,
            line_name=departure_info.line.name,
            transport_mode=transport_mode,
            direction=departure_info.direction,
            planned_departure=departure_info.plannedWhen,
            actual_departure=departure_info.when or departure_info.plannedWhen,
            delay_minutes=delay,
            platform=departure_info.platform,
            planned_platform=departure_info.plannedPlatform,
            cancelled=departure_info.cancelled,
            collected_at=fetch_time,
            longitude=departure_info.stop.location.longitude,
            latitude=departure_info.stop.location.latitude,
        )

    def _map_transport_mode(self, product_str: str) -> TransportMode:
        mode_mapping = {
            "suburban": TransportMode.SUBURBAN,
            "subway": TransportMode.SUBWAY,
            "bus": TransportMode.BUS,
            "tram": TransportMode.TRAM,
            "ferry": TransportMode.FERRY,
            "regional": TransportMode.REGIONAL,
            "express": TransportMode.EXPRESS,
        }

        return mode_mapping.get(product_str.lower(), TransportMode.NA)

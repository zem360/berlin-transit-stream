import json
import time
from datetime import datetime
from typing import Dict, List

import redis.asyncio as redis  # type: ignore[import]
from loguru import logger

from berlin_departure_board.config import settings


class RedisClient:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
        )
        logger.info(
            f"✅ Redis client initialized: {settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )

    async def store_departure(self, departure_data: dict) -> bool:
        try:
            station_id = departure_data["station_id"]

            collected_at = departure_data["collected_at"].timestamp()

            if isinstance(collected_at, datetime):
                score = collected_at.timestamp()
            else:
                score = time.time()

            key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{station_id}"

            await self.redis_client.zadd(
                key, {json.dumps(departure_data, default=str): score}
            )

            await self.redis_client.expire(key, settings.REDIS_TTL_SECONDS)

            await self.redis_client.zremrangebyrank(key, 0, -51)
            return True
        except Exception as e:
            logger.error(f"Failed to store departure in Redis: {e}")
            return False

    async def get_latest_departures(
        self, station_id: str, limit: int = 20
    ) -> List[Dict]:
        try:
            departures_key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}{station_id}"

            raw_departures = await self.redis_client.zrange(
                departures_key, 0, limit - 1
            )

            if not raw_departures:
                logger.info(f"No departures found for station {station_id}")
                return []

            departures = []
            for dep_json in raw_departures:
                try:
                    departure = json.loads(dep_json)
                    departures.append(departure)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Failed to parse departure JSON for station {station_id}: {e}"
                    )
                    continue

            logger.debug(
                f"Retrieved {len(departures)} departures for station {station_id}"
            )
            return departures

        except Exception as e:
            logger.error(f"Failed to get departures for station {station_id}: {e}")
            return []

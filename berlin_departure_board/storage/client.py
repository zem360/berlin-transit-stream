import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List

import redis  # type: ignore[import]
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
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

        logger.info(
            f"✅ Redis client initialized: {settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )

    def store_departure(self, departure_data: dict) -> bool:
        try:
            station_id = departure_data["station_id"]

            collected_at = departure_data.get("collected_at_ts")
            if collected_at:
                if isinstance(collected_at, str):
                    collected_at = datetime.fromisoformat(
                        collected_at.replace("Z", "+00:00")
                    )
                score = collected_at.timestamp()
            else:
                score = datetime.now().timestamp()

            planned_departure = departure_data["planned_departure"]
            direction = departure_data["direction"]
            line_id = departure_data["line_id"]
            platform = departure_data.get("platform", "")

            dedup_string = (
                f"{station_id}:{planned_departure}:{direction}:{line_id}:{platform}"
            )
            dedup_hash = hashlib.md5(dedup_string.encode()).hexdigest()[:8]

            redis_data = {
                "trip_id": departure_data["trip_id"],
                "station_id": station_id,
                "station_name": departure_data["station_name"],
                "line_id": line_id,
                "line_name": departure_data["line_name"],
                "transport_mode": departure_data["transport_mode"],
                "direction": direction,
                "planned_departure": planned_departure,
                "actual_departure": departure_data["actual_departure"],
                "delay_minutes": departure_data.get("delay_minutes", 0.0),
                "platform": platform,
                "planned_platform": departure_data.get("planned_platform"),
                "collected_at": departure_data["collected_at"],
                "longitude": departure_data.get("longitude"),
                "latitude": departure_data.get("latitude"),
                "dedup_hash": dedup_hash,  # Store for debugging
            }

            key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{station_id}"

            redis_member = f"{dedup_hash}:{json.dumps(redis_data, default=str)}"
            self.redis_client.zadd(key, {redis_member: score})

            self.redis_client.expire(key, settings.REDIS_TTL_SECONDS)
            self.redis_client.zremrangebyrank(key, 0, -51)

            return True

        except Exception as e:
            logger.error(f"Failed to store departure in Redis: {e}")
            return False

    def store_station_metrics(self, metrics_data: dict) -> bool:
        try:
            station_id = metrics_data["station_id"]
            window_start = metrics_data["window_start"]

            if isinstance(window_start, str):
                window_start_dt = datetime.fromisoformat(
                    window_start.replace("Z", "+00:00")
                )
            else:
                window_start_dt = window_start

            window_key = int(window_start_dt.timestamp())
            key = f"metrics:station:{station_id}:window:{window_key}"

            self.redis_client.setex(key, 7200, json.dumps(metrics_data, default=str))

            index_key = "metrics:stations:index"
            self.redis_client.sadd(index_key, station_id)
            self.redis_client.expire(index_key, 7200)

            return True

        except Exception as e:
            logger.error(f"Failed to store station metrics: {e}")
            return False

    def get_station_metrics(
        self, station_id: str, minutes_back: int = 15
    ) -> List[Dict]:
        try:
            current_time = datetime.now()
            start_time = current_time - timedelta(minutes=minutes_back)

            pattern = f"metrics:station:{station_id}:window:*"
            keys = self.redis_client.keys(pattern)

            metrics = []
            for key in keys:
                try:
                    timestamp_str = key.split(":")[-1]
                    timestamp = int(timestamp_str)
                    metric_time = datetime.fromtimestamp(timestamp)

                    if metric_time >= start_time:
                        data = self.redis_client.get(key)
                        if data:
                            metrics.append(json.loads(data))
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning(f"Failed to parse metric key {key}: {e}")
                    continue

            metrics.sort(key=lambda x: x.get("window_start", ""))
            return metrics

        except Exception as e:
            logger.error(f"Failed to get station metrics for {station_id}: {e}")
            return []

    def get_all_station_metrics(self, minutes_back: int = 15) -> Dict[str, List[Dict]]:
        try:
            index_key = "metrics:stations:index"
            station_ids = self.redis_client.smembers(index_key)

            all_metrics = {}
            for station_id in station_ids:
                metrics = self.get_station_metrics(station_id, minutes_back)
                if metrics:
                    all_metrics[station_id] = metrics

            return all_metrics

        except Exception as e:
            logger.error(f"Failed to get all station metrics: {e}")
            return {}

    def close_connection(self):
        try:
            if self.redis_client:
                self.redis_client.close()
                logger.info("✅ Redis connection closed")
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")

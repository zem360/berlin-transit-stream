import json
from datetime import datetime, timezone
from typing import List

from confluent_kafka import Producer
from loguru import logger

from berlin_departure_board.config import settings
from berlin_departure_board.models import DepartureParsedInfo


class KafkaProducerClient:
    def __init__(self):
        self.config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": settings.KAFKA_CLIENT_ID,
            "acks": "all",
            "retries": 3,
            "batch.size": 1000,
            "linger.ms": 100,
        }
        self.producer = Producer(self.config)
        logger.info(
            f"Kafka producer initialized with servers: {settings.KAFKA_BOOTSTRAP_SERVERS}"
        )

    def _delivery_callback(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
            )

    async def send_departures(self, departures: List[DepartureParsedInfo]) -> bool:
        try:
            success_count = 0
            for departure in departures:
                message = {
                    "event_type": "departure_update",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "trip_id": departure.trip_id,
                        "station_id": departure.station_id,
                        "station_name": departure.station_name,
                        "line_id": departure.line_id,
                        "line_name": departure.line_name,
                        "transport_mode": departure.transport_mode.value,
                        "direction": departure.direction,
                        "planned_departure": departure.planned_departure.isoformat(),
                        "actual_departure": departure.actual_departure.isoformat(),
                        "delay_minutes": departure.delay_minutes,
                        "platform": departure.platform,
                        "planned_platform": departure.planned_platform,
                        "collected_at": departure.collected_at.isoformat(),
                        "longitude": departure.longitude,
                        "latitude": departure.latitude,
                    },
                }

                value = json.dumps(message)
                key = departure.station_id

                self.producer.produce(
                    topic=settings.KAFKA_TOPIC_DEPARTURES,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
                success_count += 1

            self.producer.flush(timeout=10)
            logger.info(
                f"Successfully sent {success_count} departure messages to Kafka"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send departures to Kafka: {e}")
            return False

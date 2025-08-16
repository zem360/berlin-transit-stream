from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BVG_BASE_URL: str = "https://v6.bvg.transport.rest"
    BVG_REQUEST_TIMEOUT: int = 30
    BVG_RATE_LIMIT_DELAY: float = 0.6  # 100 requests/minute = 0.6s between requests

    POLLER_ENABLED_STATIONS: List[str] = [
        "900100003",  # S+U Alexanderplatz
        "900100001",  # S+U Friedrichstr.
        "900003201",  # S+U Berlin Hauptbahnhof
        "900120003",  # S Ostkreuz Bhf
        "900023201",  # S+U Zoologischer Garten
        "900017101",  # S+U Potsdamer Platz
        "900024101",  # S+U Warschauer Str.
        "900013102",  # S Hackescher Markt
        "900110001",  # S+U Gesundbrunnen
        "900230999",  # S+U Schönhauser Allee
    ]

    POLLING_INTERVAL: int = 300

    POLLER_HOST: str = "0.0.0.0"
    POLLER_PORT: int = 8000

    LOG_LEVEL: str = "INFO"
    DEBUG: bool = False

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "bvg-poller"
    KAFKA_TOPIC_DEPARTURES: str = "station-departures"

    # Spark Streaming Settings
    SPARK_APP_NAME: str = "BVG-Departure-Processor"
    SPARK_PROCESSING_INTERVAL: str = "10 seconds"
    SPARK_WATERMARK_DELAY: str = "5 minutes"
    SPARK_WINDOW_DURATION: str = "5 minutes"
    SPARK_SLIDE_DURATION: str = "1 minute"

    # Redis Settings
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None
    REDIS_DEPARTURES_KEY_PREFIX: str = "departure:"
    REDIS_TTL_SECONDS: int = 3600  # 1 hour TTL for departures
    REDIS_TTL_AGG_SECONDS: int = 900  # 15 min TTL for Aggregation

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()

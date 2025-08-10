from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BVG_BASE_URL: str = "https://v6.bvg.transport.rest"
    BVG_REQUEST_TIMEOUT: int = 30
    BVG_RATE_LIMIT_DELAY: float = 0.6  # 100 requests/minute = 0.6s between requests

    POLLER_ENABLED_STATIONS: List[str] = [
        "900100003",  # S+U Alexanderplatz
        # "900100001",  # S+U Friedrichstr.
        # "900003201",  # S+U Berlin Hauptbahnhof
        # "900120003",  # S Ostkreuz Bhf
        # "900023201",  # S+U Zoologischer Garten
    ]

    POLLING_INTERVAL: int = 30

    POLLER_HOST: str = "0.0.0.0"
    POLLER_PORT: int = 8000

    LOG_LEVEL: str = "INFO"
    DEBUG: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()

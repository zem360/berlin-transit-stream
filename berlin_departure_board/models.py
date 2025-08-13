from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, confloat


class TransportMode(str, Enum):
    SUBURBAN = "suburban"  # S-Bahn
    SUBWAY = "subway"  # U-Bahn
    REGIONAL = "regional"  # RB
    EXPRESS = "express"  # ICE
    TRAM = "tram"
    BUS = "bus"
    FERRY = "ferry"
    NA = "not defined"


class Location(BaseModel):
    type: str = "location"
    id: str
    latitude: float
    longitude: float


class Products(BaseModel):
    suburban: bool = False
    subway: bool = False
    tram: bool = False
    bus: bool = False
    ferry: bool = False
    express: bool = False
    regional: bool = False


class Stop(BaseModel):
    type: str = "stop"
    id: str
    name: str
    location: Location
    products: Products


class Line(BaseModel):
    type: str = "line"
    id: str
    name: str
    mode: str
    product: str


class Destination(BaseModel):
    type: str = "stop"
    id: str
    name: str
    location: Location
    products: Products


class Departure(BaseModel):
    tripId: str
    stop: Stop
    when: Optional[datetime] = None
    plannedWhen: datetime
    delay: Optional[int] = None
    platform: Optional[str] = None
    plannedPlatform: Optional[str] = None
    direction: str
    line: Line
    cancelled: bool = False

    @property
    def delay_minutes(self) -> float:
        return (self.delay or 0) / 60.0


class DepartureParsedInfo(BaseModel):
    trip_id: str
    station_id: str
    station_name: str
    line_id: str
    line_name: str
    transport_mode: TransportMode
    direction: str
    planned_departure: datetime
    actual_departure: datetime
    delay_minutes: float = 0.0
    platform: Optional[str] = None
    planned_platform: Optional[str] = None
    cancelled: bool
    collected_at: datetime

    latitude: float | None = Field(None, description="Station latitude", ge=-90, le=90)
    longitude: float | None = Field(
        None, description="Station longitude", ge=-180, le=180
    )

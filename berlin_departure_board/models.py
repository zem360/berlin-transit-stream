from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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
    product: Products


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
    direction: Line

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FlatOffer:
    url: str
    source: str

    price: int | None = None
    price_per_m2: int | None = None
    discount_pct: int | None = None
    deal_conditions: str | None = None

    city: str | None = None
    region: str | None = None
    district: str | None = None
    street: str | None = None
    house_number: str | None = None
    metro_stations: list[tuple[str, int]] | None = None
    transport_score: float | None = None
    lat: float | None = None
    lon: float | None = None

    rooms: int | None = None
    total_area: float | None = None
    living_area: float | None = None
    kitchen_area: float | None = None
    floor: int | None = None
    ceiling_height: float | None = None
    renovation: str | None = None
    bathrooms: str | None = None
    balcony: str | None = None
    window_view: str | None = None
    is_apartments: bool | None = None
    description: str | None = None

    total_floors: int | None = None
    year_built: int | None = None
    building_type: str | None = None
    parking: str | None = None
    elevators: str | None = None
    is_new_building: bool | None = None

    developer: str | None = None
    residential_complex: str | None = None
    completion_date: str | None = None

    publication_date: datetime | None = None
    parsed_at: datetime = field(default_factory=datetime.now)

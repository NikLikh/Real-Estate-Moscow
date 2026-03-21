"""
Схема данных для парсеров и исторических данных kaggle
"""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FlatOffer:  # Инфо о предложении квартиры

    # id
    url: str
    source: str

    # цена
    price: int | None = None
    price_per_m2: int | None = None
    discount_pct: int | None = None

    # Локация
    city: str | None = None
    region: str | None = None
    street: str | None = None
    house_number: str | None = None
    metro_stations: list[tuple[str, int]] | None = None
    lat: float | None = None
    lon: float | None = None

    # Инфо о квартире
    rooms: int | None = None
    total_area: float | None = None
    living_area: float | None = None
    kitchen_area: float | None = None
    floor: int | None = None
    ceiling_height: float | None = None
    renovation: str | None = None
    bathrooms: int | None = None
    balcony: bool | None = None
    window_view: str | None = None

    # Инфо о доме
    total_floors: int | None = None
    year_built: int | None = None
    building_type: str | None = None
    parking: bool | None = None
    elevators: int | None = None
    is_new_building: bool | None = None

    # Застройщик
    developer: str | None = None
    residential_complex: str | None = None
    completion_date: str | None = None

    # Мета
    publication_date: datetime | None = None
    parsed_at: datetime = field(default_factory=datetime.now)

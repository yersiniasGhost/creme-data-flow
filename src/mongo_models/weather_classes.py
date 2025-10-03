from typing import Dict, List
from datetime import datetime
from pydantic import BaseModel, Field as PydanticField
from config.types import PYMONGO_ID, PyObjectId


class WeatherClasses(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="WeatherClasses")
    weather_station_id: PYMONGO_ID
    interval: int   # Temperature interval in minutes
    centroids: Dict[str, List[float]] = PydanticField(default_factory=dict)
    dates: Dict[str, List[datetime]]

from datetime import datetime
from enum import Enum
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class PlantBusModel(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    plant_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_values: Dict[str, float] | None

    loads: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    generation: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    battery_charge: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    battery_discharge: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)

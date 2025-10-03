from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class LoadHistory(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    load_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_demand: float | None

    demand: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    energy: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    daily_energy_use: Dict[int, float] = PydanticField(default_factory=dict)

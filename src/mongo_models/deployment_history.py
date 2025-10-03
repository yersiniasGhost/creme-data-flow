from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class DeploymentHistory(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    deployment_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_power: float | None
    total_generation: float | None
    # battery state values for a day of the month.   The dictionary is seconds:measurements
    power_generation: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    energy_generation: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)


from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class BatterySystemHistory(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    battery_system_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_state_of_charge: float | None
    last_power: float | None
    # battery state values for a day of the month.   The dictionary is seconds:measurements
    soc: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    power: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    energy: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    time_engaged: Dict[int, Dict[int, int]] = PydanticField(default_factory=dict)

    # Eventually track the charge and discharge efficiency and other parameters:
    # eta: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    # temperature: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)

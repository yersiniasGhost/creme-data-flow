from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class PanelProduction(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    panel_string_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_power: float | None
    last_temperature: float | None
    last_eta: float | None
    last_irradiance: float | None
    last_pv_temperature: float | None
    last_conditions: Dict | None
    # last_solar_position: Dict | None

    # pv generation values for a day of the month.   The dictionary is seconds:measurements
    power: Dict[str, Dict[int, float]] = PydanticField(default_factory=dict)
    eta: Dict[str, Dict[int, float]] = PydanticField(default_factory=dict)
    conditions: Dict[str, Dict[int, Dict]] = PydanticField(default_factory=dict)
    irradiance: Dict[str, Dict[int, float]] = PydanticField(default_factory=dict)
    temperature: Dict[str, Dict[int, float]] = PydanticField(default_factory=dict)
    pv_temperature: Dict[str, Dict[int, float]] = PydanticField(default_factory=dict)
    # solar_position: Dict[str, Dict[int, Dict]] = PydanticField(default_factory=dict)
    daily_energy_production: Dict[int, float] = PydanticField(default_factory=dict)

    @classmethod
    def get_document_fields(cls) -> List:
        return [
            "power", "eta", "conditions", "irradiance", "temperature", "daily_energy_production"
        ]

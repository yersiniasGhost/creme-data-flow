from typing import Dict
from pydantic import BaseModel, Field as PydanticField

from config.py_object_id import PyObjectId


class BatterySystem(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    plant_group_id: PyObjectId
    capacity: float             # kWh of capacity
    _type: str = "RISE"
    charge_efficiency: float = 0.95
    discharge_efficiency: float = 0.95
    # The "C rating" of a battery is a measure used to describe its discharge rate capability relative to its capacity.
    c_rate_discharge: float = 2.0
    c_rate_charge: float = 1.0
    depth_of_discharge: float = 0.8  # 80% DoD for Li-Ion

    model_type: str = "RiseBatteryStorage"
    model_parameters: Dict = {"soc_charge_efficiency": 0.05, "temperature_charge_impact": 0.0001,
                              "soc_discharge_efficiency": 0.05, "temperature_discharge_impact": 0.0001}


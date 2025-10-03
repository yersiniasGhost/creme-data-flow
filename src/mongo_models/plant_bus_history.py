from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


@dataclass
class GenerationState:
    power_generated: float = 0
    power_curtailed: float = 0
    energy_generated: float = 0
    energy_curtailed: float = 0


@dataclass
class LoadState:
    power_demand: float = 0
    energy_demand: float = 0
    power_from_bess: float = 0
    energy_from_bess: float = 0
    power_from_gen: float = 0
    energy_from_gen: float = 0
    power_from_utility: float = 0
    energy_from_utility: float = 0


@dataclass
class BessState:
    discharge_power: float = 0
    charge_power: float = 0
    discharge_energy: float = 0
    charge_energy: float = 0
    engaged_time_s: float = 0
    state_of_charge: float = 0


class PlantBusHistory(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    plant_group_id: PyObjectId
    year: int
    month: int
    last_date: datetime | None
    last_bess: Dict | None
    last_load: Dict | None
    last_generation: Dict | None

    total_load_use_pv_generation: float = 0.0
    total_load_use_bess: float = 0.0
    total_load_use_utility: float = 0.0

    # Plant bus state history.
    bess: Dict[int, Dict[int, Dict]] = PydanticField(default_factory=dict)
    loads: Dict[int, Dict[int, Dict]] = PydanticField(default_factory=dict)
    generation: Dict[int, Dict[int, Dict]] = PydanticField(default_factory=dict)
    # power: Dict[int, Dict[int, float]] = PydanticField(default_factory=dict)
    generation_prediction: Dict[int, Dict[str, float]] = PydanticField(default_factory=dict)
    load_prediction: Dict[int, Dict[str, float]] = PydanticField(default_factory=dict)


    @classmethod
    def get_document_fields(cls) -> List:
        return [
            "bess", "loads", "generation"
        ]

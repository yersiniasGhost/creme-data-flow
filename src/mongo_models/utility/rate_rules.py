from typing import Optional, Dict, List
from config.py_object_id import PyObjectId
from pydantic import BaseModel, Field as PydanticField


class Rate(BaseModel):
    tier_ceiling: Optional[str] = None
    rate: float


class RateStructure(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="Rates")

    name: str
    utility_data_source: PyObjectId
    rules: List[Rate]


# Example of rules:
# [
#     { "tier_ceiling": 22.5, "rate": 0.15},
#     {"rate": 0.05}
# ]


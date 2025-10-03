from typing import Optional
from datetime import datetime
from mongo_models.mongo_fixtures import DeploymentType
from pydantic import BaseModel, Field as PydanticField
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId

SIMULATION = "Simulation"


class Plant(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    plant_type: str = PydanticField(default="Plant")
    identifier: Optional[str] = PydanticField(default="No Identifier")
    plant_group_id: PYMONGO_ID

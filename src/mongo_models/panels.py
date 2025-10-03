from typing import Dict
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class PanelString(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="PanelString")
    plant_id: PyObjectId
    name: str
    tilt: float
    orientation: float
    capacity: float
    model_type: str
    model_parameters: Dict

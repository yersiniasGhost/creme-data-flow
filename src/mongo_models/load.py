from config.py_object_id import PyObjectId
from pydantic import BaseModel, Field as PydanticField


class Load(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    plant_group_id: PyObjectId
    identifier: str


from config.py_object_id import PyObjectId
from pydantic import BaseModel, Field as PydanticField


class Utility(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    name: PyObjectId


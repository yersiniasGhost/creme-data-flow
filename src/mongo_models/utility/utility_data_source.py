from typing import Optional
from config.py_object_id import PyObjectId
from pydantic import BaseModel, Field as PydanticField


class UtilityDataSource(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="UtilityDataSource")

    source_type: str
    client_id: Optional[int] = None  # Optional, can be null
    public: bool



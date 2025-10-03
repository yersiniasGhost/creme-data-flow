from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class WeatherStation(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="WeatherStation")
    deployment_id: PyObjectId
    name: str
    temperature: float
    wind_speed: float
    precipitation: float

from datetime import datetime
from pydantic.error_wrappers import ValidationError
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from pymongo.collection import Collection
from pymongo import UpdateOne
from pymongo.results import BulkWriteResult


from config.types import PYMONGO_ID, PyObjectId
from mongo_models import WeatherStation
from mongo_models.mongo_fixtures import DEPLOYMENT_WEATHER
from mongo.mongo import Mongo
from config.log_wrapper import log


class WeatherStationTools:

    def __init__(self, ws: WeatherStation):
        if not isinstance(ws, WeatherStation):
            raise ValueError(f"Expecting panel production object!")
        self.weather_station = ws

    @classmethod
    def collection(cls):
        return Mongo().database[DEPLOYMENT_WEATHER]

    @classmethod
    def get_weather_station(cls, deployment_id: PYMONGO_ID) -> "WeatherStationTools":
        query = {"deployment_id": PyObjectId(deployment_id), "_type": "WeatherStation"}
        ds = cls.collection().find_one(query)
        if not ds:
            ws = WeatherStation(deployment_id=deployment_id, name="no name", temperature=0, precipitation=0, wind_speed=0)
            result = cls.collection().insert_one(ws.dict())
            ws.id = result.inserted_id
        else:
            ws = WeatherStation(**ds)
        return WeatherStationTools(ws)


    @classmethod
    def get_update_one(cls, weather_station_id: PYMONGO_ID,
                       temperature: float, wind_speed: float, precipitation: float) -> UpdateOne:
        document_update = {
            "temperature": temperature,
            "wind_speed": wind_speed,
            "precipitation": precipitation,
            "updated_at": datetime.utcnow()
        }
        update_op = UpdateOne(
            {"_id": weather_station_id, "_type": "WeatherStation"},
            {"$set": document_update},
            upsert=True
        )
        return update_op

    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> BulkWriteResult:
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result

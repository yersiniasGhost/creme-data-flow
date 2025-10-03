import datetime
from pydantic.error_wrappers import ValidationError
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from pymongo.collection import Collection

from config.types import DATETIME, PYMONGO_ID
from config.py_object_id import PyObjectId
from mongo_models.plants import Plant
from mongo_models.mongo_fixtures import PLANT_COLLECTION
from mongo.mongo import Mongo
from config.log_wrapper import log


class PlantTools:

    def __init__(self, plant: Plant):
        if not isinstance(plant, Plant):
            raise ValueError(f"Expecting panel production object!")
        self.plant = plant

    @classmethod
    def collection(cls):
        return Mongo().database[PLANT_COLLECTION]

    @classmethod
    def create_plant(cls, deployment_id: PYMONGO_ID) -> "PlantTools":
        p = Plant(deployment_id=PyObjectId(deployment_id))
        res = cls.collection().insert_one(p.dict())
        p.id = res.inserted_id
        return PlantTools(p)

    @classmethod
    def get_plants_by_groups(cls, group_ids: List[PyObjectId]) -> List[Plant]:
        query = {"plant_group_id": {"$in": group_ids}}
        plants = [Plant(**p) for p in cls.collection().find(query)]
        return plants

    @classmethod
    def get_plant_ids_by_groups(cls, group_ids: List[PyObjectId]) -> List[PYMONGO_ID]:
        query = {"plant_group_id": {"$in": group_ids}}
        ids = [idx['_id'] for idx in cls.collection().find(query, {"_id"})]
        return ids

    @classmethod
    def get_plant_by_raptor_id(cls, raptor_id: str) -> Plant:
        query = {"raptor_id": {"$eq": raptor_id}}
        p = cls.collection().find(query)
        return Plant(**p)

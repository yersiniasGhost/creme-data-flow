from typing import List
from config.types import PYMONGO_ID
from pymongo.collection import Collection
from mongo_models import Load
from mongo_models.mongo_fixtures import LOAD_MODEL_COLLECTION
from mongo.mongo import Mongo

from config.log_wrapper import log


class LoadTools:

    def __init__(self, load: Load):
        if not isinstance(load, Load):
            raise ValueError(f"Expecting Load object!")
        self.load = load

    @classmethod
    def get_load_ids_by_group_id(cls, group_id: PYMONGO_ID):
        query = {"plant_group_id": {"$in": [group_id]}}
        ids = [idx['_id'] for idx in cls.collection().find(query, {"_id"})]
        return ids

    @classmethod
    def get_loads_by_group_id(cls, group_id: PYMONGO_ID) -> List[Load]:
        query = {"plant_group_id": {"$in": [group_id]}}
        loads = [Load(**p) for p in cls.collection().find(query)]
        return loads


    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[LOAD_MODEL_COLLECTION]

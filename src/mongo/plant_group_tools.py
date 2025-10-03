from typing import List
from config.py_object_id import PyObjectId
from mongo_models.plant_group import PlantGroup
from mongo_models.mongo_fixtures import PLANT_GROUP_COLLECTION
from mongo.mongo import Mongo
from config.log_wrapper import log


class PlantGroupTools:

    def __init__(self, group: PlantGroup):
        if not isinstance(group, PlantGroup):
            raise ValueError(f"Expecting plant group object!")
        self.group = group

    @classmethod
    def collection(cls):
        return Mongo().database[PLANT_GROUP_COLLECTION]


    @classmethod
    def get_plant_groups_by_deployments(cls, deployment_ids: List[PyObjectId]) -> List[PlantGroup]:
        query = {"deployment_id": {"$in": deployment_ids}}
        groups = [PlantGroup(**p) for p in cls.collection().find(query)]
        return groups

    @classmethod
    def get_plant_group_ids_by_deployments(cls, deployment_ids: List[PyObjectId]) -> List[PyObjectId]:
        query = {"deployment_id": {"$in": deployment_ids}}
        ids = [idx['_id'] for idx in cls.collection().find(query, {"_id"})]
        return ids


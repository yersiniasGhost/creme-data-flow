from typing import List, Optional, Tuple, Dict, Union
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from config.log_wrapper import log
from mongo.mongo import Mongo
from mongo_models.panels import PanelString
from mongo_models.mongo_fixtures import PANEL_COLLECTION


class PanelStringTools:

    def __init__(self, panel: PanelString):
        if not isinstance(panel, PanelString):
            raise ValueError(f"Expecting panel production object!")
        self.panel_string = panel


    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[PANEL_COLLECTION]

    @classmethod
    def get_all_panels_strings(cls, plant_ids: List[PyObjectId]) -> List[PanelString]:
        query = {"plant_id": {"$in": plant_ids}}
        ps = [PanelString(**ps) for ps in cls.collection().find(query)]
        return ps


    @classmethod
    def get_all_panels_string_ids(cls, plant_ids: List[PyObjectId]) -> List[PyObjectId]:
        query = {"plant_id": {"$in": plant_ids}}
        projection = {"_id": 1}
        cursor = cls.collection().find(query, projection)
        panel_string_ids = [doc['_id'] for doc in cursor]
        return panel_string_ids

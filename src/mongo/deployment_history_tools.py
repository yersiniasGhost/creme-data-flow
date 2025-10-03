import datetime

import pandas as pd
from typing import List, Union, Optional, Dict
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from pymongo.results import BulkWriteResult
from pymongo import UpdateOne
from mongo_models.deployment_history import DeploymentHistory
from mongo_models.mongo_fixtures import TIME_SERIES_DEPLOYMENT_HISTORY
from mongo_models.plant_bus_history import GenerationState
from config.log_wrapper import log
from mongo.mongo import Mongo


class DeploymentHistoryTools:

    def __init__(self, bess_history: DeploymentHistory):
        if not isinstance(bess_history, DeploymentHistory):
            raise ValueError(f"Expecting deployment history object!")
        self.history = bess_history

    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[TIME_SERIES_DEPLOYMENT_HISTORY]

    @classmethod
    def get_deployment_history(cls, deployment_id: PYMONGO_ID,
                         date: Union[datetime.datetime, pd.Timestamp]) -> "DeploymentHistoryTools":
        collection = cls.collection()
        retrieved_data = collection.find_one({"deployment_id": deployment_id,
                                              "year": date.year,
                                              "month": date.month})
        if not retrieved_data:
            pb_history = DeploymentHistory(deployment_id=PyObjectId(deployment_id), year=date.year, month=date.month)
            result = collection.insert_one(pb_history.dict())
            pb_history.id = result.inserted_id
            return DeploymentHistoryTools(pb_history)
        pb_history = DeploymentHistory(**retrieved_data)
        return DeploymentHistoryTools(pb_history)


    @classmethod
    def get_update_one(cls, genstates: List[GenerationState],
                       date: Union[datetime.datetime, pd.Timestamp],
                       deployment_id: PYMONGO_ID,
                       include_last_values: Optional[bool] = True) -> UpdateOne:
        day = date.day
        total_energy, total_power = 0, 0
        for gen in genstates:
            total_energy += gen.energy_generated
            total_power += gen.power_generated

        seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
        document_update = {f"energy_generation.{day}.{seconds_from_midnight}": total_energy,
                           f"power_generation.{day}.{seconds_from_midnight}": total_power}

        if include_last_values:
            document_update['last_date'] = date
            document_update['last_power'] = total_power

        update_op = UpdateOne(
            {"deployment_id": deployment_id, "year": date.year, "month": date.month},
            {"$set": document_update, "$inc": {"total_generation": total_energy}},
            upsert=True
        )
        return update_op


    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> BulkWriteResult:
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result


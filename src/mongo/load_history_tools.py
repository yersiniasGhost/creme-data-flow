import datetime

import pandas as pd
from typing import List, Union, Optional
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from pymongo.results import BulkWriteResult
from pymongo import UpdateOne
from mongo_models.load_history import LoadHistory
from mongo_models.mongo_fixtures import TIME_SERIES_LOAD_HISTORY
from config.log_wrapper import log
from mongo.mongo import Mongo


class LoadHistoryTools:

    def __init__(self, load_history: LoadHistory):
        if not isinstance(load_history, LoadHistory):
            raise ValueError(f"Expecting load history object!")
        self.load_history = load_history

    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[TIME_SERIES_LOAD_HISTORY]

    @classmethod
    def get_load_history(cls, load_id: PYMONGO_ID,
                         date: Union[datetime.datetime, pd.Timestamp]) -> "LoadHistoryTools":
        collection = cls.collection()
        retrieved_data = collection.find_one({"load_id": load_id,
                                              "year": date.year,
                                              "month": date.month})
        if not retrieved_data:
            pb_history = LoadHistory(load_id=PyObjectId(load_id), year=date.year, month=date.month)
            result = collection.insert_one(pb_history.dict())
            pb_history.id = result.inserted_id
            return LoadHistoryTools(pb_history)
        pb_history = LoadHistory(**retrieved_data)
        return LoadHistoryTools(pb_history)



    @classmethod
    def get_update_one(cls, demand: float,
                       date: Union[datetime.datetime, pd.Timestamp],
                       load_id: PYMONGO_ID,
                       include_last_values: Optional[bool] = False) -> UpdateOne:
        day = date.day
        seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
        document_update = {f"demand.{day}.{seconds_from_midnight}": demand,}

        if include_last_values:
            document_update['last_date'] = date
            document_update['last_demand'] = demand

        update_op = UpdateOne(
            {"load_id": load_id, "year": date.year, "month": date.month},
            {"$set": document_update},
            upsert=True
        )
        return update_op


    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> BulkWriteResult:
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result


import datetime

import pandas as pd
from typing import List, Union, Optional, Dict
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from pymongo.results import BulkWriteResult
from pymongo import UpdateOne
from mongo_models.battery_system_history import BatterySystemHistory
from mongo_models.mongo_fixtures import TIME_SERIES_BATTERY_STORAGE_HISTORY
from config.log_wrapper import log
from mongo.mongo import Mongo


class BatterySystemHistoryTools:

    def __init__(self, bess_history: BatterySystemHistory):
        if not isinstance(bess_history, BatterySystemHistory):
            raise ValueError(f"Expecting bess history object!")
        self.bess_history = bess_history

    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[TIME_SERIES_BATTERY_STORAGE_HISTORY]

    @classmethod
    def get_bess_history(cls, battery_system_id: PYMONGO_ID,
                         date: Union[datetime.datetime, pd.Timestamp]) -> "BatterySystemHistoryTools":
        collection = cls.collection()
        retrieved_data = collection.find_one({"battery_system_id": battery_system_id,
                                              "year": date.year,
                                              "month": date.month})
        if not retrieved_data:
            pb_history = BatterySystemHistory(battery_system_id=PyObjectId(battery_system_id), 
                                               year=date.year, month=date.month)
            result = collection.insert_one(pb_history.dict())
            pb_history.id = result.inserted_id
            return BatterySystemHistoryTools(pb_history)
        pb_history = BatterySystemHistory(**retrieved_data)
        return BatterySystemHistoryTools(pb_history)


    @classmethod
    def get_last_state_of_charge(cls, battery_system_ids: List[PYMONGO_ID],
                                 date: Optional[pd.Timestamp] = None) -> Dict[str, float]:

        filter = {"battery_system_id": {"$in": battery_system_ids}}
        if date:
            day = date.day - 1
            if day == 0:
                error
            filter["year"] = date.year
            filter["month"] = date.month

            latest_documents = cls.collection().find(filter=filter,
                                                     projection={"soc": 1, "battery_system_id": 1, "_id": 0})
            soc_dict = {}
            for doc in latest_documents:
                soc_ts = doc.get('soc')
                last_soc: Optional[float] = None
                day_key = f"{day}"
                day_1_key = f"{day + 1}"
                if day_key in soc_ts.keys():
                    last_soc = soc_ts[day_key][max(soc_ts[day_key].keys())]
                elif day_1_key in soc_ts.keys():
                    last_soc = soc_ts[day_1_key][min(soc_ts[day_1_key].keys())]
                if last_soc is not None:
                    soc_dict[str(doc.get('battery_system_id'))] = last_soc

        else:

            pipeline = [
                {"$match": {"battery_system_id": {"$in": battery_system_ids}}},
                {"$sort": {"last_date": -1}},
                {"$group": {
                    "_id": "$battery_system_id",
                    "latest_document": {"$first": "$$ROOT"}
                }},
                {"$project": {
                    "_id": 0,
                    "battery_system_id": "$_id",
                    "last_date": "$latest_document.last_date",
                    "last_state_of_charge": "$latest_document.last_state_of_charge"
                    # Add more fields as needed
                }}
            ]

            latest_documents = cls.collection().aggregate(pipeline)

            soc_dict = {}
            for doc in latest_documents:
                soc_dict[str(doc.get('battery_system_id'))] = doc.get('last_state_of_charge')
        return soc_dict



    @classmethod
    def get_update_one(cls, soc: float, power: float, energy: float, time_engaged: int,
                       date: Union[datetime.datetime, pd.Timestamp],
                       battery_system_id: PYMONGO_ID,
                       include_last_values: Optional[bool] = False) -> UpdateOne:
        day = date.day
        seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
        document_update = {f"soc.{day}.{seconds_from_midnight}": soc,
                           f"power.{day}.{seconds_from_midnight}": power,
                           f"energy.{day}.{seconds_from_midnight}": energy,
                           f"time_engaged.{day}.{seconds_from_midnight}": time_engaged}

        if include_last_values:
            document_update['last_date'] = date
            document_update['last_power'] = power
            document_update['last_state_of_charge'] = soc

        update_op = UpdateOne(
            {"battery_system_id": battery_system_id, "year": date.year, "month": date.month},
            {"$set": document_update},
            upsert=True
        )
        return update_op


    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> BulkWriteResult:
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result


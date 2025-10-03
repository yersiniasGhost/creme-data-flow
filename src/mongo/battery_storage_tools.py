from datetime import timedelta
import datetime
import pandas as pd
from pydantic.error_wrappers import ValidationError
from dataclasses import dataclass
from pymongo.collection import Collection
from pymongo import UpdateOne
from pymongo.results import BulkWriteResult
from typing import List, Optional, Tuple, Dict, Union

from config.types import DATETIME, PYMONGO_ID
from config.py_object_id import PyObjectId
from mongo_models.battery_system import BatterySystem
from mongo_models.mongo_fixtures import BATTERY_STORAGE_COLLECTION
from config.log_wrapper import log
from mongo.mongo import Mongo
from solar_model_adr.simulator import InstantData


class BatterySystemTools:

    def __init__(self, batteries: BatterySystem):
        if not isinstance(batteries, BatterySystem):
            raise ValueError(f"Expecting Battery system object!")
        self.batteries = batteries


    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[BATTERY_STORAGE_COLLECTION]

    @classmethod
    def get_battery_systems(cls, plant_group_ids: List[PyObjectId]) -> Dict[PYMONGO_ID, BatterySystem]:
        query = {"plant_group_id": {"$in": plant_group_ids}}
        ps = {str(bs["plant_group_id"]): BatterySystem(**bs) for bs in cls.collection().find(query)}
        return ps

    @classmethod
    def create_battery_system(cls, plant_group_id: PYMONGO_ID,
                              capacity: float, c_rate: float,
                              charge_efficiency: float = 1,
                              discharge_efficiency: float = 1) -> "BatterySystemTools":
        p = BatterySystem(plant_group_id=PyObjectId(plant_group_id), capacity=capacity, c_rate=c_rate,
                          charge_efficiency=charge_efficiency, discharge_efficiency=discharge_efficiency)
        res = cls.collection().insert_one(p.dict())
        p.id = res.inserted_id
        return BatterySystemTools(p)

    @classmethod
    def get_battery_system_by_plant(cls, plant_id: PYMONGO_ID) -> "BatterySystemTools":
        bs = cls.collection().find_one({"plant_id": PyObjectId(plant_id)})
        if bs:
            sys = BatterySystem(**bs)
            return BatterySystemTools(sys)
        else:
            fuck


    @classmethod
    def get_battery_system(cls, system_id: PYMONGO_ID) -> "BatterySystemTools":
        bs = cls.collection().find_one({"_id": PyObjectId(system_id)})
        if bs:
            sys = BatterySystem(**bs)
            return BatterySystemTools(sys)
        else:
            fuck

    @classmethod
    def get_battery_system_by_plant(cls, plant_id: PYMONGO_ID) -> "BatterySystemTools":
        bs = cls.collection().find_one({"plant_id": PyObjectId(plant_id)})
        sys = BatterySystem(**bs)
        return BatterySystemTools(sys)

    def update_system(self):
        query = {"_id": self.batteries.id}  # Replace with the actual identifier for your document
        update_data = {"$set": self.batteries.dict()}
        # Update the document
        self.collection().update_one(query, update_data)


#
# class BatteryStorageTools:
#
#     def __init__(self, battery_storage: BatteryStorage):
#         if not isinstance(battery_storage, BatteryStorage):
#             raise ValueError(f"Expecting Battery Storage object!")
#         self.storage = battery_storage
#
#
#     @classmethod
#     def collection(cls) -> Collection:
#         return Mongo().database[BATTERY_STORAGE_COLLECTION]
#
#     @classmethod
#     def get_battery_storage(cls, battery_system_id: PYMONGO_ID,
#                             date: Union[datetime.datetime, pd.Timestamp]) -> "BatteryStorageTools":
#         collection = cls.collection()
#         retrieved_data = collection.find_one({"battery_system_id": battery_system_id,
#                                               "year": date.year,
#                                               "month": date.month})
#         if not retrieved_data:
#             battery = BatteryStorage(battery_system_id=PyObjectId(battery_system_id),
#                                         year=date.year, month=date.month, power={})
#             result = collection.insert_one(battery.dict())
#             battery.id = result.inserted_id
#             return BatteryStorageTools(battery)
#         battery = BatteryStorage(**retrieved_data)
#         return BatteryStorageTools(battery)
#
#
#     @classmethod
#     def get_all_battery_storage_ids_for_battery_system(cls,sys_ids: List[PYMONGO_ID]) -> Dict[PYMONGO_ID, PYMONGO_ID]:
#         query = {'battery_system_id': {'$in': sys_ids}}
#         documents = cls.collection().find(query, {'_id': 1, 'battery_system_id': 1})
#         output_dict = {doc['battery_system_id']: doc['_id'] for doc in documents}
#         return output_dict
#
#
#     def get_update(self, state_of_charge: float, rate_of_charge: float, rate_of_discharge: float,
#                    date: Union[datetime.datetime, pd.Timestamp],
#                    include_last_values: bool = False) -> UpdateOne:
#         day = date.day
#         seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
#         document_update = {}
#         document_update[f"state_of_charge.{day}.{seconds_from_midnight}"] = state_of_charge
#         document_update[f"rate_of_charge.{day}.{seconds_from_midnight}"] = rate_of_charge
#         document_update[f"rate_of_discharge.{day}.{seconds_from_midnight}"] = rate_of_discharge
#         if include_last_values:
#             document_update['last_state_of_charge'] = state_of_charge
#             document_update['last_date'] = date
#         update_op = UpdateOne(
#             {"_id": self.storage.id},
#             {"$set": document_update},
#             upsert=True
#         )
#         return update_op
#
#     @classmethod
#     def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> Optional[BulkWriteResult]:
#         if not update_ops:
#             return None
#         print(update_ops[0])
#         collection = cls.collection()
#         result = collection.bulk_write(update_ops)
#         return result
#

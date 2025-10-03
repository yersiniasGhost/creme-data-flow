from dataclasses import asdict
import datetime

import pandas as pd
from typing import List, Union, Dict
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from pymongo.results import BulkWriteResult
from pymongo import UpdateOne
from mongo_models.plant_bus_history import PlantBusHistory, BessState, LoadState, GenerationState
from mongo_models.mongo_fixtures import TIME_SERIES_PLANT_BUS_HISTORY
from mongo.mongo import Mongo
from config.log_wrapper import log


class PlantBusHistoryTools:

    def __init__(self, bus_history: PlantBusHistory):
        if not isinstance(bus_history, PlantBusHistory):
            raise ValueError(f"Expecting panel production object!")
        self.bus_history = bus_history

    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[TIME_SERIES_PLANT_BUS_HISTORY]

    @classmethod
    def get_plant_bus_history(cls, plant_bus_id: PYMONGO_ID,
                              date: Union[datetime.datetime, pd.Timestamp]) -> "PlantBusHistoryTools":
        collection = cls.collection()
        retrieved_data = collection.find_one({"plant_group_id": plant_bus_id,
                                              "year": date.year,
                                              "month": date.month})
        if not retrieved_data:
            pb_history = PlantBusHistory(plant_group_id=PyObjectId(plant_bus_id), year=date.year, month=date.month)
            result = collection.insert_one(pb_history.dict())
            pb_history.id = result.inserted_id
            return PlantBusHistoryTools(pb_history)
        pb_history = PlantBusHistory(**retrieved_data)
        return PlantBusHistoryTools(pb_history)

    @classmethod
    def get_prediction_updates(cls, plant_group_id: PYMONGO_ID, predictions: Dict[pd.Timestamp, dict], data_name: str):
        updates = []

        for date, prediction in predictions.items():
            if len(prediction):
                day = date.day
                document_update = {"$set": {f"{data_name}.{day}": dict(prediction)}}
                history_filter = {"plant_group_id": plant_group_id, "year": date.year, "month": date.month}
                # unset_operation = UpdateOne(history_filter, {"$unset": {"generation_prediction": ""}})
                # updates.append(unset_operation)
                updates.append(UpdateOne(history_filter, document_update))
        return updates

    @classmethod
    def get_update_one(cls,  bess_state: BessState, load_state: LoadState, gen_state: GenerationState,
                       date: Union[datetime.datetime, pd.Timestamp],
                       plant_bus_id: PYMONGO_ID,
                       include_last_values: False) -> UpdateOne:
        day = date.day
        seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
        document_update = {f"bess.{day}.{seconds_from_midnight}": asdict(bess_state),
                           f"loads.{day}.{seconds_from_midnight}": asdict(load_state),
                           f"generation.{day}.{seconds_from_midnight}": asdict(gen_state)}

        increments = {}
        if include_last_values:
            document_update['last_date'] = date
            document_update['last_bess'] = asdict(bess_state)
            document_update['last_load'] = asdict(load_state)
            document_update['last_generation'] = asdict(gen_state)

            increments["total_load_use_pv_generation"] = load_state.energy_from_gen
            increments["total_load_use_utility"] = load_state.energy_from_utility
            increments["total_load_use_bess"] = load_state.energy_from_bess

        updates = {"$set": document_update}
        if increments:
            updates["$inc"] = increments

        update_op = UpdateOne(
            {"plant_group_id": plant_bus_id, "year": date.year, "month": date.month},
            updates,
            upsert=True
        )

        return update_op


    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]) -> BulkWriteResult:
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result

from datetime import timedelta
import datetime

import pandas as pd
from typing import List, Optional, Tuple, Dict, Union
from config.types import DATETIME, PYMONGO_ID
from config.py_object_id import PyObjectId
from pymongo.collection import Collection
from pymongo import UpdateOne
from mongo_models.panel_production import PanelProduction
from mongo_models.mongo_fixtures import TIME_SERIES_PANEL_PRODUCTION
from config.log_wrapper import log
from mongo.mongo import Mongo
from solar_model_adr.simulator import InstantData


class PvProductionTools:

    def __init__(self, panel: PanelProduction):
        # if not isinstance(panel, PanelProduction):
        #     raise ValueError(f"Expecting panel production object!")
        self.panel_production = panel

    @classmethod
    def collection(cls) -> Collection:
        return Mongo().database[TIME_SERIES_PANEL_PRODUCTION]

    @classmethod
    def get_panel_production(cls, panel_id: PYMONGO_ID, date: Union[datetime.datetime, pd.Timestamp]) -> "PvProductionTools":
        collection = cls.collection()
        retrieved_data = collection.find_one({"panel_string_id": PyObjectId(panel_id),
                                              "year": date.year,
                                              "month": date.month})
        if not retrieved_data:
            panel_prod = PanelProduction(panel_string_id=PyObjectId(panel_id), year=date.year, month=date.month,
                                         power={})
            result = collection.insert_one(panel_prod.dict())
            panel_prod.id = result.inserted_id
            return PvProductionTools(panel_prod)
        panel_prod = PanelProduction(**retrieved_data)
        return PvProductionTools(panel_prod)

    @classmethod
    def get_panel_production_by_panel_string_id(cls, pv_prod_id: PYMONGO_ID, date: pd.Timestamp) -> PanelProduction:
        data = cls.collection().find_one({"panel_string_id": pv_prod_id, "year": date.year, "month": date.month})
        return PanelProduction(**data)

    @classmethod
    def get_all_panel_production_ids_for_panels(cls, panel_ids: List[PYMONGO_ID], date: DATETIME) -> Dict[PYMONGO_ID, PYMONGO_ID]:
        # THIS IS BUGGY!  There are multiple PV Production docs (year, month) for each Panel string
        year = date.year
        month = date.month
        query = {'panel_string_id': {'$in': panel_ids}, 'year': year, "month": month}
        documents = cls.collection().find(query, {'_id': 1, 'panel_string_id': 1})
        # Creating the dictionary
        # for doc in documents:
        #     print(doc['panel_string_id'], 'expecting duplicates')
        panel_production_dict = {doc['panel_string_id']: doc['_id'] for doc in documents}
        return panel_production_dict

    @classmethod
    def get_update_one(cls,  instant_data: InstantData,
                       date: Union[datetime.datetime, pd.Timestamp],
                       panel_string_id: PYMONGO_ID,
                       include_last_values: False) -> UpdateOne:
        day = date.day
        seconds_from_midnight = date.hour * 3600 + date.minute * 60 + date.second
        document_update = {}

        for key, value in instant_data._asdict().items():
            document_update[f"{key}.{day}.{seconds_from_midnight}"] = value
            if include_last_values:
                document_update[f'last_{key}'] = value

        if include_last_values:
            document_update['last_date'] = date
        update_op = UpdateOne(
            {"panel_string_id": panel_string_id, "year": date.year, "month": date.month},
            {"$set": document_update},
            upsert=True
        )

        return update_op


    @classmethod
    def insert_bulk_updates(cls, update_ops: List[UpdateOne]):
        collection = cls.collection()
        result = collection.bulk_write(update_ops)
        return result


    # Useful for fast simulation updates
    def bulk_insert(self, bulk_data: Dict[datetime.datetime, InstantData]):
        bulk_operations = []
        for date, instant_data in bulk_data.items():
            bulk_operations.append(self.get_update_one(instant_data, date, self.panel_production.id))

        # Execute all the prepared operations in bulk
        if bulk_operations:
            collection = self.collection()
            result = collection.bulk_write(bulk_operations)
            return result


    @classmethod
    def bulk_calculate_daily_energy(cls, panel_string_ids: List[PYMONGO_ID], now: pd.Timestamp):
        for ps in panel_string_ids:
            pv_tool = PvProductionTools.get_panel_production(ps, now)
            pv_tool.calculate_and_store_daily_energy(now.day)


    def calculate_daily_energy(self, day: int) -> float:
        s_day = str(day)
        daily_power = self.panel_production.power[s_day]
        energy = 0
        prev_time = -1
        prev_power = -1
        for s_seconds, value in daily_power.items():
            seconds = int(s_seconds)
            if prev_time >= 0:
                energy += ((value + prev_power) / 2) * (seconds - prev_time) / 3600.0
            prev_time = seconds
            prev_power = value
        return energy

    def calculate_and_store_daily_energy(self, day):
        energy = self.calculate_daily_energy(day)
        res = self.collection().update_one(
            {"_id": self.panel_production.id},
            {"$set": {f"daily_energy_production.{day}": energy}},
            upsert=True
        )

    # Go through all existing power data and calculate / update the daily energies
    def backfill_daily_energy(self):
        energy_by_day = {}
        for day, daily_power in self.panel_production.power.items():
            energy = 0
            prev_time = -1
            prev_power = -1
            for seconds, value in daily_power.items():
                if prev_time >= 0:
                    energy += (value + prev_power) / 2 * (seconds - prev_time) / 3600.0
                prev_time = seconds
                prev_power = value
            energy_by_day[str(day)] = energy

        res = self.collection().update_one(
            {"_id": self.panel_production.id},
            {"$set": {f"daily_energy_production": energy_by_day}},
            upsert=True
        )

        last_date = self.get_last_entry_date()
        document_update = {
            "last_power": self.get_last_entry(self.panel_production.power),
            "last_temperature": self.get_last_entry(self.panel_production.temperature),
            "last_eta": self.get_last_entry(self.panel_production.eta),
            "last_irradiance": self.get_last_entry(self.panel_production.irradiance),
            "last_pv_temperature": self.get_last_entry(self.panel_production.pv_temperature),
            "last_conditions": self.get_last_entry(self.panel_production.conditions),
            # "last_solar_position": self.get_last_entry(self.panel_production.solar_position),
            "last_date": last_date
        }
        res = self.collection().update_one(
            {"_id": self.panel_production.id},
            {"$set": document_update},
            upsert=True
        )


    def clear_daily_entry_before_backfill(self, day: int):
        collection = self.collection()
        fields_to_clear = {f"{doc_field}.{day}": "" for doc_field in PanelProduction.get_document_fields()}
        # fields_to_clear["#3"] = ""
        update_result = collection.update_one(
            {"_id": self.panel_production.id},
            {"$unset": fields_to_clear}
        )
        res = collection.update_one({"_id": self.panel_production.id},
                                    {"$unset": {"last_date": "", "last_value": ""}},
                                    upsert=True)

    def get_last_entry_date(self) -> datetime.datetime:
        data = self.panel_production.power
        if not data:
            return datetime.datetime(self.panel_production.year, self.panel_production.month, 1)
        max_day = max(data.keys(), key=int)
        max_entry = int(max(data[max_day].keys(), key=int))
        last_date = datetime.datetime(self.panel_production.year, self.panel_production.month, int(max_day))
        last_date += timedelta(seconds=int(max_entry))
        return last_date


    def get_last_entry(self, data) -> Tuple[float, float]:
        if not data:
            print("HERE")
            print(self.panel_production.id)
            print(self.panel_production.power)
            print(self.panel_production)
        max_day = max(data.keys(), key=int)
        max_entry = max(data[max_day].keys(), key=int)
        return data[max_day][max_entry]


    def get_last_power_entry(self, day: int) -> Tuple[float, float]:
        if day in self.panel_production.power.keys():
            day_data = self.panel_production.power[day]
            # Check if there are entries in the dictionary
            if day_data:
                # Get the keys (seconds_from_midnight) and values (production) as tuples
                entries = list(day_data.items())
                last_entry = sorted(entries, key=lambda item: item[0])[-1]
                seconds_from_midnight, production = last_entry
                return seconds_from_midnight, production

        # Return None if there are no entries for the specified day or if the day doesn't exist
        return 0, 0

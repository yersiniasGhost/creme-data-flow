import numpy as np
from datetime import datetime
from typing import Dict, Optional
import holidays

from config.types import PYMONGO_ID, PyObjectId
from mongo_models import LoadClasses
from mongo.weather_class_tools import WeatherClassTools, WeatherClasses
from mongo_models.mongo_fixtures import LOAD_CLUSTER_COLLECTION
from mongo.mongo import Mongo
from algorithms import k_means, closest_centroids, calculate_mcmc_prediction
from config.log_wrapper import log


class LoadClassTools:

    HOLIDAY = 'holiday'
    WEEKEND = 'weekend'
    WEEKDAY = 'weekday'

    def __init__(self, ws: LoadClasses):
        if not isinstance(ws, LoadClasses):
            raise ValueError(f"Expecting LoadClasses object!")
        self.load_classes = ws
        # self.centroids = np.array(list(self.load_classes.centroids.values()))
        # self.centroid_length = len(self.centroids[0])

    def calculate_centroid_index(self, signal: np.array, centroids: np.array) -> str:

        if self.load_classes.length != len(signal):
            raise ValueError(f"Invalid signal length in calculating load class: {len(signal)} != {self.load_classes.length}")
        centroids = np.array(list(centroids))
        closest = closest_centroids(np.array([signal]), centroids)
        index = list(centroids.keys())[closest[0]]
        return index

    def predict_load(self, weather_forecast: np.array, weather_classes: WeatherClassTools):
        weather_class_index = weather_classes.calculate_weather_class_index(weather_forecast)
        load_centroids = self.load_classes.centroids[weather_class_index]
        centroid_index = self.calculate_centroid_index()
        load_std = self.load_classes.std_devs[weather_class_index]

        profiles = calculate_mcmc_prediction(mean, std, n_samples=n_samples)


    @classmethod
    def collection(cls):
        return Mongo().database[LOAD_CLUSTER_COLLECTION]

    @classmethod
    def get_tools(cls, load_classes_id: Optional[PYMONGO_ID] = None,
                  load_id: Optional[PYMONGO_ID] = None) -> "LoadClassTools":
        if load_id:
            query = {"load_id": PyObjectId(load_id), "_type": "LoadClasses"}
        elif load_classes_id:
            query = {"_id": PyObjectId(load_classes_id), "_type": "LoadClasses"}
        else:
            raise ValueError('Must provide proper ids')

        ds = cls.collection().find_one(query)
        if ds:
            lc = LoadClasses(**ds)
            return LoadClassTools(lc)
        else:
            raise ValueError(f"Cannot locate Load Class in database by ID: ({load_id}, {load_classes_id})")


    # For each weather class, create mean & std profiles for each type of weekday and holiday
    @classmethod
    def create_load_classes_by_day(cls, load_id: PYMONGO_ID, weather_classes: WeatherClasses,
                                   load_data: Dict[datetime, np.array], interval: int, number_of_clusters: int = 5,
                                   number_of_iterations: int = 500) -> LoadClasses:
        # Ensure the length of each signal is consistent
        length = 24*60 / interval
        centroids_by_weather_class = {}
        std_devs_by_weather_class = {}
        for weather_class_id, dates in weather_classes.dates.items():
            signals = []
            for date in dates:
                load_signal = load_data[date]
                if len(load_signal) == length:
                    signals.append(load_signal)

            signals = np.array(signals)
            centroids = k_means(signals, number_of_clusters, number_of_iterations)
            results = {str(idx): centroid.tolist() for idx, centroid in enumerate(centroids)}
            centroids_by_weather_class[weather_class_id] = results

            # Now calculate the std dev of the contributing signals to each centroid
            # Save them off as a Dict[ weather_class_idx , Dict [ load_class_idx, List]]
            centroid_assignment = closest_centroids(signals, centroids)
            centroid_stddevs = {}
            for centroid_idx in range(np.max(centroid_assignment)):
                signal_indices = np.where(centroid_assignment == centroid_idx)[0]
                time_series_data = signals[signal_indices]
                stacked_data = np.vstack(time_series_data)
                std_devs = np.std(stacked_data, axis=0)
                centroid_stddevs[centroid_idx] = list(std_devs)
            std_devs_by_weather_class[weather_class_id] = centroid_stddevs

        ws = LoadClasses(load_id=load_id, centroids=centroids_by_weather_class, length=length,
                         std_devs=std_devs_by_weather_class, weather_classes_id=weather_classes.id)
        return ws


    @classmethod
    def create_load_classes(cls, load_id: PYMONGO_ID, weather_classes: WeatherClasses, load_data: Dict[datetime, np.array],
                            interval: int, number_of_clusters: int = 5,
                            number_of_iterations: int = 500) -> LoadClasses:
        us_holidays = holidays.country_holidays("US")
        # Ensure the length of each signal is consistent
        length = 24*60 / interval
        centroids_by_weather_class = {}
        std_devs_by_weather_class = {}
        for weather_class_id, dates in weather_classes.dates.items():
            signals = []
            day_type = []
            for date in dates:
                load_signal = load_data[date]
                if len(load_signal) == length:
                    signals.append(load_signal)
                    if date in us_holidays:
                        day_type.append(cls.HOLIDAY)
                    else:
                        weekday_type = cls.WEEKEND if date.weekday() > 5 else cls.WEEKDAY
                        day_type.append(weekday_type)

            signals = np.array(signals)
            day_type = np.array(day_type)
            centroids = k_means(signals, number_of_clusters, number_of_iterations)
            results = {str(idx): centroid.tolist() for idx, centroid in enumerate(centroids)}
            centroids_by_weather_class[weather_class_id] = results

            # Now calculate the std dev of the contributing signals to each centroid
            # Save them off as a Dict[ weather_class_idx , Dict [ load_class_idx, List]]
            centroid_assignment = closest_centroids(signals, centroids)
            centroid_stddevs = {}
            centroid_weekdays = {}
            for centroid_idx in range(np.max(centroid_assignment)):
                signal_indices = np.where(centroid_assignment == centroid_idx)[0]
                time_series_data = signals[signal_indices]
                stacked_data = np.vstack(time_series_data)
                std_devs = np.std(stacked_data, axis=0)
                centroid_stddevs[centroid_idx] = list(std_devs)
                centroid_weekdays[centroid_idx] = list(day_type[signal_indices])

                # Count the number of weekend/holidays
            std_devs_by_weather_class[weather_class_id] = centroid_stddevs

        ws = LoadClasses(load_id=load_id, centroids=centroids_by_weather_class, length=length,
                         std_devs=std_devs_by_weather_class, weather_classes_id=weather_classes.id,
                         day_types=centroid_weekdays)
        return ws

    # Overwrite all the time.
    # Should throw exceptions when failure
    @classmethod
    def save_load_classes(cls, load_classes: LoadClasses) -> "LoadClassTools":
        result = cls.collection().insert_one(load_classes.dict())
        load_classes.id = result.inserted_id
        return LoadClassTools(load_classes)

    @classmethod
    def delete(cls, load_classes_id: PYMONGO_ID) -> None:
        object_id = PyObjectId(load_classes_id)
        result = cls.collection().delete_one({'_id': object_id})
        if result.deleted_count == 0:
            raise ValueError(f"Cannot delete load class document by ID: {load_classes_id}")



from datetime import datetime
from collections import defaultdict
from typing import Dict
import numpy as np

from config.types import PYMONGO_ID, PyObjectId
from mongo_models import WeatherClasses
from mongo_models.mongo_fixtures import DEPLOYMENT_WEATHER
from mongo.mongo import Mongo
from algorithms import k_means, closest_centroids
from config.log_wrapper import log


class WeatherClassTools:

    def __init__(self, ws: WeatherClasses):
        if not isinstance(ws, WeatherClasses):
            raise ValueError(f"Expecting WeatherClasses object!")
        self.weather_classes = ws
        self.centroids = np.array(list(self.weather_classes.centroids.values()))
        self.centroid_length = len(self.centroids[0])


    def calculate_weather_class_index(self, signal: np.array) -> str:
        if self.centroid_length != len(signal):
            raise ValueError(f"Invalid signal length in calculating weather class: {len(signal)} != {self.centroid_length}")
        centroid = closest_centroids(np.array([signal]), self.centroids)
        index = list(self.weather_classes.centroids.keys())[centroid[0]]
        return index


    @classmethod
    def collection(cls):
        return Mongo().database[DEPLOYMENT_WEATHER]

    @classmethod
    def get_weather_classes_tool(cls, weather_classes_id: PYMONGO_ID) -> "WeatherClassTools":
        ds = cls.collection().find_one({"_id": weather_classes_id})
        if ds:
            ws = WeatherClasses(**ds)
            return WeatherClassTools(ws)
        else:
            raise ValueError(f"Cannot locate Weather Classes in database by ID: {weather_classes_id}")


    @classmethod
    def get_weather_classes_by_weather_station(cls, weather_station_id: PYMONGO_ID) -> "WeatherClassTools":
        query = {"weather_station_id": PyObjectId(weather_station_id)}
        ds = cls.collection().find_one(query)
        if ds:
            ws = WeatherClasses(**ds)
        else:
            raise ValueError(f"Cannot locate Weather Classes in database by station ID: {weather_station_id}")
        return WeatherClassTools(ws)


    @classmethod
    def create_weather_classes(cls, weather_station_id: PYMONGO_ID, all_signals: Dict[datetime, np.array],
                               interval: int, number_of_clusters: int = 5,
                               number_of_iterations: int = 500) -> WeatherClasses:
        # Ensure the length of each signal is consistent
        length = 24*60 / interval
        signals = [arr for arr in all_signals.values() if len(arr) == length]
        signals = np.array(signals)
        centroids = k_means(signals, number_of_clusters, number_of_iterations)
        results = {str(idx): centroid.tolist() for idx, centroid in enumerate(centroids)}
        # Now collect the dates from each signal by their matching centroids
        dates = defaultdict(list)
        for date, signal in all_signals.items():
            centroid = closest_centroids(np.array([signal]), centroids)
            index = list(results.keys())[centroid[0]]
            dates[index].append(date)

        ws = WeatherClasses(weather_station_id=weather_station_id, centroids=results, dates=dates, interval=interval)
        return ws

    # Overwrite all the time.
    # Should throw exceptions when failure
    @classmethod
    def save_weather_classes(cls, weather_classes: WeatherClasses) -> "WeatherClassTools":
        result = cls.collection().insert_one(weather_classes.dict())
        weather_classes.id = result.inserted_id
        return WeatherClassTools(weather_classes)

    @classmethod
    def delete_weather_classes(cls, weather_classes_id: PYMONGO_ID) -> None:
        object_id = PyObjectId(weather_classes_id)
        result = cls.collection().delete_one({'_id': object_id})
        if result.deleted_count == 0:
            raise ValueError(f"Cannot delete weather class document by ID: {weather_classes_id}")



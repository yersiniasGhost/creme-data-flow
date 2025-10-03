from typing import Dict, List
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class LoadClasses(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="LoadClasses")
    load_id: PyObjectId
    weather_classes_id: PyObjectId
    length: int
    # Sets of centroids are calculated for each weather class.
    # If not using weather classes, the key will be "0"
    #  Dict[ weather_class:   Dict [ class_idx: [ K means centroid data ] ] ]
    centroids: Dict[str, Dict[str, List[float]]] = PydanticField(default_factory=dict)
    std_devs: Dict[str, Dict[str, List[float]]] = PydanticField(default_factory=dict)

    day_types: Dict[str, List[str]]

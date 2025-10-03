from typing import Dict, Optional, List
from config.py_object_id import PyObjectId
from pydantic import BaseModel, Field as PydanticField

from .time_frame import TimeFrame


class RateDefinition(BaseModel):
    name: str
    time_frames: List[TimeFrame]
    rate_rule: PyObjectId


class UtilityRateStructure(BaseModel):
    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="UtilityRateStructure")
    utility_id: PyObjectId
    utility_data_source: PyObjectId
    name: str
    seasons: Dict[str, List[TimeFrame]]

    # Rate structures.
    weekdays: Optional[Dict[str, List[RateDefinition]]]
    holidays_weekends: Optional[Dict[str, List[RateDefinition]]]
    rate_structure: Optional[Dict[str, List[RateDefinition]]]

# # Rate structures need to look like:
# x = {
#     "season_name": [
#         {
#             "name": "OffPeak",
#             "time_frame": [
#                 {
#                     "start": "00:00",
#                     "end": "24:00"
#                 }],
#             "rate": PyObjectId
#         }
#     ]
# }

from datetime import datetime
from mongo_models.mongo_fixtures import DeploymentType, DeploymentStatus
from pydantic import BaseModel, Field as PydanticField
from config.py_object_id import PyObjectId


class Deployment(BaseModel):
    DOCUMENT_TYPE = "Deployment"

    id: PyObjectId = PydanticField(None, alias="_id")
    _type: str = PydanticField(default="Deployment")
    client_id: int
    name: str
    deployment_type: str = DeploymentType.SIMULATION
    active_status: bool
    latitude: float
    longitude: float
    install_date: datetime
    description: str
    timezone_abbreviation: str = "XXX"
    timezone_offset: float = 0
    solar_capacity: float = 0
    storage_capacity: float = 0
    status: str = DeploymentStatus.NORMAL.value


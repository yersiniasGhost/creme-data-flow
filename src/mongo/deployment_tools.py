from typing import List, Optional
from pymongo import UpdateOne
from config.types import PYMONGO_ID
from config.py_object_id import PyObjectId
from mongo_models.deployment import Deployment
from mongo_models.mongo_fixtures import DEPLOYMENT_COLLECTION, DeploymentType
from mongo.mongo import Mongo


class DeploymentTools:
    DOCUMENT_TYPE = "Deployment"

    def __init__(self, deployment: Deployment):
        if not isinstance(deployment, Deployment):
            raise ValueError(f"Expecting panel production object!")
        self.deployment = deployment


    @classmethod
    def get_deployment_by_clientid_name(cls, client_id: int, name: Optional[str] = None,  active: Optional[bool] = True):
        query = {"client_id": client_id, "_type": "Deployment"}
        if name:
            query['name'] = name
        if active:
            query["active_status"] = active
        ds = cls.collection().find(query)
        deployments = [Deployment(**doc) for doc in ds]
        return deployments



    @classmethod
    def get_all_simulators(cls, active: Optional[bool] = True) -> List[Deployment]:
        query = {"deployment_type": DeploymentType.SIMULATION.value, "_type": "Deployment"}
        if active:
            query["active_status"] = active
        ds = cls.collection().find(query)
        deployments = [Deployment(**doc) for doc in ds]
        return deployments

    @classmethod
    def get_all_simulator_ids(cls, active: Optional[bool] = None) -> List[PyObjectId]:
        query = {"deployment_type": DeploymentType.SIMULATION.value}
        if active:
            query["active_status"] = active
        ids = [idx['_id'] for idx in cls.collection().find(query, {"_id"})]
        return ids

    @classmethod
    def get_deployment_by_id(cls, d_id: PYMONGO_ID) -> Deployment:
        ds = cls.collection().find_one({"_id": PyObjectId(d_id)})
        return Deployment(**ds)

    @classmethod
    def get_deployment_by_raptor_id(cls, raptor: str) -> Deployment:
        ds = cls.collection().find_one({"raptor": raptor})
        return Deployment(**ds)

    @classmethod
    def get_deployments_by_type(cls, deploy_type: DeploymentType, active: Optional[bool] = None) -> List[Deployment]:
        query = {"deployment_type": deploy_type.value}
        if active:
            query["active_status"] = active
        ds = cls.collection().find(query)
        deployments = [Deployment(**doc) for doc in ds]
        return deployments

    @classmethod
    def get_all_deployments(cls) -> List[Deployment]:
        ds = cls.collection().find()
        deployments = [Deployment(**doc) for doc in ds]
        return deployments

    @classmethod
    def update_timezone(cls, deploy: Deployment, timezone_abbreviation: str, timezone_offset: float) -> None:
        update_op = UpdateOne(
            {"_id": deploy.id},
            {"$set": {"timezone_abbreviation": timezone_abbreviation,
                      "timezone_offset": timezone_offset}},
            upsert=True
        )
        result = cls.collection().bulk_write([update_op])



    @classmethod
    def collection(cls):
        return Mongo().database[DEPLOYMENT_COLLECTION]


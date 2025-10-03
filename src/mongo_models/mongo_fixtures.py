from enum import Enum

DEPLOYMENT_COLLECTION = "Deployment_Deployment"
DEPLOYMENT_WEATHER = "Deployment_WeatherStation"
PANEL_COLLECTION = "Deployment_PanelString"
PLANT_COLLECTION = "Deployment_Plant"
PLANT_GROUP_COLLECTION = "Deployment_PlantGroup"
BATTERY_STORAGE_COLLECTION = "Deployment_BESS"
LOAD_MODEL_COLLECTION = "Deployment_Load"
LOAD_CLUSTER_COLLECTION = "LoadModels"

TIME_SERIES_PANEL_PRODUCTION = "Panel_Production"
TIME_SERIES_PLANT_BUS_HISTORY = "PlantBusHistory"
TIME_SERIES_LOAD_HISTORY = "LoadHistory"
TIME_SERIES_DEPLOYMENT_HISTORY = "DeploymentHistory"
TIME_SERIES_BATTERY_STORAGE_HISTORY = "BatteryStorageHistory"


class DeploymentStatus(Enum):
    ERROR = "Error"
    NORMAL = "Normal"
    WARNING = "Warning"
    OFFLINE = "Offline"


class DeploymentType(Enum):
    SIMULATION = "SIMULATION"
    UNDER_DEVELOPMENT = "Under development"
    ACTIVE = "ACTIVE"

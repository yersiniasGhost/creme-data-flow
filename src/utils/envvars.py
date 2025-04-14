from typing import Optional
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os
import sys

from .singleton import Singleton


class EnvVars(metaclass=Singleton):

    def __init__(self):
        # Load .env file if it exists
        env_path = find_dotenv()
        if not env_path:
            print("No .env file found")
            exit(1)
        load_dotenv(find_dotenv(env_path))
        self.env_variables = {}
        # Database settings
        self.influx_url = self._get_required("INFLUXDB_URL")
        self.influx_token = self._get_required("INFLUXDB_TOKEN")
        self.influx_org = self._get_required("INFLUXDB_ORG")
        self.influx_bucket = self._get_required("INFLUXDB_BUCKET")

        # MQTT Configuration
        self.mqtt_url = self._get_required("MQTT_URL")
        self.mqtt_port = self._getenv("MQTT_PORT", "1883")
        self.mqtt_topic = self._get_required("MQTT_TOPIC")

        # Application settings
        self.debug = self._get_bool('DEBUG', "False")
        self.log_level = self._getenv('LOG_LEVEL', 'INFO')
        self.log_path = self._getenv("LOG_PATH", "/var/log/raptor")


    def _getenv(self, variable: str, default: Optional[str] = None) -> Optional[str]:
        return self.env_variables.get(variable) or self.env_variables.setdefault(
            variable,
            os.getenv(variable, default)
        )


    def _get_required(self, key: str) -> str:
        value = self._getenv(key)
        if value is None:
            raise ValueError(f"Missing required environment variable: {key}")
        return value

    def _get_bool(self, key: str, default: str) -> bool:
        value = self._getenv(key, default)
        return value.lower() in ('true', '1', 'yes', 'y')


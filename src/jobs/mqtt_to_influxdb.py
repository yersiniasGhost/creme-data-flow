from typing import Optional
import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from src.utils.envvars import EnvVars
from utils.logger import LogManager
from jobs.mongo_injection import MongoInjection
from jobs.data_validator import LineProtocolValidator


class MqttToInflux:
    def __init__(self, write_to_mongo: bool = False):
        self.logger = LogManager("crem3-mqtt-influx").get_logger("mqtt_influx")
        self.validator = LineProtocolValidator()

        # Initialize MongoDB injection if enabled
        self.mongo_injection: Optional[MongoInjection] = None
        if write_to_mongo:
            try:
                self.logger.info("Writing to MongoDB as well as InfluxDB")
                self.mongo_injection = MongoInjection()
            except Exception as e:
                self.logger.error(f"Failed to initialize MongoInjection: {e}")
                self.logger.warning("Continuing with InfluxDB only")
                self.mongo_injection = None

        # Create InfluxDB client
        self.influx_client = InfluxDBClient(
            url=EnvVars().influx_url,
            token=EnvVars().influx_token,
            org=EnvVars().influx_org
        )

        # Set up MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        # Create write API with synchronous mode
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.bucket = EnvVars().influx_bucket


    def _mongo_injection(self, line_protocol_data: list):
        """
        Write Line Protocol data to MongoDB.

        Args:
            line_protocol_data: List of Line Protocol strings
        """
        if not self.mongo_injection:
            return

        try:
            # Validate data before writing
            validation_result = self.validator.validate_line_protocol_batch(line_protocol_data)

            if not validation_result.is_valid:
                self.logger.error(f"MongoDB write skipped - validation failed: {validation_result.errors}")
                # Log the failed data for retry
                self._log_failed_write(line_protocol_data, validation_result.errors)
                return

            if validation_result.warnings:
                self.logger.warning(f"Validation warnings: {validation_result.warnings}")

            # Write to MongoDB
            self.mongo_injection.write(line_protocol_data)
            self.logger.debug(f"Successfully wrote {len(line_protocol_data)} lines to MongoDB")

        except Exception as e:
            self.logger.error(f"Error writing to MongoDB: {e}")
            # Log the failed data for retry
            self._log_failed_write(line_protocol_data, [str(e)])

    def _log_failed_write(self, line_protocol_data: list, errors: list):
        """
        Log failed MongoDB writes for later retry.

        Args:
            line_protocol_data: The data that failed to write
            errors: List of error messages
        """
        try:
            import datetime
            failed_write_log = EnvVars()._getenv("FAILED_WRITES_LOG_PATH", "/var/log/raptor/failed_writes.log")

            with open(failed_write_log, 'a') as f:
                f.write(f"\n=== Failed Write at {datetime.datetime.now()} ===\n")
                f.write(f"Errors: {errors}\n")
                f.write(f"Data:\n")
                for line in line_protocol_data:
                    f.write(f"{line}\n")
                f.write("=" * 50 + "\n")

            self.logger.info(f"Logged failed write to {failed_write_log}")
        except Exception as log_error:
            self.logger.error(f"Failed to log failed write: {log_error}")


    def process_mqtt_message(self, msg):
        """
        Process MQTT message and write to both InfluxDB and MongoDB.

        Implements dual-write pattern with consistency guarantees:
        1. Parse MQTT payload once
        2. Write to InfluxDB first (fast, synchronous)
        3. If successful, write to MongoDB (slower, with validation)
        4. Log any failures for retry
        """
        try:
            # Parse the MQTT payload
            full_payload = json.loads(msg.payload.decode())

            for payload in full_payload:
                # Validate payload format
                if payload.get('mode') != "line":
                    self.logger.error("Payload not in expected format")
                    continue

                # Get line protocol data
                line_protocol_data = payload.get("data")
                if not line_protocol_data or not isinstance(line_protocol_data, list):
                    self.logger.error("Invalid data format, expected list of line protocol strings")
                    continue

                # Join the lines with newline characters for InfluxDB
                line_protocol_batch = "\n".join(line_protocol_data)

                # STEP 1: Write to InfluxDB first (primary database)
                try:
                    self.write_api.write(bucket=self.bucket, record=line_protocol_batch)
                    self.logger.info(f"Wrote {len(line_protocol_data)} points to InfluxDB")
                except Exception as influx_error:
                    self.logger.error(f"Failed to write to InfluxDB: {influx_error}")
                    # If InfluxDB write fails, skip MongoDB write (maintain consistency)
                    continue

                # STEP 2: Write to MongoDB if enabled (only after InfluxDB success)
                if self.mongo_injection:
                    self._mongo_injection(line_protocol_data)

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON format: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    # MQTT client setup
    def on_connect(self, client, userdata, flags, rc):
        self.logger.info(f"Connected to MQTT broker with result code {rc}")
        client.subscribe(EnvVars().mqtt_topic)


    def on_message(self, client, userdata, msg):
        self.logger.info(f"Received message on topic {msg.topic}")
        self.process_mqtt_message(msg)


    def run(self):
        try:
            orgs_api = self.influx_client.organizations_api()
            organizations = orgs_api.find_organizations()

            # Print organizations
            print("Organizations in InfluxDB:")
            print("-" * 30)
            for org in organizations:
                print(f"ID: {org.id}")
                print(f"Name: {org.name}")
                print(f"Description: {org.description}")
                print("-" * 30)
            # Connect to MQTT broker
            port = int(EnvVars().mqtt_port)
            self.mqtt_client.connect(EnvVars().mqtt_url, port, 60)

            # Start the MQTT loop
            print("Starting MQTT client, waiting for messages...")
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            print("Service stopped")
        finally:
            self.influx_client.close()


if __name__ == "__main__":
    # Check if MongoDB injection should be enabled via environment variable
    enable_mongo = EnvVars()._getenv("ENABLE_MONGO_INJECTION", "false").lower() in ('true', '1', 'yes')

    if enable_mongo:
        print("MongoDB injection enabled")
    else:
        print("MongoDB injection disabled (InfluxDB only)")

    MqttToInflux(write_to_mongo=enable_mongo).run()

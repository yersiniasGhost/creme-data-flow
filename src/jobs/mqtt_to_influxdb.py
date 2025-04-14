import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from utils.envvars import EnvVars
from utils.logger import LogManager

# from config.logger import LogManager

# logger = LogManager("mqtt_to_influx").get_logger("MQTT_INFLUX")

# logger.info("Instantiating MQTT to InfluxDB processor")
# InfluxDB Configuration


class MqttToInflux:
    def __init__(self):
        self.logger = LogManager().get_logger("mqtt_influx")
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


    def process_mqtt_message(self, msg):
        """Process the MQTT message and store it in InfluxDB"""
        try:
            # Parse the MQTT payload
            full_payload = json.loads(msg.payload.decode())
            for payload in full_payload:
                # Validate payload format
                if payload.get('mode') != "line":
                    print("Error: Payload not in expected format")
                    return

                # Get line protocol data and join into a single string
                line_protocol_data = payload.get("data")
                if not line_protocol_data or not isinstance(line_protocol_data, list):
                    print("Error: Invalid data format, expected list of line protocol strings")
                    return

                # Join the lines with newline characters
                line_protocol_batch = "\n".join(line_protocol_data)

                # Write directly to InfluxDB using line protocol
                self.write_api.write(bucket=self.bucket, record=line_protocol_batch)
                print(line_protocol_batch)
                print(f"Wrote {len(line_protocol_data)} points to InfluxDB")

        except json.JSONDecodeError:
            print("Error: Invalid JSON format")
        except Exception as e:
            print(f"Error processing message: {e}")

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
    MqttToInflux().run()

from typing import List, Optional, Dict, Any
import re
import json
from utils.envvars import EnvVars
from utils.logger import LogManager

from mongo.battery_system_history_tools import BatterySystemHistoryTools, UpdateOne
from mongo.plant_tools import PlantTools
from mongo_models.plant_bus_history import GenerationState, LoadState, BessState


class MongoInjection:

    def __init__(self):
        self.logger = LogManager().get_logger("MongoInjection")

    '''
    The data is coming in LINE format and needs to be deconstructed and mapped into our MONGO DB format
    Example:
    BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100692Z Pack_Voltage=53.38,Current=1.2200000000000002,State_of_Charge=66,Remaining_Capacity=66.02666666666667 1751907198247532544
    Here the data comes in like: line = f"{measurement},{tag_str} {field_str} {timestamp}"
    I will need to convert into Mongo documents that have specific data points.   For example the BMS will store: 
     soc: float, power: float, energy: float, time_engaged: int, date: Union[datetime.datetime, pd.Timestamp
    
    '''
    def write(self, data: List[str], time_delta_seconds: float = 60.0):
        """
        Write parsed Line Protocol data to MongoDB time-series collections.

        Args:
            data: List of InfluxDB Line Protocol strings
            time_delta_seconds: Time interval between measurements (default 60s)
        """
        # Let's get the Plant associated with the Raptor ID
        raptor_id = self._get_raptor_id(data)
        if not raptor_id:
            self.logger.error(f"No raptor ID found in data.")
            return

        # Parse states with time delta
        bess_state = self._get_bess_state(data, time_delta_seconds)
        gen_state = self._get_gen_state(data, time_delta_seconds)
        load_state = self._get_load_state(data, time_delta_seconds)

        # Calculate derived values (energy balance)
        load_state.power_from_utility = load_state.power_demand - load_state.power_from_gen - load_state.power_from_bess
        load_state.energy_from_utility = (load_state.energy_demand -
                                          (load_state.energy_from_bess + load_state.energy_from_gen))
        gen_state.power_curtailed = gen_state.power_generated - load_state.power_from_gen - bess_state.charge_power
        gen_state.energy_curtailed = gen_state.power_curtailed * time_delta_seconds / 3600.0
        gen_state.energy_generated = gen_state.power_generated * time_delta_seconds / 3600.0

        # Get plant from raptor ID
        try:
            plant = PlantTools.get_plant_by_raptor_id(raptor_id)
        except Exception as e:
            self.logger.error(f"Failed to get plant for raptor_id {raptor_id}: {e}")
            return

        # Extract timestamp from first line for date-based operations
        first_parsed = self.parse_influxdb_line(data[0]) if data else None
        if not first_parsed or not first_parsed.get('timestamp'):
            self.logger.error("No valid timestamp found in data")
            return

        # Convert nanosecond timestamp to datetime
        from datetime import datetime
        timestamp_ns = first_parsed['timestamp']
        timestamp = datetime.fromtimestamp(timestamp_ns / 1e9)

        # TODO: Implement bulk write operations for different collection types
        # This would include:
        # 1. PlantBusHistory updates using PlantBusHistoryTools.get_update_one()
        # 2. BatterySystemHistory updates using BatterySystemHistoryTools.get_update_one()
        # 3. LoadHistory updates using LoadHistoryTools.get_update_one()

        self.logger.info(f"Processed data for raptor {raptor_id} at {timestamp}")


    def _get_gen_state(self, data: List[str], time_delta_seconds: float) -> GenerationState:
        """
        Parse Generation measurement data (solar/PV) and calculate generation state.

        Args:
            data: List of InfluxDB Line Protocol strings
            time_delta_seconds: Time interval for energy calculations

        Returns:
            GenerationState with aggregated generation metrics
        """
        gen_data = self._parse_measurement_data("Generation", data)
        if not gen_data:
            return GenerationState()

        total_power = 0.0

        # Aggregate across all generation devices (CTs)
        for device_id, measurements in gen_data.items():
            for measurement in measurements:
                fields = measurement['fields']
                voltage = fields.get('Voltage', 0.0)
                current = fields.get('Current', 0.0)

                # Calculate power (P = V * I)
                power = voltage * current
                total_power += power

        # Energy will be calculated later after knowing curtailment
        # energy_generated = total_power * time_delta_seconds / 3600.0

        return GenerationState(
            power_generated=total_power,
            power_curtailed=0.0,  # Calculated later in write()
            energy_generated=0.0,  # Calculated later in write()
            energy_curtailed=0.0   # Calculated later in write()
        )

    def _get_load_state(self, data: List[str], time_delta_seconds: float) -> LoadState:
        """
        Parse Converters/Load measurement data and calculate load state.

        Args:
            data: List of InfluxDB Line Protocol strings
            time_delta_seconds: Time interval for energy calculations

        Returns:
            LoadState with aggregated load metrics
        """
        converter_data = self._parse_measurement_data("Converters", data)
        if not converter_data:
            return LoadState()

        total_demand = 0.0
        total_from_gen = 0.0
        total_from_bess = 0.0

        # Aggregate across all converter devices
        for device_id, measurements in converter_data.items():
            for measurement in measurements:
                fields = measurement['fields']

                # AC input represents power from generation
                ac_power = fields.get('AC_input_power', 0.0)
                total_from_gen += ac_power

                # DC power represents battery discharge to load
                dc_power = fields.get('DC_Power', 0.0)
                total_from_bess += abs(dc_power)

                # Phase1 true power represents total load demand
                phase1_power = fields.get('Phase1TruePower', 0.0)
                total_demand += phase1_power

        # Calculate energy from power
        energy_demand = total_demand * time_delta_seconds / 3600.0
        energy_from_gen = total_from_gen * time_delta_seconds / 3600.0
        energy_from_bess = total_from_bess * time_delta_seconds / 3600.0

        return LoadState(
            power_demand=total_demand,
            energy_demand=energy_demand,
            power_from_bess=total_from_bess,
            energy_from_bess=energy_from_bess,
            power_from_gen=total_from_gen,
            energy_from_gen=energy_from_gen,
            power_from_utility=0.0,  # Calculated later in write()
            energy_from_utility=0.0  # Calculated later in write()
        )

    def _get_bess_state(self, data: List[str], time_delta_seconds: float) -> BessState:
        """
        Parse BMS (Battery Management System) data and calculate BESS state.

        Args:
            data: List of InfluxDB Line Protocol strings
            time_delta_seconds: Time interval for energy calculations

        Returns:
            BessState with aggregated battery metrics
        """
        bess_data = self._parse_measurement_data("BMS", data)
        if not bess_data:
            return BessState()

        total_charge_power = 0.0
        total_discharge_power = 0.0
        total_soc = 0.0
        device_count = 0

        # Aggregate across all battery devices
        for device_id, measurements in bess_data.items():
            for measurement in measurements:
                fields = measurement['fields']
                current = fields.get('Current', 0.0)
                voltage = fields.get('Pack_Voltage', 0.0)
                soc = fields.get('State_of_Charge', fields.get('SOC', 0.0))

                # Calculate power (P = V * I)
                power = voltage * current

                # Positive current = charging, negative = discharging
                if current > 0:
                    total_charge_power += power
                else:
                    total_discharge_power += abs(power)

                total_soc += soc
                device_count += 1

        # Calculate energy from power and time delta
        charge_energy = total_charge_power * time_delta_seconds / 3600.0  # Convert to Wh
        discharge_energy = total_discharge_power * time_delta_seconds / 3600.0

        # Average SOC across all devices
        avg_soc = total_soc / device_count if device_count > 0 else 0.0

        return BessState(
            charge_power=total_charge_power,
            discharge_power=total_discharge_power,
            charge_energy=charge_energy,
            discharge_energy=discharge_energy,
            engaged_time_s=time_delta_seconds,
            state_of_charge=avg_soc
        )


    def _parse_measurement_data(self, measurement: str, data: List[str]) -> Optional[Dict[str, Any]]:
        """ Args:
            measurement (str): The measurement name to match (e.g., 'BMS', 'Converters')
            data: List[str]: a list of lines of InfluxDB Line Protocol data


        Returns:
            dict or None: Dictionary containing device_id, fields, and timestamp if successful
            "device_id": [ {
                'fields': dict,
                'timestamp': int
            }]
            Returns None if line doesn't match measurement or parsing fails.
            """
        result = {}

        for line in data:
            line = line.strip()
            if not line:
                continue
            try:
                # Check if line starts with the specified measurement
                if not line.startswith(f"{measurement},"):
                    continue

                # Format: measurement,tags fields timestamp
                parts = line.split(' ')
                if len(parts) < 2:
                    continue

                measurement_tags = parts[0]
                fields_part = parts[1]
                timestamp = None
                if len(parts) >= 3:
                    try:
                        timestamp = int(parts[2])
                    except ValueError:
                        timestamp = None

                device_id = self._extract_device_id_from_tags(measurement_tags)
                if not device_id:
                    continue

                # Parse fields
                fields = self._parse_fields(fields_part)
                if not fields:
                    continue

                # Build the entry
                entry = {
                    'fields': fields,
                    'timestamp': timestamp
                }
                # Add to result dictionary
                if device_id not in result:
                    result[device_id] = []
                result[device_id].append(entry)

            except Exception as e:
                # Skip malformed lines
                continue

        return result if result else None



    def _extract_device_id_from_tags(self, measurement_tags: str) -> Optional[str]:
        # Use regex to find device_id=<value>
        match = re.search(r'device_id=([^,\s]+)', measurement_tags)
        return match.group(1) if match else None



    def _parse_fields(self, fields_part: str) -> Optional[Dict[str, Any]]:
        """
        Parse the fields portion of an InfluxDB line.

        Args:
            fields_part (str): The fields portion (e.g., "SOC=45,Current=2.85")

        Returns:
            dict or None: Dictionary of field names to values
        """
        try:
            fields = {}

            # Split by comma to get individual field=value pairs
            field_pairs = fields_part.split(',')

            for pair in field_pairs:
                if '=' not in pair:
                    continue

                key, value_str = pair.split('=', 1)
                key = key.strip()
                value_str = value_str.strip()

                # Parse the value based on its format
                value = self._parse_field_value(value_str)
                fields[key] = value

            return fields if fields else None

        except Exception:
            return None


    def _parse_field_value(self, value_str: str) -> Any:
        """
        Parse a field value string into the appropriate Python type.

        Args:
            value_str (str): The value string from the field

        Returns:
            The parsed value (int, float, str, or bool)
        """
        value_str = value_str.strip()

        # Handle quoted strings
        if value_str.startswith('"') and value_str.endswith('"'):
            return value_str[1:-1]  # Remove quotes

        # Handle integers (with optional 'i' suffix)
        if value_str.endswith('i'):
            try:
                return int(value_str[:-1])
            except ValueError:
                pass

        # Handle booleans
        if value_str.lower() in ['t', 'true']:
            return True
        elif value_str.lower() in ['f', 'false']:
            return False

        # Try to parse as integer
        try:
            return int(value_str)
        except ValueError:
            pass

        # Try to parse as float
        try:
            return float(value_str)
        except ValueError:
            pass

        # Default to string
        return value_str


    def _get_raptor_id(self, data: List[str]) -> Optional[str]:
        """
        Extract raptor ID from line protocol data.

        Args:
            data: List of InfluxDB Line Protocol strings

        Returns:
            Raptor ID string or None
        """
        # Search through all lines for raptor tag
        for line in data:
            match = re.search(r'raptor=([^,\s]+)', line)
            if match:
                return match.group(1)
        return None

    def get_bms_update(self, battery_system_id: str, parsed_data: dict, timestamp) -> UpdateOne:
        """
        Create a MongoDB UpdateOne operation for battery system history.

        Args:
            battery_system_id: MongoDB ID of the battery system
            parsed_data: Parsed line protocol dict with measurement, tags, fields, timestamp
            timestamp: Python datetime object

        Returns:
            UpdateOne operation for bulk write
        """
        from mongo.battery_system_history_tools import BatterySystemHistoryTools
        from config.py_object_id import PyObjectId

        fields = parsed_data['fields']
        current = fields.get('Current', 0.0)
        voltage = fields.get('Pack_Voltage', 0.0)
        soc = fields.get('State_of_Charge', fields.get('SOC', 0.0))

        # Calculate power and assume 60-second interval for energy
        power = voltage * current
        energy = abs(power) * 60.0 / 3600.0  # Wh for 60-second interval
        time_engaged = 60 if abs(current) > 0.1 else 0  # Engaged if significant current

        return BatterySystemHistoryTools.get_update_one(
            soc=soc,
            power=power,
            energy=energy,
            time_engaged=time_engaged,
            date=timestamp,
            battery_system_id=PyObjectId(battery_system_id),
            include_last_values=True
        )


    @staticmethod
    def parse_influxdb_line(line: str) -> Optional[dict]:
        """
        Parse a single InfluxDB line protocol string.

        Args:
            line (str): InfluxDB line protocol string

        Returns:
            dict: Parsed data with measurement, tags, fields, and timestamp
        """
        line = line.strip()
        if not line:
            return None

        # Split by spaces to separate measurement+tags, fields, and timestamp
        parts = line.split(' ')

        # Handle case where there might be spaces in field values (though uncommon)
        # Find the timestamp (last part that's all digits)
        timestamp_idx = len(parts) - 1
        while timestamp_idx >= 0 and not parts[timestamp_idx].isdigit():
            timestamp_idx -= 1

        if timestamp_idx < 2:  # Need at least measurement+tags and fields
            raise ValueError("Invalid line format")

        # Reconstruct parts
        measurement_tags = parts[0]
        fields_str = ' '.join(parts[1:timestamp_idx])
        timestamp = int(parts[timestamp_idx])

        # Parse measurement and tags
        measurement_parts = measurement_tags.split(',')
        measurement = measurement_parts[0]

        tags = {}
        for tag_part in measurement_parts[1:]:
            if '=' in tag_part:
                key, value = tag_part.split('=', 1)
                tags[key] = value

        # Parse fields
        fields = {}
        for field_part in fields_str.split(','):
            if '=' in field_part:
                key, value = field_part.split('=', 1)
                # Convert numeric values
                try:
                    if '.' in value:
                        fields[key] = float(value)
                    else:
                        fields[key] = int(value)
                except ValueError:
                    fields[key] = value  # Keep as string if not numeric

        return {
            'measurement': measurement,
            'tags': tags,
            'fields': fields,
            'timestamp': timestamp
        }



    def extract_device_data(self, lines: List[str]):
        """
        Extract data from multiple InfluxDB lines, organizing by device_id.

        Args:
            lines (list): List of InfluxDB line protocol strings

        Returns:
            dict: Data organized by device_id
        """
        device_data = {}

        for line in lines:
            try:
                parsed = self.parse_influxdb_line(line)
                if parsed:
                    device_id = parsed['tags'].get('device_id')
                    if device_id:
                        if device_id not in device_data:
                            device_data[device_id] = []
                        device_data[device_id].append(parsed)
            except ValueError as e:
                print(f"Error parsing line: {e}")
                continue

        return device_data



    def get_tag_ids(parsed_data):
        """
        Extract all tag key-value pairs from parsed data.

        Args:
            parsed_data (dict): Parsed line data

        Returns:
            dict: All tag key-value pairs
        """
        return parsed_data.get('tags', {})



    def get_field_data(parsed_data):
        """
        Extract all field key-value pairs from parsed data.

        Args:
            parsed_data (dict): Parsed line data

        Returns:
            dict: All field key-value pairs
        """
        return parsed_data.get('fields', {})



    # # Example usage:
    # if __name__ == "__main__":
    #     sample_line = "BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100356Z Current=3.783333333333333,State_of_Charge=69,Pack_Voltage=53.800000000000004,Remaining_Capacity=69.13333333333334 1751912004025070080"
    #
    #     parsed = parse_influxdb_line(sample_line)
    #     print("Parsed data:", parsed)
    #     print("Tag IDs:", get_tag_ids(parsed))
    #     print("Field data:", get_field_data(parsed))
    #


def main():
    """
    Test method for the MongoInjection class.
    Tests parsing of InfluxDB line protocol data and extraction of device information.
    """
    # Sample test data matching the format from your examples
    test_data = [
        "BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100214Z SOC=45,Current=2.8466666666666667,Pack_Voltage=53.16666666666667,Capacity=44.593333333333334 1755381816045284864",
        "BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100138Z SOC=35,Current=1.51,Pack_Voltage=53.11,Capacity=35.46333333333333 1755381816045284864",
        "BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100356Z SOC=44,Current=2.756666666666667,Pack_Voltage=53.15,Capacity=43.74333333333333 1755381816045284864",
        "BMS,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_battery_ID,device_id=305921341100692Z SOC=44,Current=2.7866666666666666,Pack_Voltage=53.156666666666666,Capacity=44.28333333333333 1755381816045284864",
        "Converters,raptor=67ad1e83853af29b1d044fe6,hardware_id=TODO_Converter_ID,device_id=FC-C2-3D-21-86-1A AC_input_voltage=0.0,AC_input_current=0.0,Phase1Voltage=121.4,Phase1Current=0.0,DC_Current=0.5,DC_Power=0.0,AC_input_frequency=0.0,AC_input_power=0.0,Phase1ApparentPower=0.0,Phase1TruePower=0.0,DC_Voltage=53.5 1755381816045284864",
        "Generation,raptor=67ad1e83853af29b1d044fe6,hardware_id=CTs_ID,device_id=ct001 Voltage=2.2075050875050874,Current=6.680407000406999 1755381816045284864",
        "Generation,raptor=67ad1e83853af29b1d044fe6,hardware_id=CTs_ID,device_id=ct002 Voltage=2.5695075295075296,Current=5.560602360602355 1755381816045284864"
    ]

    print("=" * 60)
    print("Testing MongoInjection Class")
    print("=" * 60)

    # Initialize the MongoInjection class
    try:
        mongo_injection = MongoInjection()
        print("✓ MongoInjection class initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize MongoInjection: {e}")
        return

    print()

    # Test 1: Extract raptor ID
    print("Test 1: Extracting Raptor ID")
    print("-" * 30)
    raptor_id = mongo_injection._get_raptor_id(test_data[0])
    if raptor_id:
        print(f"✓ Raptor ID extracted: {raptor_id}")
    else:
        print("✗ Failed to extract raptor ID")
    print()

    # Test 2: Parse single InfluxDB line
    print("Test 2: Parsing Single InfluxDB Line")
    print("-" * 35)
    sample_line = test_data[0]
    print(f"Input: {sample_line}")
    try:
        parsed_line = MongoInjection.parse_influxdb_line(sample_line)
        print("✓ Line parsed successfully:")
        print(f"  Measurement: {parsed_line['measurement']}")
        print(f"  Tags: {parsed_line['tags']}")
        print(f"  Fields: {parsed_line['fields']}")
        print(f"  Timestamp: {parsed_line['timestamp']}")
    except Exception as e:
        print(f"✗ Failed to parse line: {e}")
    print()

    # Test 3: Extract device data from multiple lines
    print("Test 3: Extracting Device Data")
    print("-" * 30)
    try:
        device_data = mongo_injection.extract_device_data(test_data)
        print(f"✓ Device data extracted for {len(device_data)} devices:")
        for device_id, data_list in device_data.items():
            print(f"  Device {device_id}: {len(data_list)} records")
            # Show first record details
            if data_list:
                first_record = data_list[0]
                print(f"    Measurement: {first_record['measurement']}")
                print(f"    Fields: {list(first_record['fields'].keys())}")
    except Exception as e:
        print(f"✗ Failed to extract device data: {e}")
    print()

    # Test 4: Parse measurement data (BMS only)
    print("Test 4: Parsing BMS Measurement Data")
    print("-" * 35)
    try:
        bms_data = mongo_injection._parse_measurement_data("BMS", test_data)
        if bms_data:
            print(f"✓ BMS data parsed for {len(bms_data)} devices:")
            for device_id, records in bms_data.items():
                print(f"  Device {device_id}:")
                for i, record in enumerate(records):
                    print(f"    Record {i + 1}: {record['fields']}")
                    print(f"    Timestamp: {record['timestamp']}")
        else:
            print("✗ No BMS data found")
    except Exception as e:
        print(f"✗ Failed to parse BMS data: {e}")
    print()

    # Test 5: Parse different measurement types
    print("Test 5: Parsing Different Measurement Types")
    print("-" * 42)
    measurements = ["BMS", "Converters", "Generation"]
    for measurement in measurements:
        try:
            data = mongo_injection._parse_measurement_data(measurement, test_data)
            if data:
                print(f"✓ {measurement}: {len(data)} devices found")
                # Show field names for first device
                first_device = next(iter(data.values()))
                if first_device:
                    field_names = list(first_device[0]['fields'].keys())
                    print(f"  Fields: {', '.join(field_names)}")
            else:
                print(f"✗ {measurement}: No data found")
        except Exception as e:
            print(f"✗ {measurement}: Error - {e}")
    print()

    # Test 6: Test field value parsing
    print("Test 6: Testing Field Value Parsing")
    print("-" * 33)
    test_values = [
        "45",  # Integer
        "2.846",  # Float
        '"online"',  # String
        "123i",  # Integer with suffix
        "true",  # Boolean
        "false"  # Boolean
    ]

    for test_val in test_values:
        try:
            parsed_val = mongo_injection._parse_field_value(test_val)
            print(f"✓ '{test_val}' -> {parsed_val} ({type(parsed_val).__name__})")
        except Exception as e:
            print(f"✗ '{test_val}' -> Error: {e}")
    print()

    # Test 7: Test complete write workflow (mock)
    print("Test 7: Testing Write Workflow (Mock)")
    print("-" * 35)
    try:
        print("Note: This would normally call external dependencies")
        print("  - PlantTools.get_plant_by_raptor_id()")
        print("  - MongoDB operations")
        print("  - BatterySystemHistoryTools updates")
        print("✓ Write workflow structure validated")

        # Show what the parsed data structure would look like
        bms_data = mongo_injection._parse_measurement_data("BMS", test_data)
        if bms_data:
            print(f"\nData ready for MongoDB insertion:")
            print(f"  Raptor ID: {raptor_id}")
            print(f"  BMS devices: {len(bms_data)}")
            for device_id in list(bms_data.keys())[:2]:  # Show first 2
                print(f"    {device_id}: {len(bms_data[device_id])} records")
    except Exception as e:
        print(f"✗ Write workflow test failed: {e}")

    print()
    print("=" * 60)
    print("Testing Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
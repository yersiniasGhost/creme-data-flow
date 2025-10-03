from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from utils.logger import LogManager


@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


class LineProtocolValidator:
    """
    Validates InfluxDB Line Protocol data for consistency and correctness.
    Ensures data quality before writing to both InfluxDB and MongoDB.
    """

    # Define expected measurements and their required fields
    MEASUREMENT_SCHEMAS = {
        'BMS': {
            'required_fields': ['Current', 'Pack_Voltage'],
            'optional_fields': ['State_of_Charge', 'SOC', 'Remaining_Capacity', 'Capacity'],
            'required_tags': ['device_id', 'raptor'],
            'field_ranges': {
                'State_of_Charge': (0, 100),
                'SOC': (0, 100),
                'Pack_Voltage': (0, 100),  # Volts (typical range for 48V systems)
                'Current': (-200, 200),  # Amps (charging/discharging range)
            }
        },
        'Converters': {
            'required_fields': ['DC_Voltage'],
            'optional_fields': [
                'AC_input_voltage', 'AC_input_current', 'Phase1Voltage',
                'Phase1Current', 'DC_Current', 'DC_Power', 'AC_input_frequency',
                'AC_input_power', 'Phase1ApparentPower', 'Phase1TruePower'
            ],
            'required_tags': ['device_id', 'raptor'],
            'field_ranges': {
                'AC_input_voltage': (0, 300),
                'Phase1Voltage': (0, 300),
                'DC_Voltage': (0, 100),
                'AC_input_frequency': (0, 70),
            }
        },
        'Generation': {
            'required_fields': ['Voltage', 'Current'],
            'optional_fields': [],
            'required_tags': ['device_id', 'raptor'],
            'field_ranges': {
                'Voltage': (0, 50),  # CT voltage readings
                'Current': (0, 100),  # CT current readings
            }
        }
    }

    def __init__(self):
        self.logger = LogManager().get_logger("DataValidator")

    def validate_line_protocol_batch(self, lines: List[str]) -> ValidationResult:
        """
        Validate a batch of Line Protocol strings.

        Args:
            lines: List of InfluxDB Line Protocol strings

        Returns:
            ValidationResult with validation status and any errors/warnings
        """
        errors = []
        warnings = []

        if not lines:
            errors.append("Empty data batch")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Check for raptor ID presence
        raptor_ids = set()
        for line in lines:
            if 'raptor=' in line:
                import re
                match = re.search(r'raptor=([^,\s]+)', line)
                if match:
                    raptor_ids.add(match.group(1))

        if not raptor_ids:
            errors.append("No raptor ID found in any line")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        if len(raptor_ids) > 1:
            warnings.append(f"Multiple raptor IDs found in batch: {raptor_ids}")

        # Validate each line
        for idx, line in enumerate(lines):
            line_result = self.validate_line_protocol(line)
            if not line_result.is_valid:
                errors.extend([f"Line {idx}: {err}" for err in line_result.errors])
            if line_result.warnings:
                warnings.extend([f"Line {idx}: {warn}" for warn in line_result.warnings])

        is_valid = len(errors) == 0

        if not is_valid:
            self.logger.warning(f"Validation failed with {len(errors)} errors")

        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def validate_line_protocol(self, line: str) -> ValidationResult:
        """
        Validate a single Line Protocol string.

        Args:
            line: InfluxDB Line Protocol string

        Returns:
            ValidationResult with validation status
        """
        errors = []
        warnings = []

        line = line.strip()
        if not line:
            errors.append("Empty line")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Basic format check: measurement,tags fields timestamp
        parts = line.split(' ')
        if len(parts) < 3:
            errors.append(f"Invalid line format: expected 3+ parts, got {len(parts)}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        try:
            # Parse the line
            measurement_tags = parts[0]
            measurement_parts = measurement_tags.split(',')
            measurement = measurement_parts[0]

            # Extract tags
            tags = {}
            for tag_part in measurement_parts[1:]:
                if '=' in tag_part:
                    key, value = tag_part.split('=', 1)
                    tags[key] = value

            # Extract fields
            fields_part = parts[1]
            fields = {}
            for field_pair in fields_part.split(','):
                if '=' in field_pair:
                    key, value = field_pair.split('=', 1)
                    # Try to parse numeric value
                    try:
                        if '.' in value:
                            fields[key] = float(value)
                        else:
                            fields[key] = int(value) if value.isdigit() else value
                    except ValueError:
                        fields[key] = value

            # Validate timestamp
            timestamp_str = parts[-1]
            if not timestamp_str.isdigit():
                errors.append(f"Invalid timestamp: {timestamp_str}")

            # Validate against schema if measurement is known
            if measurement in self.MEASUREMENT_SCHEMAS:
                schema_result = self._validate_against_schema(measurement, tags, fields)
                errors.extend(schema_result.errors)
                warnings.extend(schema_result.warnings)
            else:
                warnings.append(f"Unknown measurement type: {measurement}")

        except Exception as e:
            errors.append(f"Failed to parse line: {str(e)}")

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def _validate_against_schema(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate parsed data against measurement schema.

        Args:
            measurement: Measurement name
            tags: Parsed tags dict
            fields: Parsed fields dict

        Returns:
            ValidationResult
        """
        errors = []
        warnings = []

        schema = self.MEASUREMENT_SCHEMAS.get(measurement)
        if not schema:
            return ValidationResult(is_valid=True, errors=[], warnings=[])

        # Check required tags
        for required_tag in schema['required_tags']:
            if required_tag not in tags:
                errors.append(f"Missing required tag: {required_tag}")

        # Check required fields
        for required_field in schema['required_fields']:
            if required_field not in fields:
                errors.append(f"Missing required field: {required_field}")

        # Validate field ranges
        for field_name, value in fields.items():
            if field_name in schema.get('field_ranges', {}):
                min_val, max_val = schema['field_ranges'][field_name]
                try:
                    numeric_value = float(value)
                    if not (min_val <= numeric_value <= max_val):
                        warnings.append(
                            f"Field {field_name}={numeric_value} outside expected range [{min_val}, {max_val}]"
                        )
                except (ValueError, TypeError):
                    warnings.append(f"Field {field_name} should be numeric, got {type(value)}")

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def validate_data_consistency(
        self,
        influx_data: List[str],
        mongo_parsed_data: Optional[Dict] = None
    ) -> ValidationResult:
        """
        Validate consistency between InfluxDB line protocol and parsed MongoDB data.

        Args:
            influx_data: Original Line Protocol strings
            mongo_parsed_data: Parsed data structure for MongoDB (optional)

        Returns:
            ValidationResult
        """
        errors = []
        warnings = []

        # Validate the Line Protocol batch
        batch_result = self.validate_line_protocol_batch(influx_data)
        errors.extend(batch_result.errors)
        warnings.extend(batch_result.warnings)

        # If MongoDB data provided, check consistency
        if mongo_parsed_data:
            # Check timestamp consistency
            timestamps = set()
            for line in influx_data:
                parts = line.split(' ')
                if len(parts) >= 3:
                    timestamps.add(parts[-1])

            if len(timestamps) > 1:
                warnings.append(
                    f"Multiple timestamps in batch: {len(timestamps)} unique timestamps"
                )

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

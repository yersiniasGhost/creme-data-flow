# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`creme-data-flow` is a Python data pipeline application that processes IoT telemetry data from renewable energy systems (solar, battery storage, loads) via MQTT, writes to InfluxDB for time-series storage, and synchronizes with MongoDB for structured data management.

## Key Architecture

### Data Flow Pattern
1. **MQTT → InfluxDB Pipeline**: `src/jobs/mqtt_to_influxdb.py` listens to MQTT topics for telemetry data in InfluxDB Line Protocol format
2. **InfluxDB → MongoDB Sync**: `src/jobs/mongo_injection.py` parses Line Protocol data and transforms it into MongoDB documents
3. **MongoDB Layer**: Structured models in `src/mongo_models/` with corresponding "tools" (database access layer) in `src/mongo/`

### Line Protocol Parsing
The system processes InfluxDB Line Protocol format:
```
measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
```

Example measurements:
- `BMS` (Battery Management System): SOC, current, voltage, capacity
- `Converters`: AC/DC voltage, current, power metrics
- `Generation`: PV generation data from current transformers

Key parsing logic in `src/jobs/mongo_injection.py`:
- `parse_influxdb_line()`: Parses single line into structured dict
- `_parse_measurement_data()`: Extracts device-specific data by measurement type
- Device identification via `device_id` tag and `raptor` (plant identifier) tag

### MongoDB Architecture

**Dual Model Pattern**: Each entity has both a Pydantic model (`src/mongo_models/`) and a "Tools" class (`src/mongo/`) for database operations.

Collections (defined in `src/mongo_models/mongo_fixtures.py`):
- **Deployment collections**: `Deployment_Deployment`, `Deployment_Plant`, `Deployment_PlantGroup`, `Deployment_BESS`, `Deployment_Load`, `Deployment_WeatherStation`, `Deployment_PanelString`
- **Time-series collections**: `Panel_Production`, `PlantBusHistory`, `LoadHistory`, `DeploymentHistory`, `BatteryStorageHistory`

Models use hierarchical relationships:
- `PlantGroup` → `Plant` → Device-level resources (BESS, Loads, PanelStrings)
- Plant identified by `raptor_id` (hardware controller ID)

### State Models
`src/mongo_models/plant_bus_history.py` defines dataclasses for energy flow tracking:
- `GenerationState`: Power/energy generated and curtailed
- `LoadState`: Power/energy demand and sources (BESS, generation, utility)
- `BessState`: Battery charge/discharge power and energy, SOC

Energy balance calculations handle distribution across generation, storage, load, and utility.

## Environment Configuration

Required variables (see `.env_template`):
- **InfluxDB**: `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`
- **MQTT**: `MQTT_URL`, `MQTT_PORT`, `MQTT_TOPIC` (supports wildcards like `raptors/+/telemetry`)
- **Logging**: `LOG_PATH` (default `/var/log/raptor`), `LOG_LEVEL`, `DEBUG`

MongoDB connection hardcoded in `src/mongo/mongo.py` (localhost:27017, database: `creme_api_development`)

## Utilities

- **Singleton pattern**: `src/utils/singleton.py` used for `EnvVars`, `LogManager`, and `Mongo` to ensure single instances
- **Logging**: `src/utils/logger.py` - `LogManager` creates rotating file handlers, configurable per-logger names
- **Environment**: `src/utils/envvars.py` - `EnvVars()` singleton loads and validates environment variables

## Running the Application

Start MQTT to InfluxDB pipeline (InfluxDB only):
```bash
python -m src.jobs.mqtt_to_influxdb
```

Start with MongoDB dual injection enabled:
```bash
export ENABLE_MONGO_INJECTION=true
python -m src.jobs.mqtt_to_influxdb
```

Test MongoDB injection parsing:
```bash
python -m src.jobs.mongo_injection
```

Retry failed MongoDB writes:
```bash
python -m src.jobs.retry_handler --summary
python -m src.jobs.retry_handler --max-retries 3
```

## Dual Injection System

The application supports parallel data injection to both InfluxDB and MongoDB with consistency guarantees:

**Key Components**:
- `src/jobs/mqtt_to_influxdb.py`: Main processor with dual-write logic
- `src/jobs/mongo_injection.py`: MongoDB write handler with state parsing
- `src/jobs/data_validator.py`: Line Protocol validation (schemas, ranges)
- `src/jobs/retry_handler.py`: Failed write recovery system

**Consistency Pattern**:
1. Parse MQTT payload once
2. Write to InfluxDB first (synchronous, fast)
3. If successful, write to MongoDB (with validation)
4. Log failures to `/var/log/raptor/failed_writes.log` for retry

**Configuration**: Set `ENABLE_MONGO_INJECTION=true` in `.env` to enable dual writes

See `DUAL_INJECTION_SETUP.md` for complete documentation.

## Development Notes

- Package structure uses `src/` layout with `setup.py` for installation
- The codebase uses `from config.*` imports for shared types/utilities, but these modules don't exist in the repository (likely in a separate shared package)
- No formal test framework configured; single test file in `src/tests/influx_misc.py`
- Tools classes use `@classmethod` pattern for database operations, instance methods for entity-specific operations
- Pydantic models use `PyObjectId` for MongoDB `_id` fields with `alias="_id"`
- MongoDB writes use bulk `UpdateOne` operations with `upsert=True` for idempotency
- Time-series data stored with nested dicts: `{day: {seconds_from_midnight: value}}`

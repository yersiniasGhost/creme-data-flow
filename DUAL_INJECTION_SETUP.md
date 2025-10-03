# Dual-Path MQTT Data Injection Setup

This document describes the implementation of parallel data injection from MQTT to both InfluxDB and MongoDB with consistency guarantees.

## Overview

The system implements a dual-write pattern where MQTT telemetry data is:
1. Written to **InfluxDB** (primary, fast time-series database)
2. Written to **MongoDB** (legacy, structured document database)

Both databases receive the **same parsed data** from the same MQTT message, ensuring timestamp and value consistency.

## Architecture

### Data Flow
```
MQTT Message → Parse Once → Validate → Write to InfluxDB → Write to MongoDB
                                ↓                                    ↓
                          Log if invalid                    Log failures for retry
```

### Consistency Guarantees

1. **Single Source of Truth**: Both databases process the same Line Protocol strings from MQTT
2. **Transaction Ordering**: InfluxDB writes happen first; MongoDB writes only occur after InfluxDB success
3. **Timestamp Consistency**: Nanosecond timestamps from Line Protocol are used for both databases
4. **Idempotent Writes**: MongoDB uses upsert operations (safe for retries)
5. **Failure Isolation**: InfluxDB failures prevent MongoDB writes; MongoDB failures don't affect InfluxDB

## Components

### 1. Enhanced `mongo_injection.py`
- **Completed methods**:
  - `_get_gen_state()`: Parses Generation measurements (solar/PV)
  - `_get_load_state()`: Parses Converters measurements (loads)
  - `_get_bess_state()`: Parses BMS measurements (batteries)
  - `get_bms_update()`: Creates MongoDB update operations
  - Fixed `write()` method with proper time_delta_seconds parameter

### 2. New `data_validator.py`
- **Validates Line Protocol data** against schemas
- Checks required fields and tags
- Validates value ranges (SOC 0-100%, voltage, current bounds)
- Provides detailed error/warning messages

### 3. Enhanced `mqtt_to_influxdb.py`
- **Dual-write implementation** with error handling
- Validates data before MongoDB writes
- Logs failed writes for retry
- Configurable via environment variable

### 4. New `retry_handler.py`
- **Parses failed write logs** and retries
- Archives successfully retried entries
- Provides summary statistics
- Command-line interface for operations

## Configuration

### Environment Variables

Add to your `.env` file:

```bash
# Enable MongoDB dual writes (default: false)
ENABLE_MONGO_INJECTION=true

# MongoDB write timeout in milliseconds
MONGO_WRITE_TIMEOUT_MS=5000

# Path to log failed MongoDB writes
FAILED_WRITES_LOG_PATH=/var/log/raptor/failed_writes.log
```

### Dependencies

Updated `requirements.txt` includes:
- `pymongo==4.6.0` - MongoDB driver
- `pydantic==2.5.0` - Data validation

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the MQTT Processor

**InfluxDB only** (default):
```bash
python -m src.jobs.mqtt_to_influxdb
```

**With MongoDB dual writes**:
```bash
# Set environment variable
export ENABLE_MONGO_INJECTION=true

# Or update .env file, then run
python -m src.jobs.mqtt_to_influxdb
```

### Monitoring Failed Writes

Check failed writes summary:
```bash
python -m src.jobs.retry_handler --summary
```

Output:
```
=== Failed Writes Summary ===
Total entries: 5
Total lines: 42
Oldest failure: 2025-10-03 10:15:23
Newest failure: 2025-10-03 10:45:12
```

### Retrying Failed Writes

**Dry run** (validate without writing):
```bash
python -m src.jobs.retry_handler --dry-run
```

**Execute retry** (with 3 attempts per entry):
```bash
python -m src.jobs.retry_handler --max-retries 3
```

Output:
```
Retry Results:
  Successful: 4
  Failed: 1
```

## Validation Rules

### BMS (Battery Management System)
- **Required fields**: `Current`, `Pack_Voltage`
- **Optional fields**: `State_of_Charge`, `SOC`, `Remaining_Capacity`
- **Required tags**: `device_id`, `raptor`
- **Ranges**:
  - SOC: 0-100%
  - Pack_Voltage: 0-100V
  - Current: -200 to 200A

### Converters (Loads)
- **Required fields**: `DC_Voltage`
- **Optional fields**: `AC_input_power`, `DC_Power`, `Phase1TruePower`
- **Required tags**: `device_id`, `raptor`

### Generation (Solar/PV)
- **Required fields**: `Voltage`, `Current`
- **Required tags**: `device_id`, `raptor`

## Error Handling

### InfluxDB Write Failure
- MongoDB write is **skipped** (maintains consistency)
- Error logged with full message payload
- MQTT message marked as unprocessed

### MongoDB Write Failure
- InfluxDB write **already succeeded**
- Failure logged to `failed_writes.log`
- Data available for retry via `retry_handler.py`

### Validation Failure
- Data rejected before any database writes
- Errors logged with specific field/tag issues
- Original data preserved in failed writes log

## Rollout Strategy

### Phase 1: Deployment (Current)
```bash
# Deploy code with MongoDB injection disabled
ENABLE_MONGO_INJECTION=false
```

### Phase 2: Canary Testing
```bash
# Enable for single raptor ID (modify MQTT_TOPIC)
MQTT_TOPIC=raptors/67ad1e83853af29b1d044fe6/telemetry
ENABLE_MONGO_INJECTION=true
```

Monitor for 24 hours:
- Check InfluxDB and MongoDB for matching data
- Review validation warnings
- Verify timestamp consistency

### Phase 3: Full Rollout
```bash
# Enable for all raptors
MQTT_TOPIC=raptors/+/telemetry
ENABLE_MONGO_INJECTION=true
```

### Phase 4: Retry Accumulated Failures
```bash
# After stable operation, retry any accumulated failures
python -m src.jobs.retry_handler --summary
python -m src.jobs.retry_handler --max-retries 5
```

## Monitoring

### Key Metrics to Track

1. **Write Success Rates**
   - InfluxDB write success/failure count
   - MongoDB write success/failure count

2. **Write Latencies**
   - InfluxDB average write time
   - MongoDB average write time

3. **Validation Results**
   - Validation pass rate
   - Common validation errors

4. **Failed Write Accumulation**
   - Number of entries in failed_writes.log
   - Age of oldest failed write

### Log Locations

- **Main application**: `/var/log/raptor/crem3-mqtt-influx.log`
- **Failed writes**: `/var/log/raptor/failed_writes.log`
- **Archived retries**: `/var/log/raptor/failed_writes_archived.log`

## Testing

### Unit Test Recommendations

Create `tests/test_dual_injection.py`:

```python
def test_line_protocol_validation():
    """Test validation of Line Protocol strings"""
    pass

def test_bess_state_calculation():
    """Test battery state aggregation"""
    pass

def test_mongo_injection_write():
    """Test MongoDB write with mock data"""
    pass

def test_failed_write_logging():
    """Test failure logging mechanism"""
    pass

def test_retry_handler_parsing():
    """Test parsing of failed write logs"""
    pass
```

### Integration Test

```bash
# 1. Start with test MQTT broker
# 2. Publish test messages
# 3. Verify data in both InfluxDB and MongoDB
# 4. Compare timestamps and values
# 5. Test failure scenarios (disconnect MongoDB)
# 6. Verify failed writes logged
# 7. Test retry handler
```

## Troubleshooting

### MongoDB writes not happening
1. Check `ENABLE_MONGO_INJECTION` environment variable
2. Verify MongoDB connection in logs
3. Check for validation errors in logs

### High validation failure rate
1. Review validation errors: `grep "validation failed" /var/log/raptor/*.log`
2. Check data format from MQTT source
3. Adjust validation ranges if needed

### Failed writes accumulating
1. Check MongoDB connectivity
2. Review error patterns: `python -m src.jobs.retry_handler --summary`
3. Fix underlying issues
4. Run retry: `python -m src.jobs.retry_handler`

### Data inconsistency between databases
1. Compare timestamps in both databases
2. Check for clock drift on MQTT publisher
3. Review failed_writes.log for patterns
4. Verify Line Protocol parsing is consistent

## Future Enhancements

1. **Metrics Dashboard**: Prometheus/Grafana for monitoring
2. **Automated Retry**: Cron job for periodic retry attempts
3. **Dead Letter Queue**: Separate queue for permanently failed writes
4. **Schema Versioning**: Track validation schema changes
5. **Batch Optimization**: Tune batch sizes for optimal performance

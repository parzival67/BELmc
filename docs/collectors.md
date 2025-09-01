# Data Collectors

## Overview

BELmc uses three main collector scripts to gather data from industrial machines using different communication protocols. Each collector runs independently and stores data in the PostgreSQL database.

## 1. EMS Collector (collector_ems.py)

### Protocol
- Uses Modbus/EMS protocol via the `minimalmodbus` library
- Communicates with PLCs to collect energy consumption data

### Functionality
- Reads meter values from PLCs (voltage, current, power, energy)
- Converts raw register data to meaningful engineering units
- Determines machine status based on power consumption thresholds
- Tracks energy consumption per shift
- Stores data in PostgreSQL database using Pony ORM

### Key Features
- Configurable via JSON settings file
- Implements error handling and logging
- Handles meter resets and data spikes
- Updates shift-wise energy consumption

### Machine Status Determination
- Status 0 (OFF): No power consumption
- Status 1 (ON): Minimal power consumption (machine idle)
- Status 2 (PRODUCTION): Significant power consumption (machine in production)

## 2. LSV2 Collector (collector_lsv2.py)

### Protocol
- Uses LSV2 protocol via the `pyLSV2` library
- Designed for Heidenhain controls

### Functionality
- Monitors machine states (Running, Stopped, Alarm, etc.)
- Retrieves active program information
- Tracks part counts based on PLC memory flags
- Stores status data in PostgreSQL database

### Key Features
- Automatic reconnection handling
- Configuration via JSON settings file
- Program stack monitoring
- Part count tracking using rising edge detection

### Machine Status Determination
- Status 1 (ON): Machine is idle
- Status 2 (PRODUCTION): Program is running

## 3. OPC UA Collector (collector_opcua.py)

### Protocol
- Uses OPC UA protocol via the `opcua` library
- Standard industrial communication protocol

### Functionality
- Reads node values from OPC UA servers
- Monitors machine parameters (program status, operation mode, part count)
- Stores data in PostgreSQL database

### Key Features
- Secure connection with authentication
- Configurable node mappings via JSON settings
- Automatic reconnection handling
- Status monitoring based on program status

### Machine Status Determination
- Status 1 (ON): Machine is idle
- Status 2 (PRODUCTION): Program status indicates production

## Running Collectors

Each collector can be run independently:

```bash
python collector_ems.py
python collector_lsv2.py
python collector_opcua.py
```

## Data Flow

1. Collector connects to machine via specific protocol
2. Data is read from machine registers/nodes
3. Data is processed and converted to appropriate units
4. Machine status is determined based on collected data
5. Data is stored in PostgreSQL database
6. Live and historical records are maintained

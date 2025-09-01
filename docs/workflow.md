# System Workflow

## Overview

BELmc follows a data collection and processing workflow where independent collector scripts gather machine data, which is then processed by auxiliary engines to generate metrics and notifications.

## Data Flow Diagram

```
Industrial Machines
    ↓
[Collector Scripts]
    ↓
PostgreSQL Database
    ↓
[Auxiliary Engines]
    ↓
Processed Data & Notifications
```

## Detailed Workflow

### 1. Data Collection Phase

1. **EMS Collector**:
   - Connects to PLCs via Modbus/EMS
   - Reads energy meter registers
   - Converts raw data to engineering units
   - Determines machine status based on power consumption
   - Updates shift-wise energy data
   - Stores data in EMS database tables

2. **LSV2 Collector**:
   - Connects to Heidenhain controls via LSV2 protocol
   - Reads machine status and program information
   - Monitors PLC memory for part count events
   - Tracks program execution status
   - Stores data in production database tables

3. **OPC UA Collector**:
   - Connects to OPC UA servers
   - Reads machine parameter nodes
   - Monitors program status and operation mode
   - Tracks part production
   - Stores data in production database tables

### 2. Data Storage Phase

All collectors:
- Store live data in `MachineRawLive` or protocol-specific live tables
- Store historical data in corresponding history tables
- Update shift summary information
- Handle machine disconnections and status changes

### 3. Data Processing Phase

1. **OEE Engine**:
   - Continuously calculates OEE metrics
   - Retrieves shift and schedule data
   - Computes availability, performance, and quality
   - Updates ShiftSummary table with metrics

2. **Notification Engine**:
   - Daily checks for calibration due dates
   - Creates notifications for overdue items
   - Prevents duplicate notifications
   - Stores notifications in log tables

## Database Structure

### Main Tables

- **MachineRaw/MachineRawLive**: Production data from LSV2 and OPC UA collectors
- **MachineEMSHistory/MachineEMSLive**: Energy data from EMS collector
- **ShiftSummary**: OEE metrics and shift information
- **ShiftwiseEnergyHistory/ShiftwiseEnergyLive**: Shift-wise energy consumption
- **MachineCalibrationLog/InstrumentCalibrationLog**: Calibration notifications

## System Timing

- **Collectors**: Run continuously with 1-5 second intervals between reads
- **OEE Engine**: Continuous calculation loop
- **Notification Engine**: Daily checks
- **Database Updates**: Real-time as data is collected

## Error Handling

- Automatic reconnection for all collectors
- Graceful handling of machine disconnections
- Data spike detection and filtering
- Error logging for troubleshooting
- Database transaction management

## Data Consistency

- Live records always reflect current machine state
- Historical records maintain complete audit trail
- Shift data is properly rolled over at shift changes
- Part counts are tracked with rising edge detection
- Energy data includes spike filtering

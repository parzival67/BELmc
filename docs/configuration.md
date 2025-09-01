# Configuration

## Overview

BELmc requires several configuration files and environment variables to operate correctly. This document explains the configuration structure and required settings.

## Environment Variables

The system uses a `.env` file for configuration. Key variables include:

- **Database Connection**:
  - `DB_HOST`: Database server hostname
  - `DB_PORT`: Database server port
  - `DB_NAME`: Database name
  - `DB_USER`: Database username
  - `DB_PASSWORD`: Database password

- **Security**:
  - `SECRET_KEY`: Application secret key
  - `ALGORITHM`: JWT algorithm
  - `ACCESS_TOKEN_EXPIRE_MINUTES`: Token expiration time

## Collector Configuration Files

### EMS Collector (config/ems_settings.json)

Contains register mappings for each meter:

```json
{
  "1": {
    "PHASE_A VOLTAGE": [1103, 4],
    "ACTIVE ENERGY 3P DELIVERED": [1223, 4]
  }
}
```

Each meter ID maps to parameter names and their register addresses.

### LSV2 Collector (config/lsv2_settings.json)

Contains machine connection details:

```json
{
  "lsv2": [
    {
      "machine_id": 1,
      "ip_address": "192.168.1.10"
    }
  ]
}
```

### OPC UA Collector (config/opcua_settings.json)

Contains server connection details:

```json
{
  "opcua": [
    {
      "machine_id": 14,
      "ip_address": "192.168.1.20",
      "port": 4840,
      "username": "user",
      "password": "password"
    }
  ]
}
```

## Machine Thresholds

Defined in collector_ems.py, these determine machine status based on power consumption:

```python
machine_thresholds = {
    1: 4.5,  # DMU60MB
    2: 1.3,  # DMU50
    # ... more machines
}
```

## Database Models

The system uses Pony ORM with the following key models:

- **MachineRaw**: Production data records
- **MachineEMSLive/History**: Energy data records
- **ShiftSummary**: OEE metrics by shift
- **ShiftwiseEnergyLive/History**: Energy consumption by shift
- **Calibration logs**: Notification records

## Security Considerations

- Database credentials stored in environment variables
- OPC UA connections can use authentication
- Sensitive configuration not committed to version control
- Secure connection strings for database connections

# System Requirements

## Overview

BELmc requires specific software dependencies and hardware configurations to operate correctly.

## Hardware Requirements

### Minimum Specifications
- **CPU**: 2 GHz dual-core processor
- **RAM**: 4 GB RAM
- **Storage**: 10 GB available disk space
- **Network**: Ethernet connection for machine communication

### Recommended Specifications
- **CPU**: 3 GHz quad-core processor
- **RAM**: 8 GB RAM
- **Storage**: 50 GB available disk space (SSD recommended)
- **Network**: Gigabit Ethernet connection

## Software Requirements

### Operating System
- **Linux**: Ubuntu 20.04 LTS or newer
- **Windows**: Windows 10 or Windows Server 2019
- **macOS**: macOS 11.0 or newer

### Python Environment
- **Python Version**: 3.8 or newer
- **Package Manager**: pip 21.0 or newer
- **Virtual Environment**: venv or conda recommended

## Python Dependencies

Key dependencies are listed in `requirements.txt`:

### Core Dependencies
- **fastapi**: Web framework for API endpoints
- **pony**: Object-relational mapper for database access
- **psycopg2-binary**: PostgreSQL database adapter
- **python-dotenv**: Environment variable management

### Protocol Libraries
- **minimalmodbus**: Modbus/EMS communication
- **pyLSV2**: LSV2 protocol for Heidenhain controls
- **opcua**: OPC UA communication

### Utility Libraries
- **pydantic**: Data validation and settings management
- **pandas**: Data processing and analysis
- **passlib**: Password hashing utilities

### Security Libraries
- **python-jose**: JWT token handling
- **bcrypt**: Password hashing
- **cryptography**: Cryptographic operations

## Database Requirements

### PostgreSQL
- **Version**: PostgreSQL 12 or newer
- **Extensions**: Required PostgreSQL extensions
- **Storage**: 20 GB minimum for database storage
- **Backup**: Regular backup schedule recommended

### Database Configuration
- **Connection Pooling**: PgBouncer recommended for production
- **Authentication**: Strong password authentication
- **SSL**: SSL connections recommended

## Network Requirements

### Machine Communication
- **Modbus/EMS**: Serial or TCP/IP connection to PLCs
- **LSV2**: Direct Ethernet connection to Heidenhain controls
- **OPC UA**: Network access to OPC UA servers

### Ports
- **Database**: 5432 (PostgreSQL)
- **OPC UA**: 4840 (standard OPC UA port)
- **Application**: 8000 (default FastAPI port)

## Installation Steps

1. Install Python 3.8 or newer
2. Create virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Set up PostgreSQL database
5. Configure environment variables
6. Set up collector configuration files
7. Test connections to machines

## Testing Environment

For development and testing:
- **Docker**: Docker containers for database and application
- **Mock Data**: Sample data for testing without machines
- **Unit Tests**: pytest framework for component testing

# BELmc Manufacturing Execution System

## Overview

BELmc is a Manufacturing Execution System (MES) designed to monitor and collect data from industrial machines using various communication protocols. The system collects real-time data from machines and stores it in a PostgreSQL database for analysis and reporting.

## Main Components

The BELmc system consists of several key components:

1. **Data Collectors** - Scripts that connect to machines via different protocols:
   - EMS Collector (Modbus/EMS protocol)
   - LSV2 Collector (LSV2 protocol for Heidenhain controls)
   - OPC UA Collector (OPC UA protocol)

2. **Auxiliary Engines** - Processing components that work with collected data:
   - OEE Engine (calculates Overall Equipment Effectiveness)
   - Notification Engine (handles calibration notifications)

3. **Shared Utilities** - Common functionality used across components:
   - Database management
   - Shift management
   - Data processing utilities

4. **Database** - PostgreSQL database with Pony ORM for data storage

## System Architecture

The system follows a modular architecture where each collector script runs independently and connects to machines via their specific protocols. Data is stored in a centralized PostgreSQL database, and the auxiliary engines process this data for metrics and notifications.

## Technologies Used

- **Language**: Python 3.x
- **Database**: PostgreSQL with Pony ORM
- **Protocols**: Modbus/EMS, LSV2, OPC UA
- **Dependencies**: See requirements.txt for full list

## Getting Started

1. Install dependencies from requirements.txt
2. Configure database connection in environment variables
3. Set up configuration files for each collector
4. Run individual collector scripts as needed
5. Run auxiliary engines for processing

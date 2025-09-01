# Auxiliary Engines

## Overview

BELmc includes auxiliary engines that process the collected data to generate metrics and notifications.

## 1. OEE Engine (oee_engine.py)

### Purpose
Calculates Overall Equipment Effectiveness (OEE) metrics for production machines.

### Functionality
- Calculates three key metrics:
  - **Availability**: Ratio of actual production time to planned production time
  - **Performance**: Ratio of actual output to expected output
  - **Quality**: Ratio of good parts to total parts produced
- Computes overall OEE as the product of Availability × Performance × Quality
- Uses production data and shift information
- Stores OEE results in database

### Key Features
- Continuous calculation loop
- Shift-based calculations
- Integration with production scheduling data
- Stores metrics in ShiftSummary database table

### Data Sources
- Machine status data from collectors
- Production log data
- Shift information
- Scheduled job quantities

## 2. Notification Engine (notification_engine.py)

### Purpose
Manages system notifications for maintenance and calibration schedules.

### Functionality
- Monitors instrument calibration schedules
- Checks for overdue machine calibrations
- Creates notifications for overdue calibrations
- Prevents duplicate notifications
- Stores notifications in database

### Key Features
- Daily checks for overdue calibrations
- Separate tracking for machine and instrument calibrations
- Automatic notification creation
- Duplicate prevention

### Data Sources
- Machine calibration due dates
- Instrument calibration schedules
- Existing notification records

## Running Engines

Each engine can be run independently:

```bash
python oee_engine.py
python notification_engine.py
```

## Processing Flow

### OEE Engine
1. Retrieves current shift information
2. Gets machine schedule data for the shift
3. Calculates time spent in each machine state (OFF, ON, PRODUCTION)
4. Computes availability, performance, and quality metrics
5. Calculates overall OEE
6. Stores results in ShiftSummary table

### Notification Engine
1. Checks for overdue machine calibrations
2. Checks for overdue instrument calibrations
3. Creates notifications for any overdue items
4. Prevents duplicate notifications
5. Stores notifications in database

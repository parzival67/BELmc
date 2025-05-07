import os
import minimalmodbus
import serial
import struct
import time as tt
import json
from datetime import datetime, timedelta, time

from dotenv import load_dotenv
from pony.orm import db_session
from app.database.connection import connect_to_db
from app.models.ems import MachineEMSLive, MachineEMSHistory, ShiftwiseEnergyLive, ShiftwiseEnergyHistory

# Field mapping to match EMS models
EMS_FIELDS = [
    "phase_a_voltage", "phase_b_voltage", "phase_c_voltage", "avg_phase_voltage",
    "line_ab_voltage", "line_bc_voltage", "line_ca_voltage", "avg_line_voltage",
    "frequency", "total_instantaneous_power",
    "phase_a_current", "phase_b_current", "phase_c_current", "avg_three_phase_current",
    "power_factor", "active_energy_delivered"
]

# Machine status thresholds for each machine (power threshold between ON and PRODUCTION states)
# This can be loaded from a config file or database in a production system
machine_thresholds = {
    1: 2.3,
    2: 1.3,
    3: 4.3,
    4: 3.1,
    5: 0,
    6: 2.3,
    7: 2.5,
    8: 1,
    9: 1,
    10: 1,
    11: 1.8,
    12: 1.8,
    13: 2,
    14: 3,
}


class DeltaPLCReader:
    def __init__(self, port='', slave_address=1, register_file="config/ems_settings.json"):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 9600
        self.instrument.serial.bytesize = 7
        self.instrument.serial.parity = serial.PARITY_EVEN
        self.instrument.serial.stopbits = 2
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_ASCII
        self.instrument.clear_buffers_before_each_transaction = True

        # Initialize shift time boundaries
        self.shift0 = datetime.combine(self.get_current_time().date(), time(8, 30))
        self.shift1 = datetime.combine(self.get_current_time().date(), time(17, 0))
        self.shift2 = datetime.combine(self.get_current_time().date() + timedelta(days=1), time(0, 30))

        # Load meter register mappings from JSON file
        with open(register_file, 'r') as f:
            self.meters = json.load(f)

    def get_current_time(self):
        return datetime.now() + timedelta(hours=5, minutes=30)

    def convert_d_address(self, d_number):
        return d_number + 400001 - 1

    def read_multiple_d_registers(self, start_d_number, num_registers):
        try:
            modbus_address = self.convert_d_address(start_d_number)
            values = self.instrument.read_registers(modbus_address - 400001, num_registers)
            return values
        except Exception as e:
            print(f"Error reading registers: {str(e)}")
            return None

    def convert_raw_bytes_to_float(self, reg1, reg2, reg3, reg4):
        try:
            bytes_val = struct.pack('BBBB', reg2, reg1, reg4, reg3)
            return struct.unpack('<f', bytes_val)[0]
        except Exception as e:
            print(f"Float conversion error: {e}")
            return None

    def read_meter_values(self, meter_id):
        results = []
        if str(meter_id) not in self.meters:
            print(f"Meter ID {meter_id} not found")
            return results

        meter_data = self.meters[str(meter_id)]
        for name, (start_d_number, num_registers) in meter_data.items():
            values = self.read_multiple_d_registers(start_d_number, num_registers)
            if values and len(values) >= 4:
                if name == "ACTIVE ENERGY 3P DELIVERED":
                    bytes_val = struct.pack('BBBB', values[1], values[0], values[3], values[2])
                    float_value = round(struct.unpack('<i', bytes_val)[0] / 1000, 4)
                else:
                    float_value = round(self.convert_raw_bytes_to_float(*values[:4]), 4)

                results.append((name, float_value))
            else:
                results.append((name, None))
        return results

    def check_shift_update(self):
        """Check if shifts need to be reset based on current time"""
        with db_session:
            temp_time = self.get_current_time()
            shiftwise_data = ShiftwiseEnergyLive.select()[:]

            # Check if any records are from previous days
            temp_bool = False
            for record in shiftwise_data:
                if record.timestamp.date() < temp_time.date():
                    temp_bool = True
                    break

            # Reset shifts if it's a new day or any record is from a previous day
            if temp_time >= self.shift0 + timedelta(days=1) or temp_bool:
                for record in shiftwise_data:
                    # Save history record before resetting
                    ShiftwiseEnergyHistory(
                        timestamp=self.shift0,
                        machine_id=record.machine_id,
                        first_shift=record.first_shift,
                        second_shift=record.second_shift,
                        third_shift=record.third_shift,
                        total_energy=record.total_energy
                    )

                    # Reset the live record
                    record.first_shift = 0
                    record.second_shift = 0
                    record.third_shift = 0
                    record.total_energy = 0
                    record.timestamp = self.get_current_time()

                # Update shift boundaries for the new day
                self.shift0 = datetime.combine(self.get_current_time().date(), time(8, 30))
                self.shift1 = datetime.combine(self.get_current_time(), time(17, 0))
                self.shift2 = datetime.combine(self.get_current_time().date() + timedelta(days=1), time(0, 30))

                print(f"Shift Reset Successful at {self.get_current_time()}")

    def update_shiftwise_energy(self, machine_id, energy_value):
        """Update shiftwiser energy based on the current time and energy consumption"""
        with db_session:
            temp_time = self.get_current_time()

            # Get or create shiftwise energy record
            shiftwise_data = ShiftwiseEnergyLive.get(machine_id=machine_id)
            if not shiftwise_data:
                shiftwise_data = ShiftwiseEnergyLive(
                    machine_id=machine_id,
                    timestamp=temp_time,
                    first_shift=0,
                    second_shift=0,
                    third_shift=0,
                    total_energy=0
                )

            # Update the appropriate shift based on current time
            if temp_time >= self.shift0 and temp_time < self.shift1:
                shiftwise_data.first_shift += round(energy_value, 4)
            elif temp_time >= self.shift1 and temp_time < self.shift2:
                shiftwise_data.second_shift += round(energy_value, 4)
            else:  # Third shift
                shiftwise_data.third_shift += round(energy_value, 4)

            # Update total energy and timestamp
            shiftwise_data.total_energy = round(
                shiftwise_data.first_shift + shiftwise_data.second_shift + shiftwise_data.third_shift, 4
            )
            shiftwise_data.timestamp = temp_time

    @db_session
    def save_to_db(self, meter_id, readings):
        # Check if shift needs to be updated
        self.check_shift_update()

        timestamp = self.get_current_time()
        value_map = {
            "PHASE_A VOLTAGE": "phase_a_voltage",
            "PHASE_B VOLTAGE": "phase_b_voltage",
            "PHASE_C VOLTAGE": "phase_c_voltage",
            "AVERAGE PHASE VOLTAGE": "avg_phase_voltage",
            "A-B LINE VOLTAGE": "line_ab_voltage",
            "B-C LINE VOLTAGE": "line_bc_voltage",
            "C-A LINE VOLTAGE": "line_ca_voltage",
            "AVERAGE LINE VOLTAGE": "avg_line_voltage",
            "FREQUENCY": "frequency",
            "TOTAL INSTANTANEOUS ACTIVE POWER": "total_instantaneous_power",
            "PHASE_A CURRENT": "phase_a_current",
            "PHASE_B CURRENT": "phase_b_current",
            "PHASE_C CURRENT": "phase_c_current",
            "THREE-PHASE AVERAGE CURRENT": "avg_three_phase_current",
            "TOTAL POWER FACTOR": "power_factor",
            "ACTIVE ENERGY 3P DELIVERED": "active_energy_delivered"
        }

        data = {value_map[k]: v for k, v in readings if k in value_map}

        # Default threshold if machine ID not in dictionary
        default_threshold = 5.0

        # Determine machine status based on power consumption
        machine_status = 0  # Default: OFF

        if "total_instantaneous_power" in data and data["total_instantaneous_power"] is not None:
            power = data["total_instantaneous_power"]
            threshold = machine_thresholds.get(meter_id, default_threshold)

            if power == 0 or power < 0.1:  # Using a small threshold to account for measurement noise
                machine_status = 0  # OFF
            elif power < threshold:
                machine_status = 1  # ON
            else:
                machine_status = 2  # PRODUCTION

            # Calculate energy in kWh: power (kW) * interval (seconds) / 3600 seconds/hour
            # Assuming readings are taken at the interval specified in read_continuously
            interval_seconds = 1.0  # Default interval from main function
            energy_value = round(power * interval_seconds / 3600, 4)

            # Update shiftwise energy data
            self.update_shiftwise_energy(meter_id, energy_value)

        # Add machine_status to data dictionary
        # data["status"] = machine_status

        # Create history record
        MachineEMSHistory(machine_id=meter_id, timestamp=timestamp, **data)

        # Update or create live record
        live = MachineEMSLive.get(machine_id=meter_id)
        status_changed = False

        if live:
            if live.status != machine_status:
                status_changed = True
            live.timestamp = timestamp
            live.status = machine_status
            for key, value in data.items():
                setattr(live, key, value)
        else:
            MachineEMSLive(machine_id=meter_id, timestamp=timestamp, **data)
            status_changed = True  # New record = status change

        if status_changed:
            print(f"MACHINE {meter_id} >> {machine_status} | (0=OFF, 1=ON, 2=PRODUCTION)")

    def read_continuously(self, interval=5.0, meters_to_read=None):
        if meters_to_read is None:
            meters_to_read = list(map(int, self.meters.keys()))

        try:
            while True:
                for meter_id in meters_to_read:
                    try:
                        readings = self.read_meter_values(meter_id)
                        if any(val is not None for _, val in readings):
                            self.save_to_db(meter_id, readings)
                            print(f"Saved meter {meter_id} readings at {self.get_current_time()}")

                        else:
                            print(f"No valid data for meter {meter_id}")
                    except Exception as e:
                        print(f"Error processing meter {meter_id}: {e}")
                tt.sleep(interval)
        except KeyboardInterrupt:
            print("Monitoring stopped by user")


def main():
    load_dotenv()
    connect_to_db()

    plc = DeltaPLCReader(port="/dev/ttyUSB0")
    # plc = DeltaPLCReader(port="COM5")
    plc.read_continuously(interval=1.0, meters_to_read=[i for i in range(1, 15)])


if __name__ == '__main__':
    main()

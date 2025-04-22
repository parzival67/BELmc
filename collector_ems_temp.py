import minimalmodbus
import serial
import struct
import time
import json
from datetime import datetime
from pony.orm import db_session
from app.database.connection import connect_to_db
from app.models.ems import MachineEMSLive, MachineEMSHistory


# Field mapping to match EMS models
EMS_FIELDS = [
    "phase_a_voltage", "phase_b_voltage", "phase_c_voltage", "avg_phase_voltage",
    "line_ab_voltage", "line_bc_voltage", "line_ca_voltage", "avg_line_voltage",
    "frequency", "total_instantaneous_power",
    "phase_a_current", "phase_b_current", "phase_c_current", "avg_three_phase_current",
    "power_factor", "active_energy_delivered"
]


class DeltaPLCReader:
    def __init__(self, port='COM5', slave_address=1, register_file="config/ems_settings.json"):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 9600
        self.instrument.serial.bytesize = 7
        self.instrument.serial.parity = serial.PARITY_EVEN
        self.instrument.serial.stopbits = 2
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_ASCII
        self.instrument.clear_buffers_before_each_transaction = True

        # Load meter register mappings from JSON file
        with open(register_file, 'r') as f:
            self.meters = json.load(f)

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
                    float_value = values[0] + (values[1] / 1000.0)
                else:
                    float_value = self.convert_raw_bytes_to_float(*values[:4])
                results.append((name, float_value))
            else:
                results.append((name, None))
        return results

    @db_session
    def save_to_db(self, meter_id, readings):
        timestamp = datetime.now()
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

        MachineEMSHistory(machine_id=meter_id, timestamp=timestamp, **data)

        live = MachineEMSLive.get(machine_id=meter_id)
        if live:
            live.timestamp = timestamp
            for key, value in data.items():
                setattr(live, key, value)
        else:
            MachineEMSLive(machine_id=meter_id, timestamp=timestamp, **data)

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
                            print(f"Saved meter {meter_id} readings at {datetime.now()}")
                        else:
                            print(f"No valid data for meter {meter_id}")
                    except Exception as e:
                        print(f"Error processing meter {meter_id}: {e}")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Monitoring stopped by user")


def main():
    connect_to_db()

    plc = DeltaPLCReader(port='COM7')
    plc.read_continuously(interval=5.0, meters_to_read=[1])


if __name__ == '__main__':
    main()

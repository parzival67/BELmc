import os
import minimalmodbus
import serial
import struct
import time as tt
import json
from datetime import datetime, timedelta, time

from dotenv import load_dotenv
from pony.orm import db_session, commit, desc
from app.database.connection import connect_to_db
from app.models.ems import (
    MachineEMSLive,
    MachineEMSHistory,
    ShiftwiseEnergyLive,
    ShiftwiseEnergyHistory,
    EMSMachineStatusHistory,
)
from app.models.production import MachineRaw, MachineRawLive
from utils import ShiftManager, DatabaseManager

# Field mapping to match EMS models
EMS_FIELDS = [
    "phase_a_voltage", "phase_b_voltage", "phase_c_voltage", "avg_phase_voltage",
    "line_ab_voltage", "line_bc_voltage", "line_ca_voltage", "avg_line_voltage",
    "frequency", "total_instantaneous_power",
    "phase_a_current", "phase_b_current", "phase_c_current", "avg_three_phase_current",
    "power_factor", "active_energy_delivered"
]

# Machine status thresholds
machine_thresholds = {
    1: 4.5,
    2: 1.3,
    3: 3.8,
    4: 2.9,
    5: 2.5,
    6: 2.3,
    7: 2.5,
    8: 1,
    9: 1,
    10: 1,
    11: 1,
    12: 1.8,
    13: 2,
    14: 10,
}

MAX_DELTA_ENERGY = 10.0


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

        self.last_energy_values = {}

        self.shift0 = datetime.combine(self.get_current_time().date(), time(6, 0))
        self.shift1 = datetime.combine(self.get_current_time().date(), time(14, 0))
        self.shift2 = datetime.combine(self.get_current_time().date(), time(22, 0))

        with open(register_file, 'r') as f:
            self.meters = json.load(f)

    def get_current_time(self):
        return datetime.now() + timedelta(hours=5, minutes=30)

    def convert_d_address(self, d_number):
        return d_number + 400001 - 1

    def read_multiple_d_registers(self, meter_id, start_d_number, num_registers):
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
            values = self.read_multiple_d_registers(meter_id, start_d_number, num_registers)
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
        return results

    def get_current_shift(self, temp_time):
        today = temp_time.date()
        shift0_start = datetime.combine(today, time(6, 0))
        shift1_start = datetime.combine(today, time(14, 0))
        shift2_start = datetime.combine(today, time(22, 0))

        if shift0_start <= temp_time < shift1_start:
            return 0
        elif shift1_start <= temp_time < shift2_start:
            return 1
        elif temp_time >= shift2_start or temp_time < shift0_start:
            return 2

    def check_shift_update(self):
        with db_session:
            temp_time = self.get_current_time()
            shiftwise_data = ShiftwiseEnergyLive.select()[:]
            temp_bool = any(record.timestamp.date() < temp_time.date() for record in shiftwise_data)

            if temp_time >= self.shift0 + timedelta(days=1) or temp_bool:
                # Roll over shiftwise records to history
                for record in ShiftwiseEnergyLive.select()[:]:
                    ShiftwiseEnergyHistory(
                        timestamp=self.shift0,
                        machine_id=record.machine_id,
                        first_shift=record.first_shift,
                        second_shift=record.second_shift,
                        third_shift=record.third_shift,
                        total_energy=record.total_energy
                    )
                    record.first_shift = 0
                    record.second_shift = 0
                    record.third_shift = 0
                    record.total_energy = 0
                    record.timestamp = temp_time

                self.shift0 = datetime.combine(temp_time.date(), time(6, 0))
                self.shift1 = datetime.combine(temp_time.date(), time(14, 0))
                self.shift2 = datetime.combine(temp_time.date(), time(22, 0))

                commit()
                print(f"Shift Reset Successful at {temp_time}")

    @db_session
    def update_shiftwise_energy(self, machine_id, delta_energy):
        temp_time = self.get_current_time()
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

        shift = self.get_current_shift(temp_time)
        if shift == 0:
            shiftwise_data.first_shift += delta_energy
            shiftwise_data.first_shift = round(shiftwise_data.first_shift, 4)
        elif shift == 1:
            shiftwise_data.second_shift += delta_energy
            shiftwise_data.first_shift = round(shiftwise_data.first_shift, 4)
        else:
            shiftwise_data.third_shift += delta_energy
            shiftwise_data.first_shift = round(shiftwise_data.first_shift, 4)

        shiftwise_data.total_energy = round(
            shiftwise_data.first_shift + shiftwise_data.second_shift + shiftwise_data.third_shift, 4
        )
        shiftwise_data.timestamp = temp_time
        commit()

    @db_session
    def save_to_db(self, meter_id, readings):
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

        default_threshold = 5.0
        machine_status = 0

        if "total_instantaneous_power" in data and data["total_instantaneous_power"] is not None:
            power = data["total_instantaneous_power"]
            threshold = machine_thresholds.get(meter_id, default_threshold)

            if abs(power) > threshold:
                machine_status = 2
            elif data.get("frequency", 0) > 0:
                machine_status = 1
            else:
                machine_status = 0

            if "active_energy_delivered" in data and data["active_energy_delivered"] is not None:
                current_energy = data["active_energy_delivered"]
                last_energy = self.last_energy_values.get(meter_id)
                if last_energy is not None:
                    delta_energy = round(current_energy - last_energy, 4)
                    if delta_energy < 0:
                        print(f"[WARN] Meter reset or rollover detected for machine {meter_id}")
                    elif delta_energy > MAX_DELTA_ENERGY:
                        print(f"[SPIKE] Ignoring suspicious spike: Δ{delta_energy} kWh for machine {meter_id}")
                    else:
                        self.update_shiftwise_energy(meter_id, delta_energy)
                self.last_energy_values[meter_id] = current_energy

        MachineEMSHistory(machine_id=meter_id, timestamp=timestamp, **data)

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
            MachineEMSLive(machine_id=meter_id, timestamp=timestamp, status=machine_status, **data)
            status_changed = True

        if meter_id not in [1, 2, 3, 5]:
            active_signal = MachineRawLive.get(machine_id=meter_id)
            if active_signal:
                active_signal.timestamp = timestamp
                active_signal.op_mode = -1
                active_signal.prog_status = -1
                active_signal.status = machine_status
                active_signal.part_count = 0
                active_signal.selected_program = ''
                active_signal.active_program = ''
            else:
                MachineRawLive(
                    timestamp=timestamp,
                    machine_id=meter_id,
                    op_mode=-1,
                    prog_status=-1,
                    status=machine_status,
                    part_count=0,
                    selected_program='',
                    active_program=''
                )
            ShiftManager.manage_shift_summary(timestamp, meter_id)

        if status_changed:
            print(f"MACHINE {meter_id} >> {machine_status} | (0=OFF, 1=ON, 2=PRODUCTION)")
            EMSMachineStatusHistory(machine_id=meter_id, status=machine_status, timestamp=timestamp)
            if meter_id not in [1, 2, 3, 5]:
                MachineRaw(
                    timestamp=timestamp,
                    machine_id=meter_id,
                    op_mode=-1,
                    status=machine_status
                )
        # print(f"Database Saved {meter_id}")
        commit()

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
                            # print(f"Saved meter {meter_id} readings at {self.get_current_time()}")
                        else:
                            # print(f"No valid data for meter {meter_id}")
                            if meter_id not in [1, 2, 3, 5]:
                                DatabaseManager.handle_disconnection(meter_id)
                    except Exception as e:
                        print(f"Error processing meter {meter_id}: {e}")
                tt.sleep(interval)
        except KeyboardInterrupt:
            print("Monitoring stopped by user")


def main():
    load_dotenv()
    connect_to_db()
    plc = DeltaPLCReader(port="/dev/ttyUSB0")
    plc.read_continuously(interval=5.0, meters_to_read=[i for i in range(1, 15)])


if __name__ == '__main__':
    main()

from datetime import datetime
from pony.orm import Database, Required, Optional, Set
from ..database.connection import db


class MachineEMSHistory(db.Entity):
    _table_ = ('ems', 'machine_ems_history')

    machine_id = Required(int)
    timestamp = Required(datetime, default=datetime.now)
    phase_a_voltage = Optional(float)
    phase_b_voltage = Optional(float)
    phase_c_voltage = Optional(float)
    avg_phase_voltage = Optional(float)
    line_ab_voltage = Optional(float)
    line_bc_voltage = Optional(float)
    line_ca_voltage = Optional(float)
    avg_line_voltage = Optional(float)
    phase_a_current = Optional(float)
    phase_b_current = Optional(float)
    phase_c_current = Optional(float)
    avg_three_phase_current = Optional(float)
    power_factor = Optional(float)
    frequency = Optional(float)
    total_instantaneous_power = Optional(float)
    active_energy_delivered = Optional(float)


class MachineEMSLive(db.Entity):
    _table_ = ('ems', 'machine_ems_live')

    machine_id = Required(int, unique=True)
    timestamp = Required(datetime, default=datetime.now)
    status = Optional(int)
    phase_a_voltage = Optional(float)
    phase_b_voltage = Optional(float)
    phase_c_voltage = Optional(float)
    avg_phase_voltage = Optional(float)
    line_ab_voltage = Optional(float)
    line_bc_voltage = Optional(float)
    line_ca_voltage = Optional(float)
    avg_line_voltage = Optional(float)
    phase_a_current = Optional(float)
    phase_b_current = Optional(float)
    phase_c_current = Optional(float)
    avg_three_phase_current = Optional(float)
    power_factor = Optional(float)
    frequency = Optional(float)
    total_instantaneous_power = Optional(float)
    active_energy_delivered = Optional(float)


class ShiftwiseEnergyLive(db.Entity):
    _table_ = ('ems', 'shiftwise_energy_live')

    timestamp = Required(datetime, default=datetime.now)
    first_shift = Required(float)
    second_shift = Required(float)
    third_shift = Required(float)
    total_energy = Required(float)
    machine_id = Required(int)


class ShiftwiseEnergyHistory(db.Entity):
    _table_ = ('ems', 'shiftwise_energy_history')

    timestamp = Required(datetime, default=datetime.now)
    first_shift = Required(float)
    second_shift = Required(float)
    third_shift = Required(float)
    total_energy = Required(float)
    machine_id = Required(int)


class EMSMachineStatusHistory(db.Entity):
    _table_ = ('ems', 'ems_machine_status_history')

    timestamp = Required(datetime, default=datetime.now)
    machine_id = Required(int, unique=True)
    status = Optional(int)

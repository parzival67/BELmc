from datetime import datetime
from pony.orm import db_session, select, commit
from app.database.connection import connect_to_db
from app.models import Machine
from app.models.logs import MachineCalibrationLog, InstrumentCalibrationLog
from app.models.inventoryv1 import CalibrationSchedule
import time as tt


@db_session
def check_machine_calibrations():
    overdue_machines = select(
        m for m in Machine if m.calibration_due_date is not None
        and m.calibration_due_date.date() <= datetime.now().date())[:]

    for machine in overdue_machines:
        existing_notification = select(
            n for n in MachineCalibrationLog
            if n.machine_id == machine and n.calibration_due_date == machine.calibration_due_date.date())[:1]

        if not existing_notification:
            MachineCalibrationLog(
                machine_id=machine,
                calibration_due_date=machine.calibration_due_date.date()
            )
            print(f"{datetime.now()} | [MACHINE] Notification created for Machine ID {machine.id}")


@db_session
def check_instrument_calibrations():
    overdue_instruments = select(
        c for c in CalibrationSchedule if c.next_calibration.date() <= datetime.now().date())[:]

    for schedule in overdue_instruments:
        existing_notification = select(
            n for n in InstrumentCalibrationLog
            if n.instrument_id == schedule and n.calibration_due_date == schedule.next_calibration.date())[:1]

        if not existing_notification:
            InstrumentCalibrationLog(
                instrument_id=schedule,
                calibration_due_date=schedule.next_calibration.date()
            )
            print(f"{datetime.now()} | [INSTRUMENT] Notification created for Instrument ID {schedule.id}")


def main():
    connect_to_db()
    print('Successfully connected to database!')

    while True:
        try:
            with db_session():
                check_machine_calibrations()
                check_instrument_calibrations()

                commit()

        except Exception as e:
            print(f"Error during calibration check: {e}")

        tt.sleep(1)


if __name__ == '__main__':
    main()

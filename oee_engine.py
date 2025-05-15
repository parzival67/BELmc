from datetime import datetime, date, timedelta, time

from pony.orm import db_session, select, commit

from app.database.connection import connect_to_db
from app.models import ProductionLog
from app.models.production import ShiftSummary, MachineRawLive
from utils import ShiftManager, get_machine_schedule_quantities

import time as tt

try:
    connect_to_db()
    print("Database connected successfully")

    while True:
        try:
            with db_session():
                timestamp = datetime.now() + timedelta(hours=5, minutes=30)
                # timestamp = datetime.now()
                shift_id, shift_start_time, shift_end_time = ShiftManager.get_current_shift(timestamp)

                if shift_start_time < shift_end_time:
                    shift_start_dt = datetime.combine(timestamp.date(), shift_start_time)
                    shift_end_dt = datetime.combine(timestamp.date() + timedelta(days=1), shift_end_time)
                else:
                    shift_start_dt = datetime.combine(timestamp.date(), shift_start_time)
                    shift_end_dt = datetime.combine(timestamp.date(), shift_end_time)

                machine_raw_live = select(i for i in MachineRawLive)
                for i in machine_raw_live:
                    machine_schedule_0 = get_machine_schedule_quantities(i.machine_id, shift_start_dt,
                                                                         shift_end_dt)
                    if machine_schedule_0:
                        i.scheduled_job = machine_schedule_0[0].operation_id
                    else:
                        i.scheduled_job = None

                shift_summaries = select(s for s in ShiftSummary
                                         if s.shift == shift_id and s.timestamp == shift_start_dt)

                for s in shift_summaries:
                    shift_summary = ShiftSummary[s.id]
                    planned_production_time = max(
                        datetime.combine(datetime.today(), shift_summary.idle_time) - timedelta(hours=1),
                        datetime.combine(datetime.today(), time(0))) - datetime.combine(datetime.today(),
                                                                                        time(0)) + timedelta(
                        hours=shift_summary.production_time.hour, minutes=shift_summary.production_time.minute,
                        seconds=shift_summary.production_time.second)

                    actual_production_time = timedelta(
                        hours=shift_summary.production_time.hour, minutes=shift_summary.production_time.minute,
                        seconds=shift_summary.production_time.second)

                    machine_schedule_1 = get_machine_schedule_quantities(shift_summary.machine_id, shift_start_dt,
                                                                         shift_end_dt)
                    machine_schedule_2 = get_machine_schedule_quantities(shift_summary.machine_id, shift_end_dt,
                                                                         shift_end_dt + timedelta(days=1))

                    expected_quantity = 0
                    if machine_schedule_1:
                        if machine_schedule_2:
                            expected_quantity = machine_schedule_1[0].remaining_quantity - machine_schedule_2[
                                0].remaining_quantity
                        else:
                            expected_quantity = machine_schedule_1[0].remaining_quantity

                    production_log = select(i for i in ProductionLog
                                            if i.machine_id == shift_summary.machine_id
                                            and i.start_time >= shift_start_dt and i.end_time <= shift_end_dt)

                    actual_quantity = sum(
                        [i.quantity_completed for i in production_log if i.operation == machine_schedule_1[0].operation_id])

                    total_parts = actual_quantity
                    good_parts = actual_quantity - sum(
                        [i.quantity_rejected for i in production_log if i.operation == machine_schedule_1[0].operation_id])

                    if planned_production_time:
                        availability = actual_production_time / planned_production_time
                        availability = min(1, availability)
                    else:
                        availability = 0

                    if expected_quantity:
                        performance = actual_quantity / expected_quantity
                        performance = min(1, performance)
                    else:
                        performance = 0

                    if total_parts:
                        quality = good_parts / total_parts
                        quality = min(1, quality)
                    else:
                        quality = 0

                    oee = availability * performance * quality
                    # print(availability, performance, quality)

                    shift_summary.total_parts = total_parts
                    shift_summary.good_parts = good_parts
                    shift_summary.bad_parts = total_parts - good_parts

                    shift_summary.availability = round(availability * 100, 2)
                    shift_summary.performance = round(performance * 100, 2)
                    shift_summary.quality = round(quality * 100, 2)
                    shift_summary.oee = round(oee * 100, 2)

                    shift_summary.availability_loss = 100 - shift_summary.availability
                    shift_summary.performance_loss = 100 - shift_summary.performance
                    shift_summary.quality_loss = 100 - shift_summary.quality

                    shift_summary.oee = oee * 100

                    commit()

        except Exception as e:
            print(e)

        tt.sleep(5)


except Exception as e:
    print(f"Setup error: {e}")

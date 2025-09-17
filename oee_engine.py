import math
from datetime import datetime, date, timedelta, time

from pony.orm import db_session, select, commit

from app.database.connection import connect_to_db
from app.models import ProductionLog
from app.models.production import ShiftSummary, MachineRawLive
from utils import ShiftManager, get_ideal_cycle_time

import time as tt

try:
    connect_to_db()
    print("Database connected successfully")

    while True:
        try:
            with db_session():
                timestamp = datetime.now()
                shift_id, shift_start_time, shift_end_time = ShiftManager.get_current_shift(timestamp)

                print(shift_id, shift_start_time, shift_end_time)

                # Handle shifts that cross midnight
                if shift_start_time > shift_end_time:
                    shift_start_dt = datetime.combine(timestamp.date(), shift_start_time)
                    shift_end_dt = datetime.combine(timestamp.date() + timedelta(days=1), shift_end_time)
                else:
                    shift_start_dt = datetime.combine(timestamp.date(), shift_start_time)
                    shift_end_dt = datetime.combine(timestamp.date(), shift_end_time)

                # Update MachineRawLive scheduled job info
                # machine_raw_live = select(i for i in MachineRawLive)
                # for i in machine_raw_live:
                #     machine_schedule = get_machine_schedule_quantities(i.machine_id, shift_start_dt, shift_end_dt)
                #     i.scheduled_job = machine_schedule[0].operation_id if machine_schedule else None

                # Fetch all shift summaries for this shift
                shift_summaries = select(s for s in ShiftSummary
                                         if s.shift == shift_id and s.timestamp == shift_start_dt)

                for s in shift_summaries:
                    shift_summary = ShiftSummary[s.id]

                    # Actual production time (from summary)
                    actual_production_time = timedelta(
                        hours=shift_summary.production_time.hour,
                        minutes=shift_summary.production_time.minute,
                        seconds=shift_summary.production_time.second
                    )
                    
                    # Planned production time (shift length in seconds)
                    planned_production_time = max(timedelta(
                        hours=shift_summary.idle_time.hour,
                        minutes=shift_summary.idle_time.minute,
                        seconds=shift_summary.idle_time.second
                    ) - timedelta(hours=1, minutes=30), timedelta(0)) + actual_production_time

                    print(f"PRODUCTION TIME > Planned: {planned_production_time} | Actual: {actual_production_time}")

                    # Get production log entries that overlap the shift window
                    production_log = select(i for i in ProductionLog
                                            if i.machine_id == shift_summary.machine_id
                                            and i.start_time < shift_end_dt and i.end_time > shift_start_dt)[:]

                    # Group logs by operation
                    ops = {}
                    for log in production_log:
                        if log.operation.id not in ops:
                            ops[log.operation.id] = {
                                "total_parts": 0,
                                "bad_parts": 0,
                                "duration": 0  # operating time per op
                            }
                        ops[log.operation.id]["total_parts"] += log.quantity_completed or 0
                        ops[log.operation.id]["bad_parts"] += log.quantity_rejected or 0
                        if log.start_time and log.end_time:
                            ops[log.operation.id]["duration"] += (log.end_time - log.start_time).total_seconds()

                    # Aggregate results
                    total_parts = sum(op["total_parts"] for op in ops.values())
                    bad_parts = sum(op["bad_parts"] for op in ops.values())
                    good_parts = total_parts - bad_parts

                    print(f"PARTS SUMMARY   > Total: {total_parts} | Bad: {bad_parts} | Good: {good_parts}")

                    # --- OEE Metrics ---
                    
                    # AVAILABILITY CALCULATION
                    # A = Operating Time / Planned Production Time
                    if planned_production_time.total_seconds() > 0:
                        availability = actual_production_time / planned_production_time
                        availability = min(1.0, availability)
                    else:
                        availability = 0

                    # PERFORMANCE CALCULATION
                    # P = (Ideal Cycle Time × Total Parts) / Operating Time
                    perf_values = []
                    for op_id, data in ops.items():
                        if data["duration"] > 0 and data["total_parts"] > 0:
                            ict = get_ideal_cycle_time(op_id)  # returns in hours
                            ict_val = float(ict) * 3600  # convert hours to seconds
                            duration_val = float(data["duration"])  # operating time already in seconds
                            perf_op = (ict_val * data["total_parts"]) / duration_val
                            perf_values.append(min(1.0, perf_op))
                    performance = sum(perf_values) / len(perf_values) if perf_values else 0

                    # QUALITY CALCULATION
                    # Q = Good Parts / Total Parts
                    qual_values = []
                    for op_id, data in ops.items():
                        if data["total_parts"] > 0:
                            qual_op = (data["total_parts"] - data["bad_parts"]) / data["total_parts"]
                            qual_values.append(min(1.0, qual_op))
                    quality = sum(qual_values) / len(qual_values) if qual_values else 0

                    print(f"OEE VALUES      > A: {round(availability * 100, 2)} | P: {round(performance * 100, 2)} | Q: {round(quality * 100, 2)}")
                    # OEE
                    oee = availability * performance * quality
                    print(f"\nOEE             > {round(oee * 100, 2)}\n{'-' * 70}")

                    # Update shift summary fields
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

                    commit()

        except Exception as e:
            print(e)

        tt.sleep(10)

except Exception as e:
    print(f"Setup error: {e}")

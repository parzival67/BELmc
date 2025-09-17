from datetime import datetime, timezone, timedelta
from datetime import time as time_obj
from typing import List

from fastapi import HTTPException, Query
from pony.orm import db_session, commit, desc, select
from pydantic import BaseModel

# Import router and models
from app.api.v1.endpoints.scheduled import router
from app.models import Machine, ScheduleVersion, PlannedScheduleItem, Operation, Order
from app.models.production import (
    MachineRaw, StatusLookup, MachineRawLive,
    ShiftInfo, ShiftSummary, ConfigInfo, MachineDowntimes
)


# ===== Database Operations =====

class DatabaseManager:
    @staticmethod
    @db_session
    def initialize_db():
        """Initialize StatusLookup, ShiftInfo, and ConfigInfo tables if empty"""

        # Insert default statuses
        if StatusLookup.select().count() == 0:
            StatusLookup(status_id=0, status_name='OFF')
            StatusLookup(status_id=1, status_name='ON')
            StatusLookup(status_id=2, status_name='PRODUCTION')

        # Insert default shift timings
        if ShiftInfo.select().count() == 0:
            ShiftInfo(start_time=time_obj(6, 0), end_time=time_obj(14, 0))
            ShiftInfo(start_time=time_obj(14, 0), end_time=time_obj(22, 0))
            ShiftInfo(start_time=time_obj(22, 0), end_time=time_obj(6, 0))

        # Insert default config for machines
        if ConfigInfo.select().count() == 0:
            [ConfigInfo(machine_id=i, shift_duration=480, planned_non_production_time=40, planned_downtime=40)
             for i in range(1, 15)]

        commit()

    @staticmethod
    @db_session
    def close_downtime(machine_id):
        """Close the latest open downtime entry for the given machine"""
        recent_downtime = select(d for d in MachineDowntimes if d.machine_id == machine_id) \
            .order_by(lambda d: desc(d.open_dt)) \
            .first()

        current_dt = datetime.now() + timedelta(hours=5, minutes=30)

        if recent_downtime and recent_downtime.closed_dt is None:
            recent_downtime.closed_dt = current_dt
            commit()
            print(f"Closed Downtime for Machine ID: {machine_id} >> {current_dt}")

    @staticmethod
    @db_session
    def handle_disconnection(machine_id=14):
        """
        Handle disconnection of a machine:
        - Create an OFFLINE entry
        - Update live status
        - Start a new downtime if not already open
        """
        recent_status = select(s for s in MachineRaw if s.machine_id == machine_id) \
            .order_by(lambda s: desc(s.timestamp)) \
            .first()

        current_time = datetime.now() + timedelta(hours=5, minutes=30)

        # If no status or status is not OFF, mark it as OFF
        if (not recent_status) or recent_status.status.status_id != 0:
            print(f"{current_time} [DISCONNECTION] | Machine ID > {machine_id} | Past State > {recent_status.status.status_id}")

            # Insert new raw record
            MachineRaw(
                timestamp=current_time,
                machine_id=machine_id,
                op_mode=-1,
                status=0
            )

            # Update or insert live data
            active_signal = MachineRawLive.get(machine_id=machine_id)
            if active_signal:
                active_signal.timestamp = current_time
                active_signal.op_mode = -1
                active_signal.prog_status = -1
                active_signal.status = 0
                active_signal.part_count = 0
                active_signal.selected_program = ''
                active_signal.active_program = ''
            else:
                MachineRawLive(
                    timestamp=current_time,
                    machine_id=machine_id,
                    op_mode=-1,
                    prog_status=-1,
                    status=0,
                    part_count=0,
                    selected_program='',
                    active_program=''
                )

        else:
            # If status already OFF, just update timestamp and shift summary
            active_signal = MachineRawLive.get(machine_id=machine_id)
            if active_signal:
                active_signal.timestamp = current_time

            ShiftManager.manage_shift_summary(current_time, machine_id)

        # Open a new downtime entry if none is active
        recent_downtime = select(d for d in MachineDowntimes if d.machine_id == machine_id) \
            .order_by(lambda d: desc(d.open_dt)) \
            .first()

        if not recent_downtime or recent_downtime.closed_dt is not None:
            MachineDowntimes(machine_id=machine_id, open_dt=current_time)

        commit()

    @staticmethod
    @db_session
    def record_machine_data(machine_id, data):
        """
        Insert machine signal data into MachineRaw and MachineRawLive
        Creates new entry if any key value has changed
        """
        try:
            timestamp = datetime.now() + timedelta(hours=5, minutes=30)

            # Extract parameters from received data
            machine_status = data["machine_status"]
            operation_mode = data["op_mode"]
            program_status = data["prog_status"]
            active_program = data["active_program"]
            selected_program = data["selected_program"]
            part_count = data["part_count"]
            part_status = data["part_status"]

            active_signal = MachineRawLive.get(machine_id=machine_id)
            machine_raw_latest = select(i for i in MachineRaw if i.machine_id == machine_id) \
                .order_by(lambda i: desc(i.timestamp)) \
                .first()

            # Insert new MachineRaw record only if there's a change
            if active_signal is None or (
                active_signal.op_mode != operation_mode or
                active_signal.prog_status != program_status or
                active_signal.status.status_id != machine_status or
                active_signal.part_count != machine_raw_latest.part_count or
                active_signal.selected_program != selected_program or
                active_signal.active_program != active_program or
                active_signal.scheduled_job != machine_raw_latest.scheduled_job or
                active_signal.actual_job != machine_raw_latest.actual_job
            ):
                MachineRaw(
                    timestamp=timestamp,
                    machine_id=machine_id,
                    op_mode=operation_mode,
                    prog_status=program_status,
                    status=machine_status,
                    part_count=active_signal.part_count,
                    part_status=part_status,
                    selected_program=selected_program,
                    active_program=active_program,
                    scheduled_job=active_signal.scheduled_job,
                    actual_job=active_signal.actual_job
                )

                print(f'STATUS CHANGE => {timestamp} >> Machine ID: {machine_id} | '
                      f'Status: {machine_status} | Operation Mode: {operation_mode} | '
                      f'Program Status: {program_status} | Part Count: {active_signal.part_count} | '
                      f'Selected Program: {selected_program} | Active Program: {active_program} |')

            # Update live record or create if not exists
            if active_signal:
                active_signal.timestamp = timestamp
                active_signal.op_mode = operation_mode
                active_signal.prog_status = program_status
                active_signal.status = machine_status
                active_signal.part_count += part_count
                active_signal.selected_program = selected_program
                active_signal.active_program = active_program
            else:
                MachineRawLive(
                    timestamp=timestamp,
                    machine_id=machine_id,
                    op_mode=operation_mode,
                    prog_status=program_status,
                    status=machine_status,
                    part_count=part_count,
                    selected_program=selected_program,
                    active_program=active_program
                )

            ShiftManager.manage_shift_summary(timestamp, machine_id)
            commit()

        except Exception as e:
            print(f'Exception during database insertion: {e}')
            raise


# ===== Shift Management =====

class ShiftManager:
    @staticmethod
    def get_current_shift(timestamp):
        """
        Determine the current shift based on time.
        Returns shift ID and its start and end times.
        Handles shifts that cross midnight.
        """
        current_time = timestamp.time()

        with db_session:
            shifts = select(s for s in ShiftInfo)[:]

            for shift in shifts:
                start = shift.start_time
                end = shift.end_time

                # Handle overnight shifts
                if start > end:
                    if current_time >= start or current_time < end:
                        return shift.id, start, end
                else:
                    if start <= current_time < end:
                        return shift.id, start, end

        # Default to first shift if nothing matches
        first_shift = shifts[0]
        return first_shift.id, first_shift.start_time, first_shift.end_time

    @staticmethod
    @db_session
    def manage_shift_summary(timestamp, machine_id=14):
        """
        Update shift summary (OFF, IDLE, PRODUCTION durations) for the given machine
        """
        shift_id, shift_start_time, shift_end_time = ShiftManager.get_current_shift(timestamp)
        timestamp1 = timestamp
        current_date = timestamp.date()

        # Handle time ranges for shifts crossing midnight
        shift_start = datetime.combine(current_date, shift_start_time)
        shift_end = datetime.combine(current_date, shift_end_time)
        if shift_start_time > shift_end_time:
            if timestamp.time() >= shift_start_time:
                shift_end += timedelta(days=1)
            else:
                shift_start -= timedelta(days=1)

        # Get config for the machine
        config_info = ConfigInfo.get(machine_id=machine_id)

        # Create shift summary if not exists
        shift_summary = ShiftSummary.get(machine_id=machine_id, shift=shift_id, timestamp=shift_start)
        if not shift_summary:
            zero_time = time_obj(0, 0, 0)
            shift_summary = ShiftSummary(
                machine_id=machine_id, shift=shift_id, timestamp=shift_start,
                off_time=zero_time, idle_time=zero_time, production_time=zero_time,
                total_parts=0, good_parts=0, bad_parts=0,
                availability=0, performance=0, quality=0,
                availability_loss=0, performance_loss=0, quality_loss=0, oee=0
            )

        # Fetch machine statuses in the current shift
        status_changes = (
            [select(s for s in MachineRaw
                    if s.machine_id == machine_id and s.timestamp <= shift_start)
             .order_by(lambda s: desc(s.timestamp)).first()] +
            list(select(s for s in MachineRaw
                        if s.machine_id == machine_id and shift_start <= s.timestamp <= timestamp))
        )
        status_changes = [s for s in status_changes if s]
        status_changes = sorted(status_changes, key=lambda x: x.timestamp)

        # Duration calculation for OFF, IDLE, PRODUCTION
        off_duration = idle_duration = production_duration = timedelta()

        if len(status_changes) == 1:
            duration = timestamp - shift_start
            sid = status_changes[0].status.status_id
            if sid == 0: off_duration += duration
            elif sid == 1: idle_duration += duration
            elif sid == 2: production_duration += duration
        else:
            for i in range(1, len(status_changes)):
                start_time = max(shift_start, status_changes[i - 1].timestamp) if i == 1 else status_changes[i - 1].timestamp
                end_time = status_changes[i].timestamp
                duration = end_time - start_time
                sid = status_changes[i - 1].status.status_id
                if sid == 0: off_duration += duration
                elif sid == 1: idle_duration += duration
                elif sid == 2: production_duration += duration

        # Add time from last state to now
        if len(status_changes) > 1:
            last_status = status_changes[-1].status.status_id
            last_duration = timestamp - status_changes[-1].timestamp
            if last_status == 0: off_duration += last_duration
            elif last_status == 1: idle_duration += last_duration
            elif last_status == 2: production_duration += last_duration

        # Convert timedelta to time
        def timedelta_to_time(td):
            total_seconds = int(td.total_seconds())
            return time_obj((total_seconds // 3600) % 24, (total_seconds % 3600) // 60, total_seconds % 60)

        shift_summary.off_time = timedelta_to_time(off_duration)
        shift_summary.idle_time = timedelta_to_time(idle_duration)
        shift_summary.production_time = timedelta_to_time(production_duration)
        shift_summary.updatedate = timestamp1

        commit()
        return shift_summary


# ===== API: Get Machine Schedule Quantities =====

class OperationQuantityResponse(BaseModel):
    operation_id: int
    completed_quantity: int
    remaining_quantity: int
    planned_start_time: datetime
    planned_end_time: datetime
    version_number: int


class MachineScheduleResponse(BaseModel):
    machine_id: int
    data: List[OperationQuantityResponse]


@router.get("/api/machine-schedule-quantities", response_model=MachineScheduleResponse)
@db_session
def get_machine_schedule_quantities(
        machine_id: int,
        start_time: datetime = Query(..., description="Start datetime in ISO format"),
        end_time: datetime = Query(..., description="End datetime in ISO format")
):
    """
    Return completed and remaining quantities for operations scheduled on a specific machine
    between a given time range.
    """
    machine = Machine.get(id=machine_id)
    if not machine:
        raise HTTPException(status_code=404, detail=f"Machine with ID {machine_id} not found")

    # Get all relevant schedule items
    schedule_items = select(item for item in PlannedScheduleItem if item.machine.id == machine_id)

    result_data = []

    for item in schedule_items:
        latest_version = select(sv for sv in item.schedule_versions
                                if sv.is_active and
                                sv.planned_start_time <= end_time and
                                sv.planned_end_time >= start_time
                                ).order_by(desc(ScheduleVersion.version_number)).first()

        if latest_version:
            result_data.append(OperationQuantityResponse(
                operation_id=item.operation.id,
                completed_quantity=latest_version.completed_quantity,
                remaining_quantity=latest_version.remaining_quantity,
                planned_start_time=latest_version.planned_start_time,
                planned_end_time=latest_version.planned_end_time,
                version_number=latest_version.version_number
            ))
    # print(MachineScheduleResponse(machine_id=machine_id, data=result_data))
    return MachineScheduleResponse(machine_id=machine_id, data=result_data)

@db_session
def get_ideal_cycle_time(operation_id: int):
    op = Operation.get(id=operation_id)
    return op.ideal_cycle_time
import json

from app.database.connection import connect_to_db
from app.models.production import MachineRaw, StatusLookup, MachineRawLive, ShiftInfo, ShiftSummary, ConfigInfo

import time
from datetime import datetime, timezone, timedelta
from datetime import time as timett

from opcua import Client, ua
from pony.orm import db_session, commit, desc, select


def is_client_connected(client):
    try:
        if client.uaclient and client.uaclient._uasocket:
            return client.uaclient._uasocket._thread.is_alive()
        return False
    except:
        return False

def safe_disconnect(client):
    try:
        if is_client_connected(client):
            client.disconnect()
            print("Disconnected from OPC UA Server")
    except Exception as e:
        print(f"Error during OPC UA disconnect (can be ignored if already disconnected): {e}")


@db_session
def handle_disconnection(machine_id=1):
    recent_status = select(s for s in MachineRaw if s.machine_id == machine_id) \
        .order_by(lambda s: desc(s.timestamp)) \
        .first()

    current_time = datetime.now()
    if (not recent_status) or recent_status.status.status_id != 0:
        MachineRaw(
            timestamp=current_time,
            machine_id=machine_id,
            op_mode=-1,
            status=0
        )

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
        active_signal = MachineRawLive.get(machine_id=machine_id)
        if active_signal:
            active_signal.timestamp = current_time

        manage_shift_summary(current_time, machine_id)

    commit()

def get_current_shift(timestamp):
    """
    Determine current shift based on timestamp
    Returns (shift_id, shift_start_time, shift_end_time)
    """
    current_time = timestamp.time()

    with db_session:
        shifts = select(s for s in ShiftInfo)[:]

        for shift in shifts:
            start = shift.start_time
            end = shift.end_time

            # Handle shifts that cross midnight
            if start > end:
                if current_time >= start or current_time < end:
                    return shift.id, start, end
            else:
                if start <= current_time < end:
                    return shift.id, start, end

    # If no shift found, use the first shift (assuming 24/7 operation)
    first_shift = shifts[0]
    return first_shift.id, first_shift.start_time, first_shift.end_time

@db_session
def database_insertion(machine_id, machine_status, operation_mode,  program_status, active_program, selected_program, part_count, part_status):
    try:
        timestamp = datetime.now()

        active_signal = MachineRawLive.get(machine_id=machine_id)

        if active_signal is None or (
                active_signal.op_mode != operation_mode or
                active_signal.prog_status != program_status or
                active_signal.status.status_id != machine_status or
                active_signal.part_count != part_count or
                active_signal.selected_program != selected_program or
                active_signal.active_program != active_program
        ):
            MachineRaw(
                timestamp=timestamp,
                machine_id=machine_id,
                op_mode=operation_mode,
                prog_status=program_status,
                status=machine_status,
                part_count=part_count,
                part_status=part_status,
                selected_program=selected_program,
                active_program=active_program
            )

            print(f'STATUS CHANGE => {timestamp} >> '
                  f'Machine ID: {machine_id} | '
                  f'Status: {machine_status} | '
                  f'Operation Mode: {operation_mode} | '
                  f'Program Status: {program_status} | '
                  f'Part Count: {part_count} | '
                  f'Selected Program: {selected_program} | '
                  f'Active Program: {active_program} | ')

        if active_signal:
            active_signal.timestamp = timestamp
            active_signal.op_mode = operation_mode
            active_signal.prog_status = program_status
            active_signal.status = machine_status
            active_signal.part_count = part_count
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

        manage_shift_summary(timestamp, machine_id, part_count, part_status)
        commit()

    except ValueError as ve:
        print(f'Error parsing timestamp: {ve}')
    except Exception as e:
        print(f'Exception during insertion: {e}')

@db_session
def manage_shift_summary(timestamp, machine_id=1, part_count=0, part_status=0):
    shift_id, shift_start_time, shift_end_time = get_current_shift(timestamp)

    # Convert shift times to current date
    current_date = timestamp.date()
    shift_start = datetime.combine(current_date, shift_start_time)
    shift_end = datetime.combine(current_date, shift_end_time)

    # Handle shifts crossing midnight
    if shift_start_time > shift_end_time:
        if timestamp.time() >= shift_start_time:
            shift_end += timedelta(days=1)
        else:
            shift_start -= timedelta(days=1)

    # Get or create shift summary
    shift_summary = ShiftSummary.get(
        machine_id=machine_id,
        shift=shift_id,
        timestamp=shift_start
    )

    config_info = ConfigInfo.get(machine_id=machine_id)

    if not shift_summary:
        zero_time = timett(0, 0, 0)
        shift_summary = ShiftSummary(
            machine_id=machine_id,
            shift=shift_id,
            timestamp=shift_start,
            off_time=zero_time,
            idle_time=zero_time,
            production_time=zero_time,
            total_parts=0,
            good_parts=0,
            bad_parts=0,
            availability=0,
            performance=0,
            quality=0,
            availability_loss=0,
            performance_loss=0,
            quality_loss=0,
            oee=0
        )

    status_changes = ([select(s for s in MachineRaw
                              if s.machine_id == machine_id and
                              s.timestamp <= shift_start).order_by(lambda s: desc(s.timestamp)).first()]
                      +
                      select(s for s in MachineRaw
                             if s.machine_id == machine_id and
                             s.timestamp >= shift_start and
                             s.timestamp <= timestamp)[:])

    off_duration = timedelta()
    idle_duration = timedelta()
    production_duration = timedelta()

    status_changes = [s for s in status_changes if s is not None]
    status_changes = sorted(status_changes, key=lambda x: x.timestamp)

    if len(status_changes) == 1:
        duration = timestamp - shift_start
        if status_changes[0].status.status_id == 0:
            off_duration += duration
        elif status_changes[0].status.status_id == 1:
            idle_duration += duration
        elif status_changes[0].status.status_id == 2:
            production_duration += duration
    else:
        for i in range(1, len(status_changes)):
            if i == 1:
                start_time = max(shift_start, status_changes[i - 1].timestamp)
            else:
                start_time = status_changes[i - 1].timestamp

            end_time = status_changes[i].timestamp
            duration = end_time - start_time

            if status_changes[i - 1].status.status_id == 0:
                off_duration += duration
            elif status_changes[i - 1].status.status_id == 1:
                idle_duration += duration
            elif status_changes[i - 1].status.status_id == 2:
                production_duration += duration

    if len(status_changes) > 1:
        last_status = status_changes[len(status_changes) - 1].status.status_id
        last_duration = timestamp - status_changes[len(status_changes) - 1].timestamp

        if last_status == 0:
            off_duration += last_duration
        elif last_status == 1:
            idle_duration += last_duration
        elif last_status == 2:
            production_duration += last_duration

    def timedelta_to_time(td):
        total_seconds = int(td.total_seconds())
        hours = (total_seconds // 3600) % 24
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return timett(hours, minutes, seconds)

    shift_summary.off_time = timedelta_to_time(off_duration)
    shift_summary.idle_time = timedelta_to_time(idle_duration)
    shift_summary.production_time = timedelta_to_time(production_duration)

    # print(f"\n## SHIFT SUMMARY: shift: {shift_id} | start: {shift_start_time}, end: {shift_end_time}\n"
    #       f"#  OFF_TIME: {shift_summary.off_time}\n"
    #       f"#   ON_TIME: {shift_summary.idle_time}\n"
    #       f"# PROD_TIME: {shift_summary.production_time}\n")

    shift_summary.total_parts = part_count
    if part_status == 2:
        shift_summary.good_parts = part_count
        shift_summary.bad_parts = 0
    else:
        shift_summary.bad_parts = part_count - shift_summary.good_parts


    shift_summary.availability = (idle_duration.total_seconds()/60)/(config_info.shift_duration
                                                                           - config_info.planned_non_production_time
                                                                           - config_info.planned_downtime)
    if production_duration.total_seconds()/60 != 0:
        shift_summary.performance = (shift_summary.total_parts * 1) / (production_duration.total_seconds()/60)

    if shift_summary.total_parts != 0:
        shift_summary.quality = shift_summary.good_parts / shift_summary.total_parts

    shift_summary.availability_loss = 100 - shift_summary.availability
    shift_summary.performance_loss = 100 - shift_summary.performance
    shift_summary.quality_loss = 100 - shift_summary.quality

    shift_summary.updatedate = datetime.now()

    return shift_summary


if __name__ == '__main__':
    try:
        connect_to_db()
        print("Database Binded Successfully")
    except Exception as e:
        print(f"Error generating mapping: {e}")
        exit(1)

    with open("config/opcua_settings.json", "r") as file:
        config = json.load(file)['opcua'][0]

    machine_id = config['machine_id']

    client = Client(f"opc.tcp://{config['ip_address']}:{config['port']}")
    client.set_user(config['username'])
    client.set_password(config['password'])

    sleep_delay = 1
    retry_delay = 60

    while True:
        try:
            if is_client_connected(client):
                prog_status = int(client.get_node("ns=2;s=/Channel/State/progStatus").get_value())
                op_mode = int(client.get_node("ns=2;s=/Bag/State/opMode").get_value())
                part_count = int(client.get_node("ns=2;s=/Channel/State/actParts").get_value())
                active_program = client.get_node("ns=2;s=/Channel/ProgramInfo/progName").get_value()
                selected_program = client.get_node("ns=2;s=/Channel/ProgramInfo/selectedWorkPProg").get_value()

                if prog_status == 3:
                    machine_status = 2
                else:
                    machine_status = 1

                database_insertion(machine_id=machine_id, machine_status=machine_status, operation_mode=op_mode,
                                   program_status=prog_status, active_program=active_program, selected_program=selected_program,
                                   part_count=part_count, part_status=0)
            else:
                client.connect()
                print('Successfully connected to OPC UA Server')

            time.sleep(1)

        except Exception as e:
            print(e)
            handle_disconnection()
            time.sleep(retry_delay)
        # finally:
        #     print('yeet')
        #     safe_disconnect(client)
        #     print('Disconnected from Database')

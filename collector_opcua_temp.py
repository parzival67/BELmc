"""
OPC UA Machine Monitoring System
--------------------------------
This program connects to industrial machines via OPC UA protocol,
monitors machine status, and records operational data in a database.
"""

import json
import time
from datetime import datetime, timezone, timedelta
from datetime import time as time_obj

from opcua import Client, ua
from pony.orm import db_session, commit, desc, select

from app.database.connection import connect_to_db
from app.models.production import (
    MachineRaw, StatusLookup, MachineRawLive,
    ShiftInfo, ShiftSummary, ConfigInfo, MachineDowntimes
)


# ===== OPC UA Client Management =====

class OpcUaClient:
    def __init__(self, server_url, username=None, password=None):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.client = Client(server_url)

        if username:
            self.client.set_user(username)
        if password:
            self.client.set_password(password)

    def connect(self, machine_id):
        try:
            self.client.connect()
            print(f"Successfully connected to OPC UA Server: {self.server_url}")
            return True
        except Exception as e:
            print(f"Failed to connect to OPC UA Server: {e}")
            DatabaseManager.handle_disconnection(machine_id)
            time.sleep(60)
            return False

    def disconnect(self):
        try:
            if self.is_connected():
                self.client.disconnect()
                print("Disconnected from OPC UA Server")
        except Exception as e:
            print(f"Error during OPC UA disconnect: {e}")

    def is_connected(self):
        try:
            if self.client.uaclient and self.client.uaclient._uasocket:
                return self.client.uaclient._uasocket._thread.is_alive()
            return False
        except Exception:
            return False

    def get_node_value(self, node_id):
        try:
            return self.client.get_node(node_id).get_value()
        except Exception as e:
            print(f"Error reading node {node_id}: {e}")
            raise


# ===== Machine Data Collection =====

class MachineDataCollector:
    def __init__(self, machine_id, opcua_client):
        self.machine_id = machine_id
        self.opcua_client = opcua_client
        self.node_paths = {
            "prog_status": "ns=2;s=/Channel/State/progStatus",
            "op_mode": "ns=2;s=/Bag/State/opMode",
            "part_count": "ns=2;s=/Channel/State/actParts",
            "active_program": "ns=2;s=/Channel/ProgramInfo/progName",
            "selected_program": "ns=2;s=/Channel/ProgramInfo/selectedWorkPProg"
        }

    def collect_data(self):
        """Collect current machine data via OPC UA"""
        try:
            if not self.opcua_client.is_connected():
                raise ConnectionError("OPC UA client not connected")

            data = {}
            for key, node_id in self.node_paths.items():
                data[key] = self.opcua_client.get_node_value(node_id)

            # Convert numeric values to integers
            for key in ["prog_status", "op_mode", "part_count"]:
                data[key] = int(data[key])

            # Determine machine status based on program status
            if data["prog_status"] == 3:
                data["machine_status"] = 2  # Production
                DatabaseManager.close_downtime(machine_id=1)

            else:
                data["machine_status"] = 1  # Idle

            # Default part status
            data["part_status"] = 0

            return data

        except Exception as e:
            print(f"Error collecting machine data: {e}")
            raise


# ===== Database Operations =====

class DatabaseManager:
    @staticmethod
    @db_session
    def initialize_db():
        if StatusLookup.select().count() == 0:
            StatusLookup(status_id=0, status_name='OFF')
            StatusLookup(status_id=1, status_name='ON')
            StatusLookup(status_id=2, status_name='PRODUCTION')

        if ShiftInfo.select().count() == 0:
            ShiftInfo(start_time=time_obj(9, 0), end_time=time_obj(17, 0))
            ShiftInfo(start_time=time_obj(17, 0), end_time=time_obj(1, 0))
            ShiftInfo(start_time=time_obj(1, 0), end_time=time_obj(9, 0))

        if ConfigInfo.select().count() == 0:
            [ConfigInfo(machine_id=i, shift_duration=480, planned_non_production_time=40, planned_downtime=40)
             for i in range(8)]

    @staticmethod
    @db_session
    def close_downtime(machine_id):
        recent_downtime = select(d for d in MachineDowntimes if d.machine_id == machine_id) \
            .order_by(lambda d: desc(d.open_dt)) \
            .first()

        current_dt = datetime.now()

        if recent_downtime and recent_downtime.closed_dt is None:
            recent_downtime.closed_dt = current_dt
            commit()

            print(f"Closed Downtime for Machine ID: {machine_id} >> {current_dt}")

    @staticmethod
    @db_session
    def handle_disconnection(machine_id=1):
        """Update database to reflect machine disconnection"""
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

            ShiftManager.manage_shift_summary(current_time, machine_id)

        recent_downtime = select(d for d in MachineDowntimes if d.machine_id == machine_id) \
            .order_by(lambda d: desc(d.open_dt)) \
            .first()

        if not recent_downtime or recent_downtime.closed_dt is not None:
            MachineDowntimes(
                machine_id=machine_id,
                open_dt=current_time
            )

        commit()

    @staticmethod
    @db_session
    def record_machine_data(machine_id, data):
        """Insert machine data into database"""
        try:
            timestamp = datetime.now()

            machine_status = data["machine_status"]
            operation_mode = data["op_mode"]
            program_status = data["prog_status"]
            active_program = data["active_program"]
            selected_program = data["selected_program"]
            part_count = data["part_count"]
            part_status = data["part_status"]

            active_signal = MachineRawLive.get(machine_id=machine_id)

            # Check if there's a state change that needs to be recorded
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

            # Update or create the live status record
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

            ShiftManager.manage_shift_summary(timestamp, machine_id, part_count, part_status)
            commit()

        except Exception as e:
            print(f'Exception during database insertion: {e}')
            raise


# ===== Shift Management =====

class ShiftManager:
    @staticmethod
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

    @staticmethod
    @db_session
    def manage_shift_summary(timestamp, machine_id=1, part_count=0, part_status=0):
        """Update shift summary with current machine data"""
        shift_id, shift_start_time, shift_end_time = ShiftManager.get_current_shift(timestamp)

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
            zero_time = time_obj(0, 0, 0)
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

        # Calculate duration of each state
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

        # Calculate time in each state
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

        # Add time from most recent status to current time
        if len(status_changes) > 1:
            last_status = status_changes[-1].status.status_id
            last_duration = timestamp - status_changes[-1].timestamp

            if last_status == 0:
                off_duration += last_duration
            elif last_status == 1:
                idle_duration += last_duration
            elif last_status == 2:
                production_duration += last_duration

        # Convert timedeltas to time objects
        def timedelta_to_time(td):
            total_seconds = int(td.total_seconds())
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            return time_obj(hours, minutes, seconds)

        # Update shift summary with calculated durations
        shift_summary.off_time = timedelta_to_time(off_duration)
        shift_summary.idle_time = timedelta_to_time(idle_duration)
        shift_summary.production_time = timedelta_to_time(production_duration)

        # Update part counts
        shift_summary.total_parts = part_count
        if part_status == 2:
            shift_summary.good_parts = part_count
            shift_summary.bad_parts = 0
        else:
            shift_summary.bad_parts = part_count - shift_summary.good_parts

        # Calculate OEE metrics
        shift_summary.availability = (idle_duration.total_seconds() / 60) / (config_info.shift_duration
                                                                             - config_info.planned_non_production_time
                                                                             - config_info.planned_downtime)
        if production_duration.total_seconds() / 60 != 0:
            shift_summary.performance = (shift_summary.total_parts * 1) / (production_duration.total_seconds() / 60)

        if shift_summary.total_parts != 0:
            shift_summary.quality = shift_summary.good_parts / shift_summary.total_parts

        shift_summary.availability_loss = 100 - shift_summary.availability
        shift_summary.performance_loss = 100 - shift_summary.performance
        shift_summary.quality_loss = 100 - shift_summary.quality

        # Calculate OEE
        shift_summary.oee = shift_summary.availability * shift_summary.performance * shift_summary.quality / 10000

        shift_summary.updatedate = datetime.now()

        return shift_summary


# ===== Main Application =====

class MachineMonitor:
    def __init__(self, config_path="config/opcua_settings.json"):
        self.config_path = config_path
        self.config = None
        self.machine_id = None
        self.opcua_client = None
        self.data_collector = None
        self.running = False

        # Operation intervals in seconds
        self.poll_interval = 1
        self.retry_delay = 60

    def load_config(self):
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, "r") as file:
                self.config = json.load(file)['opcua'][0]

            self.machine_id = self.config['machine_id']
            return True
        except Exception as e:
            print(f"Error loading configuration: {e}")
            return False

    def setup(self):
        """Setup database connection and OPC UA client"""
        try:
            # Connect to database
            connect_to_db()
            print("Database connected successfully")

            # Load configuration
            if not self.load_config():
                return False

            # Create OPC UA client
            server_url = f"opc.tcp://{self.config['ip_address']}:{self.config['port']}"
            self.opcua_client = OpcUaClient(
                server_url,
                username=self.config['username'],
                password=self.config['password']
            )

            # Create data collector
            self.data_collector = MachineDataCollector(
                self.machine_id,
                self.opcua_client
            )

            return True
        except Exception as e:
            print(f"Setup error: {e}")
            return False

    def run(self):
        """Run the monitoring loop"""
        if not self.setup():
            print("Failed to set up the machine monitor")
            return

        self.running = True
        print(f"Starting monitoring for machine ID: {self.machine_id}")

        try:
            while self.running:
                try:
                    # Check if connected, if not, connect
                    if not self.opcua_client.is_connected():
                        self.opcua_client.connect(self.machine_id)
                        if self.opcua_client.is_connected():
                            print("Connected to OPC UA server")
                        continue  # Skip this iteration to ensure we're connected

                    # Collect data from machine
                    data = self.data_collector.collect_data()

                    # Record data to database
                    DatabaseManager.record_machine_data(self.machine_id, data)

                    # Wait for next poll interval
                    time.sleep(self.poll_interval)

                except ConnectionError:
                    print("Connection error, attempting to reconnect...")
                    self.opcua_client.disconnect()
                    DatabaseManager.handle_disconnection(self.machine_id)
                    time.sleep(self.retry_delay)

                except Exception as e:
                    print(f"Error during monitoring: {e}")
                    DatabaseManager.handle_disconnection(self.machine_id)
                    time.sleep(self.retry_delay)

        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources before exit"""
        try:
            if self.opcua_client:
                self.opcua_client.disconnect()
            print("Cleanup completed")
        except Exception as e:
            print(f"Error during cleanup: {e}")


# ===== Entry Point =====

if __name__ == '__main__':
    monitor = MachineMonitor()
    monitor.run()

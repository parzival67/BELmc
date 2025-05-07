"""
LSV2 Machine Monitoring System
-----------------------------
This program connects to industrial machines via LSV2 protocol,
monitors machine status, and records operational data in a database.
"""

import json
import re
import time
import pyLSV2
from app.database.connection import connect_to_db
from utils import DatabaseManager


# ===== LSV2 Client Management =====

class LSV2Client:
    def __init__(self, machine_id, ip_address, safe_mode=False):
        self.machine_id = machine_id
        self.ip_address = ip_address
        self.safe_mode = safe_mode
        self.client = None

    def connect(self):
        """Connect to the LSV2 server"""
        try:
            self.client = pyLSV2.LSV2(self.ip_address, safe_mode=self.safe_mode)
            self.client.connect()
            print(f"Successfully connected to machine ID: {self.machine_id} at {self.ip_address}")
            return True
        except Exception as e:
            print(f"Failed to connect to machine ID: {self.machine_id}: {e}")
            DatabaseManager.handle_disconnection(self.machine_id)
            return False

    def disconnect(self):
        """Disconnect from the LSV2 server"""
        try:
            if self.client:
                self.client.disconnect()
                print(f"Disconnected from machine ID: {self.machine_id}")
        except Exception as e:
            print(f"Error during LSV2 disconnect: {e}")

    def is_connected(self):
        """Check if the client is connected"""
        if not self.client:
            return False
        try:
            # Try to query something to check connection
            self.client.execution_state()
            return True
        except Exception:
            return False

    def get_machine_data(self):
        """Get machine data via LSV2 protocol"""
        try:
            if not self.client:
                raise ConnectionError("LSV2 client not initialized")

            data = {}

            # Get program status
            data["prog_status"] = self.client.program_status().value

            # Get operation mode
            data["op_mode"] = self.client.execution_state().value

            # Get active and selected programs
            program_stack_text = str(self.client.program_stack())
            program_match = re.search(r"Main\s+'([^']+)'\s+Current\s+'([^']+)'", program_stack_text)

            if program_match:
                data["selected_program"] = program_match.group(1)
                data["active_program"] = program_match.group(2)
            else:
                data["selected_program"] = ""
                data["active_program"] = ""

            # Part count is not available in this implementation
            data["part_count"] = 0
            data["part_status"] = 0

            # Determine machine status based on program status
            if data["prog_status"] == 0:
                data["machine_status"] = 2  # Production
            else:
                data["machine_status"] = 1  # Idle

            return data

        except Exception as e:
            print(f"Error collecting machine data: {e}")
            raise


# ===== Main Application =====

class MachineMonitor:
    def __init__(self, config_path="config/lsv2_settings.json"):
        self.config_path = config_path
        self.config = None
        self.clients = {}
        self.running = False

        # Operation intervals in seconds
        self.poll_interval = 1
        self.retry_delay = 60

    def load_config(self):
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, "r") as file:
                self.config = json.load(file)['lsv2']
            return True
        except Exception as e:
            print(f"Error loading configuration: {e}")
            return False

    def setup(self):
        """Setup database connection and LSV2 clients"""
        try:
            # Connect to database
            connect_to_db()
            DatabaseManager.initialize_db()
            print("Database connected successfully")

            # Load configuration
            if not self.load_config():
                return False

            # Create LSV2 clients for each machine
            for machine_config in self.config:
                machine_id = machine_config["machine_id"]
                ip_address = machine_config["ip_address"]

                # Create and store client
                client = LSV2Client(machine_id, ip_address, safe_mode=False)
                self.clients[machine_id] = client

                # Attempt initial connection
                client.connect()

            return True
        except Exception as e:
            print(f"Setup error: {e}")
            return False

    def run(self):
        """Run the monitoring loop for all machines"""
        if not self.setup():
            print("Failed to set up the machine monitor")
            return

        self.running = True
        print(f"Starting monitoring for {len(self.clients)} machines")

        try:
            while self.running:
                for machine_id, client in self.clients.items():
                    try:
                        # Check if connected, if not, try to reconnect
                        if not client.is_connected():
                            if client.connect():
                                print(f"Reconnected to machine ID: {machine_id}")
                            else:
                                # Handle disconnection in database
                                DatabaseManager.handle_disconnection(machine_id)
                                continue  # Skip this iteration for this machine

                        # Collect data from machine
                        data = client.get_machine_data()

                        # Record data to database
                        DatabaseManager.record_machine_data(machine_id, data)

                    except ConnectionError:
                        print(f"Connection error for machine ID: {machine_id}")
                        DatabaseManager.handle_disconnection(machine_id)

                    except Exception as e:
                        print(f"Error monitoring machine ID: {machine_id}: {e}")
                        DatabaseManager.handle_disconnection(machine_id)

                # Wait before next polling cycle
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources before exit"""
        try:
            for machine_id, client in self.clients.items():
                client.disconnect()
            print("All machines disconnected")
        except Exception as e:
            print(f"Error during cleanup: {e}")


# ===== Entry Point =====

if __name__ == '__main__':
    monitor = MachineMonitor()
    monitor.run()

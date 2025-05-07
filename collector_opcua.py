"""
OPC UA Machine Monitoring System
--------------------------------
This program connects to industrial machines via OPC UA protocol,
monitors machine status, and records operational data in a database.
"""

import json
import time
from opcua import Client, ua
from app.database.connection import connect_to_db
from utils import DatabaseManager


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
                DatabaseManager.close_downtime(machine_id=14)

            else:
                data["machine_status"] = 1  # Idle

            # Default part status
            data["part_status"] = 0

            return data

        except Exception as e:
            print(f"Error collecting machine data: {e}")
            raise


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

            DatabaseManager.initialize_db()

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

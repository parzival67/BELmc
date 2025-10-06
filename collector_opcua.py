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
    """
    Manages connection to OPC UA server and reading values from nodes.
    """
    def __init__(self, server_url, username=None, password=None):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.client = Client(server_url)

        # Optional authentication
        if username:
            self.client.set_user(username)
        if password:
            self.client.set_password(password)

    def connect(self, machine_id):
        """
        Connect to the OPC UA server. If failed, mark machine as disconnected.
        """
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
        """
        Disconnect from the OPC UA server.
        """
        try:
            if self.is_connected():
                self.client.disconnect()
                print("Disconnected from OPC UA Server")
        except Exception as e:
            print(f"Error during OPC UA disconnect: {e}")

    def is_connected(self):
        """
        Check if the OPC UA client is still connected.
        """
        try:
            if self.client.uaclient and self.client.uaclient._uasocket:
                return self.client.uaclient._uasocket._thread.is_alive()
            return False
        except Exception:
            return False

    def get_node_value(self, node_id):
        """
        Get value from a specific node ID.
        """
        try:
            return self.client.get_node(node_id).get_value()
        except Exception as e:
            print(f"Error reading node {node_id}: {e}")
            raise


# ===== Machine Data Collection =====

class MachineDataCollector:
    """
    Reads machine operational data from defined OPC UA nodes.
    """
    def __init__(self, machine_id, opcua_client):
        self.machine_id = machine_id
        self.opcua_client = opcua_client

        # Node ID mappings for relevant machine metrics
        self.node_paths = {
            "prog_status": "ns=2;s=/Channel/State/progStatus",
            "op_mode": "ns=2;s=/Bag/State/opMode",
            "part_count": "ns=2;s=/Channel/State/actParts",
            "active_program": "ns=2;s=/Channel/ProgramInfo/progName",
            "selected_program": "ns=2;s=/Channel/ProgramInfo/selectedWorkPProg"
        }

    def collect_data(self):
        """
        Collects current machine status and production data from OPC UA nodes.
        Returns a structured dictionary.
        """
        try:
            if not self.opcua_client.is_connected():
                raise ConnectionError("OPC UA client not connected")

            data = {}

            # Read all required nodes
            for key, node_id in self.node_paths.items():
                data[key] = self.opcua_client.get_node_value(node_id)

            # Ensure integer types for certain fields
            for key in ["prog_status", "op_mode", "part_count"]:
                data[key] = int(data[key])

            # Determine machine status based on program status
            if data["prog_status"] == 3:
                data["machine_status"] = 2  # Production
                DatabaseManager.close_downtime(machine_id=14)
            else:
                data["machine_status"] = 1  # Idle

            # Add placeholder part status (can be enhanced)
            data["part_status"] = 0

            return data

        except Exception as e:
            print(f"Error collecting machine data: {e}")
            raise


# ===== Main Application =====

class MachineMonitor:
    """
    Main monitoring loop: Connects to OPC UA, collects data, stores it in the database.
    """
    def __init__(self, config_path="config/opcua_settings.json"):
        self.config_path = config_path
        self.config = None
        self.machine_id = None
        self.opcua_client = None
        self.data_collector = None
        self.running = False

        # Timings
        self.poll_interval = 1        # Data collection frequency (in seconds)
        self.retry_delay = 60         # Wait time on error before retry

    def load_config(self):
        """
        Load OPC UA connection and machine config from a JSON file.
        """
        try:
            with open(self.config_path, "r") as file:
                self.config = json.load(file)['opcua'][0]
            self.machine_id = self.config['machine_id']
            return True
        except Exception as e:
            print(f"Error loading configuration: {e}")
            return False

    def setup(self):
        """
        Setup database connection, OPC UA client, and data collector.
        """
        try:
            # Connect to the database
            connect_to_db()
            print("Database connected successfully")

            # Load machine-specific OPC UA settings
            if not self.load_config():
                return False

            # Build OPC UA server URL and create client
            server_url = f"opc.tcp://{self.config['ip_address']}:{self.config['port']}"
            self.opcua_client = OpcUaClient(
                server_url,
                username=self.config['username'],
                password=self.config['password']
            )

            # Optional: Enable for unsecured connection
            # self.opcua_client.client.set_security_string("None,None,None,None")

            # Initialize data collector
            self.data_collector = MachineDataCollector(
                self.machine_id,
                self.opcua_client
            )

            # Initialize DB tables if empty
            DatabaseManager.initialize_db()

            return True

        except Exception as e:
            print(f"Setup error: {e}")
            return False

    def run(self):
        """
        Run the continuous monitoring loop.
        """
        if not self.setup():
            print("Failed to set up the machine monitor")
            return

        self.running = True
        print(f"Starting monitoring for machine ID: {self.machine_id}")

        try:
            while self.running:
                try:
                    # Ensure connection to OPC UA server
                    if not self.opcua_client.is_connected():
                        self.opcua_client.connect(self.machine_id)
                        if self.opcua_client.is_connected():
                            print("Connected to OPC UA server")
                        continue  # Skip current loop to retry connection

                    # Collect real-time machine data
                    data = self.data_collector.collect_data()

                    # Save data to the database
                    DatabaseManager.record_machine_data(self.machine_id, data)

                    # Wait for next polling interval
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
        """
        Cleanly shut down the system.
        """
        try:
            if self.opcua_client:
                self.opcua_client.disconnect()
            print("Cleanup completed")
        except Exception as e:
            print(f"Error during cleanup: {e}")


# ===== Entry Point =====

if __name__ == '__main__':
    # Start the monitoring process
    monitor = MachineMonitor()
    monitor.run()

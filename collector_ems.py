
import minimalmodbus
import serial
import struct
import time
import psycopg2
from psycopg2 import pool
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('plc_data_logger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': 'Energy_meter_plc_data',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}


class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, min_connections=1, max_connections=10):
        """
        Initialize a connection pool for database connections

        :param min_connections: Minimum number of connections to keep open
        :param max_connections: Maximum number of connections in the pool
        """
        try:
            cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=min_connections,
                maxconn=max_connections,
                **DB_CONFIG
            )
            logger.info("Database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database connection pool: {e}")
            raise

    @classmethod
    def get_connection(cls):
        """
        Get a connection from the pool

        :return: Database connection
        """
        if cls._connection_pool is None:
            cls.initialize_pool()

        try:
            return cls._connection_pool.getconn()
        except Exception as e:
            logger.error(f"Error getting database connection: {e}")
            raise

    @classmethod
    def return_connection(cls, conn):
        """
        Return a connection to the pool

        :param conn: Database connection to return
        """
        try:
            cls._connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")

    @classmethod
    def setup_database(cls):
        """
        Create schema and table for all meters
        """
        conn = None
        try:
            conn = cls.get_connection()
            cur = conn.cursor()

            # Create schema
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS plc_data;
            """)

            # Create a single table for all meters
            cur.execute("""
                CREATE TABLE IF NOT EXISTS plc_data.all_meter_readings (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    meter_id INT,
                    phase_a_voltage FLOAT,
                    phase_b_voltage FLOAT,
                    phase_c_voltage FLOAT,
                    avg_phase_voltage FLOAT,
                    line_ab_voltage FLOAT,
                    line_bc_voltage FLOAT,
                    line_ca_voltage FLOAT,
                    avg_line_voltage FLOAT,
                    frequency FLOAT,
                    total_instantaneous_power FLOAT,
                    phase_a_current FLOAT,
                    phase_b_current FLOAT,
                    phase_c_current FLOAT,
                    avg_three_phase_current FLOAT,
                    power_factor FLOAT,
                    active_energy_delivered FLOAT
                );
            """)

            conn.commit()
            logger.info("Database schema and single table created successfully")
            return True

        except Exception as e:
            logger.error(f"Database setup error: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                cls.return_connection(conn)

    @classmethod
    def insert_meter_readings(cls, meter_id, readings):
        """
        Insert meter readings into the single table

        :param meter_id: ID of the meter
        :param readings: List of readings to insert
        """
        conn = None
        try:
            conn = cls.get_connection()
            cur = conn.cursor()

            # Prepare insert SQL for the single table
            insert_sql = """
            INSERT INTO plc_data.all_meter_readings (
                meter_id, phase_a_voltage, phase_b_voltage, phase_c_voltage, 
                avg_phase_voltage, line_ab_voltage, line_bc_voltage, line_ca_voltage, 
                avg_line_voltage, frequency, total_instantaneous_power, 
                phase_a_current, phase_b_current, phase_c_current, 
                avg_three_phase_current, power_factor, active_energy_delivered
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """

            # Extract values from readings
            values = [meter_id]
            for name, value in readings:
                # Assuming the order of readings matches the table columns
                if isinstance(value, (int, float)):
                    values.append(value)

            # Insert if we have all expected values
            if len(values) == 17:  # meter_id + 16 readings
                cur.execute(insert_sql, values)
                conn.commit()
                logger.info(f"Inserted readings for Meter {meter_id}")
            else:
                logger.warning(f"Incomplete readings for Meter {meter_id}. Expected 16, got {len(values ) -1}")

        except Exception as e:
            logger.error(f"Error inserting meter readings: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cls.return_connection(conn)


class DeltaPLCReader:
    def __init__(self, port='COM5', slave_address=1):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 9600
        self.instrument.serial.bytesize = 7
        self.instrument.serial.parity = serial.PARITY_EVEN
        self.instrument.serial.stopbits = 2
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_ASCII
        self.instrument.clear_buffers_before_each_transaction = True

        # Define register mappings for all 14 meters
        self.meters = {
            1: {
                "PHASE_A VOLTAGE": (4200, 4),
                "PHASE_B VOLTAGE": (4204, 4),
                "PHASE_C VOLTAGE": (4208, 4),
                "AVERAGE PHASE VOLTAGE": (4212, 4),
                "A-B LINE VOLTAGE": (4216, 4),
                "B-C LINE VOLTAGE": (4220, 4),
                "C-A LINE VOLTAGE": (4224, 4),
                "AVERAGE LINE VOLTAGE": (4228, 4),
                "FREQUENCY": (4240, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4244, 4),
                "PHASE_A CURRENT": (4255, 4),
                "PHASE_B CURRENT": (4259, 4),
                "PHASE_C CURRENT": (4263, 4),
                "THREE-PHASE AVERAGE CURRENT": (4267, 4),
                "TOTAL POWER FACTOR": (4275, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4295, 4)
            },
            2: {
                "PHASE_A VOLTAGE": (4315, 4),
                "PHASE_B VOLTAGE": (4319, 4),
                "PHASE_C VOLTAGE": (4323, 4),
                "AVERAGE PHASE VOLTAGE": (4327, 4),
                "A-B LINE VOLTAGE": (4331, 4),
                "B-C LINE VOLTAGE": (4335, 4),
                "C-A LINE VOLTAGE": (4339, 4),
                "AVERAGE LINE VOLTAGE": (4343, 4),
                "FREQUENCY": (4355, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4359, 4),
                "PHASE_A CURRENT": (4370, 4),
                "PHASE_B CURRENT": (4374, 4),
                "PHASE_C CURRENT": (4378, 4),
                "THREE-PHASE AVERAGE CURRENT": (4382, 4),
                "TOTAL POWER FACTOR": (4390, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4410, 4)
            },
            3: {
                "PHASE_A VOLTAGE": (4430, 4),
                "PHASE_B VOLTAGE": (4434, 4),
                "PHASE_C VOLTAGE": (4438, 4),
                "AVERAGE PHASE VOLTAGE": (4442, 4),
                "A-B LINE VOLTAGE": (4446, 4),
                "B-C LINE VOLTAGE": (4450, 4),
                "C-A LINE VOLTAGE": (4454, 4),
                "AVERAGE LINE VOLTAGE": (4458, 4),
                "FREQUENCY": (4465, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4469, 4),
                "PHASE_A CURRENT": (4480, 4),
                "PHASE_B CURRENT": (4484, 4),
                "PHASE_C CURRENT": (4488, 4),
                "THREE-PHASE AVERAGE CURRENT": (4492, 4),
                "TOTAL POWER FACTOR": (4500, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4520, 4)
            },
            4: {
                "PHASE_A VOLTAGE": (4540, 4),
                "PHASE_B VOLTAGE": (4544, 4),
                "PHASE_C VOLTAGE": (4548, 4),
                "AVERAGE PHASE VOLTAGE": (4552, 4),
                "A-B LINE VOLTAGE": (4556, 4),
                "B-C LINE VOLTAGE": (4560, 4),
                "C-A LINE VOLTAGE": (4564, 4),
                "AVERAGE LINE VOLTAGE": (4568, 4),
                "FREQUENCY": (4575, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4579, 4),
                "PHASE_A CURRENT": (4590, 4),
                "PHASE_B CURRENT": (4594, 4),
                "PHASE_C CURRENT": (4598, 4),
                "THREE-PHASE AVERAGE CURRENT": (4602, 4),
                "TOTAL POWER FACTOR": (4610, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4630, 4)
            },
            5: {
                "PHASE_A VOLTAGE": (4650, 4),
                "PHASE_B VOLTAGE": (4654, 4),
                "PHASE_C VOLTAGE": (4658, 4),
                "AVERAGE PHASE VOLTAGE": (4662, 4),
                "A-B LINE VOLTAGE": (4666, 4),
                "B-C LINE VOLTAGE": (4670, 4),
                "C-A LINE VOLTAGE": (4674, 4),
                "AVERAGE LINE VOLTAGE": (4678, 4),
                "FREQUENCY": (4685, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4689, 4),
                "PHASE_A CURRENT": (4700, 4),
                "PHASE_B CURRENT": (4704, 4),
                "PHASE_C CURRENT": (4708, 4),
                "THREE-PHASE AVERAGE CURRENT": (4712, 4),
                "TOTAL POWER FACTOR": (4720, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4740, 4)
            },
            6: {
                "PHASE_A VOLTAGE": (4760, 4),
                "PHASE_B VOLTAGE": (4764, 4),
                "PHASE_C VOLTAGE": (4768, 4),
                "AVERAGE PHASE VOLTAGE": (4772, 4),
                "A-B LINE VOLTAGE": (4776, 4),
                "B-C LINE VOLTAGE": (4780, 4),
                "C-A LINE VOLTAGE": (4784, 4),
                "AVERAGE LINE VOLTAGE": (4788, 4),
                "FREQUENCY": (4795, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4799, 4),
                "PHASE_A CURRENT": (4810, 4),
                "PHASE_B CURRENT": (4814, 4),
                "PHASE_C CURRENT": (4818, 4),
                "THREE-PHASE AVERAGE CURRENT": (4822, 4),
                "TOTAL POWER FACTOR": (4830, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4850, 4)
            },
            7: {
                "PHASE_A VOLTAGE": (4870, 4),
                "PHASE_B VOLTAGE": (4874, 4),
                "PHASE_C VOLTAGE": (4878, 4),
                "AVERAGE PHASE VOLTAGE": (4882, 4),
                "A-B LINE VOLTAGE": (4886, 4),
                "B-C LINE VOLTAGE": (4890, 4),
                "C-A LINE VOLTAGE": (4894, 4),
                "AVERAGE LINE VOLTAGE": (4898, 4),
                "FREQUENCY": (4905, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4909, 4),
                "PHASE_A CURRENT": (4920, 4),
                "PHASE_B CURRENT": (4924, 4),
                "PHASE_C CURRENT": (4928, 4),
                "THREE-PHASE AVERAGE CURRENT": (4932, 4),
                "TOTAL POWER FACTOR": (4940, 4),
                "ACTIVE ENERGY 3P DELIVERED": (4960, 4)
            },
            8: {
                "PHASE_A VOLTAGE": (4980, 4),
                "PHASE_B VOLTAGE": (4984, 4),
                "PHASE_C VOLTAGE": (4988, 4),
                "AVERAGE PHASE VOLTAGE": (4992, 4),
                "A-B LINE VOLTAGE": (4996, 4),
                "B-C LINE VOLTAGE": (5000, 4),
                "C-A LINE VOLTAGE": (5004, 4),
                "AVERAGE LINE VOLTAGE": (5008, 4),
                "FREQUENCY": (5015, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (5019, 4),
                "PHASE_A CURRENT": (5035, 4),
                "PHASE_B CURRENT": (5039, 4),
                "PHASE_C CURRENT": (5043, 4),
                "THREE-PHASE AVERAGE CURRENT": (5047, 4),
                "TOTAL POWER FACTOR": (5055, 4),
                "ACTIVE ENERGY 3P DELIVERED": (5075, 4)
            },
            9: {
                "PHASE_A VOLTAGE": (6100, 4),
                "PHASE_B VOLTAGE": (6104, 4),
                "PHASE_C VOLTAGE": (6108, 4),
                "AVERAGE PHASE VOLTAGE": (6112, 4),
                "A-B LINE VOLTAGE": (6116, 4),
                "B-C LINE VOLTAGE": (6120, 4),
                "C-A LINE VOLTAGE": (6124, 4),
                "AVERAGE LINE VOLTAGE": (6128, 4),
                "FREQUENCY": (6135, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6139, 4),
                "PHASE_A CURRENT": (6150, 4),
                "PHASE_B CURRENT": (6154, 4),
                "PHASE_C CURRENT": (6158, 4),
                "THREE-PHASE AVERAGE CURRENT": (6162, 4),
                "TOTAL POWER FACTOR": (6170, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6190, 4)
            },
            10: {
                "PHASE_A VOLTAGE": (6210, 4),
                "PHASE_B VOLTAGE": (6214, 4),
                "PHASE_C VOLTAGE": (6218, 4),
                "AVERAGE PHASE VOLTAGE": (6222, 4),
                "A-B LINE VOLTAGE": (6226, 4),
                "B-C LINE VOLTAGE": (6230, 4),
                "C-A LINE VOLTAGE": (6234, 4),
                "AVERAGE LINE VOLTAGE": (6238, 4),
                "FREQUENCY": (6245, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6249, 4),
                "PHASE_A CURRENT": (6260, 4),
                "PHASE_B CURRENT": (6264, 4),
                "PHASE_C CURRENT": (6268, 4),
                "THREE-PHASE AVERAGE CURRENT": (6272, 4),
                "TOTAL POWER FACTOR": (6280, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6300, 4)
            },
            11: {
                "PHASE_A VOLTAGE": (6320, 4),
                "PHASE_B VOLTAGE": (6324, 4),
                "PHASE_C VOLTAGE": (6328, 4),
                "AVERAGE PHASE VOLTAGE": (6332, 4),
                "A-B LINE VOLTAGE": (6336, 4),
                "B-C LINE VOLTAGE": (6340, 4),
                "C-A LINE VOLTAGE": (6344, 4),
                "AVERAGE LINE VOLTAGE": (6348, 4),
                "FREQUENCY": (6355, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6359, 4),
                "PHASE_A CURRENT": (6370, 4),
                "PHASE_B CURRENT": (6374, 4),
                "PHASE_C CURRENT": (6378, 4),
                "THREE-PHASE AVERAGE CURRENT": (6382, 4),
                "TOTAL POWER FACTOR": (6390, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6410, 4)
            },
            12: {
                "PHASE_A VOLTAGE": (6430, 4),
                "PHASE_B VOLTAGE": (6434, 4),
                "PHASE_C VOLTAGE": (6438, 4),
                "AVERAGE PHASE VOLTAGE": (6442, 4),
                "A-B LINE VOLTAGE": (6446, 4),
                "B-C LINE VOLTAGE": (6450, 4),
                "C-A LINE VOLTAGE": (6454, 4),
                "AVERAGE LINE VOLTAGE": (6458, 4),
                "FREQUENCY": (6465, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6469, 4),
                "PHASE_A CURRENT": (6485, 4),
                "PHASE_B CURRENT": (6489, 4),
                "PHASE_C CURRENT": (6493, 4),
                "THREE-PHASE AVERAGE CURRENT": (6497, 4),
                "TOTAL POWER FACTOR": (6500, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6520, 4)
            },
            13: {
                "PHASE_A VOLTAGE": (6540, 4),
                "PHASE_B VOLTAGE": (6544, 4),
                "PHASE_C VOLTAGE": (6548, 4),
                "AVERAGE PHASE VOLTAGE": (6552, 4),
                "A-B LINE VOLTAGE": (6556, 4),
                "B-C LINE VOLTAGE": (6560, 4),
                "C-A LINE VOLTAGE": (6564, 4),
                "AVERAGE LINE VOLTAGE": (6568, 4),
                "FREQUENCY": (6580, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6584, 4),
                "PHASE_A CURRENT": (6600, 4),
                "PHASE_B CURRENT": (6604, 4),
                "PHASE_C CURRENT": (6608, 4),
                "THREE-PHASE AVERAGE CURRENT": (6612, 4),
                "TOTAL POWER FACTOR": (6620, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6645, 4)
            },
            14: {
                "PHASE_A VOLTAGE": (6665, 4),
                "PHASE_B VOLTAGE": (6669, 4),
                "PHASE_C VOLTAGE": (6673, 4),
                "AVERAGE PHASE VOLTAGE": (6677, 4),
                "A-B LINE VOLTAGE": (6681, 4),
                "B-C LINE VOLTAGE": (6685, 4),
                "C-A LINE VOLTAGE": (6689, 4),
                "AVERAGE LINE VOLTAGE": (6693, 4),
                "FREQUENCY": (6710, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (6714, 4),
                "PHASE_A CURRENT": (6725, 4),
                "PHASE_B CURRENT": (6729, 4),
                "PHASE_C CURRENT": (6733, 4),
                "THREE-PHASE AVERAGE CURRENT": (6737, 4),
                "TOTAL POWER FACTOR": (6745, 4),
                "ACTIVE ENERGY 3P DELIVERED": (6765, 4)
            }
        }

        # For backwards compatibility, retain the original categorized structure
        self.registers = {
            "voltage": {
                "PHASE_A VOLTAGE": (4200, 4),
                "PHASE_B VOLTAGE": (4204, 4),
                "PHASE_C VOLTAGE": (4208, 4),
                "AVERAGE PHASE VOLTAGE": (4212, 4),
                "A-B LINE VOLTAGE": (4216, 4),
                "B-C LINE VOLTAGE": (4220, 4),
                "C-A LINE VOLTAGE": (4224, 4),
                "AVERAGE LINE VOLTAGE": (4228, 4)
            },
            "frequency": {
                "FREQUENCY": (4240, 4),
                "TOTAL INSTANTANEOUS ACTIVE POWER": (4244, 4)
            },
            "current": {
                "PHASE_A CURRENT": (4255, 4),
                "PHASE_B CURRENT": (4259, 4),
                "PHASE_C CURRENT": (4263, 4),
                "THREE-PHASE AVERAGE CURRENT": (4267, 4)
            },
            "power": {
                "TOTAL POWER FACTOR": (4275, 4),
            },
            "energy": {
                "ACTIVE ENERGY 3P DELIVERED": (4295, 4)
            }
        }
    DatabaseManager.setup_database()

    def convert_d_address(self, d_number):
        return d_number + 400001 - 1

    def read_multiple_d_registers(self, start_d_number, num_registers):
        try:
            modbus_address = self.convert_d_address(start_d_number)
            values = self.instrument.read_registers(modbus_address - 400001, num_registers)
            return values
        except Exception as e:
            print(f"Error reading registers: {str(e)}")
            return None

    def convert_raw_bytes_to_float(self, reg1, reg2, reg3, reg4):
        try:
            bytes_val = struct.pack('BBBB', reg2, reg1, reg4, reg3)
            float_val = struct.unpack('<f', bytes_val)[0]
            return float_val
        except Exception as e:
            print(f"Error converting raw bytes to float: {str(e)}")
            return None

    def convert_registers_to_kWh(self, registers):
        try:
            # bytes_val = struct.pack('BBBB', reg2, reg1, reg4, reg3)
            # uint_val = struct.unpack('<I', bytes_val)[0]
            # return uint_val

            # Combine the registers into a single 32-bit integer
            value = (registers[0] << 24) + (registers[1] << 16) + (registers[2] << 8) + registers[3]
            kWh = value / 1000.0  # Assuming the energy is represented in thousandths of kWh

            return kWh
        except Exception as e:
            print(f"Error: {e}")
            return None

    def read_and_convert_raw_bytes(self, category):
        results = []
        for name, (start_d_number, num_registers) in self.registers[category].items():
            values = self.read_multiple_d_registers(start_d_number, num_registers)
            if values and len(values) >= 4:  # Ensure we have at least 4 values
                print(f"Raw values for {name}: {values}")  # Debugging output
                float_value = self.convert_raw_bytes_to_float(*values[:4])  # Read first 4 registers
                results.append((name, float_value))
            else:
                results.append((name, "Failed to read or insufficient values"))
        return results

    def read_meter_values(self, meter_id):
        results = []
        if meter_id not in self.meters:
            print(f"Meter ID {meter_id} not found")
            return results

        meter_data = self.meters[meter_id]
        for name, (start_d_number, num_registers) in meter_data.items():
            values = self.read_multiple_d_registers(start_d_number, num_registers)
            if values and len(values) >= 4:  # Ensure we have at least 4 values
                print(f"Meter {meter_id} - Raw values for {name}: {values}")  # Debugging output

                # Special case for ACTIVE ENERGY 3P DELIVERED
                if name == "ACTIVE ENERGY 3P DELIVERED":
                    # Directly process the first two registers as the value
                    float_value = values[0] + (values[1] / 1000.0)
                    float_value = f"{float_value:.3f}"  # Combine the first two registers // its a kwh
                else:
                    # Convert the first 4 registers to a float for other readings
                    float_value = self.convert_raw_bytes_to_float(*values[:4])

                results.append((name, float_value))
            else:
                results.append((name, "Failed to read or insufficient values"))
        return results

    def read_continuously(self, interval=5.0, meters_to_read=None):
        """
        Continuously read values from specified meters and save to database

        :param interval: Time between readings in seconds
        :param meters_to_read: List of meter IDs to read. If None, read all meters
        """
        if meters_to_read is None:
            meters_to_read = list(self.meters.keys())

        try:
            while True:
                for meter_id in meters_to_read:
                    try:
                        readings = self.read_meter_values(meter_id)

                        # Print readings
                        print(f"\n=== Meter {meter_id} Readings ===")
                        for name, value in readings:
                            print(f"{name}: {value}")

                        # Insert into database
                        DatabaseManager.insert_meter_readings(meter_id, readings)

                    except Exception as e:
                        logger.error(f"Error processing Meter {meter_id}: {e}")

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Stopping continuous read...")
        except Exception as e:
            logger.error(f"Error during continuous read: {str(e)}")


def main():
    # Initialize database connection pool
    DatabaseManager.initialize_pool()

    # Create PLC reader
    plc = DeltaPLCReader(port='COM5')

    # Read all meters or specify specific meters
    plc.read_continuously(
        interval=5.0,
        meters_to_read=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    )


if __name__ == "__main__":
    plc = DeltaPLCReader(port='COM5')

    # Read all meters
    # plc.read_continuously(interval=5.0)

    # Or specify which meters to read
    # plc.read_continuously(interval=5.0, meters_to_read=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])
    plc.read_continuously(interval=5.0, meters_to_read=[14])




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


if __name__ == '__main__':
    try:
        connect_to_db()
        print("Database Binded Successfully")
    except Exception as e:
        print(f"Error generating mapping: {e}")
        exit(1)

    with open("config/opcua_settings.json", "r") as file:
        config = json.load(file)['opcua'][0]

    client = Client(f"opc.tcp://{config['ip_address']}:{config['port']}")
    client.set_user(config['username'])
    client.set_password(config['password'])

    retry_delay = 60

    while True:
        pass

import os
from dotenv import load_dotenv


load_dotenv()

TEST = True

MQTT_IP = os.getenv("MQTT_IP")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
REDIS_IP = os.getenv("REDIS_IP")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))
ASMX_URL = os.getenv("ASMX_URL")

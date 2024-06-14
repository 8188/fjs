import os
from dotenv import load_dotenv

load_dotenv()

def get_my_env(para: str) -> str:
    res = os.getenv(para)
    assert res is not None
    return res

MQTT_IP = get_my_env("MQTT_IP")
MQTT_PORT = int(get_my_env("MQTT_PORT"))
MQTT_USERNAME = get_my_env("MQTT_USERNAME")
MQTT_PASSWORD = get_my_env("MQTT_PASSWORD")

REDIS_IP = get_my_env("REDIS_IP")
REDIS_PORT = int(get_my_env("REDIS_PORT"))
REDIS_DB = int(get_my_env("REDIS_DB"))
REDIS_USERNAME = get_my_env("REDIS_USERNAME")
REDIS_PASSWORD = get_my_env("REDIS_PASSWORD")
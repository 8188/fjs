import os
from dotenv import load_dotenv

load_dotenv()

def get_my_env(para: str) -> str:
    res = os.getenv(para)
    assert res is not None
    return res

MQTT_IP = get_my_env("MQTT_IP")
MQTT_PORT = int(get_my_env("MQTT_PORT"))
REDIS_IP = get_my_env("REDIS_IP")
REDIS_PORT = int(get_my_env("REDIS_PORT"))
REDIS_DB = int(get_my_env("REDIS_DB"))

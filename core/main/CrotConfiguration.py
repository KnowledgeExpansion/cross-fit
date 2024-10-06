import json
import sys
import os

""" Setting Parameters """
ROOT_PATH = r'C:\Crot'

if not os.path.exists(ROOT_PATH):
    os.makedirs(ROOT_PATH)

try:
    temp_path = sys.argv
    if len(temp_path) > 2 and temp_path[2] and temp_path[2].find("_config.json"):
        if temp_path[2].find(ROOT_PATH):
            conf_path = temp_path[2]
        else:
            conf_path = os.path.join(ROOT_PATH, temp_path[2])
    else:
        conf_path = os.path.join(ROOT_PATH, 'crot_config.json')
    with open(conf_path, 'r') as fp:
        conf = json.load(fp)

except Exception as ex:
    print(ex)
    conf_path = ""
    conf = dict()

print("=============================================")
print("Loaded settings.py")
print("ROOT_PATH : {}".format(ROOT_PATH))
print("conf_path : {}".format(conf_path))
print("=============================================")

# Connection Settings
KAFKA_ADDR = conf.get('kafka_addr') or 'localhost'
KAFKA_PORT = conf.get('kafka_port') or '8082'

# REDIS_PASSWORD = '9qX$UZ9&T?8A'
KAFKA_PASSWORD = False
KAFKA_PROTECTED = 'no'


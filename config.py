# config.py

INFLUXDB_SETTINGS = {
    "address": "influxdb",
    "port": 8086,
    "user": "root",
    "password": "root",
    "database": "home_db",
}

MQTT_SETTINGS = {
    "address": "mosquitto",
    "port": 1883,
    "user": "mqttuser",
    "password": "mqttpassword",
    "client_id": "MQTT2InfluxBridge2",
}

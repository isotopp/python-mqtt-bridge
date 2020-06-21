#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge

This script receives MQTT data and saves those to InfluxDB.

"""

import paho.mqtt.client as mqtt  # type: ignore
from influxdb import InfluxDBClient  # type: ignore
import json
from time import sleep

INFLUXDB_ADDRESS = "influxdb"
INFLUXDB_USER = "root"
INFLUXDB_PASSWORD = "root"
INFLUXDB_DATABASE = "home_db"

influx = None

MQTT_ADDRESS = "mosquitto"
MQTT_USER = "mqttuser"
MQTT_PASSWORD = "mqttpassword"
MQTT_CLIENT_ID = "MQTT2InfluxBridge2"


class MyUserdata:
    def __init__(self, influx):
        self.influx = influx
        self.device = ""
        self.tail = ""
        self.payload = ""
        self.writepoints = ""

    def __str__(self):
        return f"MyUserData: device = {device} tail = {tail} payload = {payload} writepoints = {writepoints}"

    def _generic_message(self, measurement, device, tail, payload):
        self.device = device
        self.tail = tail
        self.payload = payload

        fields = {}
        for i in payload:
            try:
                fields[i.lower()] = float(payload[i])
            except ValueError as e:
                fields[i.lower()] = payload[i]

        json_body = [
            {
                "measurement": measurement,
                "tags": {"location": device},
                "fields": fields,
            }
        ]
        self.writepoints = json_body
        self.influx.write_points(json_body)

    def house_message(self, device, tail, payload):
        payload = payload["ENERGY"]
        self._generic_message("plug", device, tail, payload)

    def zigbee2mqtt_message(self, device, tail, payload):
        self._generic_message("temp", device, tail, payload)

    def mijia_message(self, device, tail, payload):
        self._generic_message("mijia", device, tail, payload)

    def on_message(self, client, userdata, msg):
        """ Handle message passed on from on_message callback.

        Same parameters as normal mqtt on_message callback.

        message topic is split into route, device and tail part.
        We pass on the entire thing according to the registered routes.

        """
        print(f"topic = {msg.topic}")
        route, device, tail = msg.topic.split(sep="/", maxsplit=2)
        payload = json.loads(msg.payload.decode("utf-8"))

        if route == "house":
            self.house_message(device, tail, payload)
        elif route == "zigbee2mqtt":
            self.zigbee2mqtt_message(device, tail, payload)
        elif route == "mijia":
            self.mijia_message(device, tail, payload)
        else:
            raise (f"unhandled route {route}: {msg.topic} = {payload}")


def influx_init():
    influx = influx_connect()
    influx_setup_database(influx)

    return influx


def influx_connect():
    """ Build a influx to InfluxDB """
    influx = InfluxDBClient(
        INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None
    )
    return influx


def influx_setup_database(influx):
    """ If our database does not exist, we make it """

    # databases = [{'name': '_internal'}, {'name': 'home_db'}]
    databases = influx.get_list_database()

    # home_db does not exist? Make it, and a retention policy
    if INFLUXDB_DATABASE not in map(lambda x: x["name"], databases):
        influx.create_database(INFLUXDB_DATABASE)
        influx.create_retention_policy(
            name="one_year",
            duration="52w",
            replication=1,
            database=INFLUXDB_DATABASE,
            default=True,
            shard_duration="1w",
        )

    # home_db now exists and has a retention policy
    influx.switch_database(INFLUXDB_DATABASE)


def on_log(client, userdata, level, buf):
    """ mqtt log callback """
    print(f"on_log: {userdata}")
    if level in (mqtt.MQTT_LOG_ERR, mqtt.MQTT_LOG_WARNING):
        print(f"on_log: {level} {buf}")


def on_message(client, userdata, msg):
    """ mqtt message callback """
    userdata.on_message(client, userdata, msg)


def on_connect(client, userdata, flags, rc):
    print(f"on_connect: rc = {rc} flags = {flags}")
    client.subscribe("house/+/tele/SENSOR")
    client.subscribe("zigbee2mqtt/+/SENSOR")
    client.subscribe("mijia/+/SENSOR")


def main():
    sleep(3)
    influx = influx_init()

    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, userdata=MyUserdata(influx))
    #    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_message = on_message
    mqtt_client.on_log = on_log
    mqtt_client.on_connect = on_connect

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == "__main__":
    print("bridge: start")
    main()

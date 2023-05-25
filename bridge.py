#! /usr/bin/env python

from mqttbridge import setup_influxdb, setup_mqtt, MessageProcessor, MQTTInfluxBridge
from config import INFLUXDB_SETTINGS, MQTT_SETTINGS
from sys import exit


def main():
    influx_client = setup_influxdb(INFLUXDB_SETTINGS)
    message_processor = MessageProcessor(influx_client)

    mqtt_client = setup_mqtt(MQTT_SETTINGS)
    if mqtt_client is None:
        print("Could not connect to MQTT.")
        exit(1)

    bridge = MQTTInfluxBridge(mqtt_client, message_processor)

    mqtt_client.on_message = bridge.on_message
    mqtt_client.on_connect = bridge.on_connect

    mqtt_client.loop_forever()


if __name__ == "__main__":
    print("bridge: start")
    main()

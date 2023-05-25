#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge

This script receives MQTT data and saves those to InfluxDB.

"""

from paho.mqtt import client as mqtt
from influxdb import InfluxDBClient
from typing import Optional
import json


class MessageProcessor:
    """Process a MQTT message and write it into InfluxDB.

    The class must be provided with an InfluxDBClient instance as a database connection.
    """

    influx_client: InfluxDBClient

    def __init__(self, influx_client: InfluxDBClient):
        """influx_client: connection to an Influx database."""
        self.influx_client = influx_client

    def process_message(self, msg: mqtt.MQTTMessage) -> None:
        """An "on_message" handler for MQTT message.

        The method contains business rules that define how we transform
        an incoming message, and routes data to the metrics we want.

        msg: the message.
        """
        print(f"topic = {msg.topic}")
        route, device, tail = msg.topic.split(sep="/", maxsplit=2)
        try:
            payload: dict = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON payload received in message {msg.topic}")
            return

        if route == "house":
            self._process_generic_message("plug", device, tail, payload["ENERGY"])
        elif route == "zigbee2mqtt":
            self._process_generic_message("temp", device, tail, payload)
        elif route == "mijia":
            self._process_generic_message("mijia", device, tail, payload)
        elif route == "p1-mqtt":
            self._process_generic_message("p1", device, tail, payload)
        else:
            raise Exception(f"unhandled route {route}: {msg.topic} = {payload}")

    def _process_generic_message(
        self, measurement: str, device: str, tail: str, payload: dict
    ) -> None:
        """construct a JSON payload and write it to the influx_client.

        measurement: the influxdb measurement.
        device: the influxdb tag { "location": device }
        tail: not used
        payload: keys transformed to lowercase, values transformed to float, if possible.
        """
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
        self.influx_client.write_points(json_body)


class MQTTInfluxBridge:
    """Connect a MQTT client to a MessageProcessor instance."""

    mqtt_client: mqtt.Client
    message_processor: MessageProcessor

    def __init__(self, mqtt_client: mqtt.Client, message_processor: MessageProcessor):
        self.mqtt_client = mqtt_client
        self.message_processor = message_processor

    def on_connect(self, client: mqtt.Client, userdata: None, flags: int, rc: int):
        if rc != 0:
            print(f"Error: Failed to connect to MQTT broker (error code {rc})")
            return print(f"on_connect: rc = {rc} flags = {flags}")

        # Our subscriptions
        self.mqtt_client.subscribe("house/+/tele/SENSOR")
        self.mqtt_client.subscribe("zigbee2mqtt/+/SENSOR")
        self.mqtt_client.subscribe("mijia/+/SENSOR")
        self.mqtt_client.subscribe("p1-mqtt/+/tele/SENSOR")

    def on_message(self, client: mqtt.Client, userdata: None, msg: mqtt.MQTTMessage):
        self.message_processor.process_message(msg)


# The rest of the code remains the same.


def setup_influxdb(settings) -> Optional[InfluxDBClient]:
    try:
        influx = InfluxDBClient(
            settings["address"],
            settings["port"],
            settings["user"],
            settings["password"],
            None,
        )
    except Exception as e:
        print(f"Error: Failed to connect to InfluxDB: {e}")
        return None

    databases = influx.get_list_database()
    if settings["database"] not in map(lambda x: x["name"], databases):
        influx.create_database(settings["database"])
        influx.create_retention_policy(
            name="one_year",
            duration="52w",
            replication="1",
            database=settings["database"],
            default=True,
            shard_duration="1w",
        )
    influx.switch_database(settings["database"])
    return influx


def setup_mqtt(settings) -> Optional[mqtt.Client]:
    mqtt_client = mqtt.Client(client_id=settings["client_id"])
    try:
        mqtt_client.connect(settings["address"], settings["port"])
    except Exception as e:
        print(f"Error: Failed to connect to MQTT broker: {e}")
        return None

    return mqtt_client

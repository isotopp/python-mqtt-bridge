FROM python:3.8-alpine

LABEL maintainer="isotopp" \
      description="MQTT to InfluxDB Bridge"

COPY requirements-frozen.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /app
WORKDIR /app

CMD [ "python3", "-u", "bridge.py" ]

"""

Module provide convenient tools to track metrics.

Metrics are being sent via UDP protocol to Telegraf client running locally (it will perform further
data processing and/or aggregation if required) or directly to InfluxDB.

From InfluxDB documentation::

    First, a short primer on the datastore. Data in InfluxDB is organized by "time series", which
    contain a measured value, like "cpu_load" or "temperature". Time series have zero to many
    points, one for each discrete sample of the metric. Points consist of time (a timestamp), a
    measurement ("cpu_load", for example), at least one key-value field (the measured value itself,
    e.g. "value=0.64", or "temperature=21.2"), and zero to many key-value tags containing any
    metadata about the value (e.g. "host=server01", "region=EMEA", "dc=Frankfurt").

    Conceptually you can think of a measurement as an SQL table, where the primary index is always
    time. tags and fields are effectively columns in the table. tags are indexed, and fields are
    not. The difference is that, with InfluxDB, you can have millions of measurements, you don't
    have to define schemas up-front, and null values aren't stored.

It is recommended to name metrics using underscore as a separator: ``task_duration``,
``contacts_import`` and distinguish different objects within the same metric via tags.

Usage:

    from system.metrics import metric

    # Store single value
    metric('room_temp', 12.3, tags={'room': room, 'building': building})

    # Store several values
    metric('room_stats', {'temp': 10, 'humid': 12.4}, tags={'room': room}, sample_rate=0.5)

"""

import logging
import os
import random
from typing import Union

from telegraf.client import TelegrafClient

log = logging.getLogger(__name__)

TELEGRAF_HOST = os.environ.get('TELEGRAF_HOST', '127.0.0.1'),
TELEGRAF_PORT = int(os.environ.get('TELEGRAF_PORT', 8092)),

# It's UPD, so it should be safe to create it initially
metric_client = TelegrafClient(host=TELEGRAF_HOST, port=TELEGRAF_PORT)
log.info(f'Connecting to {TELEGRAF_HOST}:{TELEGRAF_PORT}')


def metric(metric_name: str, values: Union[int, float, dict], tags: dict = None, sample_rate=1):
    """
    Submit single metric with one or several values.

    :param metric_name: Name of metric to report
    :param values: Can be a single value (int or float) or dict of {name: value}.
    :param tags: dict of tags to apply to this metric
    :param float sample_rate: sample rate (1 - send all, 0 - don't send)

    """
    assert 0 <= sample_rate <= 1, 'Sample rate should be within [0..1]'

    if sample_rate and random.random() <= sample_rate:
        metric_client.metric(metric_name, values, tags=tags)

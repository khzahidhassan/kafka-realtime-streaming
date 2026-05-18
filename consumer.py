"""
consumer.py
===========
Reads prediction messages from the 'predictions' topic and prints
each result to the console in a readable format as it arrives.

Run in Terminal 3:
    python consumer.py
"""

import json
from kafka import KafkaConsumer
from config import BOOTSTRAP_SERVER, API_KEY, API_SECRET, PREDICTIONS_TOPIC

# ── Connect to Confluent Cloud ───────────────────────────────────────────────
print("Connecting to Kafka and waiting for predictions...\n")
print("=" * 60)

consumer = KafkaConsumer(
    PREDICTIONS_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=API_KEY,
    sasl_plain_password=API_SECRET,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='output-consumer-group',
    api_version_auto_timeout_ms=30000,
)

# ── Print each prediction as it arrives ──────────────────────────────────────
count = 0
for message in consumer:
    count += 1
    data = message.value
    print(f"[#{count}]  Hour: {data.get('hour', '?'):>2}  |  "
          f"Actual: {data.get('actual_cnt', '?'):>4}  |  "
          f"Predicted: {data.get('predicted_cnt', '?'):>7.2f}  |  "
          f"Error: {data.get('error', '?'):>6.2f}")

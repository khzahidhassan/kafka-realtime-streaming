import json
import time
import pandas as pd
from kafka import KafkaProducer
from config import BOOTSTRAP_SERVER, API_KEY, API_SECRET, RAW_TOPIC

print("Connecting to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=API_KEY,
    sasl_plain_password=API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version_auto_timeout_ms=30000,
)
print(f"Connected! Sending data to topic: {RAW_TOPIC}\n")

df = pd.read_csv("hour_clean.csv")
total = len(df)

for idx, row in df.iterrows():
    message = row.to_dict()
    producer.send(RAW_TOPIC, value=message)
    print(f"[{idx+1}/{total}] Sent: hr={int(message['hr'])}, "
          f"temp={message['temp']:.2f}, actual_cnt={int(message['cnt'])}")
    time.sleep(1)

producer.flush()
print("\nAll rows sent.")

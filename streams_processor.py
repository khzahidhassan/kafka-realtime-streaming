import ssl
import json
import joblib
import numpy as np
import faust
from config import BOOTSTRAP_SERVER, API_KEY, API_SECRET, RAW_TOPIC, PREDICTIONS_TOPIC

ssl_ctx = ssl.create_default_context()

app = faust.App(
    'bike-predictor',
    broker=f'kafka://{BOOTSTRAP_SERVER}',
    broker_credentials=faust.SASLCredentials(
        username=API_KEY,
        password=API_SECRET,
        mechanism='PLAIN',
        ssl_context=ssl_ctx,
    ),
    value_serializer='raw',
    topic_replication_factor=3,
)

raw_topic = app.topic(RAW_TOPIC, value_type=bytes)
predictions_topic = app.topic(PREDICTIONS_TOPIC, value_type=bytes)

print("Loading model...")
model = joblib.load("bike_model.pkl")
print("Model ready.\n")

FEATURES = [
    'season', 'yr', 'mnth', 'hr', 'holiday',
    'weekday', 'workingday', 'weathersit',
    'temp', 'atemp', 'hum', 'windspeed'
]

@app.agent(raw_topic)
async def predict(stream):
    async for message in stream:
        try:
            record = json.loads(message)
            features = np.array([[record[f] for f in FEATURES]])
            predicted = round(float(model.predict(features)[0]), 2)

            result = {
                'hour': int(record.get('hr', -1)),
                'temp': round(float(record.get('temp', 0)), 4),
                'actual_cnt': int(record.get('cnt', -1)),
                'predicted_cnt': predicted,
                'error': round(abs(predicted - int(record.get('cnt', 0))), 2)
            }

            await predictions_topic.send(value=json.dumps(result).encode())

            print(f"[AGENT] hr={result['hour']:>2} | "
                  f"actual={result['actual_cnt']:>4} | "
                  f"predicted={result['predicted_cnt']:>7.2f} | "
                  f"error={result['error']:>6.2f}")

        except Exception as e:
            print(f"[ERROR] {e}")
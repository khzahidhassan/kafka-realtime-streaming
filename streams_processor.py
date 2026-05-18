"""
streams_processor.py
====================
Kafka Streams Processor using the Agent pattern.

Implements the same conceptual model as Apache Faust:
  - StreamsApp with @app.agent decorator
  - Async generator-based stream processing
  - Concurrent agent execution via asyncio
  - Source topic -> ML inference -> sink topic pipeline

This implementation uses kafka-python as the transport layer
due to Python 3.14 / aiokafka compatibility constraints, while
preserving the full Faust Streams Agent architecture.

Run in Terminal 2:
    python streams_processor.py
"""

import asyncio
import json
import joblib
from kafka import KafkaConsumer, KafkaProducer
from config import BOOTSTRAP_SERVER, API_KEY, API_SECRET, RAW_TOPIC, PREDICTIONS_TOPIC


# ──────────────────────────────────────────────────────────────────────────────
# Minimal Streams Application — mirrors the Faust App + Agent pattern
# ──────────────────────────────────────────────────────────────────────────────
class StreamsApp:
    """
    Lightweight Streams application implementing the Faust agent pattern:
      - app.topic()  : declares a source or sink topic
      - @app.agent() : registers an async stream-processing agent
      - app.main()   : starts all agents concurrently via asyncio
    """

    def __init__(self, name, bootstrap_servers, sasl_username,
                 sasl_password, group_id='streams-group'):
        self.name          = name
        self.bootstrap     = bootstrap_servers
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.group_id      = group_id
        self._agents       = []
        self._producer     = None

    def topic(self, name, **kwargs):
        return name

    def agent(self, source_topic):
        """Register an async generator function as a stream processing agent."""
        def decorator(fn):
            self._agents.append((source_topic, fn))
            return fn
        return decorator

    def _get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                security_protocol='SASL_SSL',
                sasl_mechanism='PLAIN',
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
        return self._producer

    async def send(self, topic_name, value):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, lambda: self._get_producer().send(topic_name, value=value)
        )

    async def _stream(self, topic_name):
        loop = asyncio.get_event_loop()
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap,
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=self.group_id,
            enable_auto_commit=True,
        )
        print(f"[StreamsApp] Agent listening on topic: {topic_name}")
        try:
            while True:
                batch = await loop.run_in_executor(
                    None, lambda: consumer.poll(timeout_ms=500, max_records=5)
                )
                for _, messages in batch.items():
                    for msg in messages:
                        yield msg.value
                await asyncio.sleep(0.01)
        finally:
            consumer.close()

    async def _run_agent(self, source_topic, agent_fn):
        await agent_fn(self._stream(source_topic))

    def main(self):
        async def _run_all():
            tasks = [
                asyncio.create_task(self._run_agent(t, fn))
                for t, fn in self._agents
            ]
            await asyncio.gather(*tasks)

        print(f"[StreamsApp '{self.name}'] Starting agents...")
        asyncio.run(_run_all())


# ──────────────────────────────────────────────────────────────────────────────
# Application setup
# ──────────────────────────────────────────────────────────────────────────────
app = StreamsApp(
    name              = 'bike-predictor',
    bootstrap_servers = BOOTSTRAP_SERVER,
    sasl_username     = API_KEY,
    sasl_password     = API_SECRET,
    group_id          = 'streams-processor-group',
)

raw_topic         = app.topic(RAW_TOPIC)
predictions_topic = app.topic(PREDICTIONS_TOPIC)

# ── Load ML model once at startup ─────────────────────────────────────────────
print("Loading ML model...")
model = joblib.load("bike_model.pkl")
print("Model loaded.\n")

FEATURES = ['season', 'yr', 'mnth', 'hr', 'holiday',
            'weekday', 'workingday', 'weathersit',
            'temp', 'atemp', 'hum', 'windspeed']


# ──────────────────────────────────────────────────────────────────────────────
# Streams Agent  (@app.agent mirrors Faust agent architecture exactly)
# ──────────────────────────────────────────────────────────────────────────────
@app.agent(raw_topic)
async def predict(stream):
    """
    Stream processing agent.
    Consumes each raw record, runs ML inference, publishes prediction.
    """
    async for record in stream:
        try:
            features   = [[record[f] for f in FEATURES]]
            prediction = round(float(model.predict(features)[0]), 2)

            output = {
                'hour'         : int(record.get('hr', -1)),
                'temp'         : round(float(record.get('temp', 0)), 4),
                'actual_cnt'   : int(record.get('cnt', -1)),
                'predicted_cnt': prediction,
                'error'        : round(abs(prediction - int(record.get('cnt', 0))), 2),
            }

            await app.send(predictions_topic, output)
            print(f"[AGENT] hr={output['hour']:>2} | "
                  f"actual={output['actual_cnt']:>4} | "
                  f"predicted={output['predicted_cnt']:>7.2f} | "
                  f"error={output['error']:>6.2f}")

        except Exception as e:
            print(f"[AGENT ERROR] {e}")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    app.main()

# Real-Time Streaming with Apache Kafka — Bike Sharing Predictor

## What This Project Does
This project builds a real-time ML inference pipeline using Apache Kafka and Python.
Bike rental data is streamed row by row through Kafka topics. A Streams Processor
reads each record, runs a trained Random Forest model to predict hourly bike rentals,
and publishes the prediction to an output topic. An output consumer reads and displays
predictions live as they arrive.

```
[Producer] --> [Kafka: raw-data] --> [Streams Processor] --> [Kafka: predictions] --> [Consumer]
Sends bike       (Confluent Cloud)    Runs ML model,          (Confluent Cloud)       Prints live
data row by row                       predicts rentals                                predictions
```

## Dataset
- **Name:** Bike Sharing Dataset (Hourly)
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset)
- **Size:** 17,379 hourly records (2011–2012)
- **ML Task:** Predict hourly bike rental count (`cnt`) using weather and time features

## Streams Library
- **Python + kafka-python** with a custom `StreamsApp` class implementing the Faust Agent pattern
- `StreamsApp.topic()` declares source and sink topics
- `@app.agent()` decorator registers async stream-processing agents
- `asyncio.gather()` runs agents concurrently
- Designed to mirror the Faust Streams architecture (Producer → Agent → Consumer topology)

## ML Model
- **Algorithm:** Random Forest Regressor (100 estimators)
- **Features:** season, year, month, hour, holiday, weekday, workingday, weathersit, temp, atemp, humidity, windspeed
- **Target:** `cnt` (total hourly bike rentals)
- **R² Score:** 0.9441 (94.4% variance explained)
- **RMSE:** 42.07 rentals
- **Model file:** `bike_model.pkl` (joblib compressed)

> This is a regression task predicting a continuous count variable.
> R² and RMSE are the appropriate metrics (not F1-score or accuracy).

## Project Structure
```
kafka-realtime-streaming/
├── config.py               # Confluent Cloud credentials and topic names
├── train_model.py          # Downloads dataset, trains Random Forest, saves model
├── producer.py             # Reads CSV row by row, sends to raw-data topic
├── streams_processor.py    # Streams agent: reads raw-data, predicts, sends to predictions
├── consumer.py             # Reads predictions topic and prints results live
├── requirements.txt        # Python dependencies
├── bike_model.pkl          # Trained and compressed Random Forest model
└── README.md
```

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure credentials
Edit `config.py` with your Confluent Cloud bootstrap server, API key and secret.

### 3. Train the model (run once)
```bash
python train_model.py
```
This downloads the dataset, trains the model, and saves `bike_model.pkl` and `hour_clean.csv`.

## How to Run

Open **three separate terminals**, all with the virtual environment activated,
all inside the project folder. Start them in this order:

### Terminal 1 — Streams Processor (start FIRST)
```bash
python streams_processor.py
```
Wait until you see:
```
[StreamsApp 'bike-predictor'] Starting agents...
[StreamsApp] Agent listening on topic: raw-data
```

### Terminal 2 — Producer
```bash
python producer.py
```
Sends one bike rental record per second to the `raw-data` Kafka topic.

### Terminal 3 — Output Consumer
```bash
python consumer.py
```
Reads from the `predictions` topic and prints live predictions:
```
[#1]  Hour:  0  |  Actual:   16  |  Predicted:   14.23  |  Error:   1.77
[#2]  Hour:  1  |  Actual:   40  |  Predicted:   38.91  |  Error:   1.09
```

## Kafka Infrastructure
- **Provider:** Confluent Cloud (Basic cluster, AWS us-east-1)
- **Topics:** `raw-data` (input), `predictions` (output)
- **Protocol:** SASL_SSL with PLAIN mechanism

## Video Demo
[Watch the live pipeline demo (Google Drive)](https://drive.google.com/file/d/1GXT9YjBCR4ahkfl91xHrafEZJnvpAU02/view?usp=sharing)

The video shows all three components running simultaneously:
- Producer streaming bike data into Kafka
- Streams Processor consuming records and generating ML predictions
- Output Consumer printing real-time predictions with actual vs predicted counts

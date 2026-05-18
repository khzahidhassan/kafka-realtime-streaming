# Real-Time Bike Sharing Prediction with Apache Kafka

## What This Project Does
This project streams bike rental data in real time using Kafka.The producer reads the dataset line by line and pushes each record to Kafka.A Faust processor picks it up, runs a machine-learning model, and predicts how many bikes will be rented that hour.A consumer then prints the result live in the terminal as each prediction arrives.


```
[Producer] --> [Kafka: raw-data] --> [Streams Processor] --> [Kafka: predictions] --> [Consumer]
Sends bike       (Confluent Cloud)    Runs ML model,          (Confluent Cloud)       Prints live
data row by row                       predicts rentals                                predictions
```

## Dataset

- **Name:** Bike Sharing Dataset (Hourly)
- **Source:** https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset
- **Size:** 17,379 hourly records from 2011 to 2012
- **Task:** Predict how many bikes were rented each hour


## Streams Library

- **Library:** Python + Faust (faust-streaming)
- Faust app created with `faust.App()`
- `@app.agent(raw_topic)` used to listen to incoming records
- Agent reads from `raw-data`, runs the ML model, sends result to `predictions`

## ML Model

- **Algorithm:** Random Forest Regressor
- **Number of trees:** 100
- **Input features:** season, year, month, hour, holiday, weekday, workingday, weathersit, temp, atemp, humidity, windspeed
- **Target:** cnt (total bikes rented per hour)
- **R² Score:** 0.9441
- **RMSE:** 42.07 rentals
- **Model file:** bike_model.pkl

This is a regression task so R² and RMSE are used instead of accuracy or F1-score.


## Project Structure

```
kafka-realtime-streaming/
├── config.py               # Confluent Cloud credentials
├── train_model.py          # Trains and saves the ML model
├── producer.py             # Sends data to raw-data topic
├── streams_processor.py    # Faust agent: reads, predicts, sends
├── consumer.py             # Prints live predictions
├── requirements.txt        # Dependencies
├── bike_model.pkl          # Saved model file
└── README.md
```

## Setup

### 1. Install dependencies
```bash
py -3.11 -m pip install -r requirements.txt
```

### 2. Configure credentials
Open `config.py` and fill in your Confluent Cloud bootstrap server, API key, and secret.

### 3. Train the model (run once)
```bash
py -3.11 train_model.py
```
This downloads the dataset, trains the model, and saves `bike_model.pkl` and `hour_clean.csv`.


## How to Run

Open three separate terminals inside the project folder. Run in this order:

### Terminal 1 — Faust Streams Processor (start this first)
```bash
py -3.11 -m faust -A streams_processor worker -l info
```
Wait until you see `ready` before starting the others.

### Terminal 2 — Producer
```bash
py -3.11 producer.py
```

### Terminal 3 — Output Consumer
```bash
py -3.11 consumer.py
```

Expected output in Terminal 3:
```
[#1]  Hour:  0  |  Actual:   16  |  Predicted:   14.23  |  Error:   1.77
[#2]  Hour:  1  |  Actual:   40  |  Predicted:   38.91  |  Error:   1.09
```

## Kafka Infrastructure
- **Provider:** Confluent Cloud (Basic cluster, AWS us-east-1)
- **Topics:** `raw-data` (input), `predictions` (output)
- **Protocol:** SASL_SSL with PLAIN mechanism

## Video Demo

[Watch the live pipeline demo](https://drive.google.com/file/d/1hv_MSKWuP6Qc7A9TstSFqmmUzxaMsHvH/view?usp=sharing)

The video shows all three terminals running at the same time:
- Producer sending bike data into Kafka
- Faust Processor predicting rentals from each record
- Consumer printing live predictions in the terminal


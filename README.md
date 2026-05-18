# Real-Time Streaming with Apache Kafka — Bike Sharing Predictor

## Dataset
- **Name:** Bike Sharing Dataset (Hourly)
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset)
- **ML Task:** Predict hourly bike rental count (`cnt`) using weather and time features

## Streams Library
- **Python + Faust** (`faust-streaming`)
- Faust agents consume from `raw-data`, run ML inference, and produce to `predictions`

## ML Model
- **Algorithm:** Random Forest Regressor (100 trees)
- **Features:** season, year, month, hour, holiday, weekday, workingday, weathersit, temp, atemp, humidity, windspeed
- **R² Score:** _(fill in after running train_model.py)_
- **RMSE:** _(fill in after running train_model.py)_ rentals
- **Note:** This is a regression task (predicting a count), so R² and RMSE are the appropriate metrics rather than F1-score.

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Train the model (run once)
```bash
python train_model.py
```

## How to Run

Open **three separate terminals**, all with `(venv)` activated, all inside the project folder.

### Terminal 1 — Producer
```bash
python producer.py
```

### Terminal 2 — Streams Processor (Faust)
```bash
faust -A streams_processor worker -l info
```

### Terminal 3 — Output Consumer
```bash
python consumer.py
```

Start them in order: Terminal 2 first, then Terminal 1, then Terminal 3.

## Video Demo
[Link to demo video](YOUR_VIDEO_LINK_HERE)

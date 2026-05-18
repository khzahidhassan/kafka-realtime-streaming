"""
train_model.py
==============
Downloads the UCI Bike Sharing dataset, trains a Random Forest
regressor to predict hourly rental count (cnt), and saves the
trained model to bike_model.pkl.

Run this ONCE before starting the pipeline:
    python train_model.py
"""

import os
import zipfile
import urllib.request
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import math

# ── 1. Download dataset ──────────────────────────────────────────────────────
URL      = "https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip"
ZIP_PATH = "bike_sharing.zip"
DATA_DIR = "bike_data"

if not os.path.exists(DATA_DIR):
    print("Downloading Bike Sharing dataset...")
    urllib.request.urlretrieve(URL, ZIP_PATH)
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        z.extractall(DATA_DIR)
    os.remove(ZIP_PATH)
    print("Download complete.")
else:
    print("Dataset already downloaded.")

# ── 2. Load hourly data ──────────────────────────────────────────────────────
df = pd.read_csv(os.path.join(DATA_DIR, "hour.csv"))
print(f"\nDataset shape: {df.shape}")
print(df.head(3))

# ── 3. Select features and target ────────────────────────────────────────────
FEATURES = ['season', 'yr', 'mnth', 'hr', 'holiday',
            'weekday', 'workingday', 'weathersit',
            'temp', 'atemp', 'hum', 'windspeed']
TARGET   = 'cnt'

X = df[FEATURES]
y = df[TARGET]

# ── 4. Train / test split ────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ── 5. Train Random Forest ───────────────────────────────────────────────────
print("\nTraining Random Forest model...")
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)
print("Training complete.")

# ── 6. Evaluate ──────────────────────────────────────────────────────────────
y_pred = model.predict(X_test)
r2   = r2_score(y_test, y_pred)
rmse = math.sqrt(mean_squared_error(y_test, y_pred))

print(f"\n========== Model Performance ==========")
print(f"  R² Score : {r2:.4f}")
print(f"  RMSE     : {rmse:.2f} rentals")
print(f"=======================================")
print("(Save these numbers — you need them for README.md)")

# ── 7. Save model ────────────────────────────────────────────────────────────
joblib.dump(model, "bike_model.pkl")
print("\nModel saved to bike_model.pkl")

# ── 8. Save a sample of the dataset for the producer ─────────────────────────
df[FEATURES + [TARGET]].to_csv("hour_clean.csv", index=False)
print("Clean dataset saved to hour_clean.csv")

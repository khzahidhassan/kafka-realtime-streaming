import os
import zipfile
import urllib.request
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import math


URL = "https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip"

if not os.path.exists("bike_data"):
    print("Downloading dataset...")
    urllib.request.urlretrieve(URL, "bike_sharing.zip")
    with zipfile.ZipFile("bike_sharing.zip", 'r') as z:
        z.extractall("bike_data")
    os.remove("bike_sharing.zip")
    print("Done downloading.")
else:
    print("Dataset already exists, skipping download.")

# load the hourly data
df = pd.read_csv("bike_data/hour.csv")
print(f"Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")

# pick features and target
features = ['season', 'yr', 'mnth', 'hr', 'holiday',
            'weekday', 'workingday', 'weathersit',
            'temp', 'atemp', 'hum', 'windspeed']
target = 'cnt'

X = df[features]
y = df[target]


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42) 

print("Training model, please wait...")
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)
print("Training done.")

y_pred = model.predict(X_test)
r2 = r2_score(y_test, y_pred)
rmse = math.sqrt(mean_squared_error(y_test, y_pred))

print(f"\nR2 Score : {r2:.4f}")
print(f"RMSE     : {rmse:.2f} rentals")
print("Save these numbers for your README!")


joblib.dump(model, "bike_model.pkl")
print("Model saved as bike_model.pkl")

df[features + [target]].to_csv("hour_clean.csv", index=False)
print("Clean dataset saved as hour_clean.csv")
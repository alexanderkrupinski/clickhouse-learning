import pandas as pd
import matplotlib.pyplot as plt
from time import time

df = pd.read_csv('./data/nyc_taxi_data.csv', usecols=['pickup_datetime'], parse_dates=['pickup_datetime'])

start_time = time()
daily = (
    df.assign(date=df['pickup_datetime'].dt.date)
    .groupby('date')
    .size()
    .rename('trips_today')
    .reset_index()
    .sort_values('date')
)

daily["date"] = pd.to_datetime(daily["date"])
daily = daily.sort_values('date')
daily["moving_avg_7days"] = daily["trips_today"].rolling(window=7).mean()
daily = daily.iloc[6:]
end_time = time()
print(f"Time taken: {end_time - start_time} seconds")   

plt.figure(figsize=(10, 6))
plt.plot(daily["date"], daily["trips_today"], label="Trips Today")
plt.plot(daily["date"], daily["moving_avg_7days"], label="Moving Average 7 Days")
plt.legend()
plt.show()


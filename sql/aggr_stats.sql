SELECT
    passenger_count,
    avg(fare_amount) AS mean_fare,
    quantile(0.5)(fare_amount) AS median_fare,
    varSamp(fare_amount) AS variance_fare
FROM nyc_taxi.trips_small
GROUP BY passenger_count
ORDER BY passenger_count;

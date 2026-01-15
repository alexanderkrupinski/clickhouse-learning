SELECT 
    passenger_count,
    avg(fare_amount) as mean_fare,
    quantile(0.5)(fare_amount) as median_fare,
    varSamp(fare_amount) as variance_fare
FROM nyc_taxi.trips_small
GROUP BY passenger_count
ORDER BY passenger_count
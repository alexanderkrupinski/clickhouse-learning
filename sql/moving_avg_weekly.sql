-- use clickhouse-benchmark --query "$(cat sql/moving_avg_weekly.sql)"

SELECT 
    toDate(pickup_datetime) as date,
    count() as trips_today,
    avg(count()) OVER (
        ORDER BY toDate(pickup_datetime) 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7days
FROM nyc_taxi.trips_small
GROUP BY date
ORDER BY date;


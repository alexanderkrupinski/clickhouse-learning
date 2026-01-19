CREATE TABLE labels AS
SELECT
    user_id,
    toDate(pickup_datetime) AS prediction_date,
    0 AS target_churn_next_7d
FROM nyc_taxi.trips_small
GROUP BY
    user_id,
    prediction_date;

CREATE TABLE labels
ENGINE = MergeTree AS
SELECT
    ut.user_id AS uid,
    d.prediction_date,
    0 AS target_churn_next_7d
FROM (
    SELECT DISTINCT user_id
    FROM user_trips
) AS ut
CROSS JOIN (
    SELECT DISTINCT toDate(pickup_datetime) AS prediction_date
    FROM trips_small
) AS d
ORDER BY prediction_date;

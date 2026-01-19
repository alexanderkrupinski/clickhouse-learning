CREATE TABLE daily_amounts
ENGINE = MergeTree AS
SELECT
    user_id,
    sum(total_amount) AS amount,
    toDate(pickup_datetime) AS day
FROM (
    SELECT
        ut.user_id,
        ts.pickup_datetime,
        ts.total_amount
    FROM (
        SELECT
            user_id,
            trips
        FROM user_trips
        ARRAY JOIN trips
    ) AS ut
    JOIN trips_small AS ts ON ts.trip_id = ut.trips
    ORDER BY pickup_datetime
)
GROUP BY
    user_id,
    day
ORDER BY day ASC;

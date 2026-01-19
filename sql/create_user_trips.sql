CREATE TABLE user_trips
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    cityHash64(loc) AS user_id,
    loc,
    groupArray(trip_id) AS trips
FROM (
    SELECT
        tuple(
            round(dropoff_latitude, 0),
            round(dropoff_longitude, 0),
            round(pickup_latitude, 0),
            round(pickup_longitude, 0)
        ) AS loc,
        trip_id
    FROM trips_small
)
GROUP BY loc;

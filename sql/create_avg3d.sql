CREATE TABLE avg3d
ENGINE = MergeTree AS
WITH per_day AS (
    SELECT
        user_id,
        day,
        sum(amount) AS daily_amount
    FROM daily_amounts
    GROUP BY
        user_id,
        day
)
SELECT
    user_id,
    day,
    avg(daily_amount) OVER (
        PARTITION BY user_id
        ORDER BY toUInt32(day)
        RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_average
FROM per_day
ORDER BY
    day,
    user_id;

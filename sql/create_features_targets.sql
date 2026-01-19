CREATE TABLE features_target
ENGINE = MergeTree AS
SELECT
    l.uid,
    l.prediction_date,
    l.target_churn_next_7d,
    a.moving_average
FROM labels AS l
ASOF JOIN avg3d AS a
    ON l.uid = a.user_id
    AND l.prediction_date <= a.day;

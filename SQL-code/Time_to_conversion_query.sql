WITH first_arrival AS (
  SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    MIN(event_timestamp) AS first_event_ts
  FROM `turing_data_analytics.raw_events`
  GROUP BY user_pseudo_id, event_date
),

first_purchase AS (
  SELECT 
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    MIN(event_timestamp) AS first_purchase_ts
  FROM `turing_data_analytics.raw_events`
  WHERE event_name = 'purchase'
  GROUP BY user_pseudo_id, event_date
),

difference AS (
SELECT
  fa.event_date,
  fa.user_pseudo_id,
  TIMESTAMP_MICROS(fa.first_event_ts) AS first_event_time,
  TIMESTAMP_MICROS(fp.first_purchase_ts) AS first_purchase_time,
  TIMESTAMP_DIFF(TIMESTAMP_MICROS(fp.first_purchase_ts),
                 TIMESTAMP_MICROS(fa.first_event_ts),
                 MINUTE) AS difference_in_minutes
FROM first_arrival AS fa
JOIN first_purchase AS fp
  ON fa.user_pseudo_id = fp.user_pseudo_id
 AND fa.event_date = fp.event_date
)

SELECT
  event_date,
  COUNT(DISTINCT user_pseudo_id) AS users_with_purchase,
  ROUND(AVG(difference_in_minutes),2) AS avg_conversion_delay_minutes,
  ROUND(APPROX_QUANTILES(difference_in_minutes, 2)[OFFSET(1)], 2) AS median_conversion_delay_minutes
FROM difference
GROUP BY event_date
ORDER BY event_date;
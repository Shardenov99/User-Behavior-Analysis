WITH first_arrival AS (
  SELECT
    user_pseudo_id,
    country,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    MIN(event_timestamp) AS first_event_ts
  FROM `turing_data_analytics.raw_events`
  GROUP BY user_pseudo_id, country, event_date
),

first_purchase AS (
  SELECT 
    user_pseudo_id,
    country,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    MIN(event_timestamp) AS first_purchase_ts
  FROM `turing_data_analytics.raw_events`
  WHERE event_name = 'purchase'
  GROUP BY user_pseudo_id, country, event_date
),

difference AS (
  SELECT
    fa.event_date,
    fa.country,
    fa.user_pseudo_id,
    TIMESTAMP_MICROS(fa.first_event_ts) AS first_event_time_utc,
    TIMESTAMP_MICROS(fp.first_purchase_ts) AS first_purchase_time_utc,
    TIMESTAMP_DIFF(
      TIMESTAMP_MICROS(fp.first_purchase_ts),
      TIMESTAMP_MICROS(fa.first_event_ts),
      MINUTE
    ) AS difference_in_minutes
  FROM first_arrival AS fa
  JOIN first_purchase AS fp
    ON fa.user_pseudo_id = fp.user_pseudo_id
   AND fa.event_date = fp.event_date
   AND fa.country = fp.country
),

with_offsets AS (
  SELECT
    *,
    TIMESTAMP_ADD(
      first_purchase_time_utc,
      INTERVAL CASE country
        WHEN 'United States' THEN -300
        WHEN 'Pakistan' THEN 300
        WHEN 'Canada' THEN -300
        WHEN 'Turkey' THEN 180
        WHEN 'India' THEN 330
        WHEN 'Japan' THEN 540
        WHEN 'United Kingdom' THEN 0
        WHEN 'Singapore' THEN 480
        WHEN 'China' THEN 480
        WHEN 'Serbia' THEN 60
        WHEN '(not set)' THEN 0
        WHEN 'Russia' THEN 180
        WHEN 'Denmark' THEN 60
        WHEN 'Australia' THEN 600
        WHEN 'Lebanon' THEN 120
        WHEN 'Netherlands' THEN 60
        WHEN 'Mexico' THEN -360
        WHEN 'Brazil' THEN -180
        WHEN 'South Korea' THEN 540
        WHEN 'Belgium' THEN 60
        WHEN 'Morocco' THEN 0
        WHEN 'Italy' THEN 60
        WHEN 'Hong Kong' THEN 480
        WHEN 'Spain' THEN 60
        WHEN 'Taiwan' THEN 480
        WHEN 'Greece' THEN 120
        WHEN 'Philippines' THEN 480
        WHEN 'Thailand' THEN 420
        WHEN 'Indonesia' THEN 420
        WHEN 'Norway' THEN 60
        WHEN 'Malaysia' THEN 480
        WHEN 'Macao' THEN 480
        WHEN 'Ireland' THEN 0
        WHEN 'Germany' THEN 60
        WHEN 'Vietnam' THEN 420
        WHEN 'France' THEN 60
        WHEN 'Bahrain' THEN 180
        WHEN 'Kuwait' THEN 180
        WHEN 'Cyprus' THEN 120
        WHEN 'Portugal' THEN 0
        WHEN 'United Arab Emirates' THEN 240
        WHEN 'Argentina' THEN -180
        WHEN 'Ecuador' THEN -300
        WHEN 'Sweden' THEN 60
        WHEN 'Sri Lanka' THEN 330
        WHEN 'El Salvador' THEN -360
        WHEN 'North Macedonia' THEN 60
        WHEN 'Jamaica' THEN -300
        WHEN 'Ukraine' THEN 120
        WHEN 'Slovakia' THEN 60
        WHEN 'Poland' THEN 60
        WHEN 'Colombia' THEN -300
        WHEN 'Switzerland' THEN 60
        WHEN 'Belarus' THEN 180
        WHEN 'Mongolia' THEN 480
        WHEN 'Nigeria' THEN 60
        WHEN 'Guatemala' THEN -360
        WHEN 'Israel' THEN 120
        WHEN 'Saudi Arabia' THEN 180
        WHEN 'Chile' THEN -180
        WHEN 'Peru' THEN -300
        WHEN 'Oman' THEN 240
        WHEN 'Czechia' THEN 60
        WHEN 'Romania' THEN 120
        WHEN 'Austria' THEN 60
        WHEN 'Finland' THEN 120
        WHEN 'Slovenia' THEN 60
        WHEN 'Kosovo' THEN 60
        WHEN 'Azerbaijan' THEN 240
        WHEN 'Croatia' THEN 60
        WHEN 'Ghana' THEN 0
        WHEN 'Egypt' THEN 120
        WHEN 'Qatar' THEN 180
        WHEN 'Algeria' THEN 60
        WHEN 'Bulgaria' THEN 120
        WHEN 'Hungary' THEN 60
        WHEN 'Palestine' THEN 120
        WHEN 'New Zealand' THEN 720
        WHEN 'Lithuania' THEN 120
        WHEN 'Latvia' THEN 120
        WHEN 'Bangladesh' THEN 360
        WHEN 'Kenya' THEN 180
        WHEN 'Iraq' THEN 180
        WHEN 'South Africa' THEN 120
        WHEN 'Bahamas' THEN -300
        WHEN 'Myanmar (Burma)' THEN 390
        WHEN 'Puerto Rico' THEN -240
        WHEN 'Albania' THEN 60
        WHEN 'Iceland' THEN 0
        WHEN 'Dominican Republic' THEN -240
        WHEN 'Estonia' THEN 120
        WHEN 'Uruguay' THEN -180
        WHEN 'Venezuela' THEN -240
        WHEN 'Kazakhstan' THEN 360
        WHEN 'Costa Rica' THEN -360
        WHEN 'Honduras' THEN -360
        WHEN 'Nepal' THEN 345
        WHEN 'Trinidad & Tobago' THEN -240
        WHEN 'Bolivia' THEN -240
        WHEN 'Panama' THEN -300
        WHEN 'Bosnia & Herzegovina' THEN 60
        WHEN 'Jordan' THEN 120
        WHEN 'Georgia' THEN 240
        WHEN 'Tunisia' THEN 60
        WHEN 'Cambodia' THEN 420
        WHEN 'Malta' THEN 60
        WHEN 'Armenia' THEN 240
        WHEN 'Paraguay' THEN -240
        WHEN 'Luxembourg' THEN 60
        ELSE 0
      END MINUTE
    ) AS first_purchase_time_local
  FROM difference
),

categorized AS (
  SELECT
    country,
    EXTRACT(DAYOFWEEK FROM first_purchase_time_local) AS day_of_week,
    CASE 
      WHEN EXTRACT(HOUR FROM first_purchase_time_local) BETWEEN 6 AND 13 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM first_purchase_time_local) BETWEEN 14 AND 21 THEN 'Afternoon'
      ELSE 'Night'
    END AS time_of_day,
    user_pseudo_id,
    difference_in_minutes
  FROM with_offsets
)

SELECT
  CASE day_of_week
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,

  COUNT(DISTINCT IF(time_of_day = 'Morning', user_pseudo_id, NULL)) AS morning_users,
  APPROX_QUANTILES(IF(time_of_day = 'Morning', difference_in_minutes, NULL), 100)[OFFSET(50)] AS morning_median,

  COUNT(DISTINCT IF(time_of_day = 'Afternoon', user_pseudo_id, NULL)) AS afternoon_users,
  APPROX_QUANTILES(IF(time_of_day = 'Afternoon', difference_in_minutes, NULL), 100)[OFFSET(50)] AS afternoon_median,

  COUNT(DISTINCT IF(time_of_day = 'Night', user_pseudo_id, NULL)) AS night_users,
  APPROX_QUANTILES(IF(time_of_day = 'Night', difference_in_minutes, NULL), 100)[OFFSET(50)] AS night_median


FROM categorized
GROUP BY day_of_week
ORDER BY day_of_week;
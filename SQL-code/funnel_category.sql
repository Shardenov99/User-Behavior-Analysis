WITH unique_events AS (	
SELECT	
user_pseudo_id,	
event_name,
category
FROM `tc-da-1.turing_data_analytics.raw_events`	
WHERE event_name IN (	
'session_start',	
'view_item',	
'add_to_cart',	
'begin_checkout',
'add_shipping_info',	
'add_payment_info',	
'purchase'	
)	
GROUP BY user_pseudo_id, event_name	,category
),	
user_journey AS (	
SELECT	
category,
user_pseudo_id,		
MAX(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS session_start,	
MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS view_item,	
MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,	
MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
MAX(CASE WHEN event_name = 'add_shipping_info' THEN 1 ELSE 0 END) AS add_shipping_info,	
MAX(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END) AS add_payment_info,	
MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase	
FROM unique_events	
GROUP BY user_pseudo_id, category
)	
SELECT		
category,
SUM(session_start) AS session_start,	
SUM(view_item) AS viewed_products,	
SUM(add_to_cart) AS added_to_cart,	
SUM(begin_checkout) AS begin_checkout,
SUM(add_shipping_info) AS added_shipping_info,	
SUM(add_payment_info) AS added_payment_info,	
SUM(purchase) AS completed_purchase,	
ROUND(SUM(view_item) * 100.0 / SUM(session_start), 2) AS pct_user_activity,	
ROUND(SUM(add_to_cart) * 100.0 / SUM(session_start), 2) AS pct_viewed_products,	
ROUND(SUM(begin_checkout) * 100.0 / SUM(session_start), 2) AS began_checkout,	
ROUND(SUM(add_shipping_info) * 100.0 / SUM(session_start), 2) AS pct_added_shipping_info,	
ROUND(SUM(add_payment_info) * 100.0 / SUM(session_start), 2) AS pct_added_payment_info,	
ROUND(SUM(purchase) * 100.0 / SUM(session_start), 2) AS pct_completed_purchase	
FROM user_journey	
GROUP BY category
ORDER BY session_start DESC	
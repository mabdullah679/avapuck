SELECT
  oi.id                AS transaction_id,
  oi.order_id          AS order_id,
  oi.created_at        AS transaction_time,
  oi.sale_price        AS sale_price,
  oi.status            AS order_status,
  p.name               AS product_name,
  p.category           AS product_category,
  p.brand              AS product_brand,
  u.first_name         AS customer_first_name,
  u.last_name          AS customer_last_name,
  u.email              AS customer_email,
  u.street_address     AS customer_street_address,
  u.postal_code        AS customer_postal_code,
  u.city               AS customer_city,
  u.state              AS customer_state,
  u.traffic_source     AS traffic_source
FROM `bigquery-public-data.thelook_ecommerce.order_items` AS oi
JOIN `bigquery-public-data.thelook_ecommerce.users`    AS u ON u.id = oi.user_id
JOIN `bigquery-public-data.thelook_ecommerce.products` AS p ON p.id = oi.product_id
WHERE oi.created_at >= @window_start
  AND oi.created_at <  @window_end
  AND oi.id IS NOT NULL
ORDER BY oi.created_at
LIMIT @row_limit

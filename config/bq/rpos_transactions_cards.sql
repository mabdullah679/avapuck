-- Retail point-of-sale transactions with TEST payment card numbers.
--
-- The transaction and customer data is REAL, read live from
-- bigquery-public-data.thelook_ecommerce. The card numbers are NOT: no
-- BigQuery public dataset carries card numbers (correctly), so they are
-- synthesised here from the published Visa/Mastercard sandbox PANs. Those
-- are Luhn-valid, route to nothing, and authorise nothing.
--
-- Assigned deterministically by transaction id, so a re-run of the same
-- logical date produces the same card for the same transaction -- the
-- pipeline's idempotency guarantee has to hold for this column too.
WITH test_cards AS (
  SELECT * FROM UNNEST([
    '4111111111111111',  -- Visa
    '4012888888881881',
    '4000056655665556',
    '4242424242424242',
    '5555555555554444',  -- Mastercard
    '5105105105105100',
    '5200828282828210',
    '5454545454545454',
    '2223003122003222'
  ]) AS pan WITH OFFSET AS idx
)
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
  u.traffic_source     AS traffic_source,
  tc.pan               AS card_info
FROM `bigquery-public-data.thelook_ecommerce.order_items` AS oi
JOIN `bigquery-public-data.thelook_ecommerce.users`    AS u ON u.id = oi.user_id
JOIN `bigquery-public-data.thelook_ecommerce.products` AS p ON p.id = oi.product_id
JOIN test_cards AS tc ON tc.idx = MOD(oi.id, 9)
WHERE oi.created_at >= @window_start
  AND oi.created_at <  @window_end
  AND oi.id IS NOT NULL
ORDER BY oi.created_at
LIMIT @row_limit

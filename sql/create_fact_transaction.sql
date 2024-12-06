CREATE OR REPLACE MATERIALIZED VIEW fact.f_transaction_materialized
AS SELECT 
  DATE(doi.order_purchase_timestamp) AS order_purchase_date,
  dd.is_weekday,
  dd.is_holiday,
  doi.order_id,
  doi.product_id,
  doi.order_item_id,
  do.order_approved_at,
  doi.order_status,
  dp.segment AS product_segment,
  dp.product_category,
  doi.customer_id,
  dc.customer_unique_id,
  dc.customer_state,
  SUM(doi.freight_value) AS freight_value,
  AVG(doi.price) AS avg_price,
  COUNT(doi.product_id) AS product_quantity,

FROM dimensions.d_order_item doi 

INNER JOIN dimensions.d_order do
ON doi.order_id = do.order_id

INNER JOIN dimensions.d_product dp
ON doi.product_id = dp.product_id

INNER JOIN dimensions.d_customer dc
ON doi.customer_id = dc.customer_id

LEFT JOIN raw.d_date dd
ON DATE(doi.order_purchase_timestamp) = dd.date

GROUP BY order_purchase_date, dd.is_weekday, 
        dd.is_holiday, doi.order_id, doi.order_status, 
        do.order_approved_at, doi.order_item_id, doi.product_id,
        dp.segment, dp.product_category, doi.customer_id, 
        dc.customer_unique_id, dc.customer_state
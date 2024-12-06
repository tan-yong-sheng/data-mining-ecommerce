MERGE INTO dimensions.d_order AS target
USING staging.d_order AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET
    customer_id = source.customer_id,
    order_status = source.order_status,
    order_purchase_timestamp = source.order_purchase_timestamp,
    order_approved_at = source.order_approved_at,
    order_delivered_carrier_date = source.order_delivered_carrier_date,
    order_delivered_customer_date = source.order_delivered_customer_date,
    order_estimated_delivery_date = source.order_estimated_delivery_date
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
  VALUES (source.order_id, source.customer_id, source.order_status, source.order_purchase_timestamp, source.order_approved_at, source.order_delivered_carrier_date, source.order_delivered_customer_date, source.order_estimated_delivery_date);

MERGE INTO dimensions.d_order_item AS target
USING staging.d_order_item AS source
ON target.order_id = source.order_id AND target.order_item_id = source.order_item_id AND target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET
    order_id = source.order_id,
    order_item_id = source.order_item_id,
    order_purchase_timestamp = source.order_purchase_timestamp,
    order_approved_at = source.order_approved_at,
    customer_id = source.customer_id,
    product_id = source.product_id,
    price = source.price,
    freight_value = source.freight_value,
    order_status = source.order_status
WHEN NOT MATCHED THEN
  INSERT (order_id, order_item_id, order_purchase_timestamp, order_approved_at, customer_id, product_id, price, freight_value, order_status)
  VALUES (source.order_id, source.order_item_id, source.order_purchase_timestamp, source.order_approved_at, source.customer_id, source.product_id, source.price, source.freight_value, source.order_status);

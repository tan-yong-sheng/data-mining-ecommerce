MERGE INTO dimensions.d_customer AS target
USING staging.d_customer AS source
ON target.customer_id = source.customer_id AND target.customer_unique_id = source.customer_unique_id
WHEN MATCHED THEN
UPDATE SET
    customer_id = source.customer_id,
    customer_unique_id = source.customer_unique_id,
    customer_zip_code_prefix = source.customer_zip_code_prefix,
    customer_city = source.customer_city,
    customer_state = source.customer_state
WHEN NOT MATCHED THEN
INSERT (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
VALUES (source.customer_id, source.customer_unique_id, source.customer_zip_code_prefix, source.customer_city, source.customer_state);
MERGE INTO dimensions.d_product AS target
USING staging.d_product AS source
ON
  source.product_id = target.product_id
WHEN MATCHED THEN
  UPDATE SET
    product_category = source.product_category,
    segment = source.segment
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    product_id,
    product_category,
    segment
  )
  VALUES (
    source.product_id,
    source.product_category,
    source.segment
  );

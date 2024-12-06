CREATE OR REPLACE VIEW model.m_monthly_off_day_count AS
SELECT first_day_of_the_month, 
SUM(CASE WHEN is_weekday is true THEN 1 ELSE 0 END) AS weekday_count, 
SUM(CASE WHEN is_holiday is true THEN 1 ELSE 0 END) AS holiday_count
FROM raw.d_date
GROUP BY first_day_of_the_month;


CREATE OR REPLACE VIEW model.m_monthly_sales_by_prod_cat AS

SELECT dd.first_day_of_the_month,
        mmofc.weekday_count,
        mmofc.holiday_count,
        dp.product_category, 
        COUNT(DISTINCT customer_id) AS active_purchaser,
        COALESCE(SUM(fr.product_quantity),0) AS product_quantity,
        COALESCE(AVG(fr.avg_order_value), 0) AS avg_order_value,
        COALESCE(SUM(total_revenue),0) as total_revenue
FROM raw.d_date dd
CROSS JOIN (
    SELECT DISTINCT product_category
    FROM dimensions.d_product
) dp
LEFT JOIN fact.f_transaction fr
ON fr.order_purchase_date = dd.date AND fr.product_category = dp.product_category

LEFT JOIN model.m_monthly_off_day_count mmofc
ON mmofc.first_day_of_the_month = dd.first_day_of_the_month


WHERE dd.date >= '2017-01-01' AND dd.date <='2018-08-31'
GROUP BY dd.first_day_of_the_month, dp.product_category, 
        mmofc.weekday_count,
        mmofc.holiday_count
ORDER by dd.first_day_of_the_month ASC;

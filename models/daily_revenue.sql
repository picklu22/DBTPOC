{{ config(materialized='table') }}
SELECT
    d.DATE_VALUE,
    SUM((oi.QUANTITY * oi.UNIT_PRICE) - oi.DISCOUNT_AMOUNT) AS DAILY_REV
FROM {{ source('ecommerce','fact_orders') }} o
JOIN {{ source('ecommerce','fact_order_items') }} oi
    ON o.ORDER_ID = oi.ORDER_ID
JOIN {{ source('ecommerce','dim_date') }} d
    ON o.ORDER_DATE_KEY = d.DATE_KEY
GROUP BY d.DATE_VALUE
ORDER BY d.DATE_VALUE
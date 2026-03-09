{{ config(materialized='table') }}

SELECT
    c.CUSTOMER_NAME,
    c.SEGMENT,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM((oi.QUANTITY * oi.UNIT_PRICE) - oi.DISCOUNT_AMOUNT) AS TOTAL_SPENT
FROM {{ source('ecommerce','fact_orders') }} o
JOIN {{ source('ecommerce','fact_order_items') }} oi
    ON o.ORDER_ID = oi.ORDER_ID
JOIN {{ source('ecommerce','dim_customer') }} c
    ON o.CUSTOMER_KEY = c.CUSTOMER_KEY
GROUP BY
    c.CUSTOMER_NAME,
    c.SEGMENT
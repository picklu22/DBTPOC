{{ config(materialized='table') }}

SELECT
    oi.ORDER_ID,
    SUM(oi.QUANTITY * oi.UNIT_PRICE) AS GROSS_REVENUE,
    SUM(oi.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT,
    SUM((oi.QUANTITY * oi.UNIT_PRICE) - oi.DISCOUNT_AMOUNT) AS NET_REVENUE
FROM {{ source('ecommerce','fact_order_items') }} oi
GROUP BY oi.ORDER_ID
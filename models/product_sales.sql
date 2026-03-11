{{ config(materialized='table') }}

SELECT
    p.PRODUCT_NAME,
    p.BRAND,
    SUM(oi.QUANTITY) AS TOTAL_Qty,
    SUM((oi.QUANTITY * oi.UNIT_PRICE) - oi.DISCOUNT_AMOUNT) AS TOTAL_REVENUE
FROM {{ source('ecommerce','fact_order_items') }} oi
JOIN {{ source('ecommerce','dim_product') }} p
    ON oi.PRODUCT_KEY = p.PRODUCT_KEY
GROUP BY
    p.PRODUCT_NAME,
    p.BRAND
{{ config(materialized='view') }}

SELECT
    ORDER_ID,
    ORDER_DATE_KEY,
    CUSTOMER_KEY,
    CHANNEL_KEY,
    PROMO_KEY,
    ORDER_STATUS,
    PAYMENT_METHOD as METHOD
FROM {{ source('ecommerce','fact_orders') }}
WHERE ORDER_STATUS = 'Completed'
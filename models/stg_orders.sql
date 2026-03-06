{{ config(materialized='view') }}

SELECT
    ORDER_ID,
    ORDER_DATE_KEY,
    CUSTOMER_KEY,
    CHANNEL_KEY,
    PROMO_KEY,
    ORDER_STATUS,
    PAYMENT_METHODS
FROM {{ source('ecommerce','fact_orders') }}
WHERE ORDER_STATUS = 'Completed'
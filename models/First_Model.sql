SELECT
    customer_id,
    customer_name as customer_name,
    signup_date as signup_date ,
    UPPER(customer_name) AS name_upper,
    YEAR(signup_date) AS signup_year,
    MONTH(signup_date) AS signup_month
FROM {{ source('raw', 'customers') }}
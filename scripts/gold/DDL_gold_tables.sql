CREATE OR ALTER VIEW gold.dim_customer AS
SELECT
    customer_id,
    customer_city AS city,
    customer_state AS state
FROM silver.customer;

GO
CREATE OR ALTER VIEW gold.dim_Product AS
SELECT
    product_id,
    product_category_name AS category_name
FROM silver.products;

GO

CREATE OR ALTER VIEW gold.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp AS order_date,
    o.order_status,
    SUM(oi.total_price) AS total_order_value,
    SUM(oi.total_freight_value) AS total_freight_value,
    o.days_from_purchase_to_delivery
FROM silver.orders o
LEFT JOIN silver.order_items oi
    ON o.order_id = oi.order_id
GROUP BY
    o.order_id, o.customer_id, o.order_purchase_timestamp, o.order_status, o.days_from_purchase_to_delivery;

GO
CREATE OR ALTER VIEW gold.fact_order_items AS
SELECT
    order_id,
    product_id,
    quantity,
    price AS unit_price,
    total_price
FROM silver.order_items;

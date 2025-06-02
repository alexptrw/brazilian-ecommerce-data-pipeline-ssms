SELECT * FROM bronze.order_items
-- expected to have many duplicates -> surrogate key
SELECT DISTINCT order_id, COUNT(*) FROM bronze.order_items
GROUP BY order_id
HAVING COUNT(*) > 1

-- nulls check
SELECT * FROM bronze.order_items
WHERE order_id IS NULL

SELECT * FROM bronze.order_items
WHERE product_id IS NULL


SELECT * FROM bronze.order_items
WHERE seller_id IS NULL

SELECT * FROM bronze.order_items
WHERE price IS NULL or price <= 0

SELECT * FROM bronze.order_items
WHERE freight_value IS NULL or price <= 0

--date check -> min and max dates make sense, no date after curr date
SELECT MIN(shipping_limit_date) FROM bronze.order_items

SELECT MAX(shipping_limit_date) FROM bronze.order_items
SELECT * FROM bronze.order_items
WHERE shipping_limit_date >= GETDATE()

SELECT * FROM bronze.order_items
WHERE shipping_limit_date IS NULL

WITH new_t(
    order_id ,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
	total_freight_value,
	quantity,
	total_price) as(
SELECT 
    order_id, 
    product_id, 
    seller_id, 
    CAST(shipping_limit_date AS DATETIME),
    price,
	freight_value,
    SUM(freight_value) AS total_freight_value,
    COUNT(*) AS quantity,
	price * COUNT(*) as total_price
FROM bronze.order_items
GROUP BY 
    order_id, product_id, seller_id, shipping_limit_date, price, freight_value)

SELECT order_id ,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
	total_freight_value,
	quantity,
	total_price FROM new_t

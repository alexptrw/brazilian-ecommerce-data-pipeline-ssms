CREATE OR ALTER FUNCTION dbo.ProperCase (@Text NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @Index INT = 1,
            @Char NVARCHAR(1),
            @Output NVARCHAR(MAX) = '',
            @IsNewWord BIT = 1;

    WHILE @Index <= LEN(@Text)
    BEGIN
        SET @Char = SUBSTRING(@Text, @Index, 1)
        
        IF @IsNewWord = 1
        BEGIN
            SET @Output += UPPER(@Char)
            SET @IsNewWord = 0
        END
        ELSE
            SET @Output += LOWER(@Char)

        IF @Char LIKE '[^A-Za-z0-9]' OR @Char = '_'
            SET @IsNewWord = 1

        SET @Index += 1
    END

    RETURN @Output
END
-- _________________________________________________________________
DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME
PRINT '===============================';
PRINT 'Load Silver Layer';
PRINT '===============================';
PRINT '>>Deleting orders_payments table'
DELETE FROM silver.orders_payments;
PRINT '>>Deleting order_items'
DELETE FROM silver.order_items;
PRINT '>>Deleting orders'
DELETE FROM silver.orders;
PRINT '>>Deleting products'
DELETE FROM silver.products;
PRINT '>>Deleting sellers'
DELETE FROM silver.sellers;
PRINT '>>Deleting customer table'
DELETE FROM silver.customer;


PRINT '>>Start Loading customer table'
SET @start_time = GETDATE()
INSERT INTO silver.customer(
customer_id, 
customer_unique_id, 
customer_zip_code_prefix, 
customer_city, 
customer_state)

SELECT customer_id, 
customer_unique_id, 
CAST(customer_zip_code_prefix AS varchar) as customer_zip_code_prefix, 
dbo.ProperCase(customer_city) as customer_city, 
CAST(customer_state as CHAR) as customer_state
FROM bronze.customers
SET @end_time = GETDATE()
PRINT '>> End loading customer table';
PRINT 'Time to load: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' sec.'
PRINT '-------------------------------';
PRINT '===============================';

PRINT '>>Start Loading orders table'
SET @start_time = GETDATE()
INSERT INTO silver.orders(order_id, 
customer_id,
order_status,
order_purchase_timestamp,
order_approved_at,
order_delivered_carrier_date,
order_delivered_customer_date,
order_estimated_delivery_date,
days_from_purchase_to_delivery)

SELECT order_id, 
customer_id,
order_status,
order_purchase_timestamp, 
CASE 
WHEN order_approved_at  IS NULL and  order_status = 'delivered' THEN order_purchase_timestamp
	ELSE order_approved_at
	END as order_approved_at,
CASE 
	WHEN order_delivered_carrier_date < order_purchase_timestamp THEN DATEADD(HOUR, 1, order_purchase_timestamp)
	WHEN order_delivered_carrier_date IS NULL and order_delivered_customer_date IS NOT NULL THEN order_delivered_customer_date
	WHEN order_delivered_carrier_date IS NULL and order_delivered_customer_date IS NULL THEN order_estimated_delivery_date
	ELSE order_delivered_carrier_date
END as order_delivered_carrier_date,
CASE 
	WHEN order_delivered_customer_date IS NULL and order_status = 'delivered'THEN order_estimated_delivery_date
	ELSE order_delivered_customer_date
	END as order_delivered_customer_date,
order_estimated_delivery_date,
DATEDIFF(DAY, order_purchase_timestamp, order_estimated_delivery_date) as days_from_purchase_to_delivery
FROM  bronze.orders
WHERE NOT (order_approved_at IS NULL AND order_status = 'created')
SET @end_time = GETDATE()
PRINT '>> End loading orders table';
PRINT 'Time to load: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' sec.'
PRINT '-------------------------------';
PRINT '===============================';

PRINT '>>Start Loading products table'
SET @start_time = GETDATE()
INSERT INTO silver.products(product_id,
    product_category_name,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm)
SELECT product_id,
CASE WHEN c.product_category_name_english = 'pc_gamer' THEN 'pc_gamer'
	WHEN c.product_category_name_english = 'ortateis_cozinha_e_preparadores_de_alimentos' THEN 'kitchen_appliances_and_food_processors'
	ELSE c.product_category_name_english
	END AS english_name,
CAST(product_photos_qty AS INT) as product_photos_qty,
CAST(product_weight_g as DECIMAL(10, 2)) as product_weight_g,
CAST(product_length_cm as DECIMAL(10, 2)) as product_length_cm,
CAST(product_height_cm as DECIMAL(10, 2)) as product_height_cm,
CAST(product_width_cm as DECIMAL(10, 2)) as product_width_cm
FROM bronze.products p
LEFT JOIN bronze.category_name_transactions c 
    ON c.product_category_name = p.product_category_name
WHERE p.product_category_name is NOT NULL;
SET @end_time = GETDATE()
PRINT '>> End loading products table';
PRINT 'Time to load: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' sec.'
PRINT '-------------------------------';
PRINT '===============================';

PRINT '>>Start Loading order_items table'
SET @start_time = GETDATE()
INSERT INTO silver.order_items(
    order_id ,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
	total_freight_value,
	quantity,
	total_price) 
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
    order_id, product_id, seller_id, shipping_limit_date, price, freight_value; 
SET @end_time = GETDATE()
PRINT '>> End loading order_items table';
PRINT 'Time to load: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' sec.'
PRINT '-------------------------------';
PRINT '===============================';

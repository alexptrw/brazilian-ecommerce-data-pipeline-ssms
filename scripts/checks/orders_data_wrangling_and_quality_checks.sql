/*
===========================================================
Checks ran for cleaning, transforming, validating, and reshaping raw data before it moving it to silver for orders table. 
Includes for observations made on the data and decisions taken to continue working on the project
===========================================================

*/


-- checks for duplicates 
SELECT order_id, order_purchase_timestamp, order_approved_at,
order_delivered_carrier_date,
CASE WHEN order_delivered_carrier_date < order_purchase_timestamp THEN DATEADD(Hour, 1, order_delivered_carrier_date)
	ELSE order_delivered_carrier_date
END as order_delivered_carrier_date
FROM  bronze.orders
SELECT *  FROM  bronze.orders
GROUP BY order_id
HAVING COUNT(*) > 1

-- order puchase always > approved - an order cannot be approved before being purchased
SELECT order_id, order_purchase_timestamp, order_approved_at FROM  bronze.orders
WHERE order_purchase_timestamp > order_approved_at

-- order cannot be delivered before being purchase
SELECT order_id, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date FROM  bronze.orders
WHERE order_delivered_customer_date < order_purchase_timestamp

SELECT order_id, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date) as days_delivery
FROM  bronze.orders
WHERE DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date)  < 0


-- there are 166 values where order_delivered_carrier_date < order_purchase_timestamp, from kaggle discussion it seems like this is an error since the logs also come from a curries. it can also be a timezone issue
-- since data owner cannot be directly asked possible approaches are to remove those entried, or to set them to order_purchase_timestamp + a particular time to make sense 
--
SELECT order_id, 
order_purchase_timestamp, 
order_approved_at,
order_delivered_carrier_date,
CASE 
	WHEN order_delivered_carrier_date < order_purchase_timestamp THEN DATEADD(HOUR, 1, order_purchase_timestamp)
	ELSE order_delivered_carrier_date
END as order_delivered_carrier_date_ALTERED,
order_delivered_customer_date,
order_estimated_delivery_date
FROM  bronze.orders
WHERE order_delivered_carrier_date < order_purchase_timestamp

--NULL CHECKS
SELECT * FROM  bronze.orders
WHERE order_id IS NULL

SELECT * FROM  bronze.orders
WHERE order_purchase_timestamp IS NULL

--expected to have nulls when order is cancelled
SELECT * FROM  bronze.orders
WHERE order_approved_at IS NULL
AND order_status != 'canceled' 

--issue here, how is delivered without a payment again cannot contact the data owner, so asuming, it is a data issue since it is delivered
--decided to drop data with create and no approval or delivery as it does not make sense.
SELECT * FROM  bronze.orders
WHERE order_approved_at IS NULL
AND order_status = 'created' 

--leaving processing as nulls assuming data is old
SELECT * FROM  bronze.orders
WHERE order_delivered_carrier_date IS NULL
AND (order_approved_at IS NOT NULL and order_status != 'created')
AND order_status = 'invoiced'

SELECT * FROM  bronze.orders
WHERE order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NOT NULL
-- NO NULLS
SELECT * FROM  bronze.orders
WHERE order_estimated_delivery_date IS NULL

SELECT * FROM  bronze.orders
WHERE order_delivered_customer_date is NULL and order_status = 'delivered'
-- order_estimated_delivery_date cannot be less than order_purchase

-- checks quality of transformd data before moving to silver.orders
WITH fixed_table( 
 order_id ,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at ,
    order_delivered_carrier_date ,
    order_delivered_customer_date ,
    order_estimated_delivery_date ,
	days_from_purchase_to_delivery 
)AS (
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
DATEDIFF(DAY, order_purchase_timestamp, order_estimated_delivery_date)
FROM  bronze.orders
WHERE NOT (order_approved_at IS NULL AND order_status = 'created')
)

SELECT 
	order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date 
FROM fixed_table
--WHERE order_approved_at is NULL
--and order_purchase_timestamp = 'delivered';
--WHERE order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NOT NULL
--WHERE order_id = '2d858f451373b04fb5c984a1cc2defaf'
WHERE order_delivered_customer_date is null

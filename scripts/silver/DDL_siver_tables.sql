/*
=========================================================================================
DDL Scirpt: Create Silver Layer
=========================================================================================
Script purpose:
  This script creates the tables in the silver schema.
  First drops child tables to avoid FK dependency errors,
  then creates tables with their constraints.
=========================================================================================
*/

IF OBJECT_ID('silver.orders_payments', 'U') IS NOT NULL DROP TABLE silver.orders_payments;
IF OBJECT_ID('silver.order_items', 'U') IS NOT NULL DROP TABLE silver.order_items;
IF OBJECT_ID('silver.orders', 'U') IS NOT NULL DROP TABLE silver.orders;
IF OBJECT_ID('silver.products', 'U') IS NOT NULL DROP TABLE silver.products;
IF OBJECT_ID('silver.sellers', 'U') IS NOT NULL DROP TABLE silver.sellers;
IF OBJECT_ID('silver.customer', 'U') IS NOT NULL DROP TABLE silver.customer;

GO


CREATE TABLE silver.customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL UNIQUE,
    customer_zip_code_prefix VARCHAR(255),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);
GO

CREATE TABLE silver.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) FOREIGN KEY REFERENCES silver.customer(customer_id),
    order_status VARCHAR(50) CHECK(order_status IN (
        'Approved', 'Delivered', 'Created', 'Invoiced', 'Processing', 'Unavailable', 'Canceled', 'Shipped'
    )),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
GO

CREATE TABLE silver.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_photos_qty DECIMAL(10, 2),
    product_weight_g DECIMAL(10, 2),
    product_length_cm DECIMAL(10, 2),
    product_height_cm DECIMAL(10, 2),
    product_width_cm DECIMAL(10, 2),
    days_from_purchase_to_delivery INT
);
GO

CREATE TABLE silver.sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(255),
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);
GO

CREATE TABLE silver.order_items (
    order_item_pk INT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES silver.orders(order_id),
    order_item_id INT,
    product_id VARCHAR(50) REFERENCES silver.products(product_id),
    seller_id VARCHAR(50) REFERENCES silver.sellers(seller_id),
    shipping_limit_date VARCHAR(50),
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);
GO

CREATE TABLE silver.orders_payments (
    payment_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES silver.orders(order_id),
    payment_sequential INT,
    payment_type VARCHAR(255) 
        CHECK (payment_type IN ('Boleto', 'Credit_Card', 'Debit_Card', 'Not_Defined', 'Voucher')),
    payment_installments INT CHECK(payment_installments > 0),
    payment_value DECIMAL(10, 2)
);

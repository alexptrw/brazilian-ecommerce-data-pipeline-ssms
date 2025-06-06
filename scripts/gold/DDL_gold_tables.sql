IF OBJECT_ID('gold.DimCustomer', 'U') IS NOT NULL
	DROP TABLE gold.DimCustomer
CREATE TABLE gold.DimCustomer (
    customer_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    state CHAR(2)
);

IF OBJECT_ID('gold.DimProduct', 'U') IS NOT NULL
CREATE TABLE gold.DimProduct (
    product_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(255)
);

IF OBJECT_ID('gold.FactOrders', 'U') IS NOT NULL
CREATE TABLE gold.FactOrders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_date DATETIME,
    order_status VARCHAR(50),
    total_order_value DECIMAL(18, 2),
    total_freight_value DECIMAL(18, 2),
    days_from_purchase_to_delivery INT,
    FOREIGN KEY (customer_id) REFERENCES gold.DimCustomer(customer_id)
);

IF OBJECT_ID('gold.FactOrderItems', 'U') IS NOT NULL
CREATE TABLE gold.FactOrderItems (
    order_item_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(18, 2),
    FOREIGN KEY (order_id) REFERENCES gold.FactOrders(order_id),
    FOREIGN KEY (product_id) REFERENCES gold.DimProduct(product_id)
);

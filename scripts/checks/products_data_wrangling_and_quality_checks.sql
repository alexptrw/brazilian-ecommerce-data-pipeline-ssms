-- no duplicate ID
SELECT DISTINCT product_id, COUNT(*) FROM bronze.products
GROUP BY product_id
HAVING COUNT(*) > 1

-- check values | only prd weight_g 0....
SELECT * FROM bronze.products
WHERE product_photos_qty <= 0 OR
product_weight_g <= 0 OR
product_height_cm <=0 OR
product_width_cm <= 0
-- null values
SELECT * FROM bronze.products
WHERE product_category_name IS NULL

SELECT COUNT(*) FROM bronze.products
SELECT p.product_id,
c.product_category_name_english as product_category_name,
FROM bronze.products p
INNER JOIN bronze.category_name_transactions c ON c.product_category_name = p.product_category_name
GROUP BY p.product_id,c.product_category_name_english


SELECT COUNT(*) FROM bronze.products
WHERE product_category_name IS NOT NULL
SELECT COUNT(*) AS total_products
FROM bronze.products p
INNER JOIN bronze.category_name_transactions c 
    ON c.product_category_name = p.product_category_name;

SELECT COUNT(*) FROM bronze.products where product_category_name NOT IN (SELECT product_category_name_english FROM bronze.category_name_transactions)

SELECT * FROM bronze.products
WHERE product_category_name IS NOT NULL AND
product_category_name NOT IN (SELECT product_category_name_english FROM bronze.category_name_transactions)

SELECT p. product_category_name,
c.product_category_name_english AS english_name
FROM bronze.products p
LEFT JOIN bronze.category_name_transactions c 
    ON c.product_category_name = p.product_category_name
WHERE product_category_name_english IS NULL AND
p.product_category_name is NOT NULL;

-- two value without translation and not null
SELECT DISTINCT p. product_category_name
FROM bronze.products p
LEFT JOIN bronze.category_name_transactions c 
    ON c.product_category_name = p.product_category_name
WHERE product_category_name_english IS NULL AND
p.product_category_name is NOT NULL;



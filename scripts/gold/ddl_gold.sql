/*
================================================================================
DDL Script: Create Gold Views
================================================================================

Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.

================================================================================
*/

================================================================================
-- Create Dimension: gold.dim_customers
================================================================================
CREATE VIEW gold.dim_customers AS
WITH DeduplicatedData AS (
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_create_date,
        ca.bdate,
        la.cntry,
        -- This CASE statement consolidates the gender information.
        -- It prioritizes the gender from crm_cust_info, then erp_cust_az12, and defaults to 'n/a'.
        CASE 
            WHEN ci.cst_gndr IS NOT NULL AND ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
            WHEN ca.gen IS NOT NULL THEN ca.gen
            ELSE 'n/a' 
        END AS gender,
        -- Assign a row number to each record for a given customer.
        -- We can pick the 'first' one later.
        ROW_NUMBER() OVER(PARTITION BY ci.cst_id ORDER BY ci.cst_create_date DESC) as rn
    FROM 
        crm_cust_info AS ci
    LEFT JOIN 
        erp_cust_az12 AS ca ON ci.cst_key = ca.cid
    LEFT JOIN 
        erp_loc_a101 AS la ON ci.cst_key = la.cid
)
-- Now, select only the unique rows from the de-duplicated data
SELECT 
    ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key,
    cst_id AS customer_id,
    cst_key AS customer_number,
    cst_firstname AS first_name,
    cst_lastname AS last_name,
	cntry AS country,
    cst_marital_status AS marital_status,
    gender,
	bdate AS birthdate,
    cst_create_date AS create_date
FROM 
    DeduplicatedData
WHERE 
    rn = 1;
================================================================================
-- Create Dimension: gold.dim_products
================================================================================
CREATE VIEW gold.dim_products AS 
WITH DeDuplicated AS(
SELECT 
pn.prd_id,
pn.prd_key,
pn.cat_id,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_end_dt,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenence,
ROW_NUMBER () OVER (PARTITION BY pn.prd_id ORDER BY pn.prd_start_dt DESC) AS rn 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL)       ##FILTER OUT HISTORIC DATA

SELECT 
ROW_NUMBER () OVER(ORDER BY prd_start_dt, prd_key) AS product_key,
prd_id AS product_id,
prd_key AS product_number,
prd_nm AS product_name,
cat_id AS category_id,
cat AS category,
subcat AS subcategory,
maintenence,
prd_cost AS cost,
prd_line AS product_line,
prd_start_dt AS start_date
FROM 
DeDuplicated
WHERE rn = 1;
================================================================================
-- Create Dimension: gold.fact_sales
================================================================================
CREATE OR REPLACE VIEW gold.fact_sales AS

WITH RankedSales AS (
    SELECT 
        -- Foreign keys from dimension tables (Surrogate Keys)
        pr.product_key,
        cu.customer_key,

        -- Degenerate Dimension
        sd.sls_ord_num,

        -- Date Keys
        sd.sls_order_dt,
        sd.sls_ship_dt,
        sd.sls_due_dt,

        -- Measures
        sd.sls_sales,
        sd.sls_quantity,
        sd.sls_price,

        -- Use ROW_NUMBER to identify and remove duplicate sales records
        ROW_NUMBER() OVER(PARTITION BY sd.sls_ord_num, pr.product_key ORDER BY sd.sls_order_dt) as rn
    FROM 
        silver.crm_sales_details AS sd
    LEFT JOIN 
        gold.dim_products AS pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN 
        gold.dim_customers AS cu ON sd.sls_cust_id = cu.customer_id
)
-- Select only the unique records (the first instance of each)
SELECT
    product_key,
    customer_key,
    sls_ord_num AS order_number,
    sls_order_dt AS order_date,
    sls_ship_dt AS shipping_date,
    sls_due_dt AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM
    RankedSales
WHERE
    rn = 1;


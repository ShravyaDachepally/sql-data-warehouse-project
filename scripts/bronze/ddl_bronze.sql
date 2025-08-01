-- DDL Script: Create Bronze Tables
-- ==============================================================================
-- Script Purpose:
-- This script creates tables in the 'bronze' schema, dropping existing tables
-- if they already exist.
-- Run this script to re-define the DDL structure of 'bronze' Tables
-- ==============================================================================

-- Ensure you are using the correct database, e.g.:
-- USE your_database_name;

-- Table: bronze.crm_cust_info
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT PRIMARY KEY,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- Table: bronze.crm_prd_info
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

-- Table: bronze.crm_sales_details
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT, -- Consider if this should be DATE or DATETIME if it represents a specific date
    sls_ship_dt INT,  -- Consider if this should be DATE or DATETIME if it represents a specific date
    sls_due_dt INT,   -- Consider if this should be DATE or DATETIME if it represents a specific date
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Table: bronze.erp_cust_aZ12
DROP TABLE IF EXISTS bronze.erp_cust_aZ12;
CREATE TABLE bronze.erp_cust_aZ12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

-- Table: bronze.erp_loc_a101
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

-- Table: bronze.erp_px_cat_g1v2
-- Note: The previous request had 'erp_px_cat_g1v2crm_prd_info' as the table name,
-- but the structure suggests 'erp_px_cat_g1v2'. Assuming 'erp_px_cat_g1v2' is correct.
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenence VARCHAR(50)
);

/*
=============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================================
Script Purpose:

This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
=============================================================================
*/

CREATE DEFINER=`root`@`localhost` PROCEDURE `load_silver`()
BEGIN
	TRUNCATE TABLE silver.crm_cust_info;

	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)

	SELECT cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
		 ELSE 'n/a'
	END   cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info 
		WHERE cst_id IS NOT NULL)t WHERE flag_last =1;
		
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT prd_id, 
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	CASE TRIM(prd_line) 
	WHEN 'M' THEN 'Mountain'
	WHEN 'S' THEN 'Othersales'
	WHEN 'R' THEN 'Road'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	CAST((prd_start_dt) AS DATE) prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -INTERVAL 1 DAY AS DATE)  AS prd_end_dt
	FROM bronze.crm_prd_info;

	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CAST(sls_order_dt AS DATE) AS sls_order_dt,
	CAST(sls_ship_dt AS DATE) AS sls_ship_dt,
	CAST(sls_due_dt AS DATE) AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	FROM bronze.crm_sales_details;

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT 
	CASE WHEN cid  LIKE 'NAS%' THEN 
		SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate > CURRENT_DATE () THEN NULL
	ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
	ELSE gen
	END AS new_gen
	FROM erp_cust_az12
	WHERE CASE WHEN cid  LIKE 'NAS%' THEN 
		SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid
	END
	IN (SELECT cst_key FROM silver.crm_cust_info);

	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)
	SELECT 
	REPLACE(cid,"-","") AS cid,
	CASE WHEN TRIM(cntry) = 'US' THEN 'UNITED STATES'
		WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
		WHEN cntry IS NULL THEN "n/a"
	ELSE cntry
	END as cntry    
	FROM erp_loc_a101;

	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenence
	)
	SELECT id,cat,subcat,maintenence
	FROM bronze.erp_px_cat_g1v2;
END

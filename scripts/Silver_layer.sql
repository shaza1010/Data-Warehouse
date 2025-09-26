use DataWarehouse;

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo');
END
GO

------------------------------------
--DDL for the silver layer
------------------------------------


IF OBJECT_ID('silver.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    -- meta data column
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
END;

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NULL
BEGIN
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATETIME,
    prd_end_dt      DATETIME,
    -- meta data column
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
END;


IF OBJECT_ID('silver.crm_sales_details', 'U') IS NULL
BEGIN
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    -- meta data column
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
END;

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NULL
BEGIN
CREATE TABLE silver.erp_loc_a101 (
    cid     NVARCHAR(50),
    cntry   NVARCHAR(50),
    -- meta data column
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
END;

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NULL
BEGIN
CREATE TABLE silver.erp_cust_az12
(
    cid     NVARCHAR(50),
    bdate   DATE,
    gen     NVARCHAR(50),
    -- meta data column
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
END;

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NULL
BEGIN
CREATE TABLE silver.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50),
    -- meta data column
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
END;


---------------------------------------------------
--Quality Check on tabels and Insert with selection
---------------------------------------------------
--1) crm_cust_info
-------------------

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
SELECT 
    -- No proplems in those
    cst_id,
    cst_key,

    -- trim the whitespase in cst_firstname
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,

    -- normalize the martital status
    CASE
	    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END as cst_marital_status,

    -- normailize the gender
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'N/A'
    END as cst_gndr,

    cst_create_date -- no proplems found to this column

    -- filter the nulls and duplicates by taking the newist entry
FROM (select *, row_number() over(partition by cst_id order by cst_create_date DESC) as flag_last
      from bronze.crm_cust_info 
      where cst_id is not null) t
WHERE flag_last = 1;



-------------------
--2) crm_prd_info
-------------------

TRUNCATE TABLE silver.crm_prd_info;
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
SELECT
    prd_id,

    -- split the prodoct key into (catigory and key)
    -- mach the category fromat from another table 
    REPLACE( SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,

    prd_nm, -- no proplems found

    -- replace the 'null' with '0'
    ISNULL(prd_cost,0) as prd_cost,
    
    -- normlize the line of the prodoct
    CASE 
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'S' THEN 'Sport'
        WHEN prd_line = 'T' THEN 'Touring'
        WHEN prd_line is null THEN 'Other'
    END as prd_line,

    -- change the data type from datetime to date
    -- fix the date for the prodoct's livecycle
    CAST(prd_start_dt as DATE) as prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) as DATE) as prd_end_dt

FROM bronze.crm_prd_info; -- no duplicates detected

-------------------------
-- 3) crm_sales_details
-------------------------

TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
    -- No problems
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- fix the 0 and incorrect value in order date
    -- Convert data type from int to date
    CASE 
        WHEN LEN(sls_order_dt) != 8 THEN null
        ELSE CAST(CAST(sls_order_dt as NVARCHAR(10)) as DATE)
    END as  sls_order_dt,
    
    -- Convert data type from int to date
    CAST(CAST(sls_ship_dt as NVARCHAR(10)) as DATE) as sls_ship_num,
    CAST(CAST(sls_due_dt as NVARCHAR(10)) as DATE) as sls_due_num,

    -- Calculate the sales right if price not null
    CASE
        WHEN sls_price is not null THEN (ABS(sls_price) * sls_quantity)
        ELSE sls_price 
    END as sls_sales,
    sls_quantity, -- No probems

    -- Make all prices positive
    CASE
        WHEN sls_price is null THEN (sls_sales / sls_quantity)
        ELSE ABS(sls_price)
    END as sls_sales

    -- No dublicates found 
FROM bronze.crm_sales_details;


-------------------------
-- 4) erp_cst_az12
-------------------------

TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
SELECT
    -- Remove 'NAS' fron id
    CASE 
        WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END as cid,

    -- Replase future dates with 
    CASE
        WHEN bdate > GETDATE() THEN null
        ELSE bdate
    END as bdate,

    -- Normlize gender
    CASE
        WHEN gen in ('F', 'Female') THEN 'Female'
        WHEN gen in ('M', 'Male') THEN 'Male'
        ELSE 'N/A'
    END as gen

    -- No dublicates found
FROM bronze.erp_cust_az12;

-------------------------
-- 5) erp_loc_a101
-------------------------

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
	)
SELECT 
    cid, -- No problems

    -- Nomlize country names and handel missing and codes
    CASE
        WHEN cntry in ('USA', 'US') THEN 'United States'
        WHEN cntry = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' or cntry is null THEN 'N/A'
        ELSE cntry
    END as cntry

FROM bronze.erp_loc_a101

--------------------------
-- 6) erp_px_cat_g1v2
--------------------------

TRUNCATE TABLE silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)
SELECT
    -- No problems in table
	id,
	cat,
	subcat,
	maintenance

FROM bronze.erp_px_cat_g1v2;
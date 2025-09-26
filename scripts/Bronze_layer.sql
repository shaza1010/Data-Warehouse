use DataWarehouse;

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo');
END
GO
----------------------------------
--DDL for Bronze layer
----------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
END

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
END

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
END

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
END

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
END

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
END

GO
---------------------------------------------------------
-- procedure to loead the data to the bronze layer
---------------------------------------------------------

IF OBJECT_ID('bronze.load_bronze', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE bronze.load_bronze;
END;
GO

Create procedure bronze.load_bronze as
Begin
		
	Truncate Table bronze.crm_cust_info
	Bulk Insert bronze.crm_cust_info
	from 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_crm\cust_info.csv'
	with (
		Firstrow=2,
		FIELDTERMINATOR =',',
		Tablock
	)


	TRUNCATE TABLE bronze.crm_prd_info;
	PRINT '>> Inserting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT '>> Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);


	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'D:\DEPI\34_T_Data_Warehouse_Project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
END;

GO
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


--EXEC bronze.load_bronze

use datawarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    DECLARE @start_date DATETIME , @end_date DATETIME , @batch_strat_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_strat_time = GETDATE();
        PRINT '===============================';
        PRINT 'Loading data into bronze layer...';
        PRINT '===============================';
        
        PRINT '-------------------------------';
        PRINT 'Loading CRM tables...';
        PRINT '-------------------------------';

        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE crm_cust_info';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading crm_cust_info...';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';

        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE crm_prd_info';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Loading crm_prd_info...';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';


        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE crm_sales_details';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading crm_sales_details...';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';

        PRINT '-------------------------------';
        PRINT 'Loading ERP tables...';
        PRINT '-------------------------------';

        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE erp_cust_az12';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Loading erp_cust_az12...';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';

        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE erp_loc_a101';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading erp_loc_a101...';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/datasets/source_erp/loc_a101.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';

        PRINT '--------------------------------------------------------------';
        PRINT '>> TRUNCATING TABLE erp_px_cat_g1v2';
        SET @start_date = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading erp_px_cat_g1v2...';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv' -- the file inside docker container
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>> Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR(50)) + ' seconds';
        PRINT '--------------------------------------------------------------';
    END TRY
    BEGIN CATCH
        PRINT '===============================';
        PRINT 'Error occurred while loading data into bronze layer: ';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Status: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===============================';
    END CATCH
    SET @batch_end_time = GETDATE();
    PRINT '===============================';
    PRINT 'Total time taken to load data into bronze layer: ' + CAST(DATEDIFF(SECOND, @batch_strat_time, @batch_end_time) AS NVARCHAR(50)) + ' seconds';
    PRINT '===============================';
END;
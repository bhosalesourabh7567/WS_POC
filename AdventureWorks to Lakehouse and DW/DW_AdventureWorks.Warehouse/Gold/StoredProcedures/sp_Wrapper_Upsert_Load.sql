CREATE PROCEDURE [Gold].[sp_Wrapper_Upsert_Load] 
AS
BEGIN
    DECLARE @StartTime DATETIME2(3) = SYSDATETIME();    -- Start time of the wrapper execution
    DECLARE @EndTime DATETIME2(3);                        -- End time of the wrapper execution
    DECLARE @ExecutedBy VARCHAR(500) = USER_NAME();       -- User executing the wrapper
    DECLARE @Status VARCHAR(50);
    DECLARE @ErrorMessage VARCHAR(1000);
    DECLARE @ProcedureName VARCHAR(255);

    BEGIN TRY
        -- 1. Log and execute sp_Upsert_DimProduct
        SET @ProcedureName = 'sp_Upsert_DimProduct';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [DW_AdventureWorks].[Gold].[sp_Upsert_DimProduct];
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

        -- 2. Log and execute sp_Upsert_DimCustomer
        SET @ProcedureName = 'sp_Upsert_DimCustomer';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [DW_AdventureWorks].[Gold].[sp_Upsert_DimCustomer];
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

        -- 3. Log and execute sp_Upsert_DimSalesTerritory (First Execution)
        SET @ProcedureName = 'sp_Upsert_DimSalesTerritory';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [DW_AdventureWorks].[Gold].[sp_Upsert_DimSalesTerritory];
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

        -- 4. Log and execute sp_Upsert_DimSalesTerritory (Second Execution)
        SET @ProcedureName = 'sp_Upsert_DimSalesTerritory';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [DW_AdventureWorks].[Gold].[sp_Upsert_DimSalesTerritory];
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

        -- 5. Log and execute sp_Load_DimDate
        SET @ProcedureName = 'sp_Load_DimDate';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [Gold].[sp_Load_DimDate] @StartDate = '2001-01-01', @EndDate = '2026-12-31';
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

        -- 6. Log and execute sp_FactInternetSales
        SET @ProcedureName = 'sp_FactInternetSales';
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, NULL, 'Started', NULL, @ExecutedBy;
        EXEC [DW_AdventureWorks].[Gold].[SP_FactInternetSales];
        SET @EndTime = SYSDATETIME();
        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] @ProcedureName, @StartTime, @EndTime, 'Success', NULL, @ExecutedBy;

    END TRY
    BEGIN CATCH
        -- Log the failure of the wrapper procedure
        SET @Status = 'Failed';
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @EndTime = SYSDATETIME();

        EXEC [DW_AdventureWorks].[Gold].[LogProcedureExecution] 'sp_AuditWrapper', @StartTime, @EndTime, @Status, @ErrorMessage, @ExecutedBy;
    END CATCH
END;
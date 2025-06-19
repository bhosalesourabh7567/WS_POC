CREATE PROCEDURE [Gold].[SP_FactInternetSales]
AS
BEGIN
    -- Step 1: Truncate the FactInternetSales table
    TRUNCATE TABLE [DW_AdventureWorks].[Gold].[FactInternetSales];
    
    -- Step 2: Insert fresh data into the FactInternetSales table
    INSERT INTO [DW_AdventureWorks].[Gold].[FactInternetSales]
    (
        SalesOrderID,
        SalesOrderDetailID,
        CustomerKey,
        ProductKey,
        OrderDateKey,
        DueDateKey,
        ShipDateKey,
        SalesTerritoryKey,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal,
        RevisionNumber,
        CurrencyRateID,
       -- CarrierTrackingNumber,
        ModifiedDate,
        CreatedDate,
        CreatedBy,
        UpdatedBy,
        UpdatedDate
    )
    SELECT 
        SalesOrderID,
        SalesOrderDetailID,
        CustomerKey,
        ProductKey,
        OrderDateKey,
        DueDateKey,
        ShipDateKey,
        SalesTerritoryKey,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal,
        RevisionNumber,
        CurrencyRateID,
        --CarrierTrackingNumber,
        ModifiedDate,
        SYSDATETIME() AS CreatedDate,
        'system' AS CreatedBy,  -- Replace 'system' with your logic for setting the user
        'system' AS UpdatedBy,  -- Replace 'system' with your logic for setting the user
        NULL AS UpdatedDate
    FROM silver.VW_FactInternetSales;
    
END;
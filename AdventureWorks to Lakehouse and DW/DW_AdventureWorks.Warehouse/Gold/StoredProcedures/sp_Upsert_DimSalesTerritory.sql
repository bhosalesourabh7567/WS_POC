CREATE PROCEDURE [Gold].[sp_Upsert_DimSalesTerritory]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------
    -- Step 1: Load for potential updates --
    ----------------------------------------
    SELECT 
        SalesTerritoryKey,
        SalesTerritoryRegion,
        CountryRegionCode,
        SalesTerritoryGroup,
        SalesYTD,
        SalesLastYear,
        CostYTD,
        CostLastYear
    INTO #data_Update   
    FROM silver.VW_DimSalesTerritory;

    -- Keep only existing keys
    DELETE FROM #data_Update
    WHERE SalesTerritoryKey NOT IN (
        SELECT SalesTerritoryKey FROM Gold.DimSalesTerritory
    );

    -- Perform UPDATE if column values differ
    UPDATE T
    SET 
        T.SalesTerritoryRegion = S.SalesTerritoryRegion,
        T.CountryRegionCode = S.CountryRegionCode,
        T.SalesTerritoryGroup = S.SalesTerritoryGroup,
        T.SalesYTD = S.SalesYTD,
        T.SalesLastYear = S.SalesLastYear,
        T.CostYTD = S.CostYTD,
        T.CostLastYear = S.CostLastYear,
        T.UpdatedDate = SYSUTCDATETIME(),
        T.UpdatedBy = SESSION_USER
    FROM Gold.DimSalesTerritory T
    INNER JOIN #data_Update S ON T.SalesTerritoryKey = S.SalesTerritoryKey
    WHERE 
        ISNULL(T.SalesTerritoryRegion, '')  <> ISNULL(S.SalesTerritoryRegion, '') OR
        ISNULL(T.CountryRegionCode, '')     <> ISNULL(S.CountryRegionCode, '') OR
        ISNULL(T.SalesTerritoryGroup, '')   <> ISNULL(S.SalesTerritoryGroup, '') OR
        ISNULL(T.SalesYTD, 0)               <> ISNULL(S.SalesYTD, 0) OR
        ISNULL(T.SalesLastYear, 0)          <> ISNULL(S.SalesLastYear, 0) OR
        ISNULL(T.CostYTD, 0)                <> ISNULL(S.CostYTD, 0) OR
        ISNULL(T.CostLastYear, 0)           <> ISNULL(S.CostLastYear, 0);

    ----------------------------------------
    -- Step 2: Load for potential inserts --
    ----------------------------------------
    SELECT 
        SalesTerritoryKey,
        SalesTerritoryRegion,
        CountryRegionCode,
        SalesTerritoryGroup,
        SalesYTD,
        SalesLastYear,
        CostYTD,
        CostLastYear
    INTO #data_Insert   
    FROM silver.VW_DimSalesTerritory;

    -- Keep only non-existing keys
    DELETE FROM #data_Insert
    WHERE SalesTerritoryKey IN (
        SELECT SalesTerritoryKey FROM Gold.DimSalesTerritory
    );

    -- Perform INSERT for new rows
    INSERT INTO Gold.DimSalesTerritory (
        SalesTerritoryKey,
        SalesTerritoryRegion,
        CountryRegionCode,
        SalesTerritoryGroup,
        SalesYTD,
        SalesLastYear,
        CostYTD,
        CostLastYear,
        CreatedDate,
        CreatedBy
    )
    SELECT 
        SalesTerritoryKey,
        SalesTerritoryRegion,
        CountryRegionCode,
        SalesTerritoryGroup,
        SalesYTD,
        SalesLastYear,
        CostYTD,
        CostLastYear,
        SYSUTCDATETIME(),
        SESSION_USER
    FROM #data_Insert;

    -- Cleanup
    DROP TABLE IF EXISTS #data_Update;
    DROP TABLE IF EXISTS #data_Insert;
END;
CREATE PROCEDURE [Gold].[sp_Upsert_DimProduct]
AS
BEGIN
    --SET NOCOUNT ON;

    -- Step 1: Load potential updates into temp table
    SELECT 
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate
    INTO #data_Update
    FROM silver.VW_DimProduct;

    -- Filter for existing ProductKeys (for update)
    DELETE FROM #data_Update
    WHERE ProductKey NOT IN (
        SELECT ProductKey FROM [DW_AdventureWorks].[Gold].[DimProduct]
    );

    -- Perform UPDATE for existing records
    UPDATE T
    SET 
        T.ProductName = S.ProductName,
        T.ProductNumber = S.ProductNumber,
        T.Color = S.Color,
        T.StandardCost = S.StandardCost,
        T.ListPrice = S.ListPrice,
        T.Size = S.Size,
        T.Weight = S.Weight,
        T.SizeUnitMeasureCode = S.SizeUnitMeasureCode,
        T.WeightUnitMeasureCode = S.WeightUnitMeasureCode,
        T.DaysToManufacture = S.DaysToManufacture,
        T.ProductLine = S.ProductLine,
        T.Class = S.Class,
        T.Style = S.Style,
        T.SubCategory = S.SubCategory,
        T.Category = S.Category,
        T.SellStartDate = S.SellStartDate,
        T.SellEndDate = S.SellEndDate,
        T.DiscontinuedDate = S.DiscontinuedDate
        , UpdatedDate =  SYSUTCDATETIME()
    FROM [DW_AdventureWorks].[Gold].[DimProduct] T
    INNER JOIN #data_Update S ON T.ProductKey = S.ProductKey;

    -- Step 2: Load all source data into another temp table
    SELECT 
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate
    INTO #data_Insert
    FROM silver.VW_DimProduct;

    -- Filter for new ProductKeys (for insert)
    DELETE FROM #data_Insert
    WHERE ProductKey IN (
        SELECT ProductKey FROM [DW_AdventureWorks].[Gold].[DimProduct]
    );

    -- Perform INSERT for new records
    INSERT INTO [DW_AdventureWorks].[Gold].[DimProduct] (
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate, CreatedDate,CreatedBy 
    )
    SELECT 
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate, SYSUTCDATETIME(),SESSION_USER  
    FROM #data_Insert;

    -- Clean up temp tables
    DROP TABLE IF EXISTS #data_Update;
    DROP TABLE IF EXISTS #data_Insert;

END;
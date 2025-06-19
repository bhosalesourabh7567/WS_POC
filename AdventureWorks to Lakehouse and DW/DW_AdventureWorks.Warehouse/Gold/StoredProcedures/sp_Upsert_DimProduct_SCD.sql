CREATE PROCEDURE [Gold].[sp_Upsert_DimProduct_SCD]
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

    -------Get SCD Records
       
SELECT 
        S.ProductKey, S.ProductName, S.ProductNumber, S.Color, S.StandardCost, S.ListPrice, 
        S.Size, S.Weight, S.SizeUnitMeasureCode, S.WeightUnitMeasureCode, S.DaysToManufacture, 
        S.ProductLine, S.Class, S.Style, S.SubCategory, S.Category, S.SellStartDate, 
        S.SellEndDate, S.DiscontinuedDate
        INTO #UPDATE_SCD
    FROM [DW_AdventureWorks].[Gold].[DimProduct] T
    INNER JOIN #data_Update S ON T.ProductKey = S.ProductKey
         AND (
            ISNULL(T.Color, '') <> ISNULL(S.Color, '') OR
            CAST(ISNULL(T.StandardCost, -1) AS DECIMAL(18,2)) <> CAST(ISNULL(S.StandardCost, -1) AS DECIMAL(18,2)) OR
            CAST(ISNULL(T.ListPrice, -1) AS DECIMAL(18,2)) <> CAST(ISNULL(S.ListPrice, -1) AS DECIMAL(18,2)) OR
            ISNULL(T.Size, '') <> ISNULL(S.Size, '') OR
            CAST(ISNULL(T.Weight, -1) AS DECIMAL(8,2)) <> CAST(ISNULL(S.Weight, -1) AS DECIMAL(8,2))
            
       )
       AND T.ValidTo IS NULL;





    
    -- Perform UPDATE for existing records
    UPDATE T
    SET 
        T.ProductName = S.ProductName,
        T.ProductNumber = S.ProductNumber,
       -- T.Color = S.Color,
       -- T.StandardCost = S.StandardCost,
       -- T.ListPrice = S.ListPrice,
       -- T.Size = S.Size,
       -- T.Weight = S.Weight,
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
        , ValidTo = SYSUTCDATETIME()
        , IsActive = 0
    FROM [DW_AdventureWorks].[Gold].[DimProduct] T
    INNER JOIN #UPDATE_SCD S ON T.ProductKey = S.ProductKey
       AND T.ValidTo IS NULL;
    ;
    ----------------------Insert Updated record 
    INSERT INTO [DW_AdventureWorks].[Gold].[DimProduct] (
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate, CreatedDate,CreatedBy ,ValidFrom,IsActive
    )
    SELECT 
        S.ProductKey, S.ProductName, S.ProductNumber, S.Color, S.StandardCost, S.ListPrice, 
        S.Size, S.Weight, S.SizeUnitMeasureCode, S.WeightUnitMeasureCode, S.DaysToManufacture, 
        S.ProductLine, S.Class, S.Style, S.SubCategory, S.Category, S.SellStartDate, 
        S.SellEndDate, S.DiscontinuedDate, SYSUTCDATETIME(),SESSION_USER ,SYSUTCDATETIME() ,1 
    FROM #UPDATE_SCD S;

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
        SellEndDate, DiscontinuedDate, CreatedDate,CreatedBy ,ValidFrom,IsActive
    )
    SELECT 
        ProductKey, ProductName, ProductNumber, Color, StandardCost, ListPrice, 
        Size, Weight, SizeUnitMeasureCode, WeightUnitMeasureCode, DaysToManufacture, 
        ProductLine, Class, Style, SubCategory, Category, SellStartDate, 
        SellEndDate, DiscontinuedDate, SYSUTCDATETIME(),SESSION_USER ,SYSUTCDATETIME(),1 
    FROM #data_Insert;

    -- Clean up temp tables
    DROP TABLE IF EXISTS #data_Update;
    DROP TABLE IF EXISTS #data_Insert;

END;
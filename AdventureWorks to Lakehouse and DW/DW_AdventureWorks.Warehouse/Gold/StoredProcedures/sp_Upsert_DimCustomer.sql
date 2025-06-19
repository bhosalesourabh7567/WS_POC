CREATE PROCEDURE [Gold].[sp_Upsert_DimCustomer]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------
    -- Step 1: Load for potential updates --
    ----------------------------------------
    SELECT 
        CustomerKey, FullName, Title, FirstName, MiddleName, LastName,
        PersonID, AddressLine1, AddressLine2, City, StateProvince,
        CountryRegion, PostalCode, CountryRegionCode, StoreID, TerritoryID
        
    INTO #data_Update
    FROM silver.VW_DimCustomer;

    -- Keep only records where CustomerKey already exists
    DELETE FROM #data_Update
    WHERE CustomerKey NOT IN (
        SELECT CustomerKey FROM Gold.DimCustomer
    );

    -- Perform UPDATE only if values differ
    UPDATE T
    SET 
        T.FullName = S.FullName,
        T.Title = S.Title,
        T.FirstName = S.FirstName,
        T.MiddleName = S.MiddleName,
        T.LastName = S.LastName,
        T.PersonID = S.PersonID,
        T.AddressLine1 = S.AddressLine1,
        T.AddressLine2 = S.AddressLine2,
        T.City = S.City,
        T.StateProvince = S.StateProvince,
        T.CountryRegion = S.CountryRegion,
        T.PostalCode = S.PostalCode,
        T.CountryRegionCode = S.CountryRegionCode,
        T.StoreID = S.StoreID,
        T.TerritoryID = S.TerritoryID,
        T.UpdatedDate = SYSUTCDATETIME(),
        T.UpdatedBy = SESSION_USER
    FROM Gold.DimCustomer T
    INNER JOIN #data_Update S ON T.CustomerKey = S.CustomerKey
    WHERE 
        ISNULL(T.FullName, '')           <> ISNULL(S.FullName, '') OR
        ISNULL(T.Title, '')              <> ISNULL(S.Title, '') OR
        ISNULL(T.FirstName, '')          <> ISNULL(S.FirstName, '') OR
        ISNULL(T.MiddleName, '')         <> ISNULL(S.MiddleName, '') OR
        ISNULL(T.LastName, '')           <> ISNULL(S.LastName, '') OR
        ISNULL(T.PersonID, -1)           <> ISNULL(S.PersonID, -1) OR
        ISNULL(T.AddressLine1, '')       <> ISNULL(S.AddressLine1, '') OR
        ISNULL(T.AddressLine2, '')       <> ISNULL(S.AddressLine2, '') OR
        ISNULL(T.City, '')               <> ISNULL(S.City, '') OR
        ISNULL(T.StateProvince, '')      <> ISNULL(S.StateProvince, '') OR
        ISNULL(T.CountryRegion, '')      <> ISNULL(S.CountryRegion, '') OR
        ISNULL(T.PostalCode, '')         <> ISNULL(S.PostalCode, '') OR
        ISNULL(T.CountryRegionCode, '')  <> ISNULL(S.CountryRegionCode, '') OR
        ISNULL(T.StoreID, -1)            <> ISNULL(S.StoreID, -1) OR
        ISNULL(T.TerritoryID, -1)        <> ISNULL(S.TerritoryID, -1);

    ----------------------------------------
    -- Step 2: Load for potential inserts --
    ----------------------------------------
    SELECT 
        CustomerKey, FullName, Title, FirstName, MiddleName, LastName,
        PersonID, AddressLine1, AddressLine2, City, StateProvince,
        CountryRegion, PostalCode, CountryRegionCode, StoreID, TerritoryID
        
    INTO #data_Insert
    FROM silver.VW_DimCustomer;

    -- Remove rows already in DimCustomer
    DELETE FROM #data_Insert
    WHERE CustomerKey IN (
        SELECT CustomerKey FROM Gold.DimCustomer
    );

    -- Perform INSERT for new records
    INSERT INTO Gold.DimCustomer (
        CustomerKey, FullName, Title, FirstName, MiddleName, LastName,
        PersonID, AddressLine1, AddressLine2, City, StateProvince,
        CountryRegion, PostalCode, CountryRegionCode, StoreID, TerritoryID,
        CreatedDate, CreatedBy
    )
    SELECT 
        CustomerKey, FullName, Title, FirstName, MiddleName, LastName,
        PersonID, AddressLine1, AddressLine2, City, StateProvince,
        CountryRegion, PostalCode, CountryRegionCode, StoreID, TerritoryID,
        SYSUTCDATETIME(), SESSION_USER
    FROM #data_Insert;

    -- Clean up temp tables
    DROP TABLE IF EXISTS #data_Update;
    DROP TABLE IF EXISTS #data_Insert;

END;
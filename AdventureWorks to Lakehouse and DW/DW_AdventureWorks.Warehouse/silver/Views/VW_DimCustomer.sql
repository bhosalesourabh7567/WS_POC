-- Auto Generated (Do not modify) 01E6F94E4DD3C102AB4E9868500C1DAA10AD2F359C1D6B80470DCBC02079F22A
CREATE VIEW silver.VW_DimCustomer AS
SELECT Distinct
    c.CustomerID AS CustomerKey,
    p.FirstName + ' ' + ISNULL(p.MiddleName + ' ', '') + p.LastName AS FullName,
    p.Title,
    p.FirstName,
    p.MiddleName,
    p.LastName,
    c.PersonID,
    a.AddressLine1,
    a.AddressLine2,
    a.City,
    sp.Name AS StateProvince,
    cr.Name AS CountryRegion,
    a.PostalCode,
    sp.CountryRegionCode,
    c.StoreID,
    c.TerritoryID
FROM
    Bronze_AdventureworksOLTP.Bronze.Customer c
    INNER JOIN Bronze_AdventureworksOLTP.Bronze.Person p ON c.PersonID = p.BusinessEntityID
    INNER JOIN silver.VW_BusinessEntityAddress bea ON bea.BusinessEntityID = p.BusinessEntityID
    INNER JOIN Bronze_AdventureworksOLTP.Bronze.Address a ON a.AddressID = bea.AddressID
    INNER JOIN Bronze_AdventureworksOLTP.Bronze.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
    INNER JOIN Bronze_AdventureworksOLTP.Bronze.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
WHERE
    c.PersonID IS NOT NULL AND bea.RNO = 1
-- Auto Generated (Do not modify) 4C9C028FEE6E280716C105FB2B19BAD09A086D914D3EDBF30E23828BB960F387
CREATE VIEW [silver].VW_DimSalesTerritory AS
SELECT
    TerritoryID AS SalesTerritoryKey,
    Name AS SalesTerritoryRegion,
    CountryRegionCode,
    [Group] AS SalesTerritoryGroup,
    SalesYTD,
    SalesLastYear,
    CostYTD,
    CostLastYear
FROM Bronze_AdventureworksOLTP.Bronze.SalesTerritory;
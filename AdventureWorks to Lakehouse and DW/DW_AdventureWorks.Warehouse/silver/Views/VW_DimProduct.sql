-- Auto Generated (Do not modify) 39B3E48C1B9D4D2B6F0AF3802C850E8C8D72DA6A5BBADF5B5EEF545498CB943B


CREATE VIEW [silver].VW_DimProduct AS
SELECT
    p.ProductID AS ProductKey,
    p.Name AS ProductName,
    p.ProductNumber,
    COALESCE(p.Color,'NA') AS Color,
    COALESCE(p.StandardCost,0) AS StandardCost,
    COALESCE(p.ListPrice,0) AS ListPrice,
    COALESCE(p.Size,'0') AS Size,
    COALESCE(p.Weight,0) AS Weight,
    COALESCE(p.SizeUnitMeasureCode,'NA') AS SizeUnitMeasureCode,
    COALESCE(p.WeightUnitMeasureCode,'NA') AS WeightUnitMeasureCode,
    p.DaysToManufacture,
    COALESCE(p.ProductLine,'NA') AS ProductLine,
    COALESCE(p.Class,'NA') AS Class,
    COALESCE(p.Style,'NA') AS Style,
    COALESCE(ps.Name,'NA') AS SubCategory,
    COALESCE(pc.Name,'NA') AS Category,
    COALESCE(p.SellStartDate,'1900-01-01 00:00:00.000000') AS SellStartDate,
    COALESCE(p.SellEndDate,'1900-01-01 00:00:00.000000') AS SellEndDate,
    COALESCE(p.DiscontinuedDate,'1900-01-01 00:00:00.000000') AS DiscontinuedDate,
    CURRENT_TIMESTAMP AS CreatedDate
FROM
    Bronze_AdventureworksOLTP.Bronze.Product p
    left JOIN Bronze_AdventureworksOLTP.Bronze.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    left JOIN Bronze_AdventureworksOLTP.Bronze.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID;
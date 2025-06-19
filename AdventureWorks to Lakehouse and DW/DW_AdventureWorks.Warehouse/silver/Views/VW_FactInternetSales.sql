-- Auto Generated (Do not modify) 97E5DCB4429B562DCE0CB16AA55BF9FB196CD3ED18B6DB5CEC71EDBE585A30FC

CREATE VIEW silver.VW_FactInternetSales AS
SELECT
    soh.SalesOrderID,
    sod.SalesOrderDetailID,
    dc.CustomerKey,
    dp.ProductKey,
    dd_order.DateKey AS OrderDateKey,
    dd_due.DateKey AS DueDateKey,
    dd_ship.DateKey AS ShipDateKey,
    dst.SalesTerritoryKey,
    sod.OrderQty,
    sod.UnitPrice,
    sod.UnitPriceDiscount,
    sod.LineTotal,
    soh.RevisionNumber,
    soh.CurrencyRateID,
    ---soh.CarrierTrackingNumber,
    soh.ModifiedDate,
    SYSDATETIME() AS CreatedDate
FROM Bronze_AdventureworksOLTP.Bronze.SalesOrderHeader soh
JOIN Bronze_AdventureworksOLTP.Bronze.SalesOrderDetail sod
    ON soh.SalesOrderID = sod.SalesOrderID
LEFT JOIN Gold.DimCustomer dc
    ON soh.CustomerID = dc.CustomerKey
LEFT JOIN Gold.DimProduct dp
    ON sod.ProductID = dp.ProductKey
LEFT JOIN Gold.DimDate dd_order
    ON CAST(soh.OrderDate AS DATE) = dd_order.FullDate
LEFT JOIN Gold.DimDate dd_due
    ON CAST(soh.DueDate AS DATE) = dd_due.FullDate
LEFT JOIN Gold.DimDate dd_ship
    ON CAST(soh.ShipDate AS DATE) = dd_ship.FullDate
LEFT JOIN Gold.DimSalesTerritory dst
    ON soh.TerritoryID = dst.SalesTerritoryKey
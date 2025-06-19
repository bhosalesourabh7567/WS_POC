CREATE TABLE [Gold].[FactInternetSales] (

	[FactInternetSalesKey] int NULL, 
	[SalesOrderID] int NOT NULL, 
	[SalesOrderDetailID] int NOT NULL, 
	[CustomerKey] int NULL, 
	[ProductKey] int NULL, 
	[OrderDateKey] int NULL, 
	[DueDateKey] int NULL, 
	[ShipDateKey] int NULL, 
	[SalesTerritoryKey] int NULL, 
	[OrderQty] smallint NULL, 
	[UnitPrice] decimal(18,2) NULL, 
	[UnitPriceDiscount] decimal(18,2) NULL, 
	[LineTotal] decimal(18,2) NULL, 
	[RevisionNumber] smallint NULL, 
	[CurrencyRateID] int NULL, 
	[CarrierTrackingNumber] varchar(25) NULL, 
	[ModifiedDate] datetime2(3) NULL, 
	[CreatedDate] datetime2(3) NULL, 
	[CreatedBy] varchar(500) NULL, 
	[UpdatedBy] varchar(500) NULL, 
	[UpdatedDate] datetime2(3) NULL
);
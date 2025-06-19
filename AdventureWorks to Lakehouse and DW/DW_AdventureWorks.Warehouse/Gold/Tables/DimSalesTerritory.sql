CREATE TABLE [Gold].[DimSalesTerritory] (

	[SalesTerritoryKey] int NOT NULL, 
	[SalesTerritoryRegion] varchar(100) NULL, 
	[CountryRegionCode] varchar(10) NULL, 
	[SalesTerritoryGroup] varchar(50) NULL, 
	[SalesYTD] decimal(18,2) NULL, 
	[SalesLastYear] decimal(18,2) NULL, 
	[CostYTD] decimal(18,2) NULL, 
	[CostLastYear] decimal(18,2) NULL, 
	[CreatedDate] datetime2(3) NULL, 
	[CreatedBy] varchar(500) NULL, 
	[UpdatedBy] varchar(500) NULL, 
	[UpdatedDate] datetime2(3) NULL
);
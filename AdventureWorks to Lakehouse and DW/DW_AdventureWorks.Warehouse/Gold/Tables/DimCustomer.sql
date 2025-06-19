CREATE TABLE [Gold].[DimCustomer] (

	[CustomerKey] int NOT NULL, 
	[FullName] varchar(255) NULL, 
	[Title] varchar(20) NULL, 
	[FirstName] varchar(50) NULL, 
	[MiddleName] varchar(50) NULL, 
	[LastName] varchar(50) NULL, 
	[PersonID] int NULL, 
	[AddressLine1] varchar(255) NULL, 
	[AddressLine2] varchar(255) NULL, 
	[City] varchar(100) NULL, 
	[StateProvince] varchar(100) NULL, 
	[CountryRegion] varchar(100) NULL, 
	[PostalCode] varchar(20) NULL, 
	[CountryRegionCode] varchar(10) NULL, 
	[StoreID] int NULL, 
	[TerritoryID] int NULL, 
	[CreatedDate] datetime2(3) NULL, 
	[CreatedBy] varchar(500) NULL, 
	[UpdatedBy] varchar(500) NULL, 
	[UpdatedDate] datetime2(3) NULL
);
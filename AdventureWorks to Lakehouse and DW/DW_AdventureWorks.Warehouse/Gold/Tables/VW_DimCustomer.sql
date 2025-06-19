CREATE TABLE [Gold].[VW_DimCustomer] (

	[CustomerKey] int NULL, 
	[FullName] varchar(8000) NULL, 
	[Title] varchar(8000) NULL, 
	[FirstName] varchar(8000) NULL, 
	[MiddleName] varchar(8000) NULL, 
	[LastName] varchar(8000) NULL, 
	[PersonID] int NULL, 
	[AddressLine1] varchar(8000) NULL, 
	[AddressLine2] varchar(8000) NULL, 
	[City] varchar(8000) NULL, 
	[StateProvince] varchar(8000) NULL, 
	[CountryRegion] varchar(8000) NULL, 
	[PostalCode] varchar(8000) NULL, 
	[CountryRegionCode] varchar(8000) NULL, 
	[StoreID] int NULL, 
	[TerritoryID] int NULL, 
	[CreatedDate] datetime2(3) NULL
);
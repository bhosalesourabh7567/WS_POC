CREATE TABLE [dbo].[Configuration] (

	[SrcTableName] varchar(500) NULL, 
	[SrcSchema] varchar(200) NULL, 
	[DestTableName] varchar(100) NULL, 
	[DestSchema] varchar(100) NULL, 
	[TargetLakehousePath] varchar(200) NULL, 
	[IsIncremental] bit NULL, 
	[IncrementalColumn] varchar(1000) NULL, 
	[Query] varchar(1000) NULL, 
	[LastLoadedValue] datetime2(3) NULL, 
	[LoadType] varchar(20) NULL, 
	[SortOrder] int NULL, 
	[ETLType] varchar(20) NULL
);
CREATE TABLE [Gold].[AuditLog] (

	[LogID] varchar(500) NULL, 
	[ProcedureName] varchar(255) NOT NULL, 
	[StartTime] datetime2(3) NOT NULL, 
	[EndTime] datetime2(3) NULL, 
	[Status] varchar(50) NOT NULL, 
	[ErrorMessage] varchar(1000) NULL, 
	[ExecutedBy] varchar(500) NULL, 
	[ExecutionDuration] int NULL
);
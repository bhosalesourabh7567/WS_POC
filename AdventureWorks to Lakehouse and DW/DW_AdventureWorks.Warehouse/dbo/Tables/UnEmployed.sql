CREATE TABLE [dbo].[UnEmployed] (

	[Education_Level] varchar(4000) NULL, 
	[Line_Number] int NULL, 
	[Year] int NULL, 
	[Month] varchar(350) NULL, 
	[State] varchar(350) NULL, 
	[Labor_Force] int NULL, 
	[Employed] int NULL, 
	[Unemployed] int NULL, 
	[Industry] varchar(350) NULL, 
	[Gender] varchar(4000) NULL, 
	[Date_Inserted] date NULL, 
	[UnEmployed_Rate_Percentage] real NULL, 
	[Min_Salary_USD] int NULL, 
	[Max_Salary_USD] int NULL, 
	[dense_rank] int NULL
);
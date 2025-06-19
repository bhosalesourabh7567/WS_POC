CREATE PROCEDURE [Gold].[sp_Load_DimDate]
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Generate number sequence (up to 100,000 days using cross joins)
    WITH Ones AS (
        SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ),
    Numbers AS (
        SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Num
        FROM Ones t1
        CROSS JOIN Ones t2
        CROSS JOIN Ones t3
        CROSS JOIN Ones t4
    ),
    DateSequence AS (
        SELECT DATEADD(DAY, Num, @StartDate) AS FullDate
        FROM Numbers
    )

    -- Insert only if DateKey doesn't exist (UPSERT pattern)
    INSERT INTO [Gold].[DimDate] (
        DateKey,
        FullDate,
        Day,
        DaySuffix,
        DayName,
        Weekday,
        IsWeekend,
        DOWInMonth,
        DayOfYear,
        WeekOfYear,
        Month,
        MonthName,
        Quarter,
        QuarterName,
        Year,
        FirstDayOfMonth,
        LastDayOfMonth,
        FirstDayOfYear,
        LastDayOfYear,
        IsLeapYear,
        CreatedDate
    )
    SELECT 
        CAST(CONVERT(CHAR(8), FullDate, 112) AS INT) AS DateKey,
        FullDate,
        DAY(FullDate),
        RIGHT('0' + CAST(DAY(FullDate) AS VARCHAR), 2) +
            CASE 
                WHEN DAY(FullDate) IN (11, 12, 13) THEN 'th'
                WHEN RIGHT(CAST(DAY(FullDate) AS VARCHAR),1) = '1' THEN 'st'
                WHEN RIGHT(CAST(DAY(FullDate) AS VARCHAR),1) = '2' THEN 'nd'
                WHEN RIGHT(CAST(DAY(FullDate) AS VARCHAR),1) = '3' THEN 'rd'
                ELSE 'th'
            END AS DaySuffix,
        DATENAME(WEEKDAY, FullDate),
        DATEPART(WEEKDAY, FullDate),
        CASE WHEN DATEPART(WEEKDAY, FullDate) IN (1, 7) THEN 1 ELSE 0 END,
        (DATEPART(DAY, FullDate) - 1) / 7 + 1,
        DATEPART(DAYOFYEAR, FullDate),
        DATEPART(WEEK, FullDate),
        MONTH(FullDate),
        DATENAME(MONTH, FullDate),
        DATEPART(QUARTER, FullDate),
        'Q' + CAST(DATEPART(QUARTER, FullDate) AS VARCHAR),
        YEAR(FullDate),
        DATEFROMPARTS(YEAR(FullDate), MONTH(FullDate), 1),
        EOMONTH(FullDate),
        DATEFROMPARTS(YEAR(FullDate), 1, 1),
        DATEFROMPARTS(YEAR(FullDate), 12, 31),
        CASE 
            WHEN (YEAR(FullDate) % 4 = 0 AND YEAR(FullDate) % 100 <> 0) OR (YEAR(FullDate) % 400 = 0) THEN 1
            ELSE 0
        END,
        SYSDATETIME()
    FROM DateSequence DS
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Gold].[DimDate] D
        WHERE D.DateKey = CAST(CONVERT(CHAR(8), DS.FullDate, 112) AS INT)
    );
END;
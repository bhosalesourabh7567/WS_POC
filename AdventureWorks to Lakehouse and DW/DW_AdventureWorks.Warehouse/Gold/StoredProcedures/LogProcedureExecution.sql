CREATE PROCEDURE [Gold].[LogProcedureExecution]
(
    @ProcedureName VARCHAR(255),        -- Name of the stored procedure
    @StartTime DATETIME2(3),            -- Start time of execution
    @EndTime DATETIME2(3),              -- End time of execution
    @Status VARCHAR(50),                -- Status ('Success' or 'Failed')
    @ErrorMessage VARCHAR(1000),        -- Error message if any
    @ExecutedBy VARCHAR(500)            -- User executing the procedure
)
AS
BEGIN
    DECLARE @LogID VARCHAR(50);
    
    -- Generate a unique LogID using a combination of NEWID and current timestamp
    SET @LogID = CONCAT(CONVERT(VARCHAR(36), NEWID()), '-', CONVERT(VARCHAR, GETDATE(), 121));

    -- Insert a log entry into the AuditLog table
    INSERT INTO [DW_AdventureWorks].[Gold].[AuditLog]
    (
        LogID,                           -- Unique LogID
        ProcedureName,
        StartTime,
        EndTime,
        Status,
        ErrorMessage,
        ExecutedBy,
        ExecutionDuration
    )
    VALUES
    (
        @LogID,                          -- Unique LogID
        @ProcedureName,                  -- Procedure Name
        @StartTime,                      -- Start Time
        @EndTime,                        -- End Time
        @Status,                         -- Status
        @ErrorMessage,                   -- Error Message
        @ExecutedBy,                     -- Executed By
        DATEDIFF(SECOND, @StartTime, @EndTime) -- Execution Duration in seconds
    );
END;
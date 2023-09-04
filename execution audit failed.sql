CREATE   PROCEDURE [dbo].[opp_audit_execution_failed]
	 @parent_audit_ts VARCHAR(50)
	,@root_ts VARCHAR(50)
	,@step_type NVARCHAR(255)
	,@stepname NVARCHAR(255)
	,@errmsg NVARCHAR(2000) 
	,@target_table NVARCHAR(255) 
AS 

	DECLARE @audit_ts VARCHAR(21);
BEGIN 

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	BEGIN TRY
				
		SET @audit_ts = FORMAT(SYSDATETIME(), 'yyyyMMddHHmmssfffffff');
		IF @parent_audit_ts = '-1' SET @parent_audit_ts = @audit_ts;
		IF @root_ts = '-1' SET @root_ts = @parent_audit_ts;

		DECLARE @started_execution_time datetime2;
			--get execution time of started event
			SELECT @started_execution_time = execution_time
			 FROM dbo.opt_execution_audit
			WHERE execution_status = 'started'
			  AND parent_audit_ts = @parent_audit_ts
			  AND step_name = @stepname
			  AND target_table = @target_table
			
			
		DECLARE @execution_time DATETIME2;
		SET @execution_time = TRY_CAST(
								TRY_CAST(
									SUBSTRING(@audit_ts, 1, 4) + '-' + 
									SUBSTRING(@audit_ts, 5, 2) + '-' + 
									SUBSTRING(@audit_ts, 7, 2) + ' ' + 
									SUBSTRING(@audit_ts, 9, 2) + ':' + 
									SUBSTRING(@audit_ts, 11, 2) + ':' + 
									SUBSTRING(@audit_ts, 13, 2) + '. ' + 
									SUBSTRING(@audit_ts, 15, 7)
									AS DATETIME2(7)
								) AS DATETIME);

		--create step and set starttime
		INSERT INTO dbo.opt_execution_audit (
			 audit_ts 
			,parent_audit_ts
			,root_ts
			,step_type
			,step_name
			,execution_time 
			,execution_status
			,execution_user
			,execution_error_message 
			,target_table
			,duration
		)
		SELECT 
			 @audit_ts   
			,@parent_audit_ts 
			,@root_ts 
			,@step_type
			,@stepname
			,@execution_time AS execution_time
			,'failed'
			,SYSTEM_USER
			,@errmsg
			,@target_table
			,ABS(DATEDIFF(SECOND, @started_execution_time, @execution_time))
		;

	END TRY

	BEGIN CATCH

		THROW;

	END CATCH
  

END;
GO
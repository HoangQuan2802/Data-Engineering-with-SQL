--	================Velocity==============
--	Description: - This initiate the started record for tracking execution by pipeline (Stored procedure, Azure Data Factory pipeline, Azure Data Brick pipeline, SSIS package) 
--				 with insert only strategy. 
				 
--				 - Everytime a pipeline is executed, it must call this store procedure to genereate started execution record, unless there is other way to track execution

--				 - This stored procedure also call etl.sp_save_dependencies to track dependencies between object in the pipeline. it is done via Json @dependencies parameter
--	======================================
CREATE PROCEDURE [etl].[sp_start_execution_audit]
	 @audit_ts VARCHAR(50) = '-1'					--	audit timestamp  in format yyyyMMddHHmmssfffffff
	,@parent_audit_ts VARCHAR(50) = '-1' 			--	Optional parameter, 1 upper level audit timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff, if equal to -1, it should equal to audit_ts
	,@root_ts VARCHAR(50) = '-1' 					--	Optional parameter, the timestamp of the root caller in the calling hierarchy in format yyyyMMddHHmmssfffffff, if equal to -1, it should be audit_ts
	,@step_type NVARCHAR(255)						--	Type can be: SSIS Package, ADF pipeline, stored procedure, ADB pipeline
	,@stepname NVARCHAR(255)						--	Name of step, for instance: sp_sit_stt_customer, stt_customer
	,@target_table NVARCHAR(255)					--	The table to be the target of the execution, for instance: load to dim_customer, dim_customer is the target table
	/*
	This parameter is the json-based body of dependencies within the executed object

	Json body:
	[
		{
			"sysschema_name":"db1",								--can be database name, file name
			"host_name":"spbt.database.windows.net",			--the location where the db or file is located
			"object_type":"table",								-- type of object used in the executed proc, pipeline: table, view, file...
			"object_name":"stt.test",							-- name of object used in the executed proc, pipeline.. including the object schema: sit.customer, stt.customer...
			"object_role":"source"								-- role of object used in the executed proc, pipeline.. does it used as lookup table, source or target
		},
		{
			"sysschema_name":"db2",								--can be database name, file name
			"host_name":"spbt.database.windows.net",			--the location where the db or file is located
			"object_type":"table",
			"object_name":"stt.test",
			"object_role":"lookup"
		},
		{
			"sysschema_name":"db3",								--can be database name, file name
			"host_name":"spbt.database.windows.net",			--the location where the db or file is located
			"object_type":"table",
			"object_name":"dmt.test",
			"object_role":"target"
		}
	]
	*/	
	,@dependencies NVARCHAR(MAX) = NULL				--  Optional parameter for managing dependencies in the executed object
AS

BEGIN 

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

BEGIN TRY
		
		--if there is no audit timestamp passed, get current sysdatetime and converter into format yyyyMMddHHmmssfffffff
		IF @audit_ts = '-1' SET @audit_ts = SYSDATETIME();

		--if there is no parent audit timestamp passed, assign the current @audit_ts to @parent_audit_ts
		IF @parent_audit_ts = '-1' 
			SET @parent_audit_ts = @audit_ts;
		
		--if there is no root audit timestamp passed, assign the current @parent_audit_ts to @root_ts
		IF @root_ts = '-1' 
			SET @root_ts = @parent_audit_ts;


		--create started step and set starttime
		INSERT INTO etl.execution_audit 
		(
			 audit_ts 
			,parent_audit_ts
			,root_ts
			,step_type
			,step_name
			,execution_time
			,execution_status
			,execution_user
			,target_table
		)
		SELECT 
			 @audit_ts   
			,@parent_audit_ts 
			,@root_ts 
			,@step_type
			,@stepname
			,@audit_ts  AS execution_time		-- call this function to convert current audit_ts to execution time
			,'started'											-- set execution status to started
			,SUSER_NAME()
			,@target_table;

		/*
			Purpose: To save object dependencies used in proc, pipeline into dependencies_log table
		*/
		IF(@dependencies is not null)
		BEGIN
			--if there is no parent audit timestamp passed, assign the current @audit_ts to @parent_audit_ts
			SET @parent_audit_ts = iif(@parent_audit_ts = -1,  @audit_ts, @parent_audit_ts);

			--if there is no root audit timestamp passed, assign the current @parent_audit_ts to @root_ts
			SET @root_ts = iif(@root_ts  = -1,  @audit_ts, @parent_audit_ts);

			-- calling sp_save_dependencies to save the information
			EXEC sp_save_dependencies @audit_id = @audit_ts,					--	audit timestamp  in format yyyyMMddHHmmssfffffff
									  @parent_audit_id = @parent_audit_ts,		--	1 upper level audit timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff, if equal to -1, it should equal to audit_ts
									  @root_id = @root_ts,						--	the timestamp of the root caller in the calling hierarchy in format yyyyMMddHHmmssfffffff, if equal to -1, it should be audit_ts
									  @dependencies = @dependencies				--  This parameter is the json-based body of dependencies within the executed object
									  ;
		END

END TRY

BEGIN CATCH

	THROW;

END CATCH
END
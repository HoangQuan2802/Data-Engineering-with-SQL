--	================Velocity==============
--	Description: - This is to record the point of time, mark when a table is last loaded
--	======================================
CREATE PROCEDURE [etl].[sp_set_watermark]
(
@object_name			VARCHAR(255),					-- the process name that extracts and loads data betweeen source and target
@parent_audit_ts VARCHAR(50),							-- timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff when the watermark is recorded via an execution
@key_val_1				VARCHAR(255),					-- watermark value, it can be the max of the delta column in the source system, it can be sysdate of the source when data extracted
@key_data_type			VARCHAR(255),					-- data type of the delta column (datetime, date or number)
@root_ts VARCHAR(50) = '-1'
)

AS
/*
*/
BEGIN TRY

	DECLARE @step_type NVARCHAR(255);
	DECLARE @stepname NVARCHAR(255);
	DECLARE @INSERT_STATEMENT NVARCHAR(1000);
	DECLARE @param_definition NVARCHAR(1000)
	DECLARE @watermarktable NVARCHAR(255);
	DECLARE @target_table NVARCHAR(255);
	DECLARE @audit_ts DATETIME;
	DECLARE @new_rows INT;
	DECLARE @deleted_rows INT;
	DECLARE @updated_rows INT;

	SET @step_type = N'stored procedure';
	SET @stepname = N'sp_set_watermark for ' + @object_name; 
	SET @watermarktable = N'etl.watermark';
	SET @target_table = N'watermark';

	SET @audit_ts = GETDATE();

	EXEC etl.sp_start_execution_audit @parent_audit_ts = @parent_audit_ts 
								 , @root_ts = @root_ts 
								 , @step_type = @step_type  
								 , @stepname = @stepname
								 , @target_table = @target_table;

	/*
		Prepare dynamic insert statement with input value from parameters
			Requires parametes:
				- @audit_ts
				- @object_name
				- @key_val_1
				- @key_data_type
	*/
	SET @insert_statement = 'BEGIN TRANSACTION
							INSERT INTO ' + @watermarktable
									+ '(
											timestamp	
											,[object_name]
											,key_1
											,key_1_desc
										)
									VALUES
										(@audit_ts,
										@object_name,
										@key_val_1,
										@key_data_type
										);
										SELECT @rowc = @@ROWCOUNT;
										COMMIT;'
										;

	

	/*
		Prepare parameter mapping
	*/
	SET @param_definition = N'@object_name			VARCHAR(255),
							@audit_ts DATETIME, 
							@key_val_1				VARCHAR(255),
							@key_data_type			VARCHAR(255),
							@rowc INT OUTPUT';

	--execute dynamic insert statement with required parameters and output number of insert record
	EXEC SP_EXECUTESQL @insert_statement, @param_definition, @object_name, @audit_ts, @key_val_1, @key_data_type, @new_rows OUTPUT;

	SET ROWCOUNT 0;

	

	EXEC etl.sp_end_execution_audit @parent_audit_ts = @parent_audit_ts 
						   , @root_ts = @root_ts 
						   , @step_type = @step_type  
						   , @stepname = @stepname
						   , @new_rows = @new_rows
						   , @updated_rows = @updated_rows
						   , @deleted_rows = @deleted_rows
						   , @target_table = @target_table;

END TRY
BEGIN CATCH 

    IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK;
	END

	DECLARE @errmsg NVARCHAR(2000);
	SET  @errmsg = ERROR_MESSAGE();

	EXEC etl.sp_fail_execution_audit @parent_audit_ts = @parent_audit_ts 
								, @root_ts = @root_ts 
								, @step_type = @step_type  
								, @stepname = @stepname
								, @errmsg = @errmsg
								, @target_table = @target_table;

	THROW;
	
END CATCH
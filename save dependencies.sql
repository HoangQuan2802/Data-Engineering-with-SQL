--	================Velocity==============
--	Description: - This stored procedure is called to save the dependencies between within the executed pipeline 
--				   via an input json-based parameter with below json structure
--	======================================
CREATE PROCEDURE sp_save_dependencies
( 
	 @parent_audit_id INT = -1 
	,@root_id INT = -1 
	,@audit_id INT
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
	,@dependencies NVARCHAR(MAX)
)

AS
BEGIN TRY

	INSERT INTO etl.dependencies_log
    (
		[audit_id]
		,[parent_audit_id]
		,[root_id]
		,[sysschema_name]
		,[host]
		,[object_type]
		,[object_name]
		,[object_role]
	)
	SELECT @audit_id
		   ,@parent_audit_id
		   ,@root_id
		   ,details.sysschema_name
		   ,details.host_name
		   ,details.object_type
		   ,details.object_name
		   ,details.object_role
	FROM OPENJSON(@dependencies)
		   WITH (
				 sysschema_name nvarchar(255)
				,[host_name] nvarchar(255)
				,[object_type] nvarchar(255)
				,[object_name] nvarchar(255)
				,[object_role] nvarchar(255)
			) details;

END TRY

BEGIN CATCH

	THROW;

END CATCH
--	================Velocity==============
--	Description: - This table to keep the track dependencies between object in the executed pipeline, it is done via Json @dependencies parameter
--	======================================
CREATE TABLE [etl].[dependencies_log](
	[audit_id] [int] NOT NULL,								--	audit timestamp  in format yyyyMMddHHmmssfffffff
	[parent_audit_id] [int] NOT NULL,						--	Optional parameter, 1 upper level audit timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff, if equal to -1, it should equal to audit_ts
	[root_id] [int] NOT NULL,								--	Optional parameter, the timestamp of the root caller in the calling hierarchy in format yyyyMMddHHmmssfffffff, if equal to -1, it should be audit_ts
	[sysschema_name] [nvarchar](255) NOT NULL,				--  can be database name, file name
	[host] [nvarchar](255) NOT NULL,						--  the location where the db or file is located
	[object_type] [nvarchar](255) NULL,						--	type of object used in the executed proc, pipeline: table, view, file...
	[object_name] [nvarchar](255) NOT NULL,					--	name of object used in the executed proc, pipeline.. including the object schema: sit.customer, stt.customer...
	[object_role] [nvarchar](255) NOT NULL,					--	role of object used in the executed proc, pipeline.. does it used as lookup table, source or target
PRIMARY KEY CLUSTERED 
(
	[audit_id] ASC,
	[parent_audit_id] ASC,
	[root_id] ASC,
	[sysschema_name] ASC,
	[host] ASC,
	[object_name] ASC,
	[object_role] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
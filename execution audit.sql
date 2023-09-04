--	================Velocity==============
--	Description: - This table is to keep track of pipeline execution (Stored procedure, Azure Data Factory pipeline, Azure Data Brick pipeline, SSIS package) 
--				 with insert only strategy. 
				 
--				 - Everytime a pipeline is executed, a record is inserted into this table
--	======================================

CREATE TABLE [etl].[execution_audit] (
    [audit_ts]                DATETIME2 (7)   NOT NULL,					--	audit timestamp  in format yyyyMMddHHmmssfffffff
    [parent_audit_ts]         DATETIME2 (7)   NOT NULL,					--	1 upper level audit timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff, if equal to -1, it should equal to audit_ts
    [root_ts]                 DATETIME2 (7)   NOT NULL,					--	the timestamp of the root caller in the calling hierarchy in format yyyyMMddHHmmssfffffff, if equal to -1, it should be audit_ts
    [step_type]               VARCHAR (255)  NULL,						--	Type can be: SSIS Package, ADF pipeline, stored procedure, ADB pipeline
    [step_name]               VARCHAR (255)  NULL,						--	Name of step, for instance: sp_sit_stt_customer, stt_customer
    [target_table]            VARCHAR (255)  NULL,						--	The table to be the target of the execution, for instance: load to dim_customer, dim_customer is the target table
    [execution_time]          DATETIME2 (7)  NULL,						--	Executed time got from the audit_ts using following function: dbo.string_to_ts(@audit_ts)
    [new_rows]                INT            DEFAULT ((0)) NULL,		--	Indicate number of row inserted into target table
    [updated_rows]            INT            DEFAULT ((0)) NULL,		--	Indicate number of rows updated in target table
    [deleted_rows]            INT            DEFAULT ((0)) NULL,		--	Indicate number of rows marked soft delete in target table by updating deleted_audit_ts = @parent_audit_ts
    [execution_user]          VARCHAR (255)  NULL,						--  The user who made the execution, normally it is the service account.
    [execution_status]        VARCHAR (50)   NULL,						--	Execution status: started, failed and succeeded
    [execution_error_message] VARCHAR (2000) NULL,						--	Store any error message when the execution failed
    [execution_info_message]  VARCHAR (4000) NULL,						--  Store any auxiliary message if needed
    [duration]                INT            NULL						--  Execution duration is calculated when the execution reachs succeeeded or failed status
);


GO

--it is important to create this nonclustered index to maintain the uniqueness of execution record for data linage purpose.
CREATE NONCLUSTERED INDEX [nci_wi_execution_audit_F018D352807F9A3A1F5275E15AE3A152]
    ON [etl].[execution_audit]([execution_status] ASC, [step_name] ASC, [step_type] ASC, [root_ts] ASC)
    INCLUDE([audit_ts]);
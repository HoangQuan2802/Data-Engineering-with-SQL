SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[sip_T001W] AS
BEGIN 
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @audit_ts AS VARCHAR(22);
    DECLARE @parent_audit_ts AS VARCHAR(22);
    DECLARE @root_ts AS VARCHAR(22);
	DECLARE @step_type NVARCHAR(255);
	DECLARE @stepname NVARCHAR(255); 
	DECLARE @watermarktable NVARCHAR(255);
	DECLARE @target_table NVARCHAR(255);
	DECLARE @new_rows INT = 0;
    DECLARE @deleted_rows INT = 0;
	DECLARE @updated_rows INT = 0;

	SET @stepname = 'sip_T001W'
	SET @step_type = 'stored procedure';
	SET @target_table = 'dbo.sit_T001W';
	SET @audit_ts = FORMAT(SYSDATETIME(), 'yyyyMMddHHmmssfffffff');
    set @parent_audit_ts = @audit_ts;
    set @root_ts = @parent_audit_ts;

	EXEC dbo.opp_audit_execution_start
			  @audit_ts = @audit_ts
            , @parent_audit_ts = @parent_audit_ts 
			, @root_ts = @root_ts 
			, @step_type = @step_type  
			, @stepname = @stepname
			, @target_table = @target_table;

    --------  Start Stage  --------
	BEGIN TRY
	    TRUNCATE TABLE sit_T001W

		BEGIN TRANSACTION
        INSERT INTO sit_T001W (
		 [WERKS]
		,[NAME1]
		,[BWKEY]
		,[KUNNR]
		,[LIFNR]
		,[FABKL]
		,[NAME2]
		,[STRAS]
		,[PFACH]
		,[PSTLZ]
		,[ORT01]
		,[EKORG]
		,[VKORG]
		,[CHAZV]
		,[KKOWK]
		,[KORDB]
		,[BEDPL]
		,[LAND1]
		,[REGIO]
		,[COUNC]
		,[CITYC]
		,[ADRNR]
		,[IWERK]
		,[TXJCD]
		,[VTWEG]
		,[SPART]
		,[SPRAS]
		,[WKSOP]
		,[AWSLS]
		,[CHAZV_OLD]
		,[VLFKZ]
		,[BZIRK]
		,[ZONE1]
		,[TAXIW]
		,[BZQHL]
		,[LET01]
		,[LET02]
		,[LET03]
		,[TXNAM_MA1]
		,[TXNAM_MA2]
		,[TXNAM_MA3]
		,[BETOL]
		,[J_1BBRANCH]
		,[VTBFI]
		,[FPRFW]
		,[ACHVM]
		,[DVSART]
		,[NODETYPE]
		,[NSCHEMA]
		,[PKOSA]
		,[MISCH]
		,[MGVUPD]
		,[VSTEL]
		,[MGVLAUPD]
		,[MGVLAREVAL]
		,[SOURCING]
		,[SGT_STAT]
		,[FSH_MG_ARUN_REQ]
		,[FSH_SEAIM]
		,[FSH_BOM_MAINTENANCE]
		,[OILIVAL]
		,[OIHVTYPE]
		,[OIHCREDIPI]
		,[STORETYPE]
		,[DEP_STORE]
		,[audit_ts]
        )
        (SELECT      
 		 [WERKS]
		,[NAME1]
		,[BWKEY]
		,[KUNNR]
		,[LIFNR]
		,[FABKL]
		,[NAME2]
		,[STRAS]
		,[PFACH]
		,[PSTLZ]
		,[ORT01]
		,[EKORG]
		,[VKORG]
		,[CHAZV]
		,[KKOWK]
		,[KORDB]
		,[BEDPL]
		,[LAND1]
		,[REGIO]
		,[COUNC]
		,[CITYC]
		,[ADRNR]
		,[IWERK]
		,[TXJCD]
		,[VTWEG]
		,[SPART]
		,[SPRAS]
		,[WKSOP]
		,[AWSLS]
		,[CHAZV_OLD]
		,[VLFKZ]
		,[BZIRK]
		,[ZONE1]
		,[TAXIW]
		,[BZQHL]
		,[LET01]
		,[LET02]
		,[LET03]
		,[TXNAM_MA1]
		,[TXNAM_MA2]
		,[TXNAM_MA3]
		,[BETOL]
		,[J_1BBRANCH]
		,[VTBFI]
		,[FPRFW]
		,[ACHVM]
		,[DVSART]
		,[NODETYPE]
		,[NSCHEMA]
		,[PKOSA]
		,[MISCH]
		,[MGVUPD]
		,[VSTEL]
		,[MGVLAUPD]
		,[MGVLAREVAL]
		,[SOURCING]
		,[SGT_STAT]
		,[FSH_MG_ARUN_REQ]
		,[FSH_SEAIM]
		,[FSH_BOM_MAINTENANCE]
		,[OILIVAL]
		,[OIHVTYPE]
		,[OIHCREDIPI]
		,[STORETYPE]
		,[DEP_STORE]
		,[audit_ts]
        FROM (
            SELECT 
            *
            ,ROW_NUMBER() OVER (PARTITION BY WERKS ORDER BY audit_ts DESC) AS rn
            FROM ext_sap_T001W
        ) ranked
        WHERE rn = 1)
        OPTION (LABEL = 'insert');

        SET @new_rows = (SELECT top 1 row_count
                        FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
                        Where r.request_id = s.request_id 
                        and row_count > -1
                        and r.[label] = 'insert'
                        order by r.[end_time] desc);
		COMMIT;
    	--------  End Stage  --------
		
		EXEC dbo.opp_audit_execution_end 
        		  @parent_audit_ts = @parent_audit_ts 
			    , @root_ts = @root_ts 
				, @step_type = @step_type  
				, @stepname = @stepname
				, @new_rows = @new_rows
				, @updated_rows = @updated_rows
				, @deleted_rows = @deleted_rows
				, @target_table = @target_table
		;
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT > 0 
		BEGIN
			ROLLBACK;
		END

		DECLARE @errmsg NVARCHAR(2000);
		SET  @errmsg = ERROR_MESSAGE();

		EXEC dbo.opp_audit_execution_failed 
        		  @parent_audit_ts = @parent_audit_ts 
			    , @root_ts = @root_ts 
				, @step_type = @step_type  
				, @stepname = @stepname
				, @errmsg = @errmsg
				, @target_table = @target_table;
		THROW;
	END CATCH;	
END;
GO
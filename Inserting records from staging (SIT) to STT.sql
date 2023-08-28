CREATE PROCEDURE [dbo].[stp_T001W]
AS
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

	SET @stepname = 'stp_T001W'
	SET @step_type = 'stored procedure';
	SET @target_table = 'dbo.stt_T001W';
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
	-----------TEMP TABLE -------------------------
		IF OBJECT_ID(N'tempdb..#s') IS NOT NULL
		BEGIN
			DROP TABLE #s;
		END

		SELECT 		 
			 [MANDT]
			,[WERKS]
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
			,CONVERT( VARCHAR(100), HASHBYTES('SHA2_256' , 
					COALESCE(TRIM(MANDT), '^')
			+ '|' + COALESCE(TRIM(WERKS), '^')
			+ '|' + COALESCE(TRIM(NAME1), '^')
			+ '|' + COALESCE(TRIM(BWKEY), '^')
			+ '|' + COALESCE(TRIM(KUNNR), '^')
			+ '|' + COALESCE(TRIM(LIFNR), '^')
			+ '|' + COALESCE(TRIM(FABKL), '^')
			+ '|' + COALESCE(TRIM(NAME2), '^')
			+ '|' + COALESCE(TRIM(STRAS), '^')
			+ '|' + COALESCE(TRIM(PFACH), '^')
			+ '|' + COALESCE(TRIM(PSTLZ), '^')
			+ '|' + COALESCE(TRIM(ORT01), '^')
			+ '|' + COALESCE(TRIM(EKORG), '^')
			+ '|' + COALESCE(TRIM(VKORG), '^')
			+ '|' + COALESCE(TRIM(CHAZV), '^')
			+ '|' + COALESCE(TRIM(KKOWK), '^')
			+ '|' + COALESCE(TRIM(KORDB), '^')
			+ '|' + COALESCE(TRIM(BEDPL), '^')
			+ '|' + COALESCE(TRIM(LAND1), '^')
			+ '|' + COALESCE(TRIM(REGIO), '^')
			+ '|' + COALESCE(TRIM(COUNC), '^')
			+ '|' + COALESCE(TRIM(CITYC), '^')
			+ '|' + COALESCE(TRIM(ADRNR), '^')
			+ '|' + COALESCE(TRIM(IWERK), '^')
			+ '|' + COALESCE(TRIM(TXJCD), '^')
			+ '|' + COALESCE(TRIM(VTWEG), '^')
			+ '|' + COALESCE(TRIM(SPART), '^')
			+ '|' + COALESCE(TRIM(SPRAS), '^')
			+ '|' + COALESCE(TRIM(WKSOP), '^')
			+ '|' + COALESCE(TRIM(AWSLS), '^')
			+ '|' + COALESCE(TRIM(CHAZV_OLD), '^')
			+ '|' + COALESCE(TRIM(VLFKZ), '^')
			+ '|' + COALESCE(TRIM(BZIRK), '^')
			+ '|' + COALESCE(TRIM(ZONE1), '^')
			+ '|' + COALESCE(TRIM(TAXIW), '^')
			+ '|' + COALESCE(TRIM(BZQHL), '^')
			+ '|' + COALESCE(CONVERT(VARCHAR(50),LET01), '^')
			+ '|' + COALESCE(CONVERT(VARCHAR(50),LET02), '^')
			+ '|' + COALESCE(CONVERT(VARCHAR(50),LET03), '^')
			+ '|' + COALESCE(TRIM(TXNAM_MA1), '^')
			+ '|' + COALESCE(TRIM(TXNAM_MA2), '^')
			+ '|' + COALESCE(TRIM(TXNAM_MA3), '^')
			+ '|' + COALESCE(TRIM(BETOL), '^')
			+ '|' + COALESCE(TRIM(J_1BBRANCH), '^')
			+ '|' + COALESCE(TRIM(VTBFI), '^')
			+ '|' + COALESCE(TRIM(FPRFW), '^')
			+ '|' + COALESCE(TRIM(ACHVM), '^')
			+ '|' + COALESCE(TRIM(DVSART), '^')
			+ '|' + COALESCE(TRIM(NODETYPE), '^')
			+ '|' + COALESCE(TRIM(NSCHEMA), '^')
			+ '|' + COALESCE(TRIM(PKOSA), '^')
			+ '|' + COALESCE(TRIM(MISCH), '^')
			+ '|' + COALESCE(TRIM(MGVUPD), '^')
			+ '|' + COALESCE(TRIM(VSTEL), '^')
			+ '|' + COALESCE(TRIM(MGVLAUPD), '^')
			+ '|' + COALESCE(TRIM(MGVLAREVAL), '^')
			+ '|' + COALESCE(TRIM(SOURCING), '^')
			+ '|' + COALESCE(TRIM(SGT_STAT), '^')
			+ '|' + COALESCE(TRIM(FSH_MG_ARUN_REQ), '^')
			+ '|' + COALESCE(TRIM(FSH_SEAIM), '^')
			+ '|' + COALESCE(TRIM(FSH_BOM_MAINTENANCE), '^')
			+ '|' + COALESCE(TRIM(OILIVAL), '^')
			+ '|' + COALESCE(TRIM(OIHVTYPE), '^')
			+ '|' + COALESCE(TRIM(OIHCREDIPI), '^')
			+ '|' + COALESCE(TRIM(STORETYPE), '^')
			+ '|' + COALESCE(TRIM(DEP_STORE), '^')
			 ), 2) as row_hash
		  INTO #s
		  FROM dbo.sit_T001W	
	-----------TEMP TABLE -------------------------

    --------  Start Stage  --------
	BEGIN TRY
		BEGIN TRANSACTION
		/***
		--Soft delete stt table when not existing in source
		DECLARE @is_table_empty int;
		SELECT 
			@is_table_empty = COUNT(1) 
		FROM #s;

		IF(@is_table_empty > 0)
		BEGIN
			 
			UPDATE t
			SET
				deleted_audit_ts = @audit_ts
			FROM  dbo.dim_customer t
			WHERE NOT EXISTS (
				SELECT *
				FROM #s s
				WHERE  t.artikel_id = s.artikel_id

			)
			and t.deleted_audit_ts is null;

			SET @deleted_rows = @@ROWCOUNT;
			SET ROWCOUNT 0;

		END	
		***/

		--INSERT data if not exists based on business key
		INSERT INTO dbo.stt_T001W( 
		 	 [MANDT]
			,[WERKS]
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
			------ Audit ------
			,audit_ts
			,row_hash
		)
		SELECT 
		 	 s.[MANDT]
			,s.[WERKS]
			,s.[NAME1]
			,s.[BWKEY]
			,s.[KUNNR]
			,s.[LIFNR]
			,s.[FABKL]
			,s.[NAME2]
			,s.[STRAS]
			,s.[PFACH]
			,s.[PSTLZ]
			,s.[ORT01]
			,s.[EKORG]
			,s.[VKORG]
			,s.[CHAZV]
			,s.[KKOWK]
			,s.[KORDB]
			,s.[BEDPL]
			,s.[LAND1]
			,s.[REGIO]
			,s.[COUNC]
			,s.[CITYC]
			,s.[ADRNR]
			,s.[IWERK]
			,s.[TXJCD]
			,s.[VTWEG]
			,s.[SPART]
			,s.[SPRAS]
			,s.[WKSOP]
			,s.[AWSLS]
			,s.[CHAZV_OLD]
			,s.[VLFKZ]
			,s.[BZIRK]
			,s.[ZONE1]
			,s.[TAXIW]
			,s.[BZQHL]
			,s.[LET01]
			,s.[LET02]
			,s.[LET03]
			,s.[TXNAM_MA1]
			,s.[TXNAM_MA2]
			,s.[TXNAM_MA3]
			,s.[BETOL]
			,s.[J_1BBRANCH]
			,s.[VTBFI]
			,s.[FPRFW]
			,s.[ACHVM]
			,s.[DVSART]
			,s.[NODETYPE]
			,s.[NSCHEMA]
			,s.[PKOSA]
			,s.[MISCH]
			,s.[MGVUPD]
			,s.[VSTEL]
			,s.[MGVLAUPD]
			,s.[MGVLAREVAL]
			,s.[SOURCING]
			,s.[SGT_STAT]
			,s.[FSH_MG_ARUN_REQ]
			,s.[FSH_SEAIM]
			,s.[FSH_BOM_MAINTENANCE]
			,s.[OILIVAL]
			,s.[OIHVTYPE]
			,s.[OIHCREDIPI]
			,s.[STORETYPE]
			,s.[DEP_STORE]
			----- Audit ------
			,s.audit_ts
			,s.row_hash
		FROM #s s
        LEFT JOIN stt_T001W t
			ON  s.SPRAS = t.SPRAS
			AND s.WERKS = t.WERKS
		WHERE s.row_hash <> coalesce(t.row_hash,'')
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
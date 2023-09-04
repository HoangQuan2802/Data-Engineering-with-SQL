CREATE PROCEDURE [etl].[sp_dmt_dim_template_scd2]
	@parent_audit_ts VARCHAR(50) = '-1', --optional parameter 
	@root_ts VARCHAR(50) = '-1'--optional parameter
AS

BEGIN TRY

	DECLARE @step_type NVARCHAR(255);
	DECLARE @stepname NVARCHAR(255); 
	DECLARE @watermarktable NVARCHAR(255);
	DECLARE @target_table NVARCHAR(255);
	DECLARE @new_rows INT;
	DECLARE @deleted_rows INT;
	DECLARE @updated_rows INT;
	DECLARE @from_date VARCHAR(100);
	
	--SET @from_date = dateadd(day, -30, convert(date, getdate()));-- load data from current date, it can be 1 day, 1 week, 1 month... back
	SET @step_type = N'stored procedure';
	SET @stepname = N'sp_stt_dmt_dim_customer';
	SET @target_table = N'dmt.dim_customer';


	EXEC etl.start_execution_audit @parent_audit_ts = @parent_audit_ts 
								 , @root_ts = @root_ts 
								 , @step_type = @step_type  
								 , @stepname = @stepname
								 , @target_table = @target_table;

	/*2019-05-18: Added by Di Truong- Set from_date = max timestamp from dmt*/
	SELECT @from_date =  COALESCE(MAX(COALESCE(deleted_audit_ts, updated_audit_ts, audit_ts, '-1')), '-1')
	FROM dmt.dim_customer
	/*2019-05-18: Added by Di Truong- Set from_date = max timestamp from dmt*/

	--load lastest records from stt from a particulate date @from_date
	SELECT *
	  INTO #tmp_lastest_stt
	FROM (
	SELECT DIST_CD, CUST_CD,
		   COALESCE(DELETED_AUDIT_TS, AUDIT_TS, '-1') AS AUDIT_TS, 
		   RANK() OVER (PARTITION BY DIST_CD, 
									 CUST_CD 
							ORDER BY AUDIT_TS DESC) AS RN
	FROM stt.rpm_mst_cust 
	WHERE COALESCE(DELETED_AUDIT_TS, AUDIT_TS, '-1') > @from_date
	) lastest_stt
	WHERE RN = 1


	/*
		Have to add coalesce around each business column, since there should be no NULL value in dimension
	*/
	
	SELECT cust.audit_ts,
			cust.updated_audit_ts,
			cust.deleted_audit_ts,
			getdate() as scd_start,
			'1900-01-01' as scd_from,
			'2999-12-31' as scd_to,
			1 as scd_active,
			1 as scd_version,
			0 as inferred_flag,
			cust.source_id, 
			CONVERT(varchar(100),HASHBYTES('SHA2_256', COALESCE(cust.DIST_CD,'') + '|' +  	COALESCE(cust.CUST_CD,'') + '|' +  	COALESCE(cust.INT_CUST_CD,'') + '|' +  	COALESCE(cust.BE_CUST_CD,'') + '|' +  	COALESCE(cust.CUST_NAME,'') + '|' +  	COALESCE(cust.CUST_NAME2,'') + '|' +  	(CASE WHEN cust.CUST_OPENDT IS NULL THEN '' ELSE format(cust.CUST_OPENDT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	COALESCE(cust.CUST_REGNO,'') + '|' +  	COALESCE(cust.CUST_TYPE,'') + '|' +  	COALESCE(cust.PAYMENT_MODE,'') + '|' +  	COALESCE(cust.PRICEGRP_CD,'') + '|' +  	COALESCE(cust.AREA_CD,'') + '|' +  	COALESCE(cust.CUST_HIER3,'') + '|' +  	COALESCE(cust.BILLTO_CD,'') + '|' +  	COALESCE(cust.KEYACCCLS_CD,'') + '|' +  	COALESCE(cust.KEYACCCAT_CD,'') + '|' +  	COALESCE(cust.KEYACCSUBCAT_CD,'') + '|' +  	COALESCE(cust.KEYACCGRP1_CD,'') + '|' +  	COALESCE(cust.KEYACCGRP2_CD,'') + '|' +  	COALESCE(cust.KEYACCGRP3_CD,'') + '|' +  	COALESCE(cust.ADDR_1,'') + '|' +  	COALESCE(cust.ADDR_2,'') + '|' +  	COALESCE(cust.ADDR_3,'') + '|' +  	COALESCE(cust.ADDR_4,'') + '|' +  	COALESCE(cust.ADDR_5,'') + '|' +  	COALESCE(cust.ADDR_POSTAL,'') + '|' +  	COALESCE(cust.CONT_PR,'') + '|' +  	COALESCE(cust.CONT_NO,'') + '|' +  	COALESCE(cust.CONT_NO_EXT,'') + '|' +  	COALESCE(cust.ADD_CONT_NO,'') + '|' +  	COALESCE(cust.MOBILE_NO,'') + '|' +  	COALESCE(cust.CONT_FAXNO,'') + '|' +  	COALESCE(cust.CONT_EMAIL,'') + '|' +  	COALESCE(cust.INVTERM_CD,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_CRDLMT),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.OUTSTANDING_BAL),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_DISC),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_TAX),'0') + '|' +  	COALESCE(cust.CUST_TAXNO,'') + '|' +  	COALESCE(cust.CUST_TAXREGNO,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_REG),'0') + '|' +  	COALESCE(cust.CUST_TINNO,'') + '|' +  	COALESCE(cust.CUST_LSTNO,'') + '|' +  	COALESCE(cust.CUST_CSTNO,'') + '|' +  	COALESCE(cust.CUST_GSTNO,'') + '|' +  	COALESCE(cust.REMARKS,'') + '|' +  	COALESCE(cust.BANK_CD,'') + '|' +  	COALESCE(cust.BANK_BRANCH,'') + '|' +  	COALESCE(cust.BANK_ACCNO,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.OPENING_BAL),'0') + '|' +  	(CASE WHEN cust.OPENING_BAL_DT IS NULL THEN '' ELSE format(cust.OPENING_BAL_DT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	COALESCE(cust.USER_DEFINE1,'') + '|' +  	COALESCE(cust.USER_DEFINE2,'') + '|' +  	COALESCE(cust.USER_DEFINE3,'') + '|' +  	COALESCE(cust.USER_DEFINE4,'') + '|' +  	COALESCE(cust.USER_DEFINE5,'') + '|' +  	COALESCE(cust.HQ_DEFINE1,'') + '|' +  	COALESCE(cust.HQ_DEFINE2,'') + '|' +  	COALESCE(cust.HQ_DEFINE3,'') + '|' +  	COALESCE(cust.HQ_DEFINE4,'') + '|' +  	COALESCE(cust.HQ_DEFINE5,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_STATUS),'0') + '|' +  	COALESCE(cust.CUSTCLASS_STATUS,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUSTCLASS_UPLFLAG),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUSTCLASS_DWLFLAG),'0') + '|' +  	(CASE WHEN cust.CUSTCLASS_UPDDT IS NULL THEN '' ELSE format(cust.CUSTCLASS_UPDDT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.BLOCK_IND),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.INV_COUNT),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_BARCODE),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.CUST_SEASON),'0') + '|' +  	COALESCE(cust.SEASON_START_DT,'') + '|' +  	COALESCE(cust.SEASON_END_DT,'') + '|' +  	(CASE WHEN cust.BIRTH_DT IS NULL THEN '' ELSE format(cust.BIRTH_DT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	(CASE WHEN cust.ANNIVERSARY_DT IS NULL THEN '' ELSE format(cust.ANNIVERSARY_DT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	COALESCE(cust.DISC_TYPE,'') + '|' +  	COALESCE(cust.SUB_TYPE,'') + '|' +  	COALESCE(cust.APPLY_ON,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.SPOKE_TYPE),'') + '|' +  	COALESCE(cust.VPO_QTY,'') + '|' +  	COALESCE(cust.VPO_VAL,'') + '|' +  	COALESCE(cust.SUPPLY_TIME,'') + '|' +  	COALESCE(cust.CALL_TIME,'') + '|' +  	COALESCE(cust.NEW_CUST_CD,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.SALES_PERSON),'') + '|' +  	COALESCE(cust.NEW_IND,'') + '|' +  	COALESCE(cust.UPLDW_STATUS,'') + '|' +  	(CASE WHEN cust.SVR_UPLDW_DT IS NULL THEN '' ELSE format(cust.SVR_UPLDW_DT, 'yyyy-MM-dd HH:mm:ss.ffff') END) + '|' +  	COALESCE(cust.OPEN_TIME,'') + '|' +  	COALESCE(cust.CLOSE_TIME,'') + '|' +  	COALESCE(cust.CONT_OWNER,'') + '|' +  	COALESCE(cust.LANDMARK,'') + '|' +  	COALESCE(cust.AREA_CLASS_CD,'') + '|' +  	COALESCE(cust.CHAIN_CD,'') + '|' +  	COALESCE(cust.KEY_ACC,'') + '|' +  	COALESCE(cust.OUTLET_TYPE_CD,'') + '|' +  	COALESCE(cust.REF_DISTCD,'') + '|' +  	COALESCE(cust.REF_CUSTCD,'') + '|' +  	COALESCE(cust.REL_PEMS,'') + '|' +  	COALESCE(cust.DIST_PARENT_CD,'') + '|' +  	COALESCE(cust.MEM_DW_IND,'') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.LONGITUDE),'0') + '|' +  	COALESCE(CONVERT(nvarchar(255), cust.LATITUDE),'0') + '|' +  	COALESCE(cust.HH_CUSTCOLOR,'') + '|' +  	COALESCE(cust.XREF_ID,'') + '|' +  	COALESCE(cust.EXPRESS_CD,'') ),2) as row_hash, 
			COALESCE(cust.DIST_CD,'') AS DIST_CD,
			COALESCE(cust.CUST_CD,'') AS CUST_CD,
			COALESCE(cust.INT_CUST_CD,'') AS INT_CUST_CD,
			COALESCE(cust.BE_CUST_CD,'') AS BE_CUST_CD,
			COALESCE(cust.CUST_NAME,'') AS CUST_NAME,
			COALESCE(cust.CUST_NAME2,'') AS CUST_NAME2,
			COALESCE(cust.CUST_OPENDT,'') AS CUST_OPENDT,
			COALESCE(cust.CUST_REGNO,'') AS CUST_REGNO,
			COALESCE(cust.CUST_TYPE,'') AS CUST_TYPE,
			COALESCE(cust.PAYMENT_MODE,'') AS PAYMENT_MODE,
			COALESCE(cust.PRICEGRP_CD,'') AS PRICEGRP_CD,
			COALESCE(cust.AREA_CD,'') AS AREA_CD,
			COALESCE(cust.CUST_HIER3,'') AS CUST_HIER3,
			COALESCE(cust.BILLTO_CD,'') AS BILLTO_CD,
			COALESCE(cust.KEYACCCLS_CD,'') AS KEYACCCLS_CD,
			COALESCE(cust.KEYACCCAT_CD,'') AS KEYACCCAT_CD,
			COALESCE(cust.KEYACCSUBCAT_CD,'') AS KEYACCSUBCAT_CD,
			COALESCE(cust.KEYACCGRP1_CD,'') AS KEYACCGRP1_CD,
			COALESCE(cust.KEYACCGRP2_CD,'') AS KEYACCGRP2_CD,
			COALESCE(cust.KEYACCGRP3_CD,'') AS KEYACCGRP3_CD,
			COALESCE(cust.ADDR_1,'') AS ADDR_1,
			COALESCE(cust.ADDR_2,'') AS ADDR_2,
			COALESCE(cust.ADDR_3,'') AS ADDR_3,
			COALESCE(cust.ADDR_4,'') AS ADDR_4,
			COALESCE(cust.ADDR_5,'') AS ADDR_5,
			COALESCE(cust.ADDR_POSTAL,'') AS ADDR_POSTAL,
			COALESCE(cust.CONT_PR,'') AS CONT_PR,
			COALESCE(cust.CONT_NO,'') AS CONT_NO,
			COALESCE(cust.CONT_NO_EXT,'') AS CONT_NO_EXT,
			COALESCE(cust.ADD_CONT_NO,'') AS ADD_CONT_NO,
			COALESCE(cust.MOBILE_NO,'') AS MOBILE_NO,
			COALESCE(cust.CONT_FAXNO,'') AS CONT_FAXNO,
			COALESCE(cust.CONT_EMAIL,'') AS CONT_EMAIL,
			COALESCE(cust.INVTERM_CD,'') AS INVTERM_CD,
			COALESCE(cust.CUST_CRDLMT,0) AS CUST_CRDLMT,
			COALESCE(cust.OUTSTANDING_BAL,0) AS OUTSTANDING_BAL,
			COALESCE(cust.CUST_DISC,0) AS CUST_DISC,
			COALESCE(cust.CUST_TAX,0) AS CUST_TAX,
			COALESCE(cust.CUST_TAXNO,'') AS CUST_TAXNO,
			COALESCE(cust.CUST_TAXREGNO,'') AS CUST_TAXREGNO,
			COALESCE(cust.CUST_REG,0) AS CUST_REG,
			COALESCE(cust.CUST_TINNO,'') AS CUST_TINNO,
			COALESCE(cust.CUST_LSTNO,'') AS CUST_LSTNO,
			COALESCE(cust.CUST_CSTNO,'') AS CUST_CSTNO,
			COALESCE(cust.CUST_GSTNO,'') AS CUST_GSTNO,
			COALESCE(cust.REMARKS,'') AS REMARKS,
			COALESCE(cust.BANK_CD,'') AS BANK_CD,
			COALESCE(cust.BANK_BRANCH,'') AS BANK_BRANCH,
			COALESCE(cust.BANK_ACCNO,'') AS BANK_ACCNO,
			COALESCE(cust.OPENING_BAL,0) AS OPENING_BAL,
			COALESCE(cust.OPENING_BAL_DT,'') AS OPENING_BAL_DT,
			COALESCE(cust.USER_DEFINE1,'') AS USER_DEFINE1,
			COALESCE(cust.USER_DEFINE2,'') AS USER_DEFINE2,
			COALESCE(cust.USER_DEFINE3,'') AS USER_DEFINE3,
			COALESCE(cust.USER_DEFINE4,'') AS USER_DEFINE4,
			COALESCE(cust.USER_DEFINE5,'') AS USER_DEFINE5,
			COALESCE(cust.HQ_DEFINE1,'') AS HQ_DEFINE1,
			COALESCE(cust.HQ_DEFINE2,'') AS HQ_DEFINE2,
			COALESCE(cust.HQ_DEFINE3,'') AS HQ_DEFINE3,
			COALESCE(cust.HQ_DEFINE4,'') AS HQ_DEFINE4,
			COALESCE(cust.HQ_DEFINE5,'') AS HQ_DEFINE5,
			COALESCE(cust.CUST_STATUS,0) AS CUST_STATUS,
			COALESCE(cust.CUSTCLASS_STATUS,'') AS CUSTCLASS_STATUS,
			COALESCE(cust.CUSTCLASS_UPLFLAG,0) AS CUSTCLASS_UPLFLAG,
			COALESCE(cust.CUSTCLASS_DWLFLAG,0) AS CUSTCLASS_DWLFLAG,
			COALESCE(cust.CUSTCLASS_UPDDT,'') AS CUSTCLASS_UPDDT,
			COALESCE(cust.BLOCK_IND,0) AS BLOCK_IND,
			COALESCE(cust.INV_COUNT,0) AS INV_COUNT,
			COALESCE(cust.CUST_BARCODE,'') AS CUST_BARCODE,
			COALESCE(cust.CUST_SEASON,0) AS CUST_SEASON,
			COALESCE(cust.SEASON_START_DT,'') AS SEASON_START_DT,
			COALESCE(cust.SEASON_END_DT,'') AS SEASON_END_DT,
			COALESCE(cust.BIRTH_DT,'') AS BIRTH_DT,
			COALESCE(cust.ANNIVERSARY_DT,'') AS ANNIVERSARY_DT,
			COALESCE(cust.DISC_TYPE,'') AS DISC_TYPE,
			COALESCE(cust.SUB_TYPE,'') AS SUB_TYPE,
			COALESCE(cust.APPLY_ON,'') AS APPLY_ON,
			COALESCE(cust.SPOKE_TYPE,0) AS SPOKE_TYPE,
			COALESCE(cust.VPO_QTY,'') AS VPO_QTY,
			COALESCE(cust.VPO_VAL,'') AS VPO_VAL,
			COALESCE(cust.SUPPLY_TIME,'') AS SUPPLY_TIME,
			COALESCE(cust.CALL_TIME,'') AS CALL_TIME,
			COALESCE(cust.NEW_CUST_CD,'') AS NEW_CUST_CD,
			COALESCE(cust.SALES_PERSON,0) AS SALES_PERSON,
			COALESCE(cust.NEW_IND,'') AS NEW_IND,
			COALESCE(cust.UPLDW_STATUS,'') AS UPLDW_STATUS,
			COALESCE(cust.SVR_UPLDW_DT,'') AS SVR_UPLDW_DT,
			COALESCE(cust.OPEN_TIME,'') AS OPEN_TIME,
			COALESCE(cust.CLOSE_TIME,'') AS CLOSE_TIME,
			COALESCE(cust.CONT_OWNER,'') AS CONT_OWNER,
			COALESCE(cust.LANDMARK,'') AS LANDMARK,
			COALESCE(cust.AREA_CLASS_CD,'') AS AREA_CLASS_CD,
			COALESCE(cust.CHAIN_CD,'') AS CHAIN_CD,
			COALESCE(cust.KEY_ACC,'') AS KEY_ACC,
			COALESCE(cust.OUTLET_TYPE_CD,'') AS OUTLET_TYPE_CD,
			COALESCE(cust.REF_DISTCD,'') AS REF_DISTCD,
			COALESCE(cust.REF_CUSTCD,'') AS REF_CUSTCD,
			COALESCE(cust.REL_PEMS,'') AS REL_PEMS,
			COALESCE(cust.DIST_PARENT_CD,'') AS DIST_PARENT_CD,
			COALESCE(cust.MEM_DW_IND,'') AS MEM_DW_IND,
			COALESCE(cust.LONGITUDE,0) AS LONGITUDE,
			COALESCE(cust.LATITUDE,0) AS LATITUDE,
			COALESCE(cust.HH_CUSTCOLOR,'') AS HH_CUSTCOLOR,
			COALESCE(cust.XREF_ID,'') AS XREF_ID,
			COALESCE(cust.EXPRESS_CD,'') AS EXPRESS_CD,
			COALESCE(cust.SYNCVERSION,'') AS SYNCVERSION
	INTO #tmp_stt
	FROM stt.rpm_mst_cust cust
		JOIN #tmp_lastest_stt tmp_stt
			ON tmp_stt.CUST_CD = cust.CUST_CD
		   AND tmp_stt.DIST_CD = cust.DIST_CD
		   AND COALESCE(cust.deleted_audit_ts, cust.audit_ts, '-1') = tmp_stt.audit_ts

	BEGIN TRANSACTION;

	--insert scd2 dim record
	INSERT INTO dmt.dim_customer
	SELECT tmp_stt.audit_ts,
		null as updated_audit_ts,
		null as deleted_audit_ts,
		tmp_stt.scd_start,
		tmp_stt.scd_from,
		tmp_stt.scd_to,   
		tmp_stt.scd_active,
		tmp_stt.scd_version,
		tmp_stt.inferred_flag,
        tmp_stt.source_id,
		tmp_stt.row_hash, 
        tmp_stt.DIST_CD,
        tmp_stt.CUST_CD,
        tmp_stt.INT_CUST_CD,
        tmp_stt.BE_CUST_CD,
        tmp_stt.CUST_NAME,
        tmp_stt.CUST_NAME2,
        tmp_stt.CUST_OPENDT,
        tmp_stt.CUST_REGNO,
        tmp_stt.CUST_TYPE,
        tmp_stt.PAYMENT_MODE,
        tmp_stt.PRICEGRP_CD,
        tmp_stt.AREA_CD,
        tmp_stt.CUST_HIER3,
        tmp_stt.BILLTO_CD,
        tmp_stt.KEYACCCLS_CD,
        tmp_stt.KEYACCCAT_CD,
        tmp_stt.KEYACCSUBCAT_CD,
        tmp_stt.KEYACCGRP1_CD,
        tmp_stt.KEYACCGRP2_CD,
        tmp_stt.KEYACCGRP3_CD,
        tmp_stt.ADDR_1,
        tmp_stt.ADDR_2,
        tmp_stt.ADDR_3,
        tmp_stt.ADDR_4,
        tmp_stt.ADDR_5,
        tmp_stt.ADDR_POSTAL,
        tmp_stt.CONT_PR,
        tmp_stt.CONT_NO,
        tmp_stt.CONT_NO_EXT,
        tmp_stt.ADD_CONT_NO,
        tmp_stt.MOBILE_NO,
        tmp_stt.CONT_FAXNO,
        tmp_stt.CONT_EMAIL,
        tmp_stt.INVTERM_CD,
        tmp_stt.CUST_CRDLMT,
        tmp_stt.OUTSTANDING_BAL,
        tmp_stt.CUST_DISC,
        tmp_stt.CUST_TAX,
        tmp_stt.CUST_TAXNO,
        tmp_stt.CUST_TAXREGNO,
        tmp_stt.CUST_REG,
        tmp_stt.CUST_TINNO,
        tmp_stt.CUST_LSTNO,
        tmp_stt.CUST_CSTNO,
        tmp_stt.CUST_GSTNO,
        tmp_stt.REMARKS,
        tmp_stt.BANK_CD,
        tmp_stt.BANK_BRANCH,
        tmp_stt.BANK_ACCNO,
        tmp_stt.OPENING_BAL,
        tmp_stt.OPENING_BAL_DT,
        tmp_stt.USER_DEFINE1,
        tmp_stt.USER_DEFINE2,
        tmp_stt.USER_DEFINE3,
        tmp_stt.USER_DEFINE4,
        tmp_stt.USER_DEFINE5,
        tmp_stt.HQ_DEFINE1,
        tmp_stt.HQ_DEFINE2,
        tmp_stt.HQ_DEFINE3,
        tmp_stt.HQ_DEFINE4,
        tmp_stt.HQ_DEFINE5,
        tmp_stt.CUST_STATUS,
        tmp_stt.CUSTCLASS_STATUS,
        tmp_stt.CUSTCLASS_UPLFLAG,
        tmp_stt.CUSTCLASS_DWLFLAG,
        tmp_stt.CUSTCLASS_UPDDT,
        tmp_stt.BLOCK_IND,
        tmp_stt.INV_COUNT,
        tmp_stt.CUST_BARCODE,
        tmp_stt.CUST_SEASON,
        tmp_stt.SEASON_START_DT,
        tmp_stt.SEASON_END_DT,
        tmp_stt.BIRTH_DT,
        tmp_stt.ANNIVERSARY_DT,
        tmp_stt.DISC_TYPE,
        tmp_stt.SUB_TYPE,
        tmp_stt.APPLY_ON,
        tmp_stt.SPOKE_TYPE,
        tmp_stt.VPO_QTY,
        tmp_stt.VPO_VAL,
        tmp_stt.SUPPLY_TIME,
        tmp_stt.CALL_TIME,
        tmp_stt.NEW_CUST_CD,
        tmp_stt.SALES_PERSON,
        tmp_stt.NEW_IND,
        tmp_stt.UPLDW_STATUS,
        tmp_stt.SVR_UPLDW_DT,
        tmp_stt.OPEN_TIME,
        tmp_stt.CLOSE_TIME,
        tmp_stt.CONT_OWNER,
        tmp_stt.LANDMARK,
        tmp_stt.AREA_CLASS_CD,
        tmp_stt.CHAIN_CD,
        tmp_stt.KEY_ACC,
        tmp_stt.OUTLET_TYPE_CD,
        tmp_stt.REF_DISTCD,
        tmp_stt.REF_CUSTCD,
        tmp_stt.REL_PEMS,
        tmp_stt.DIST_PARENT_CD,
        tmp_stt.MEM_DW_IND,
        tmp_stt.LONGITUDE,
        tmp_stt.LATITUDE,
        tmp_stt.HH_CUSTCOLOR,
        tmp_stt.XREF_ID,
        tmp_stt.EXPRESS_CD,
		tmp_stt.SYNCVERSION
	FROM #tmp_stt tmp_stt
		INNER JOIN dmt.dim_customer dmt
				ON tmp_stt.DIST_CD = dmt.DIST_CD
			   AND tmp_stt.CUST_CD = dmt.CUST_CD
			   AND dmt.scd_active = 1
	 WHERE tmp_stt.row_hash <> dmt.row_hash;

	/*TEMP TABLE FOR UPDATE DIM SCD2*/
	SELECT T.*
	INTO #TMP_UPDATE_SCD2
	FROM (SELECT 
			MIN(SCD_START) OVER (PARTITION BY SUB.CUST_CD,SUB.DIST_CD 
													 ORDER BY SUB.CUST_CD,SUB.DIST_CD)
			AS SCD_START
		   ,CASE WHEN LAG(SUB.SYNCVERSION) OVER (PARTITION BY SUB.CUST_CD,SUB.DIST_CD 
													 ORDER BY SUB.SYNCVERSION) IS NULL
				THEN '1900-01-01' ELSE SUB.SYNCVERSION END 
				AS SCD_FROM		
		   ,CASE WHEN LEAD(SUB.SYNCVERSION) OVER (PARTITION BY SUB.CUST_CD,SUB.DIST_CD 
													  ORDER BY SUB.SYNCVERSION) IS NULL 
				THEN '2999-12-31' 
				ELSE LEAD(SUB.SYNCVERSION) OVER (PARTITION BY SUB.CUST_CD,SUB.DIST_CD 
													 ORDER BY SUB.SYNCVERSION) END 
				AS SCD_TO			
		   ,ROW_NUMBER() OVER (PARTITION BY CUST_CD,SUB.DIST_CD 
								   ORDER BY SUB.SYNCVERSION) 
				AS SCD_VERSION		
		   ,CASE WHEN LEAD(SUB.SYNCVERSION) OVER (PARTITION BY SUB.CUST_CD,SUB.DIST_CD 
													  ORDER BY SUB.SYNCVERSION) IS NULL 
				THEN 1 
				ELSE 0 END 
				AS SCD_ACTIVE	
		   ,SUB.customer_skey
		   ,SUB.CUST_CD
		   ,SUB.DIST_CD
		   FROM DMT.DIM_CUSTOMER SUB) T
	WHERE EXISTS (SELECT 1 FROM 
					#tmp_stt S
					WHERE S.CUST_CD = T.CUST_CD
					  AND S.DIST_CD = T.DIST_CD);
	
	SET ROWCOUNT 0;

	UPDATE T
	SET SCD_FROM = S.SCD_FROM,
		SCD_TO = S.SCD_TO,
		SCD_VERSION = S.SCD_VERSION,
		SCD_ACTIVE = S.SCD_ACTIVE,
		SCD_START = S.SCD_START
	FROM 
	  DMT.DIM_CUSTOMER T
	  JOIN #TMP_UPDATE_SCD2 S
		ON s.customer_skey = t.customer_skey;


	SET @updated_rows = @@ROWCOUNT;

	
	SET ROWCOUNT 0;
	--INSERT data if not exists based on business key
	INSERT INTO dmt.dim_customer 
	(audit_ts
	,updated_audit_ts
	,deleted_audit_ts
	,scd_start
	,scd_from
	,scd_to
	,scd_active
	,scd_version
	,inferred_flag
	,source_id
	,row_hash  
      , DIST_CD 
      , CUST_CD 
      , INT_CUST_CD 
      , BE_CUST_CD 
      , CUST_NAME 
      , CUST_NAME2 
      , CUST_OPENDT 
      , CUST_REGNO 
      , CUST_TYPE 
      , PAYMENT_MODE 
      , PRICEGRP_CD 
      , AREA_CD 
      , CUST_HIER3 
      , BILLTO_CD 
      , KEYACCCLS_CD 
      , KEYACCCAT_CD 
      , KEYACCSUBCAT_CD 
      , KEYACCGRP1_CD 
      , KEYACCGRP2_CD 
      , KEYACCGRP3_CD 
      , ADDR_1 
      , ADDR_2 
      , ADDR_3 
      , ADDR_4 
      , ADDR_5 
      , ADDR_POSTAL 
      , CONT_PR 
      , CONT_NO 
      , CONT_NO_EXT 
      , ADD_CONT_NO 
      , MOBILE_NO 
      , CONT_FAXNO 
      , CONT_EMAIL 
      , INVTERM_CD 
      , CUST_CRDLMT 
      , OUTSTANDING_BAL 
      , CUST_DISC 
      , CUST_TAX 
      , CUST_TAXNO 
      , CUST_TAXREGNO 
      , CUST_REG 
      , CUST_TINNO 
      , CUST_LSTNO 
      , CUST_CSTNO 
      , CUST_GSTNO 
      , REMARKS 
      , BANK_CD 
      , BANK_BRANCH 
      , BANK_ACCNO 
      , OPENING_BAL 
      , OPENING_BAL_DT 
      , USER_DEFINE1 
      , USER_DEFINE2 
      , USER_DEFINE3 
      , USER_DEFINE4 
      , USER_DEFINE5 
      , HQ_DEFINE1 
      , HQ_DEFINE2 
      , HQ_DEFINE3 
      , HQ_DEFINE4 
      , HQ_DEFINE5 
      , CUST_STATUS 
      , CUSTCLASS_STATUS 
      , CUSTCLASS_UPLFLAG 
      , CUSTCLASS_DWLFLAG 
      , CUSTCLASS_UPDDT 
      , BLOCK_IND 
      , INV_COUNT 
      , CUST_BARCODE 
      , CUST_SEASON 
      , SEASON_START_DT 
      , SEASON_END_DT 
      , BIRTH_DT 
      , ANNIVERSARY_DT 
      , DISC_TYPE 
      , SUB_TYPE 
      , APPLY_ON 
      , SPOKE_TYPE 
      , VPO_QTY 
      , VPO_VAL 
      , SUPPLY_TIME 
      , CALL_TIME 
      , NEW_CUST_CD 
      , SALES_PERSON 
      , NEW_IND 
      , UPLDW_STATUS 
      , SVR_UPLDW_DT 
      , OPEN_TIME 
      , CLOSE_TIME 
      , CONT_OWNER 
      , LANDMARK 
      , AREA_CLASS_CD 
      , CHAIN_CD 
      , KEY_ACC 
      , OUTLET_TYPE_CD 
      , REF_DISTCD 
      , REF_CUSTCD 
      , REL_PEMS 
      , DIST_PARENT_CD 
      , MEM_DW_IND 
      , LONGITUDE 
      , LATITUDE 
      , HH_CUSTCOLOR 
      , XREF_ID 
      , EXPRESS_CD
	  , SYNCVERSION) 
	SELECT tmp_stt.audit_ts,
		null as updated_audit_ts,
		null as deleted_audit_ts,
		tmp_stt.scd_start,
		tmp_stt.scd_from,
		tmp_stt.scd_to,   
		tmp_stt.scd_active,
		tmp_stt.scd_version,
		tmp_stt.inferred_flag,
        tmp_stt.source_id,
		tmp_stt.row_hash, 
        tmp_stt.DIST_CD,
        tmp_stt.CUST_CD,
        tmp_stt.INT_CUST_CD,
        tmp_stt.BE_CUST_CD,
        tmp_stt.CUST_NAME,
        tmp_stt.CUST_NAME2,
        tmp_stt.CUST_OPENDT,
        tmp_stt.CUST_REGNO,
        tmp_stt.CUST_TYPE,
        tmp_stt.PAYMENT_MODE,
        tmp_stt.PRICEGRP_CD,
        tmp_stt.AREA_CD,
        tmp_stt.CUST_HIER3,
        tmp_stt.BILLTO_CD,
        tmp_stt.KEYACCCLS_CD,
        tmp_stt.KEYACCCAT_CD,
        tmp_stt.KEYACCSUBCAT_CD,
        tmp_stt.KEYACCGRP1_CD,
        tmp_stt.KEYACCGRP2_CD,
        tmp_stt.KEYACCGRP3_CD,
        tmp_stt.ADDR_1,
        tmp_stt.ADDR_2,
        tmp_stt.ADDR_3,
        tmp_stt.ADDR_4,
        tmp_stt.ADDR_5,
        tmp_stt.ADDR_POSTAL,
        tmp_stt.CONT_PR,
        tmp_stt.CONT_NO,
        tmp_stt.CONT_NO_EXT,
        tmp_stt.ADD_CONT_NO,
        tmp_stt.MOBILE_NO,
        tmp_stt.CONT_FAXNO,
        tmp_stt.CONT_EMAIL,
        tmp_stt.INVTERM_CD,
        tmp_stt.CUST_CRDLMT,
        tmp_stt.OUTSTANDING_BAL,
        tmp_stt.CUST_DISC,
        tmp_stt.CUST_TAX,
        tmp_stt.CUST_TAXNO,
        tmp_stt.CUST_TAXREGNO,
        tmp_stt.CUST_REG,
        tmp_stt.CUST_TINNO,
        tmp_stt.CUST_LSTNO,
        tmp_stt.CUST_CSTNO,
        tmp_stt.CUST_GSTNO,
        tmp_stt.REMARKS,
        tmp_stt.BANK_CD,
        tmp_stt.BANK_BRANCH,
        tmp_stt.BANK_ACCNO,
        tmp_stt.OPENING_BAL,
        tmp_stt.OPENING_BAL_DT,
        tmp_stt.USER_DEFINE1,
        tmp_stt.USER_DEFINE2,
        tmp_stt.USER_DEFINE3,
        tmp_stt.USER_DEFINE4,
        tmp_stt.USER_DEFINE5,
        tmp_stt.HQ_DEFINE1,
        tmp_stt.HQ_DEFINE2,
        tmp_stt.HQ_DEFINE3,
        tmp_stt.HQ_DEFINE4,
        tmp_stt.HQ_DEFINE5,
        tmp_stt.CUST_STATUS,
        tmp_stt.CUSTCLASS_STATUS,
        tmp_stt.CUSTCLASS_UPLFLAG,
        tmp_stt.CUSTCLASS_DWLFLAG,
        tmp_stt.CUSTCLASS_UPDDT,
        tmp_stt.BLOCK_IND,
        tmp_stt.INV_COUNT,
        tmp_stt.CUST_BARCODE,
        tmp_stt.CUST_SEASON,
        tmp_stt.SEASON_START_DT,
        tmp_stt.SEASON_END_DT,
        tmp_stt.BIRTH_DT,
        tmp_stt.ANNIVERSARY_DT,
        tmp_stt.DISC_TYPE,
        tmp_stt.SUB_TYPE,
        tmp_stt.APPLY_ON,
        tmp_stt.SPOKE_TYPE,
        tmp_stt.VPO_QTY,
        tmp_stt.VPO_VAL,
        tmp_stt.SUPPLY_TIME,
        tmp_stt.CALL_TIME,
        tmp_stt.NEW_CUST_CD,
        tmp_stt.SALES_PERSON,
        tmp_stt.NEW_IND,
        tmp_stt.UPLDW_STATUS,
        tmp_stt.SVR_UPLDW_DT,
        tmp_stt.OPEN_TIME,
        tmp_stt.CLOSE_TIME,
        tmp_stt.CONT_OWNER,
        tmp_stt.LANDMARK,
        tmp_stt.AREA_CLASS_CD,
        tmp_stt.CHAIN_CD,
        tmp_stt.KEY_ACC,
        tmp_stt.OUTLET_TYPE_CD,
        tmp_stt.REF_DISTCD,
        tmp_stt.REF_CUSTCD,
        tmp_stt.REL_PEMS,
        tmp_stt.DIST_PARENT_CD,
        tmp_stt.MEM_DW_IND,
        tmp_stt.LONGITUDE,
        tmp_stt.LATITUDE,
        tmp_stt.HH_CUSTCOLOR,
        tmp_stt.XREF_ID,
        tmp_stt.EXPRESS_CD,
		tmp_stt.SYNCVERSION
	  FROM #tmp_stt tmp_stt
	  WHERE NOT EXISTS (SELECT 1 
						FROM dmt.dim_customer dmt
					   WHERE dmt.DIST_CD = tmp_stt.DIST_CD
						 and dmt.CUST_CD = tmp_stt.CUST_CD)


	
	SET @new_rows = @@ROWCOUNT;
	/*STATRT-2019-05-21: removed by Wiwis*/
	/*DECLARE @is_table_empty int;
	SELECT @is_table_empty = COUNT(1) 
	  FROM #tmp_stt;

	IF(@is_table_empty > 0)
	BEGIN

	 UPDATE dmt SET        
         deleted_audit_ts = @parent_audit_ts  
	 FROM #tmp_stt tmp_stt
		RIGHT JOIN dmt.dim_customer dmt
			ON tmp_stt.DIST_CD = dmt.DIST_CD 
			AND tmp_stt.CUST_CD = dmt.CUST_CD  
	 WHERE tmp_stt.DIST_CD IS NULL 
	 AND tmp_stt.CUST_CD IS NULL;

	 SET @deleted_rows = @@ROWCOUNT;

	END*/
	/*STATRT-2019-05-21: removed by Wiwis*/


	COMMIT;

	EXEC etl.end_execution_audit @parent_audit_ts = @parent_audit_ts 
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

	EXEC etl.fail_execution_audit @parent_audit_ts = @parent_audit_ts 
								, @root_ts = @root_ts 
								, @step_type = @step_type  
								, @stepname = @stepname
								, @errmsg = @errmsg
								, @target_table = @target_table;

	THROW;
	
END CATCH

GO
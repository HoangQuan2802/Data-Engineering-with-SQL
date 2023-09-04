











 CREATE procedure [etl].[sp_dmt_fact_budget_snaphot](
	@parent_audit_ts VARCHAR(50) = '-1', --optional parameter 
	@root_ts VARCHAR(50) = '-1'--optional parameter
	)
AS

BEGIN TRY

	DECLARE @step_type NVARCHAR(255);
	DECLARE @stepname NVARCHAR(255); 
	DECLARE @watermarktable NVARCHAR(255);
	DECLARE @target_table NVARCHAR(255);
	DECLARE @new_rows INT;
	DECLARE @deleted_rows INT;
	DECLARE @updated_rows INT;
	DECLARE @from_date DATETIME;
	DECLARE @audit_ts DATETIME = GETDATE();
	
	SET @step_type = N'stored procedure';
	SET @stepname = N'sp_dmt_fact_budget_snapshot';
	SET @target_table = N'dmt.fact_budget_snapshot';


	EXEC etl.sp_start_execution_audit @parent_audit_ts = @parent_audit_ts 
								 , @root_ts = @root_ts 
								 , @step_type = @step_type  
								 , @stepname = @stepname
								 , @target_table = @target_table;

	
	--get max audit_ts from dmt.fact_budget_snapshot
	--DECLARE @from_date VARCHAR(100);
	--SELECT @from_date = COALESCE(MAX(COALESCE(deleted_audit_ts, audit_ts)) , '1900-01-01')
	--FROM dmt.fact_budget;

	--load lastest records from header stt from a particulate date @from_date
	SELECT budget.*
	  INTO #tmp_stt
	FROM (
	SELECT
		[audit_ts]
      ,[updated_audit_ts]
      ,[deleted_audit_ts]
      ,[source_id]
      ,[row_hash]
      ,CURRENT_TIMESTAMP AS [snapshot_date]
      ,FORMAT(CURRENT_TIMESTAMP,'yyyyMMdd')  AS [snapshot_date_key]
      ,[budget_skey]
      ,[DeptRef]
      ,[DeptCode]
      ,[department_skey]
      ,[AcctCode]
      ,[Acct_skey]
      ,[Brand]
      ,[brand_skey]
      ,[SaleCode]
      ,[salesperson_skey]
	  ,[sales_area_skey]
      ,[DepartmentType]
      ,[BudgetType]
      ,[budget_date]
      ,[budget_date_key]
      ,[budget_amount]
  FROM [analytics-dw].[dmt].[fact_budget]
  ) budget
  ;

	BEGIN TRANSACTION;
	
	SET ROWCOUNT 0;
	--INSERT data if not exists based on business key
	INSERT INTO dmt.fact_budget_snapshot
	(audit_ts,deleted_audit_ts,source_id,row_hash
		,snapshot_date
		,snapshot_date_key
		,budget_skey
		,DeptRef
		,DeptCode
		,department_skey
		,AcctCode
		,Acct_skey
		,Brand
		,brand_skey
		,SaleCode
		,salesperson_skey
		,sales_area_skey
		,DepartmentType
		,BudgetType
		,budget_date
		,budget_date_key
		,budget_amount
	)	
	  SELECT tmp_stt.audit_ts
			, null as deleted_audit_ts 
			, tmp_stt.source_id 
			, tmp_stt.row_hash
			, tmp_stt.snapshot_date
			, tmp_stt.snapshot_date_key
			, tmp_stt.budget_skey
			, tmp_stt.DeptRef
			, tmp_stt.DeptCode
			, tmp_stt.department_skey
			, tmp_stt.AcctCode
			, tmp_stt.Acct_skey
			, tmp_stt.Brand
			, tmp_stt.brand_skey
			, tmp_stt.SaleCode
			, tmp_stt.salesperson_skey
			, tmp_stt.sales_area_skey
			, tmp_stt.DepartmentType 
			, tmp_stt.BudgetType
			, tmp_stt.budget_date
			, tmp_stt.budget_date_key
			, tmp_stt.budget_amount
	  FROM #tmp_stt tmp_stt
	
	SET @new_rows = @@ROWCOUNT;

	COMMIT;

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
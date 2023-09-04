












 CREATE procedure [etl].[sp_dmt_fact_budget](
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
	SET @stepname = N'sp_dmt_fact_budget';
	SET @target_table = N'dmt.fact_budget';


	EXEC etl.sp_start_execution_audit @parent_audit_ts = @parent_audit_ts 
								 , @root_ts = @root_ts 
								 , @step_type = @step_type  
								 , @stepname = @stepname
								 , @target_table = @target_table;

	
	--get max audit_ts from dmt.fact_budget
	--DECLARE @from_date VARCHAR(100);
	SELECT @from_date = COALESCE(MAX(COALESCE(deleted_audit_ts, audit_ts)) , '1900-01-01')
	FROM dmt.fact_budget;

	--load lastest records from header stt from a particulate date @from_date
	SELECT *
	  INTO #tmp_lastest_header
	FROM (
		SELECT
		b.[audit_ts]
		,b.[deleted_audit_ts]
		,b.[source_id]
		,b.[source_file]
		,b.[row_hash]
		,NULL AS snapshot_date
		,NULL AS snapshot_date_key
		,COALESCE(b.DeptRef,'') AS DeptRef
		,COALESCE(b.DeptCode,'') AS DeptCode
		,COALESCE(b.DeptName,'') AS DeptName
		,COALESCE(b.AccCode,'') AS AcctCode
		,COALESCE(b.AccName,'') AS AccName
		,COALESCE(b.Brand,'') AS Brand
		,COALESCE(b.SaleCode,'') AS SaleCode
		,COALESCE(b.DepartmentType,'') AS DepartmentType
		,COALESCE(b.BudgetType,'') AS BudgetType
		,COALESCE(b.budget_date,FORMAT(COALESCE(b.budget_date,convert(date, '2999-12-31')), 'yyyy-MM-dd HH:mm:ss.ffff')) AS budget_date
		,COALESCE(b.budget_date_key,0) AS budget_date_key
		,COALESCE(b.budget_amount,0) AS budget_amount
		,ROW_NUMBER() OVER (PARTITION BY b.DeptRef,
											b.DeptCode,
											b.AccCode,
											b.Brand,
											b.SaleCode,
											b.DepartmentType,
											b.BudgetType,
											b.budget_date
							ORDER BY b.audit_ts DESC) AS RN
		FROM
		(
		SELECT 
			[audit_ts]
			  ,[deleted_audit_ts]
			  ,[source_id]
			  ,[source_file]
			  ,[row_hash]
			  ,[DeptRef]
			  ,[DeptCode]
			  ,[DeptName]
			  ,[AccCode]
			  ,[AccName]
			  ,[Brand]
			  ,[SaleCode]
			  ,[DepartmentType]
			  ,[BudgetType]
			  ,CONCAT([Year],'-',FORMAT(CAST(([MonthName] + '01, 1900') AS date),'MM'),'-','01') AS budget_date
			  ,CONCAT([Year],FORMAT(CAST(([MonthName] + '01, 1900') AS date),'MM'),'01') AS budget_date_key
			  ,[budget_amount]
		FROM 
		(
		SELECT *
		 FROM [analytics-dw].[stt].[man_budget]
		 ) p

		 UNPIVOT

		 (budget_amount FOR MonthName IN
		(    
			 [Jan]
			,[Feb]
			,[Mar]
			,[Apr]
			,[May]
			,[Jun]
			,[Jul]
			,[Aug]
			,[Sep]
			,[Oct]
			,[Nov]
			,[Dec]
		) 
		) AS unpvt
		) AS b
	WHERE COALESCE(b.deleted_audit_ts, b.audit_ts, '-1') > @from_date
	) lastest_stt
	WHERE RN = 1

	/*
		Have to add coalesce around each business column, since there should be no NULL value in dimension
	*/
		SELECT
		@audit_ts AS audit_ts
		,null deleted_audit_ts
		,tmp_stt.source_id
		,CONVERT(VARCHAR(100), HASHBYTES('SHA2_256', + '^' + COALESCE(TRIM(CAST(tmp_stt.DeptRef as varchar(300))), '') + '^' + format(coalesce(CONVERT(DATETIME,tmp_stt.budget_date, 103) ,convert(date, '2999-12-31')), 'yyyy-MM-dd HH:mm:ss.ffff') + '^' +  COALESCE(TRIM(CAST(tmp_stt.DeptCode as varchar(300))), '') + '^' + COALESCE(TRIM(CAST(tmp_stt.AcctCode as varchar(300))), '') + '^' + COALESCE(TRIM(CAST(tmp_stt.Brand AS varchar(300))), '') + '^' + COALESCE(TRIM(CAST(tmp_stt.SaleCode as varchar(300))), '') + '^' + COALESCE(TRIM(CAST(tmp_stt.DepartmentType as varchar(300))), '')+ '^' + COALESCE(TRIM(CAST(tmp_stt.BudgetType as varchar(300))),'')+ '^' + COALESCE(TRIM(CAST(tmp_stt.budget_date_key as varchar(10))), '')+ '^' + COALESCE(TRIM(CAST(tmp_stt.budget_amount as varchar(20))), '')), 2)  as row_hash
		,tmp_stt.snapshot_date AS snapshot_date
		,COALESCE(tmp_stt.snapshot_date_key,-1) AS snapshot_date_key
		,tmp_stt.DeptRef AS DeptRef 
		,tmp_stt.DeptCode AS DeptCode 
		,COALESCE(d.department_skey,-1) AS department_skey
		,tmp_stt.AcctCode AS AcctCode 
		,COALESCE(ba.Acct_skey,-1) AS Acct_skey
		,tmp_stt.Brand AS Brand 
		,COALESCE(b.brand_skey,-1) AS brand_skey
		,tmp_stt.SaleCode AS SaleCode
		,COALESCE(s.salesperson_skey,-1) AS salesperson_skey
		,COALESCE(sa.sales_area_skey,-1) AS sales_area_skey
		,tmp_stt.DepartmentType AS DepartmentType
		,tmp_stt.BudgetType AS BudgetType
		,tmp_stt.budget_date AS budget_date
		,COALESCE(tmp_stt.budget_date_key,-1) AS budget_date_key
		,tmp_stt.budget_amount AS budget_amount
		INTO #tmp_stt
		FROM #tmp_lastest_header tmp_stt
		LEFT JOIN dmt.dim_budget_account ba 
			ON CAST (tmp_stt.AcctCode AS varchar) = CAST(ba.AcctCode AS varchar)
			AND (tmp_stt.budget_date > ba.scd_from AND tmp_stt.budget_date <= ba.scd_to)
		LEFT JOIN dmt.dim_department d 
			ON tmp_stt.DeptCode = d.department_code
			AND (tmp_stt.budget_date > d.scd_from AND tmp_stt.budget_date <= d.scd_to)
		LEFT JOIN dmt.dim_brand b 
			ON tmp_stt.Brand = b.brand_code
			AND (tmp_stt.budget_date > b.scd_from AND tmp_stt.budget_date <= b.scd_to)
		LEFT JOIN dmt.dim_salesperson s 
			ON tmp_stt.SaleCode = s.U_SalesArea
			AND U_SalesArea <> '' and  U_SalesArea <> '-'
			AND (tmp_stt.budget_date > s.scd_from AND tmp_stt.budget_date <= s.scd_to)
		LEFT JOIN dmt.dim_sales_area sa 
			ON tmp_stt.SaleCode = sa.sales_area
			AND sales_area <> '' and  sales_area <> '-'
			AND (tmp_stt.budget_date > sa.scd_from AND tmp_stt.budget_date <= sa.scd_to)

	BEGIN TRANSACTION;

	--Delete existing Fact if matched from STT
	/*
	DELETE dmt.fact_budget
	where exists (SELECT tmp_stt.*
						 FROM #tmp_stt tmp_stt
						 WHERE tmp_stt.DeptRef = fact_budget.DeptRef
						 AND tmp_stt.DeptCode = fact_budget.DeptCode
						 AND tmp_stt.AcctCode = fact_budget.AcctCode
						 AND tmp_stt.Brand = fact_budget.Brand
						 AND tmp_stt.SaleCode = fact_budget.SaleCode
						 AND tmp_stt.DepartmentType = fact_budget.DepartmentType
						 AND tmp_stt.BudgetType = fact_budget.BudgetType
						 AND tmp_stt.budget_date = fact_budget.budget_date);
						 */
	
	SET ROWCOUNT 0;
	
	--INSERT data if not exists based on business key
	INSERT INTO dmt.fact_budget
	(audit_ts,deleted_audit_ts,source_id,row_hash
		,snapshot_date
		,snapshot_date_key
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
			, null snapshot_date
			, null snapshot_date_key
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
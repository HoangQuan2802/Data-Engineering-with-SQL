ALTER PROC [dbo].[dwp_fact_sales] AS
BEGIN	
	SET NOCOUNT on;
	SET XACT_ABORT on;
	DECLARE @currentdate AS datetime = GETDATE();
	DECLARE @audit_ts AS varchar(22);
	DECLARE @parent_audit_ts AS VARCHAR(22);
    DECLARE @root_ts AS VARCHAR(22);
	DECLARE @step_type NVARCHAR(255);
	DECLARE @stepname NVARCHAR(255); 
	DECLARE @watermarktable NVARCHAR(255);
	DECLARE @target_table NVARCHAR(255);
	DECLARE @new_rows INT;
	DECLARE @deleted_rows INT;
	DECLARE @updated_rows INT;
	DECLARE @19000101 AS datetime = '1900-01-01';
	DECLARE @29991231 AS datetime = '2999-12-31';

	SET @stepname = 'dwp_fact_sales'
	SET @step_type = 'stored procedure';
	SET @target_table = 'dbo.dwt_fact_sales';
	SET @audit_ts = FORMAT(SYSDATETIME(), 'yyyyMMddHHmmssfffffff');
    SET @parent_audit_ts = @audit_ts;
    SET @root_ts = @parent_audit_ts;

	EXEC dbo.opp_audit_execution_start
			  @audit_ts = @audit_ts
			, @parent_audit_ts = @parent_audit_ts 
			, @root_ts = @root_ts 
			, @step_type = @step_type  
			, @stepname = @stepname
			, @target_table = @target_table;

		----------- watermark -----------
		declare @watermark_audit_ts as varchar(21);
		declare @watermark_value as varchar(21);
		declare @watermark_value_upperbound as varchar(21) ;
		
		IF OBJECT_ID(N'tempdb..#watermark_temp') IS NOT NULL
		BEGIN
			DROP TABLE #watermark_temp;
		END

		exec dbo.opp_get_watermark
						@transform = 'stp_COPA',
						@transform_segment_1 = '',
						@transform_segment_2 = '';

		select @watermark_value = wtm_value_1
			 , @watermark_audit_ts = audit_ts
		from #watermark_temp;

	----------------- create the latest version of dimension -------------------------
	BEGIN TRY
		 select 
		 	document_number
			,material_number
			,product_skey
			,audit_ts
			,CONVERT( VARCHAR(100), HASHBYTES('SHA2_256' , 
								coalesce(trim(material_number), '^')
						+ '|' + coalesce(CONVERT(VARCHAR(50),product_skey), '^')
				 ), 2) as row_hash
		into #s
		from (
			 SELECT 
			    sales.[Document number]						as document_number
				,sales.[Product]							as material_number
				,coalesce(dim_product.product_skey,-1) 		as product_skey
				,sales.audit_ts
			from dbo.stt_COPA as sales
			left join dbo.dwt_dim_product dim_product
				on sales.[Product] = dim_product.material_number
			where sales.audit_ts > @watermark_value
		) as temp;	

		---- get upper bound watermark
		select @watermark_value_upperbound = format(max(audit_ts), 'yyyy-MM-dd HH:mm:ss')
		from #s;
	
	--------  Start Stage  --------
	BEGIN TRANSACTION

		/* Update Column in dwt */
		UPDATE t
		SET 
			 t.material_number = s.material_number
			,t.product_skey = s.product_skey
			,t.updated_audit_ts = @currentdate
			,t.row_hash = s.row_hash
		FROM dbo.dwt_fact_sales t
		JOIN #s as s
			ON  t.document_number = s.document_number
		WHERE t.row_hash <> s.row_hash
		OPTION (LABEL = 'update');
		
        SET @updated_rows = (SELECT top 1 row_count
							FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
							Where r.request_id = s.request_id 
							and row_count > -1
							and r.[label] = 'update'
							order by r.[end_time] desc);

		/* New records : Insert*/
		INSERT into dbo.dwt_fact_sales( 
			document_number
			,material_number
			,product_skey
			------ Audit ------
			,audit_ts
			,updated_audit_ts
			,row_hash
		)
		select 
			document_number
			,material_number
			,product_skey
			----- Audit ------
			,@currentdate as audit_ts
			,@currentdate as updated_audit_ts
			,row_hash
		from #s s
		where not exists (
			select 1 
			from dbo.dwt_fact_sales t
			where t.document_number = s.document_number
		)
		OPTION (LABEL = 'insert');
		
        SET @new_rows = (SELECT top 1 row_count
                        FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
                        Where r.request_id = s.request_id 
                        and row_count > -1
                        and r.[label] = 'insert'
                        order by r.[end_time] desc);
		
		COMMIT TRANSACTION
		
		------Water mark ------
		exec opp_set_watermark 
			@parent_audit_ts = @audit_ts
			,@transform	= @stepname
			,@transform_segment_1 = null
            ,@transform_segment_2 = null
			,@wtm_value_1 = @watermark_value_upperbound
			,@wtm_value_2 = null
			,@last_execution_watermark_audit_ts = @watermark_audit_ts
		-------- close execution log --------
		
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


	end TRY
	BEGIN CATCH

		IF @@TRANCOUNT > 0 
		BEGIN
			ROLLBACK TRANSACTION
		end

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

	end CATCH;		
end
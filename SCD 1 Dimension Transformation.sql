CREATE PROCEDURE [dbo].[dwp_dim_product]
AS
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

	SET @stepname = 'dwp_dim_product'
	SET @step_type = 'stored procedure';
	SET @target_table = 'dbo.dwt_dim_product';
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

	----------------- create the latest version of dimension -------------------------
	BEGIN TRY
		 -- latest verion of artikel dimension
		 select 
			 material_number
			,created_on
			,last_changed
			,material_description
			,material_description_upper_case
			,material_type
			,material_type_description
			,material_group
			,material_group_description
			,base_unit_of_measure
			,laboratory
			,laboratory_description
			,product_hierarchy
			,product_hierarchy_description
			,external_material_group
			,external_material_group_description
			, CONVERT( VARCHAR(100), HASHBYTES('SHA2_256' , 
								format(coalesce(created_on,convert(date, '1900-01-01')), 'yyyy-MM-dd HH:mm:ss.ffff')
						+ '|' + format(coalesce(last_changed,convert(date, '1900-01-01')), 'yyyy-MM-dd HH:mm:ss.ffff')
						+ '|' +	coalesce(trim(material_description), '^')
						+ '|' + coalesce(trim(material_description_upper_case), '^')
						+ '|' + coalesce(trim(material_type), '^')
						+ '|' + coalesce(trim(material_type_description), '^')
						+ '|' + coalesce(trim(material_group), '^')
						+ '|' + coalesce(trim(material_group_description), '^')
						+ '|' + coalesce(trim(base_unit_of_measure), '^')
						+ '|' + coalesce(trim(laboratory), '^')
						+ '|' + coalesce(trim(laboratory_description), '^')
						+ '|' + coalesce(trim(product_hierarchy), '^')
						+ '|' + coalesce(trim(product_hierarchy_description), '^')
						+ '|' + coalesce(trim(external_material_group), '^')
						+ '|' + coalesce(trim(external_material_group_description), '^')
				 ), 2) as row_hash_scd1
			, '' as row_hash_scd2
		into #s
		from (
			 SELECT 
			 	 mara.MATNR																as material_number
				,CAST(mara.ERSDA AS DATETIME)											as created_on
				,CAST(COALESCE(NULLIF(mara.LAEDA, '00000000'), '19000101') AS DATETIME)	as last_changed
				,mara.MTART																as material_description
				,coalesce(makt.MAKTX,'')												as material_description_upper_case
				,coalesce(makt.MAKTG,'')												as material_type
				,coalesce(t134t.MTBEZ,'')												as material_type_description
				,mara.MATKL																as material_group
				,coalesce(t023t.WGBEZ,'')												as material_group_description
				,mara.MEINS																as base_unit_of_measure
				,mara.LABOR																as laboratory
				,coalesce(t024x.LBTXT,'')												as laboratory_description
				,mara.PRDHA																as product_hierarchy
				,coalesce(t179t.VTEXT,'')												as product_hierarchy_description
				,mara.EXTWG																as external_material_group
				,coalesce(twewt.EWBEZ,'')												as external_material_group_description
			from dbo.stt_MARA as mara
			left join dbo.stt_MAKT as makt
				on mara.MATNR = makt.MATNR
			left join dbo.stt_T134T as t134t
				on mara.MTART = t134t.MTART
				and t134t.SPRAS = 'E'
			left join dbo.stt_T023T as t023t
				on mara.MATKL = t023t.MATKL
				and t023t.SPRAS = 'E'
			left join dbo.stt_T024X as t024x
				on mara.LABOR = t024x.LABOR
				and t024x.SPRAS = 'E'
			left join dbo.stt_T179T as t179t
				on mara.PRDHA = t179t.PRODH
				and t179t.SPRAS = 'E'
			left join dbo.stt_TWEWT as twewt
				on mara.EXTWG = twewt.EXTWG
				and twewt.SPRAS = 'E'
		) as temp;	
	
	--------  Start Stage  --------
	BEGIN TRANSACTION

		/* New records : Insert*/
		INSERT into dbo.dwt_dim_product( 
			 material_number
			,created_on
			,last_changed
			,material_description
			,material_description_upper_case
			,material_type
			,material_type_description
			,material_group
			,material_group_description
			,base_unit_of_measure
			,laboratory
			,laboratory_description
			,product_hierarchy
			,product_hierarchy_description
			,external_material_group
			,external_material_group_description
			----- Meta data ------
			,scd_from
			,scd_to
			------ Audit ------
			,audit_ts
			,updated_audit_ts
			,row_hash_scd1
			,row_hash_scd2
		)
		select 
			 material_number
			,created_on
			,last_changed
			,material_description
			,material_description_upper_case
			,material_type
			,material_type_description
			,material_group
			,material_group_description
			,base_unit_of_measure
			,laboratory
			,laboratory_description
			,product_hierarchy
			,product_hierarchy_description
			,external_material_group
			,external_material_group_description
			----- Meta data ------
			,@19000101 as scd_fom
			,@29991231 as scd_tom
			----- Audit ------
			,@currentdate as audit_ts
			,@currentdate as updated_audit_ts
			,row_hash_scd1
			,'' as row_hash_scd2
		from #s s
		where not exists (
			select 1 
			from dbo.dwt_dim_product t
			where t.material_number = s.material_number
		)
		OPTION (LABEL = 'insert');
		
        SET @new_rows = (SELECT top 1 row_count
                        FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
                        Where r.request_id = s.request_id 
                        and row_count > -1
                        and r.[label] = 'insert'
                        order by r.[end_time] desc);
		
		
		/* Update SCD1 Column in dwt */
		UPDATE t
		SET 
			 t.laboratory = s.laboratory
			,t.laboratory_description = s.laboratory_description
			,t.product_hierarchy = s.product_hierarchy
			,t.product_hierarchy_description = s.product_hierarchy_description
			,t.updated_audit_ts = @currentdate
			,t.row_hash_scd1 = s.row_hash_scd1
		FROM dbo.dwt_dim_product t
		JOIN #s as s
			ON  t.material_number = s.material_number
		WHERE t.row_hash_scd1 <> s.row_hash_scd1
		OPTION (LABEL = 'update');
		
        SET @updated_rows = (SELECT top 1 row_count
							FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
							Where r.request_id = s.request_id 
							and row_count > -1
							and r.[label] = 'update'
							order by r.[end_time] desc);
		
		COMMIT TRANSACTION
		
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


GO



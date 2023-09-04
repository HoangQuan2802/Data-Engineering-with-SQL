ALTER PROC [dbo].[opp_get_watermark] @transform [VARCHAR](255),@transform_segment_1 [VARCHAR](255),@transform_segment_2 [VARCHAR](255) AS
BEGIN TRY

	WITH watermark as (
		select transform_segment_1 , transform_segment_2 , wtm_value_1, wtm_value_2 , audit_ts 
		from (
			select transform , transform_segment_1 , transform_segment_2 , wtm_value_1, wtm_value_2 , audit_ts
				, row_number() over(partition by transform , transform_segment_1 , transform_segment_2  order by audit_ts desc, created_date  desc) rnk
			from dbo.opt_watermark
			where transform = @transform
				and ((transform_segment_1 = @transform_segment_1) or ( @transform_segment_1 is null))
				and ((transform_segment_2 = @transform_segment_2) or ( @transform_segment_2 is null))
		) wtm
		where wtm.rnk = 1
	),
	defaultwatermark AS
	(
			SELECT 
				  @transform_segment_1 as transform_segment_1
				, @transform_segment_2 as transform_segment_2
				, '1900-01-01 00:00:00' as wtm_value_1
				, '1900-01-01 00:00:00' as wtm_value_2
				, '1900-01-01 00:00:00' audit_ts
	)
	SELECT * 
	INTO #watermark_temp
	FROM(
	SELECT *
	FROM watermark
	UNION  all
	SELECT *
	FROM defaultwatermark)a;

END TRY
BEGIN CATCH 

    IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK;
	END

	DECLARE @errmsg NVARCHAR(2000);
	SET  @errmsg = ERROR_MESSAGE();

	THROW;
	
END CATCH
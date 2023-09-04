CREATE PROCEDURE [etl].[master_inferred_member] @segment  NVARCHAR(100), 
                                               @audit_id INT
AS
     BEGIN
         IF(@segment = 'etl.load_dmt_fact_sales_extenda')
             BEGIN
                 EXEC etl.load_dmt_dim_store_inferred 
                      'stt.receipt_header', 
                      'store_id', 
                      @audit_id; -- index
                 EXEC etl.load_dmt_dim_employee_inferred 
                      'stt.receipt_item', 
                      'sales_rep_no', 
                      @audit_id; -- index
                 EXEC etl.load_dmt_dim_employee_inferred 
                      'stt.receipt_cashier', 
                      'employee_no', 
                      @audit_id;
                 EXEC etl.load_dmt_dim_product_inferred 
                      'stt.receipt_item', 
                      'product_part_no', 
                      @audit_id; -- index	
             END;
         IF(@segment = 'etl.load_dmt_fact_sales_budget_daily')
             BEGIN
                 EXEC etl.load_dmt_dim_store_inferred 
                      'stt.sales_budget_daily', 
                      'store_id', 
                      @audit_id;
             END;
         IF(@segment = 'etl.load_dmt_fact_inventory_balance')
             BEGIN
                 EXEC etl.load_dmt_dim_store_inferred 
                      'stt.inventory_balance', 
                      'store_id', 
                      @audit_id; -- index
                 EXEC etl.load_dmt_dim_product_inferred 
                      'stt.inventory_balance', 
                      'product_part_no', 
                      @audit_id; -- index
             END;
         IF(@segment = 'etl.load_dmt_fact_work_schedule')
             BEGIN
                 --EXEC etl.load_dmt_dim_store_inferred 
                 --     'stt.quinyx_get_schedules_v2', 
                 --     'costCentreExtCode', 
                 --     @audit_id; -- index
                 EXEC etl.load_dmt_dim_employee_inferred 
                      'stt.quinyx_get_schedules_v2', 
                      'badgeNo', 
                      @audit_id; -- index
             END;
         IF(@segment = 'etl.load_dmt_fact_time_report')
             BEGIN
                 --EXEC etl.load_dmt_dim_store_inferred 
                 --     'stt.quinyx_get_payroll_v2', 
                 --     'costCentre', 
                 --     @audit_id; -- index
                 EXEC etl.load_dmt_dim_employee_inferred 
                      'stt.quinyx_get_payroll_v2', 
                      'badgeNo', 
                      @audit_id; -- index
             END;
         IF(@segment = 'etl.load_dmt_fact_time_report')
             BEGIN
                 --EXEC etl.load_dmt_dim_store_inferred 
                 --     'stt.quinyx_get_payroll_v2', 
                 --     'costCentre', 
                 --     @audit_id; -- index
                 EXEC etl.load_dmt_dim_employee_inferred 
                      'stt.quinyx_get_payroll_v2', 
                      'badgeNo', 
                      @audit_id; -- index
             END;
         IF(@segment = 'etl.load_dmt_fact_sales_garp')
             BEGIN
                 --EXEC etl.load_dmt_dim_store_inferred 
                 --     'stt.quinyx_get_payroll_v2', 
                 --     'costCentre', 
                 --     @audit_id; -- index
                 EXEC etl.load_dmt_dim_customer_inferred 
                      'stt.garp_history_items', 
                      'part_id', 
                      @audit_id; -- index
                 EXEC etl.load_dmt_dim_product_inferred 
                      'stt.garp_history_items', 
                      'product_id', 
                      @audit_id; -- index
             END;
     END;
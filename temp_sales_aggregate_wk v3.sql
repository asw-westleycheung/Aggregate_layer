spark.sql("""
SELECT 
     i.product_key
    ,i.store_key
	,i.bu_key
    ,d.fiscal_year 
    ,d.fiscal_month 
    ,d.fiscal_week_of_year
    ,d.fiscal_year_week
    ,d.max_fiscal_date
    ,d.vs_max_sales_year
    ,d.vs_max_sales_month
    ,d.vs_max_sales_week
    ,SUM(CASE WHEN inventory_adjustment_reason_type='2' THEN inventory_adjustment_retail_local_amount ELSE 0 END) 	AS sales_amt_local
    ,SUM(CASE WHEN inventory_adjustment_reason_type='2' THEN inventory_adjustment_cost_local_amount ELSE 0 END) 	AS cogs_amt_local
    ,SUM(CASE WHEN inventory_adjustment_reason_type='2' THEN NVL(inventory_adjustment_retail_local_amount,0) - NVL(inventory_adjustment_cost_local_amount,0) ELSE 0 END) + SUM(CASE WHEN inventory_adjustment_reason_type='22' AND inventory_adjustment_reason_code IN ('-9','-8','-5','1','4','10','11','78','79','203','206','221','400','401') THEN NVL(inventory_adjustment_cost_local_amount,0) ELSE 0 END) as gross_profit_amt_local
    ,SUM(CASE WHEN inventory_adjustment_reason_type='22' AND inventory_adjustment_reason_code IN ('-9','-8','-5','1','4','10','11','78','79','203','206','221','400','401') THEN inventory_adjustment_cost_local_amount ELSE 0 END) AS adj_amt_local
    ,SUM(CASE WHEN inventory_adjustment_reason_type='22' AND inventory_adjustment_reason_code IN ('9001', '9002', '9003', '9004', '9005') THEN inventory_adjustment_cost_local_amount ELSE 0 END) AS compensation
    ,SUM(CASE WHEN inventory_adjustment_reason_type='2' THEN inventory_adjustment_quantity ELSE 0 END) AS qty
    ,SUM(CASE WHEN inventory_adjustment_reason_type = '22' AND inventory_adjustment_reason_code IN ('-10','-6','-2','7','8','54','74','75','76','77','80','81','82','83','84','85','86','87','88','89','90','91','92','93','204','205','207','208','210','211','212') THEN inventory_adjustment_retail_local_amount ELSE 0 END) AS sales_amt_local_shrink
    ,SUM(CASE WHEN inventory_adjustment_reason_type = '22' AND inventory_adjustment_reason_code IN ('-10','-6','-2','7','8','54','74','75','76','77','80','81','82','83','84','85','86','87','88','89','90','91','92','93','204','205','207','208','210','211','212') THEN inventory_adjustment_cost_local_amount ELSE 0 END) AS cogs_amt_local_shrink
    ,SUM(CASE WHEN inventory_adjustment_reason_type = '22' AND inventory_adjustment_reason_code IN ('-10','-6','-2','7','8','54','74','75','76','77','80','81','82','83','84','85','86','87','88','89','90','91','92','93','204','205','207','208','210','211','212') THEN inventory_adjustment_quantity ELSE 0 END) AS qty_shrink
    ,current_timestamp as table_last_update
FROM rdm.f_inventory_adjustment i
JOIN default.bl_business_date d ON i.date_key = d.fiscal_date_key
where d.fiscal_week_end_date <= date_add(d.max_fiscal_date,1)
and (case when COALESCE(i.inventory_adjustment_retail_local_amount,0) <> 0 then 1
        when COALESCE(i.inventory_adjustment_cost_local_amount,0) <> 0 then 1
        when COALESCE(i.inventory_adjustment_quantity,0) <> 0 then 1 ELSE 0 END) = 1
group by 
     i.product_key
    ,i.store_key
	,i.bu_key
    ,d.fiscal_year 
    ,d.fiscal_month 
    ,d.fiscal_week_of_year
    ,d.fiscal_year_week
    ,d.max_fiscal_date
    ,d.vs_max_sales_year
    ,d.vs_max_sales_month
    ,d.vs_max_sales_week
""").write.mode("overwrite").saveAsTable("default.temp_sales_aggregate_wk")

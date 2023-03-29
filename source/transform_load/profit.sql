CREATE OR REPLACE PROCEDURE profit_calculation()
LANGUAGE plpgsql
AS $$
BEGIN

  -- Create enriched view
  CREATE OR REPLACE VIEW transformed_data.profit_calc_view AS 
    WITH option_costs AS (
      SELECT od.option_code, od.model, od.material_cost, AVG(od.material_cost) OVER (PARTITION BY od.option_code) AS avg_material_cost, od.load_at
      FROM transformed_data.options_data od
    ), 
     enriched_data  AS (
      SELECT ba.*, oc.*
	    FROM transformed_data.base_data ba
      LEFT JOIN option_costs oc
      ON ba.options_code = oc.option_code 
        AND ba.model_Text= oc.model
    ),
    get_data AS (
    SELECT ed.*, 
    CASE 
          WHEN ed.sales_price <= 0 THEN 0
          WHEN ed.options_code = ed.option_code AND ed.model_text = ed.model THEN ed.material_cost
          WHEN ed.options_code != ed.option_code OR ed.model_text != ed.model THEN ed.avg_material_cost
          ELSE 0.45 * ed.sales_price END AS production_cost
    FROM enriched_data ed
    )
    SELECT gd.gen_id, gd.vin, gd.option_quantities, gd.options_code,gd.option_desc, gd.model_text, 
    		gd.sales_price, gd.material_cost, gd.avg_material_cost, round(production_cost, 2) AS production_cost, 
        round(gd.Sales_Price - gd.production_cost, 2) as profit, CURRENT_TIMESTAMP as load_at
    FROM 
        get_data gd;

    
    CREATE OR REPLACE VIEW transformed_data.profit_view AS
        SELECT pc.gen_id, pc.vin, pc.option_quantities, pc.options_code, pc.option_desc, pc.model_text,
             pc.sales_price, pc.material_cost, pc.avg_material_cost, pc.production_cost, pc.profit, CURRENT_TIMESTAMP AS load_at
        FROM transformed_data.profit_calc_view  pc
        WHERE pc.profit IS NOT NULL
        ORDER BY 2 ASC;    
      
    CREATE OR REPLACE VIEW transformed_data.profit_null_view AS
        SELECT pc.gen_id, pc.vin, pc.option_quantities, pc.options_code, pc.option_desc, pc.model_text,
             pc.sales_price, pc.material_cost, pc.avg_material_cost, pc.production_cost, pc.profit, CURRENT_TIMESTAMP AS load_at
        FROM transformed_data.profit_calc_view  pc
        WHERE pc.profit IS NULL
        ORDER BY 2 ASC;

    CREATE OR REPLACE VIEW transformed_data.profit_vin_null AS
        SELECT pc.gen_id, pc.vin, pc.option_quantities, pc.options_code, pc.option_desc, pc.model_text,
             pc.sales_price, pc.material_cost, pc.avg_material_cost, pc.production_cost, pc.profit, CURRENT_TIMESTAMP AS load_at
        FROM transformed_data.profit_calc_view  pc
        WHERE pc.vin IS NULL; 

    CREATE OR REPLACE VIEW transformed_data.profit_vin_unknow AS
        SELECT pc.gen_id, pc.vin, pc.option_quantities, pc.options_code, pc.option_desc, pc.model_text,
             pc.sales_price, pc.material_cost, pc.avg_material_cost, pc.production_cost, pc.profit, CURRENT_TIMESTAMP AS load_at
        FROM transformed_data.profit_calc_view  pc
        WHERE pc.vin = 'unknown';        

END;
$$;

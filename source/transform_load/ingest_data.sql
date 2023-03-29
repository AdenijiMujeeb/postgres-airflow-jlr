CREATE OR REPLACE PROCEDURE ingest_data() AS $$
BEGIN
  -- Insert data with null sales_price into db.tb1.null
INSERT INTO output_data.profit_table (gen_id, vin, option_quantities, options_code, option_desc, model_text,
      sales_price, material_cost, avg_material_cost, production_cost, profit, load_at)
SELECT pv.gen_id, pv.vin, pv.option_quantities, pv.options_code, pv.option_desc, pv.model_text,
      pv.sales_price, pv.material_cost, pv.avg_material_cost, pv.production_cost, pv.profit, pv.load_at
FROM transformed_data.profit_view pv
WHERE pv.gen_id NOT IN (
  SELECT pt.gen_id
  FROM output_data.profit_table pt
);

END;
$$ LANGUAGE plpgsql;


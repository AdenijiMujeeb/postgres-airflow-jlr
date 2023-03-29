CREATE OR REPLACE PROCEDURE load_data() AS $$
BEGIN
  -- Insert data with null sales_price into db.tb1.null
INSERT INTO transformed_data.base_data (gen_id, vin, option_quantities, options_code, option_desc, model_text, sales_price, load_at)
SELECT uuid_generate_v4(), pb.vin, pb.option_quantities, pb.options_code, pb.option_desc, pb.model_text, pb.sales_price, pb.created_at
FROM public.base_data pb
WHERE pb.created_at NOT IN (
  SELECT bd.load_at
  FROM transformed_data.base_data bd
);


  -- Insert data with null sales_price into db.tb1.null
INSERT INTO transformed_data.options_data (model, option_code, option_desc, material_cost, load_at)
SELECT po.model, po.option_code, po.option_desc, po.material_cost, po.created_at
FROM public.options_data po
WHERE po.created_at NOT IN (
SELECT od.load_at
  FROM transformed_data.options_data od
);

INSERT INTO transformed_data.vehicle_line_mapping (nameplate_code, brand, platform, nameplate_display, load_at)
  SELECT pv.nameplate_code, pv.brand, pv.platform, pv.nameplate_display, pv.created_at
FROM public.vehicle_line_mapping pv
WHERE pv.created_at NOT IN (
SELECT vl.load_at
  FROM transformed_data.vehicle_line_mapping vl
);

END;
$$ LANGUAGE plpgsql;


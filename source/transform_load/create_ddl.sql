CREATE OR REPLACE PROCEDURE create_tables() 
LANGUAGE plpgsql 
AS $$
BEGIN

-- Enable the uuid-ossp extension
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- create table schema raw_data
    CREATE SCHEMA IF NOT EXISTS public;
    
-- create table base_data
    CREATE TABLE IF NOT EXISTS  public.base_data (
        vin VARCHAR(17),
        option_quantities INTEGER,
        options_code VARCHAR(5),
        option_desc VARCHAR(255),
        model_text VARCHAR(50),
        sales_price DECIMAL(10,2),
        created_at TIMESTAMP NOT NULL DEFAULT now()
);
-- create table options_data
    CREATE TABLE IF NOT EXISTS public.options_data (
        model VARCHAR(50),
        option_code VARCHAR(50),
        option_desc VARCHAR(255),
        material_cost NUMERIC(10,2),
        created_at TIMESTAMP NOT NULL DEFAULT now()
);
-- create table vehicle_line_mapping
    CREATE TABLE IF NOT EXISTS public.vehicle_line_mapping (
        nameplate_code VARCHAR(10),
        brand VARCHAR(50),
        platform VARCHAR(50),
        nameplate_display VARCHAR(50),
        created_at TIMESTAMP NOT NULL DEFAULT now()
);
   -- create table schema transform_data
    CREATE SCHEMA IF NOT EXISTS transformed_data;
    
-- create table base_data
    CREATE TABLE IF NOT EXISTS transformed_data.base_data (
        Gen_ID UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
        VIN VARCHAR(17),
        Option_Quantities INTEGER,
        Options_Code VARCHAR(5),
        Option_Desc VARCHAR(255),
        Model_Text VARCHAR(50),
        Sales_Price DECIMAL(10,2),       
        load_at TIMESTAMP NOT NULL DEFAULT now()
);
-- create table options_data
    CREATE TABLE IF NOT EXISTS transformed_data.options_data (
        Model VARCHAR(50),
        Option_Code VARCHAR(50),
        Option_Desc VARCHAR(255),
        Material_Cost NUMERIC(10,2),
        load_at TIMESTAMP NOT NULL DEFAULT now()
);
-- create table vehicle_line_mapping
    CREATE TABLE IF NOT EXISTS transformed_data.vehicle_line_mapping (
        nameplate_code VARCHAR(10),
        brand VARCHAR(50),
        platform VARCHAR(50),
        nameplate_display VARCHAR(50),
        load_at TIMESTAMP NOT NULL DEFAULT now()
);
    -- create table schema curated data
    CREATE SCHEMA IF NOT EXISTS output_data;

-- create table base_data without null
    CREATE TABLE IF NOT EXISTS output_data.profit_table (
        Gen_ID UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
        VIN VARCHAR(17),
        Option_Quantities INTEGER,
        Options_Code VARCHAR(5),
        Option_Desc VARCHAR(255),
        Model_Text VARCHAR(50),
        Sales_Price DECIMAL(10,2),
        Material_Cost NUMERIC(10,2),
        Avg_Material_Cost NUMERIC(10,2),
        production_cost DECIMAL(10,2),
        profit DECIMAL(10,2),
        load_at TIMESTAMP NOT NULL DEFAULT now()
);

RAISE NOTICE 'Databases and tables created successfully!';
EXCEPTION
  WHEN OTHERS THEN
    RAISE EXCEPTION 'Error: %', SQLERRM;
END;
$$;

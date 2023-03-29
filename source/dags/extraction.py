import os
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Define the connection parameters for PostgreSQL
pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
# store procedure file location
sql_directory = os.path.join(os.path.dirname(__file__), '..', 'transform_load')

csv_directory = os.path.join(os.path.dirname(__file__), '..', 'datasets')


# Define the function to execute SQL scripts
def execute_sql(filename, stored_proc_name):
    with open(os.path.join(sql_directory, filename), 'r') as f:
        sql = f.read()
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(f"CALL {stored_proc_name}()")

def clean_and_save_csv(input_file_path_1, input_file_path_2 ):
    # Read the input CSV file into a pandas dataframe
    df_base = pd.read_csv(input_file_path_1)
    df_options = pd.read_csv(input_file_path_2)
    # Remove commas from the 'Option_Desc' column in the dataframe
    df_base['Option_Desc'] = df_base['Option_Desc'].str.replace(',', '')
    df_options['Option_Desc'] = df_options['Option_Desc'].str.replace(',', '')
    # Save the updated dataframe to the same CSV file, overwriting the original file
    df_base.to_csv(input_file_path_1, index=False)
    df_options.to_csv(input_file_path_2, index=False)

    return df_base, df_options

def load_csv_to_postgres(directory, conn_id):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            table_name = os.path.splitext(filename)[0]
            with open(os.path.join(directory, filename), 'r') as f:
                if table_name == 'base_data':
                    next(f) # skip the first row
                    cursor.copy_from(f, table_name, sep=',', null='', columns=('vin', 'option_quantities', 'options_code', 'option_desc', 'model_text', 'sales_price'))
                
                elif table_name == 'options_data':
                    next(f) # skip the first row
                    cursor.copy_from(f, table_name, sep=',', null='', columns=('model', 'option_code', 'option_desc', 'material_cost'))
                
                elif table_name == 'vehicle_line_mapping':
                    next(f) # skip the first row
                    cursor.copy_from(f, table_name, sep=',', null='', columns=('nameplate_code', 'brand', 'platform', 'nameplate_display'))
                conn.commit()
    cursor.close()
    conn.close()
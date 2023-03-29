import os
from dq import check_data_quality
from extraction import execute_sql, clean_and_save_csv, load_csv_to_postgres
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup


# Define the connection parameters for PostgreSQL
pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
# store procedure file location
sql_directory = os.path.join(os.path.dirname(__file__), '..', 'transform_load')
# CSV file location
csv_directory = os.path.join(os.path.dirname(__file__), '..', 'datasets')


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'csv_to_postgres',
    description='Load CSV files to PostgreSQL using stored procedures',
    schedule_interval=None,
    default_args=default_args,
)

# Define the tasks
with dag:
    # Task 1: Create the database tables
    create_ddl_table = PythonOperator(
        task_id='create_ddl_table',
        python_callable=execute_sql,
        op_kwargs={'filename': 'create_ddl.sql', 
                   'stored_proc_name': 'create_tables'
                   },
    )
    # Task 2: Task grouping of Cleaning and loading of CSV files
    with TaskGroup("clean_and_load_to_staging") as clean_and_load_to_staging:
        # Load the CSV files to PostgreSQL
        clean_csv_task = PythonOperator(
        task_id='clean_csv',
        python_callable=clean_and_save_csv,
        op_kwargs={'input_file_path_1': './datasets/base_data.csv',
                   'input_file_path_2':'./datasets/options_data.csv'
                    },
        )
        #Load the CSV files to PostgreSQL
        load_to_staging = PythonOperator(
            task_id='load_to_staging',
            python_callable=load_csv_to_postgres,
            op_kwargs={'directory': csv_directory,
                       'conn_id': 'postgres_localhost'
                   },
        )
    clean_csv_task >> load_to_staging

    # Task 3: load staging data
    load_staging_data = PythonOperator(
        task_id='load_staging_data',
        python_callable=execute_sql,
        op_kwargs={'filename': 'load_staging_data.sql', 
                   'stored_proc_name': 'load_data'
                   },
    )
     # Task 4: Task grouping of profit calculation and data quality checks
    with TaskGroup("cal_dq_check_ingest") as calc_dq_check_ingest:
    # Task 4: Calculate the profit using a stored procedure
        calculate_profit = PythonOperator(
            task_id='calculate_profit',
            python_callable=execute_sql,
            op_kwargs={'filename': 'profit.sql', 
                       'stored_proc_name': 'profit_calculation'
                       },
        )
        check_data_quality = PythonOperator(
            task_id='check_data_quality',
            python_callable=check_data_quality,
            op_kwargs={'conn_id': 'postgres_localhost'},
        )            
    calculate_profit >> check_data_quality

    # Task 5: Ingest the transformed data into output
    ingest_result_data = PythonOperator(
        task_id='ingest_result_data',
        python_callable=execute_sql,
        op_kwargs={'filename': 'ingest_data.sql', 
                   'stored_proc_name': 'ingest_data'
                   },
    )

    # Set the dependencies between the tasks
    create_ddl_table >> clean_and_load_to_staging >> load_staging_data >> calc_dq_check_ingest >> ingest_result_data

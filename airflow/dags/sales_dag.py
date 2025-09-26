from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import requests , logging , os , zipfile
from pathlib import Path
from datetime import datetime

def extract_data(**context) : 
    
    data_link = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'
    filePath = '/opt/airflow/data/online+retail.zip'

    logging.info("Extracting file has been started")
    if os.path.exists(filePath) : 
        logging.info("File already exists")
    else : 
        logging.info(f"File {filePath} does not exist... Start Downloading")
        response = requests.get(data_link)
        response.raise_for_status()
        with open(filePath, 'wb') as f : 
            f.write(response.content)
            logging.info("File downloaded successfully")

    logging.info("Reading file started")

    with zipfile.ZipFile(filePath) as z : 
        logging.info(f"Files in the zipfile : {z.namelist()}")

        for filename in z.namelist() :
                basefile = Path(filename).stem
                csv_path = f"/opt/airflow/data/sales_data_{basefile}.csv"
                if os.path.exists(csv_path) : 
                    logging.info("File already processed to CSV")
                    continue
                try : 
                    with z.open(filename) as f : 
                        df = pd.read_excel(f)
                        df.to_csv(csv_path, index = False)
                    logging.info(f"Extracted {filename} -> {csv_path}")

                except Exception as e:
                    logging.info(f"Error happened while processing {filename} : {e}")
                    continue

        logging.info("All files extracted well")

        ti = context['ti']
        ti.xcom_push(key='csv_path' , value=csv_path)
        return csv_path

def clean_data(**context) : 
    logging.info("Cleaning data has been started")

    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='extraction_and_cleaning.extract_data' , key='csv_path')
    cleaned_path = csv_path.replace("sales_data_", "cleaned_sales_data_")
    if os.path.exists(cleaned_path) :
        logging.info("File already cleaned")
        return cleaned_path
    
    df = pd.read_csv(csv_path)
    number_of_rows = df.shape[0]

    ## Remove rows with missing Customer ID
    df.dropna(subset=['CustomerID'] , inplace=True)

    ## Remove zero quantity
    df = df[df['Quantity'] != 0]
    
    ## Remove invalid negative quantities (not returns)
    df = df[~((df['Quantity'] < 0) & (~df['InvoiceNo'].str.startswith('C')))]

    number_of_rows_after_cleaning = df.shape[0]
    logging.info(f"Number of rows before cleaning: {number_of_rows}")
    logging.info(f"Number of rows after cleaning: {number_of_rows_after_cleaning}")
    logging.info(f"Number of rows removed : {number_of_rows - number_of_rows_after_cleaning}")

    ## Convert InvoiceDate to datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    df.to_csv(cleaned_path , index=False)
    logging.info(f"Cleaned data saved to {cleaned_path}")
    ti.xcom_push(key='cleaned_csv_path' , value=cleaned_path)
    return cleaned_path

def create_schemas_tables() : 
    create_schemas_query = """
    CREATE SCHEMA IF NOT EXISTS sales;
    CREATE SCHEMA IF NOT EXISTS DW_sales;
    """

    create_tables_query = """
    CREATE TABLE IF NOT EXISTS sales.transactions_staging (
        unique_row_id text,
        InvoiceNo text,
        StockCode text,
        Description text,
        Quantity double precision,
        InvoiceDate timestamp,
        UnitPrice double precision,
        CustomerID double precision,
        Country text
    );
    CREATE TABLE IF NOT EXISTS sales.transactions (
        unique_row_id text PRIMARY KEY,
        InvoiceNo text,
        StockCode text,
        Description text,
        Quantity double precision,
        InvoiceDate timestamp,
        UnitPrice double precision,
        CustomerID double precision,
        Country text
    );
    """

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run(create_schemas_query)
    logging.info("Schemas created successfully")
    pg_hook.run(create_tables_query)
    logging.info("Tables created successfully")

def load_data_to_staging(**context) :
    logging.info("Loading data to staging has been started")
    
    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='extraction_and_cleaning.clean_data' ,key='cleaned_csv_path')

    logging.info(f"CSV path received: {csv_path}")

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    copy_query = f"""
                COPY sales.transactions_staging (
                InvoiceNo, StockCode, Description, Quantity,
                InvoiceDate, UnitPrice, CustomerID, Country
                )
                FROM STDIN WITH CSV HEADER DELIMITER ','  NULL AS '';
            """
    try:
        with open(csv_path, 'r') as f:
            cursor.copy_expert(copy_query, f)
        conn.commit()
        cursor.execute("SELECT COUNT(InvoiceNo) FROM sales.transactions_staging")
        logging.info(f"Loading {cursor.fetchone()[0]} to staging table has been done")
    except Exception as e : 
        logging.info(f"Error has happend when loading data into staging table : {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def update_staging_table() : 
    logging.info("Updating staging table has been started")
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    update_Query = """
    UPDATE sales.transactions_staging
    SET unique_row_id =md5(
        concat_ws('||',
            upper(trim(coalesce(InvoiceNo::text,''))),
            upper(trim(coalesce(StockCode::text,''))),
            upper(trim(coalesce(Description::text,''))),
            Quantity::text,
            to_char(InvoiceDate::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS'),
            to_char(round(UnitPrice::numeric, 2), 'FM999999990.00'),
            coalesce(CustomerID::text,''),
            upper(trim(coalesce(Country::text,'')))
        )
    );

    """

    # To delete dublicated transactins
    delete_query = """
    DELETE FROM sales.transactions_staging a
    USING sales.transactions_staging b
    WHERE a.ctid < b.ctid
    AND a.unique_row_id = b.unique_row_id;
    """

    pg_hook.run(update_Query)
    pg_hook.run(delete_query)
    logging.info("Updating data in staging table is done")

def merge_data_to_mainTable () :
    logging.info("Mergeing data from staging to main table has been started")
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run("""
    MERGE INTO sales.transactions AS T
    USING sales.transactions_staging AS S
    ON T.unique_row_id = S.unique_row_id
    WHEN MATCHED THEN 
        UPDATE SET 
            InvoiceNo = S.InvoiceNo,
            StockCode = S.StockCode,
            Description = S.Description,
            Quantity    = S.Quantity,
            InvoiceDate = S.InvoiceDate,
            UnitPrice   = S.UnitPrice,
            CustomerID  = S.CustomerID,
            Country     = S.Country       
    WHEN NOT MATCHED THEN
    INSERT(unique_row_id ,InvoiceNo ,StockCode ,Description ,Quantity ,InvoiceDate ,UnitPrice ,CustomerID ,Country)
    VALUES(S.unique_row_id ,S.InvoiceNo ,S.StockCode ,S.Description ,S.Quantity ,S.InvoiceDate ,S.UnitPrice ,S.CustomerID ,S.Country)
    """        
    )
    logging.info("Mergeing data into main table done")

def delete_staging_data() : 
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run('Truncate table sales.transactions_staging')

with DAG(
    'sales_dag',
    schedule_interval='@daily',
    start_date=datetime(2023,1,1) ,
    catchup=False
) as dag : 
    
    create_schemas_tables = PythonOperator(
        task_id= 'create_schemas_tables',
        python_callable= create_schemas_tables
    )

    with TaskGroup("extraction_and_cleaning") as extraction_and_cleaning : 
            extract_data = PythonOperator(
                task_id= 'extract_data',
                python_callable= extract_data,
                provide_context=True
            )

            clean_data = PythonOperator(
                task_id = 'clean_data',
                python_callable= clean_data,
                provide_context=True
            )

            extract_data >> clean_data

    with TaskGroup("staging_ops") as staging_ops : 
            delete_staging_data = PythonOperator(
            task_id = 'delete_staging_data',
            python_callable = delete_staging_data
            )

            load_data_to_staging = PythonOperator(
                task_id= 'load_data_to_staging',
                python_callable= load_data_to_staging,
                provide_context= True
            )

            update_staging_table = PythonOperator(
                task_id= 'update_staging_table',
                python_callable= update_staging_table
            )

            delete_staging_data >> load_data_to_staging >> update_staging_table
    
    with TaskGroup("final_merge") as final_merge:
            merge_data_to_mainTable = PythonOperator(
                task_id = 'merge_data_to_mainTable',
                python_callable = merge_data_to_mainTable
            )

            merge_data_to_mainTable

    [extraction_and_cleaning , create_schemas_tables] >> staging_ops >> final_merge
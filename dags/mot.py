import os
import logging
import downloader

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from zipfile import ZipFile
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = "/opt/airflow/data/"
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", 'mot_data')

# Use 2020 | 2021 | 2 (for both years) | 1 for a single file (testing) <------------------------
year = 1

result_dataset_files = list()
failure_dataset_files = list()

#Populate the csv files list to be used based on the year flag
if(year == 2021 or year == 2):
    result_dataset_files = result_dataset_files + list(
        map(lambda counter: f"test_result_2021_{counter}.csv", range(1, 13)))
    failure_dataset_files = failure_dataset_files + list(
        map(lambda counter: f"test_item_2021_{counter}.csv", range(1, 13)))

if(year == 2020 or year == 2):
    result_dataset_files = result_dataset_files + \
        (list(
            map(lambda counter: f"test_result_2020_{counter}.csv", range(1, 5))))
    failure_dataset_files = failure_dataset_files + \
        (list(
            map(lambda counter: f"test_item_2020_{counter}.csv", range(1, 5))))

if(year == 1):
    result_dataset_files.append('test_result_2021_1.csv')
    failure_dataset_files.append('test_item_2021_1.csv')

#Populate the parquet files list from the csv files list
result_dataset_files_parquet = [
    s.replace('.csv', '.parquet') for s in result_dataset_files]
failure_dataset_files_parquet = [
    s.replace('.csv', '.parquet') for s in failure_dataset_files]

#Import the result CSV file in a dataframe, do the required cleaning then export it to a parquet file
def format_result_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    # import csv as a dataframe
    df = pd.read_csv(src_file, on_bad_lines='skip')
    # cast the 2 date columns as datetime (Using coerce to put NaT if an error is invoked in a row)
    df['test_date'] = pd.to_datetime(df['test_date'], errors='coerce')
    df['first_use_date'] = pd.to_datetime(df['first_use_date'], errors='coerce')
    # dropping all rows with na/nan/nat values
    df = df.dropna().copy().reset_index()
    # cast some columns as categorical
    df['test_type'] = pd.Categorical(df['test_type'])
    df['test_result'] = pd.Categorical(df['test_result'])
    df['fuel_type'] = pd.Categorical(df['fuel_type'])
    # cast the mileage column to in
    df['test_mileage'] = df['test_mileage'].astype(int)
    # drop duplicates
    df = df.drop_duplicates().copy().reset_index()
    # export the df as a parquet file
    df.to_parquet(src_file.replace('.csv', '.parquet'))

#Import the item CSV file in a dataframe, do the required cleaning then export it to a parquet file
def format_item_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    # import csv as a dataframe
    df = pd.read_csv(src_file, on_bad_lines='skip')
    # convert the dangerous_mark to boolean since the only 2 results are D when true or Null when false
    df['dangerous_mark'] = ~df['dangerous_mark'].isna()
    # dropping all rows with na/nan/nat values
    df = df.dropna().copy().reset_index()
    # cast rfr_type_code column as categorical
    df['rfr_type_code'] = pd.Categorical(df['rfr_type_code'])
    # drop duplicates
    df = df.drop_duplicates().copy().reset_index()
    # export the df as a parquet file
    df.to_parquet(src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="mot_de_group16",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de-final-group16'],
) as dag:

    start_task = DummyOperator(
        task_id="start_task"
    )

    end_task = DummyOperator(
        task_id="end_task"
    )

    start_test_result_task = DummyOperator(
        task_id="start_test_result_task"
    )

    end_test_result_task = DummyOperator(
        task_id="end_test_result_task"
    )

    start_test_failure_task = DummyOperator(
        task_id="start_test_failure_task"
    )

    end_test_failure_task = DummyOperator(
        task_id="end_test_failure_task"
    )

    end_insert_task = DummyOperator(
        task_id="end_insert_task"
    )

    download_result = PythonOperator(
        task_id="download_result",
        python_callable=downloader.downloadForBothYears,
        op_kwargs={
                "dataFileType": "result",
                "LocalFolderPath": path_to_local_home,
                "year": year,
        },
    )

    download_item = PythonOperator(
        task_id=f"download_item",
        python_callable=downloader.downloadForBothYears,
        op_kwargs={
            "dataFileType": "item",
            "LocalFolderPath": path_to_local_home,
            "year": year,
        },
    )

    extract_result = PythonOperator(
        task_id="extract_result",
        python_callable=downloader.extractForBothYears,
        op_kwargs={
                "dataFileType": "result",
                "LocalFolderPath": path_to_local_home,
                "year": year,
        },
    )

    extract_item = PythonOperator(
        task_id=f"extract_item",
        python_callable=downloader.extractForBothYears,
        op_kwargs={
            "dataFileType": "item",
            "LocalFolderPath": path_to_local_home,
            "year": year,
        },
    )

    clean_result = PythonOperator(
        task_id="clean_result",
        python_callable=downloader.cleanForBothYears,
        op_kwargs={
                "dataFileType": "result",
                "LocalFolderPath": path_to_local_home,
                "year": year,
        },
    )

    clean_item = PythonOperator(
        task_id=f"clean_item",
        python_callable=downloader.cleanForBothYears,
        op_kwargs={
            "dataFileType": "item",
            "LocalFolderPath": path_to_local_home,
            "year": year,
        },
    )

    transform_results = BigQueryExecuteQueryOperator(
            task_id="bigquery_create_partioned_results",
            sql="""
            CREATE OR REPLACE TABLE `mot_data.test_result_partitioned`
            PARTITION BY TIMESTAMP_TRUNC(test_date, MONTH)
            CLUSTER BY make AS
            SELECT test_id, test_date, test_result, make, model
            FROM `mot_data.test_result`
            WHERE test_class_id = 4
            AND test_type = 'NT'
            AND (test_result='F' OR test_result='ABA')
            """,
            use_legacy_sql=False,
    )

    transform_items = BigQueryExecuteQueryOperator(
            task_id="bigquery_create_partioned_items",
            sql="""
            CREATE OR REPLACE TABLE `mot_data.test_item_partitioned`
            PARTITION BY TIMESTAMP_TRUNC(test_date, MONTH)
            CLUSTER BY make AS
            SELECT rfr_id, r.test_result, f.rfr_type_code,r.test_date, r.make, r.model, f.dangerous_mark
            FROM  `mot_data.test_failure` f JOIN `mot_data.test_result` r on f.test_id = r.test_id
            WHERE r.test_class_id = 4
            AND r.test_type = 'NT'
            AND (r.test_result='F' OR r.test_result='ABA')
            """,
            use_legacy_sql=False,
    )

    def format_result_to_parquet_task(file):
        task = PythonOperator(
            task_id=f"format_to_parquet_task_{file}",
            python_callable=format_result_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}{file}",
            },
        )
        return task

    def format_item_to_parquet_task(file):
        task = PythonOperator(
            task_id=f"format_to_parquet_task_{file}",
            python_callable=format_item_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}{file}",
            },
        )
        return task

    def file_to_gcs_task(file):
        task = PythonOperator(
            task_id=f"{file}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{file}",
                "local_file": f"{path_to_local_home}/{file}",
            },
        )
        return task

    def cleanup_file(file):
        task = BashOperator(
            task_id=f"{file}_cleanup",
            bash_command=f"rm {path_to_local_home}/{file}",
        )
        return task

    def bigquery_delete_table(table_name):
        task = BigQueryDeleteTableOperator(
            task_id=f"bigquery_delete_{table_name}_task",
            ignore_if_missing=True,
            deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        )
        return task

    def bigquery_create_table_task(files, table_name):
        task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_create_{table_name}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": table_name,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": list(map(lambda file: f"gs://{BUCKET}/raw/{file}", files))

                },
            },
        )
        return task



    chain(start_test_result_task,
          [format_result_to_parquet_task(file) for file in result_dataset_files], [
              cleanup_file(file) for file in result_dataset_files],
          [file_to_gcs_task(file) for file in result_dataset_files_parquet], [
              cleanup_file(file) for file in result_dataset_files_parquet],
          bigquery_delete_table('test_result'), bigquery_create_table_task(result_dataset_files_parquet, 'test_result'), end_test_result_task)

    chain(start_test_failure_task,
          [format_item_to_parquet_task(file) for file in failure_dataset_files], [
              cleanup_file(file) for file in failure_dataset_files],
          [file_to_gcs_task(file) for file in failure_dataset_files_parquet], [
              cleanup_file(file) for file in failure_dataset_files_parquet],
          bigquery_delete_table('test_failure'), bigquery_create_table_task(failure_dataset_files_parquet, 'test_failure'), end_test_failure_task)

    start_task >> [download_result, download_item]
    download_result >> extract_result >> clean_result >> start_test_result_task
    download_item >> extract_item >> clean_item >> start_test_failure_task
    [end_test_result_task, end_test_failure_task] >> end_insert_task >> [transform_results, transform_items]
    end_task << [transform_results, transform_items]

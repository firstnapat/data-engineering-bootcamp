# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-csv

import json
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

load_dotenv()
keyfile = os.getenv("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info
)
project_id = "data-bootcamp-skooldio"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

# todo write function for extract header from csv for use in job_config
# todo write function for partitioned csv
# events and order are partition
# todo write function for run load csv to bq

def adjust_job_config(file_name: str):
    if file_name in ["events", "users", "orders"]:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            autodetect=True,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
        )
    return job_config

def list_file_in_folder(folder_path:str) -> list:
    folder_name_list = os.listdir(folder_path)
    # print(folder_name_list)
    return folder_name_list

def load_table_from_csv():
    import re

    folder_name_list = list_file_in_folder("/Users/napatsakornpianchana/Documents/m2/project/data-engineering-bootcamp/dataset/greenery")
    file_name_list = []
    for file_path in folder_name_list:
        file_name = file_path.split(".csv")[0]
        file_name_list.append(file_name)
        job_config = adjust_job_config(file_path)
        with open(file_path, "rb") as f:
            table_id = f"{project_id}.dbi.{file_name}"
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

load_table_from_csv()

from google.cloud import bigquery
from airflow.decorators import dag, task

import os
import pendulum


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2023, 3, 23, tz="UTC"),
    catchup=False,
    tags=["ETL_To_BQ"],
)

def etl_gcs_to_bq_taskflow():
    """Main ETL flow to load data into GCS"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./plugins/.gc/dtc-de-project-380614-38429ed300c2.json"
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    fileNames = ["server_map_rotation", "server_players", "server_infos", "server_bridge_players"]
    
    @task()
    def el_gcs_to_bq(fileName: str) -> None:
        uri = f"gs://dtc_data_lake_dtc-de-project-380614/data/battlefield/{fileName}.parquet"

        load_job = client.load_table_from_uri(
            uri, f"dtc-de-project-380614.battlefield_vault.stg_{fileName}", job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(f"dtc-de-project-380614.battlefield_vault.stg_{fileName}")
        print("Loaded {} rows.".format(destination_table.num_rows))
        

    for i in fileNames:
        el_gcs_to_bq(i)
    
etl_gcs_to_bq_taskflow()
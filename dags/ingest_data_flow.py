from google.cloud import storage
from airflow.decorators import dag, task

import requests
import pandas as pd
import os
import pandas as pd
import pendulum

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2023, 3, 23, tz="UTC"),
    catchup=False,
    tags=["Ingest_Data"],
)

def etl_ingest_data_taskflow():
    """Main ETL flow to load data into GCS"""

    @task()
    def extract_data(url: str) -> dict:
        x = requests.get(url)
        server_json = x.json()
        return server_json
        
    @task()
    def transform_and_load(server_json: dict) -> None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./plugins/dtc-de-project-380614-38429ed300c2.json"
        server_map_rotation = pd.DataFrame.from_dict(server_json['rotation'])
        server_players = pd.DataFrame.from_dict(server_json['players'])

        entries_to_remove = ('rotation', 'settings', 'players', 'teams')

        for item in entries_to_remove:
            del server_json[item]

        server_infos = pd.DataFrame(server_json, index=[0])
        server_infos['server_id'] = 0 # for now only one server, if we add more servers we can change this to the index of the passed list

        server_bridge_players = pd.DataFrame(server_players['player_id'])
        server_bridge_players['server_id'] = 0 # for now only one server, if we add more servers we can change this to the index of the passed list
        
        client = storage.Client()
        bucket = client.get_bucket('dtc_data_lake_dtc-de-project-380614')
            
        bucket.blob('data/battlefield/server_map_rotation.parquet').upload_from_string(server_map_rotation.to_parquet(compression="gzip"), 'text/parquet')
        bucket.blob('data/battlefield/server_players.parquet').upload_from_string(server_players.to_parquet(compression="gzip"), 'text/parquet')
        bucket.blob('data/battlefield/server_infos.parquet').upload_from_string(server_infos.to_parquet(compression="gzip"), 'text/parquet')
        bucket.blob('data/battlefield/server_bridge_players.parquet').upload_from_string(server_bridge_players.to_parquet(compression="gzip"), 'text/parquet')
        return
    
    url = 'https://api.gametools.network/bf4/detailedserver/?name=SUPER%40%20%5BSiC%5D%20S6%20Rush%20Best%20Maps%20-%2060Hz&platform=pc&lang=en-us'
    server_json = extract_data(url)
    transform_and_load(server_json)
    

